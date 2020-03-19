// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type ResolutionLevel int64

const (
	ResolutionLevelRaw = ResolutionLevel(downsample.ResLevel0)
	ResolutionLevel5m  = ResolutionLevel(downsample.ResLevel1)
	ResolutionLevel1h  = ResolutionLevel(downsample.ResLevel2)
)

// Syncer synchronizes block metas from a bucket into a local directory.
// It sorts them into compaction groups based on equal label sets.
type Syncer struct {
	logger                   log.Logger
	reg                      prometheus.Registerer
	bkt                      objstore.Bucket
	fetcher                  block.MetadataFetcher
	mtx                      sync.Mutex
	blocks                   map[ulid.ULID]*metadata.Meta
	blockSyncConcurrency     int
	metrics                  *syncerMetrics
	acceptMalformedIndex     bool
	enableVerticalCompaction bool
	duplicateBlocksFilter    *block.DeduplicateFilter
	ignoreDeletionMarkFilter *block.IgnoreDeletionMarkFilter
}

type syncerMetrics struct {
	garbageCollectedBlocks    prometheus.Counter
	garbageCollections        prometheus.Counter
	garbageCollectionFailures prometheus.Counter
	garbageCollectionDuration prometheus.Histogram
	compactions               *prometheus.CounterVec
	compactionRunsStarted     *prometheus.CounterVec
	compactionRunsCompleted   *prometheus.CounterVec
	compactionFailures        *prometheus.CounterVec
	verticalCompactions       *prometheus.CounterVec
	blocksMarkedForDeletion   prometheus.Counter
}

func newSyncerMetrics(reg prometheus.Registerer, blocksMarkedForDeletion prometheus.Counter) *syncerMetrics {
	var m syncerMetrics

	m.garbageCollectedBlocks = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collected_blocks_total",
		Help: "Total number of deleted blocks by compactor.",
	})
	m.garbageCollections = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_total",
		Help: "Total number of garbage collection operations.",
	})
	m.garbageCollectionFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_failures_total",
		Help: "Total number of failed garbage collection operations.",
	})
	m.garbageCollectionDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_compact_garbage_collection_duration_seconds",
		Help:    "Time it took to perform garbage collection iteration.",
		Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720},
	})

	m.compactions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compactions_total",
		Help: "Total number of group compaction attempts that resulted in a new block.",
	}, []string{"group"})
	m.compactionRunsStarted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compaction_runs_started_total",
		Help: "Total number of group compaction attempts.",
	}, []string{"group"})
	m.compactionRunsCompleted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compaction_runs_completed_total",
		Help: "Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.",
	}, []string{"group"})
	m.compactionFailures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_compactions_failures_total",
		Help: "Total number of failed group compactions.",
	}, []string{"group"})
	m.verticalCompactions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_group_vertical_compactions_total",
		Help: "Total number of group compaction attempts that resulted in a new block based on overlapping blocks.",
	}, []string{"group"})
	m.blocksMarkedForDeletion = blocksMarkedForDeletion

	return &m
}

// NewMetaSyncer returns a new Syncer for the given Bucket and directory.
// Blocks must be at least as old as the sync delay for being considered.
func NewSyncer(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, fetcher block.MetadataFetcher, duplicateBlocksFilter *block.DeduplicateFilter, ignoreDeletionMarkFilter *block.IgnoreDeletionMarkFilter, blocksMarkedForDeletion prometheus.Counter, blockSyncConcurrency int, acceptMalformedIndex bool, enableVerticalCompaction bool) (*Syncer, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Syncer{
		logger:                   logger,
		reg:                      reg,
		bkt:                      bkt,
		fetcher:                  fetcher,
		blocks:                   map[ulid.ULID]*metadata.Meta{},
		metrics:                  newSyncerMetrics(reg, blocksMarkedForDeletion),
		duplicateBlocksFilter:    duplicateBlocksFilter,
		ignoreDeletionMarkFilter: ignoreDeletionMarkFilter,
		blockSyncConcurrency:     blockSyncConcurrency,
		acceptMalformedIndex:     acceptMalformedIndex,
		// The syncer offers an option to enable vertical compaction, even if it's
		// not currently used by Thanos, because the compactor is also used by Cortex
		// which needs vertical compaction.
		enableVerticalCompaction: enableVerticalCompaction,
	}, nil
}

// UntilNextDownsampling calculates how long it will take until the next downsampling operation.
// Returns an error if there will be no downsampling.
func UntilNextDownsampling(m *metadata.Meta) (time.Duration, error) {
	timeRange := time.Duration((m.MaxTime - m.MinTime) * int64(time.Millisecond))
	switch m.Thanos.Downsample.Resolution {
	case downsample.ResLevel2:
		return time.Duration(0), errors.New("no downsampling")
	case downsample.ResLevel1:
		return time.Duration(downsample.DownsampleRange1*time.Millisecond) - timeRange, nil
	case downsample.ResLevel0:
		return time.Duration(downsample.DownsampleRange0*time.Millisecond) - timeRange, nil
	default:
		panic(errors.Errorf("invalid resolution %v", m.Thanos.Downsample.Resolution))
	}
}

func (s *Syncer) SyncMetas(ctx context.Context) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	metas, _, err := s.fetcher.Fetch(ctx)
	if err != nil {
		return retry(err)
	}
	s.blocks = metas

	return nil
}

// GroupKey returns a unique identifier for the group the block belongs to. It considers
// the downsampling resolution and the block's labels.
func GroupKey(meta metadata.Thanos) string {
	return groupKey(meta.Downsample.Resolution, labels.FromMap(meta.Labels))
}

func groupKey(res int64, lbls labels.Labels) string {
	return fmt.Sprintf("%d@%v", res, lbls.Hash())
}

// Groups returns the compaction groups for all blocks currently known to the syncer.
// It creates all groups from the scratch on every call.
func (s *Syncer) Groups() (res []*Group, err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	groups := map[string]*Group{}
	for _, m := range s.blocks {
		groupKey := GroupKey(m.Thanos)
		g, ok := groups[groupKey]
		if !ok {
			g, err = newGroup(
				log.With(s.logger, "compactionGroup", groupKey),
				s.bkt,
				labels.FromMap(m.Thanos.Labels),
				m.Thanos.Downsample.Resolution,
				s.acceptMalformedIndex,
				s.enableVerticalCompaction,
				s.metrics.compactions.WithLabelValues(groupKey),
				s.metrics.compactionRunsStarted.WithLabelValues(groupKey),
				s.metrics.compactionRunsCompleted.WithLabelValues(groupKey),
				s.metrics.compactionFailures.WithLabelValues(groupKey),
				s.metrics.verticalCompactions.WithLabelValues(groupKey),
				s.metrics.garbageCollectedBlocks,
				s.metrics.blocksMarkedForDeletion,
			)
			if err != nil {
				return nil, errors.Wrap(err, "create compaction group")
			}
			groups[groupKey] = g
			res = append(res, g)
		}
		if err := g.Add(m); err != nil {
			return nil, errors.Wrap(err, "add compaction group")
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key() < res[j].Key()
	})
	return res, nil
}

// GarbageCollect deletes blocks from the bucket if their data is available as part of a
// block with a higher compaction level.
// Call to SyncMetas function is required to populate duplicateIDs in duplicateBlocksFilter.
func (s *Syncer) GarbageCollect(ctx context.Context) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	begin := time.Now()

	duplicateIDs := s.duplicateBlocksFilter.DuplicateIDs()
	deletionMarkMap := s.ignoreDeletionMarkFilter.DeletionMarkBlocks()

	// GarbageIDs contains the duplicateIDs, since these blocks can be replaced with other blocks.
	// We also remove ids present in deletionMarkMap since these blocks are already marked for deletion.
	garbageIDs := []ulid.ULID{}
	for _, id := range duplicateIDs {
		if _, exists := deletionMarkMap[id]; !exists {
			garbageIDs = append(garbageIDs, id)
		}
	}

	for _, id := range garbageIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Spawn a new context so we always delete a block in full on shutdown.
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

		level.Info(s.logger).Log("msg", "marking outdated block for deletion", "block", id)

		err := block.MarkForDeletion(delCtx, s.logger, s.bkt, id)
		cancel()
		if err != nil {
			s.metrics.garbageCollectionFailures.Inc()
			return retry(errors.Wrapf(err, "delete block %s from bucket", id))
		}
		s.metrics.blocksMarkedForDeletion.Inc()

		// Immediately update our in-memory state so no further call to SyncMetas is needed
		// after running garbage collection.
		delete(s.blocks, id)
		s.metrics.garbageCollectedBlocks.Inc()
	}
	s.metrics.garbageCollections.Inc()
	s.metrics.garbageCollectionDuration.Observe(time.Since(begin).Seconds())
	return nil
}

// Group captures a set of blocks that have the same origin labels and downsampling resolution.
// Those blocks generally contain the same series and can thus efficiently be compacted.
type Group struct {
	logger                      log.Logger
	bkt                         objstore.Bucket
	labels                      labels.Labels
	resolution                  int64
	mtx                         sync.Mutex
	blocks                      map[ulid.ULID]*metadata.Meta
	acceptMalformedIndex        bool
	enableVerticalCompaction    bool
	compactions                 prometheus.Counter
	compactionRunsStarted       prometheus.Counter
	compactionRunsCompleted     prometheus.Counter
	compactionFailures          prometheus.Counter
	verticalCompactions         prometheus.Counter
	groupGarbageCollectedBlocks prometheus.Counter
	blocksMarkedForDeletion     prometheus.Counter
}

// newGroup returns a new compaction group.
func newGroup(
	logger log.Logger,
	bkt objstore.Bucket,
	lset labels.Labels,
	resolution int64,
	acceptMalformedIndex bool,
	enableVerticalCompaction bool,
	compactions prometheus.Counter,
	compactionRunsStarted prometheus.Counter,
	compactionRunsCompleted prometheus.Counter,
	compactionFailures prometheus.Counter,
	verticalCompactions prometheus.Counter,
	groupGarbageCollectedBlocks prometheus.Counter,
	blocksMarkedForDeletion prometheus.Counter,
) (*Group, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	g := &Group{
		logger:                      logger,
		bkt:                         bkt,
		labels:                      lset,
		resolution:                  resolution,
		blocks:                      map[ulid.ULID]*metadata.Meta{},
		acceptMalformedIndex:        acceptMalformedIndex,
		enableVerticalCompaction:    enableVerticalCompaction,
		compactions:                 compactions,
		compactionRunsStarted:       compactionRunsStarted,
		compactionRunsCompleted:     compactionRunsCompleted,
		compactionFailures:          compactionFailures,
		verticalCompactions:         verticalCompactions,
		groupGarbageCollectedBlocks: groupGarbageCollectedBlocks,
		blocksMarkedForDeletion:     blocksMarkedForDeletion,
	}
	return g, nil
}

// Key returns an identifier for the group.
func (cg *Group) Key() string {
	return groupKey(cg.resolution, cg.labels)
}

// Add the block with the given meta to the group.
func (cg *Group) Add(meta *metadata.Meta) error {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	if !labels.Equal(cg.labels, labels.FromMap(meta.Thanos.Labels)) {
		return errors.New("block and group labels do not match")
	}
	if cg.resolution != meta.Thanos.Downsample.Resolution {
		return errors.New("block and group resolution do not match")
	}
	cg.blocks[meta.ULID] = meta
	return nil
}

// IDs returns all sorted IDs of blocks in the group.
func (cg *Group) IDs() (ids []ulid.ULID) {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	for id := range cg.blocks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})
	return ids
}

// Labels returns the labels that all blocks in the group share.
func (cg *Group) Labels() labels.Labels {
	return cg.labels
}

// Resolution returns the common downsampling resolution of blocks in the group.
func (cg *Group) Resolution() int64 {
	return cg.resolution
}

// Compact plans and runs a single compaction against the group. The compacted result
// is uploaded into the bucket the blocks were retrieved from.
func (cg *Group) Compact(ctx context.Context, dir string, comp tsdb.Compactor) (bool, ulid.ULID, error) {
	cg.compactionRunsStarted.Inc()

	subDir := filepath.Join(dir, cg.Key())

	defer func() {
		if err := os.RemoveAll(subDir); err != nil {
			level.Error(cg.logger).Log("msg", "failed to remove compaction group work directory", "path", subDir, "err", err)
		}
	}()

	if err := os.RemoveAll(subDir); err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "clean compaction group dir")
	}
	if err := os.MkdirAll(subDir, 0777); err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "create compaction group dir")
	}

	shouldRerun, compID, err := cg.compact(ctx, subDir, comp)
	if err != nil {
		cg.compactionFailures.Inc()
		return false, ulid.ULID{}, err
	}
	cg.compactionRunsCompleted.Inc()
	return shouldRerun, compID, nil
}

// Issue347Error is a type wrapper for errors that should invoke repair process for broken block.
type Issue347Error struct {
	err error

	id ulid.ULID
}

func issue347Error(err error, brokenBlock ulid.ULID) Issue347Error {
	return Issue347Error{err: err, id: brokenBlock}
}

func (e Issue347Error) Error() string {
	return e.err.Error()
}

// IsIssue347Error returns true if the base error is a Issue347Error.
func IsIssue347Error(err error) bool {
	_, ok := errors.Cause(err).(Issue347Error)
	return ok
}

// HaltError is a type wrapper for errors that should halt any further progress on compactions.
type HaltError struct {
	err error
}

func halt(err error) HaltError {
	return HaltError{err: err}
}

func (e HaltError) Error() string {
	return e.err.Error()
}

// IsHaltError returns true if the base error is a HaltError.
// If a multierror is passed, any halt error will return true.
func IsHaltError(err error) bool {
	if multiErr, ok := err.(terrors.MultiError); ok {
		for _, err := range multiErr {
			if _, ok := errors.Cause(err).(HaltError); ok {
				return true
			}
		}
		return false
	}

	_, ok := errors.Cause(err).(HaltError)
	return ok
}

// RetryError is a type wrapper for errors that should trigger warning log and retry whole compaction loop, but aborting
// current compaction further progress.
type RetryError struct {
	err error
}

func retry(err error) error {
	if IsHaltError(err) {
		return err
	}
	return RetryError{err: err}
}

func (e RetryError) Error() string {
	return e.err.Error()
}

// IsRetryError returns true if the base error is a RetryError.
// If a multierror is passed, all errors must be retriable.
func IsRetryError(err error) bool {
	if multiErr, ok := err.(terrors.MultiError); ok {
		for _, err := range multiErr {
			if _, ok := errors.Cause(err).(RetryError); !ok {
				return false
			}
		}
		return true
	}

	_, ok := errors.Cause(err).(RetryError)
	return ok
}

func (cg *Group) areBlocksOverlapping(include *metadata.Meta, excludeDirs ...string) error {
	var (
		metas   []tsdb.BlockMeta
		exclude = map[ulid.ULID]struct{}{}
	)

	for _, e := range excludeDirs {
		id, err := ulid.Parse(filepath.Base(e))
		if err != nil {
			return errors.Wrapf(err, "overlaps find dir %s", e)
		}
		exclude[id] = struct{}{}
	}

	for _, m := range cg.blocks {
		if _, ok := exclude[m.ULID]; ok {
			continue
		}
		metas = append(metas, m.BlockMeta)
	}

	if include != nil {
		metas = append(metas, include.BlockMeta)
	}

	sort.Slice(metas, func(i, j int) bool {
		return metas[i].MinTime < metas[j].MinTime
	})
	if overlaps := tsdb.OverlappingBlocks(metas); len(overlaps) > 0 {
		return errors.Errorf("overlaps found while gathering blocks. %s", overlaps)
	}
	return nil
}

// RepairIssue347 repairs the https://github.com/prometheus/tsdb/issues/347 issue when having issue347Error.
func RepairIssue347(ctx context.Context, logger log.Logger, bkt objstore.Bucket, blocksMarkedForDeletion prometheus.Counter, issue347Err error) error {
	ie, ok := errors.Cause(issue347Err).(Issue347Error)
	if !ok {
		return errors.Errorf("Given error is not an issue347 error: %v", issue347Err)
	}

	level.Info(logger).Log("msg", "Repairing block broken by https://github.com/prometheus/tsdb/issues/347", "id", ie.id, "err", issue347Err)

	tmpdir, err := ioutil.TempDir("", fmt.Sprintf("repair-issue-347-id-%s-", ie.id))
	if err != nil {
		return err
	}

	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			level.Warn(logger).Log("msg", "failed to remote tmpdir", "err", err, "tmpdir", tmpdir)
		}
	}()

	bdir := filepath.Join(tmpdir, ie.id.String())
	if err := block.Download(ctx, logger, bkt, ie.id, bdir); err != nil {
		return retry(errors.Wrapf(err, "download block %s", ie.id))
	}

	meta, err := metadata.Read(bdir)
	if err != nil {
		return errors.Wrapf(err, "read meta from %s", bdir)
	}

	resid, err := block.Repair(logger, tmpdir, ie.id, metadata.CompactorRepairSource, block.IgnoreIssue347OutsideChunk)
	if err != nil {
		return errors.Wrapf(err, "repair failed for block %s", ie.id)
	}

	// Verify repaired id before uploading it.
	if err := block.VerifyIndex(logger, filepath.Join(tmpdir, resid.String(), block.IndexFilename), meta.MinTime, meta.MaxTime); err != nil {
		return errors.Wrapf(err, "repaired block is invalid %s", resid)
	}

	level.Info(logger).Log("msg", "uploading repaired block", "newID", resid)
	if err = block.Upload(ctx, logger, bkt, filepath.Join(tmpdir, resid.String())); err != nil {
		return retry(errors.Wrapf(err, "upload of %s failed", resid))
	}

	level.Info(logger).Log("msg", "deleting broken block", "id", ie.id)

	// Spawn a new context so we always delete a block in full on shutdown.
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// TODO(bplotka): Issue with this will introduce overlap that will halt compactor. Automate that (fix duplicate overlaps caused by this).
	if err := block.MarkForDeletion(delCtx, logger, bkt, ie.id); err != nil {
		return errors.Wrapf(err, "deleting old block %s failed. You need to delete this block manually", ie.id)
	}
	blocksMarkedForDeletion.Inc()
	return nil
}

func (cg *Group) compact(ctx context.Context, dir string, comp tsdb.Compactor) (shouldRerun bool, compID ulid.ULID, err error) {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	// Check for overlapped blocks.
	overlappingBlocks := false
	if err := cg.areBlocksOverlapping(nil); err != nil {
		if !cg.enableVerticalCompaction {
			return false, ulid.ULID{}, halt(errors.Wrap(err, "pre compaction overlap check"))
		}

		overlappingBlocks = true
	}

	// Planning a compaction works purely based on the meta.json files in our future group's dir.
	// So we first dump all our memory block metas into the directory.
	for _, meta := range cg.blocks {
		bdir := filepath.Join(dir, meta.ULID.String())
		if err := os.MkdirAll(bdir, 0777); err != nil {
			return false, ulid.ULID{}, errors.Wrap(err, "create planning block dir")
		}
		if err := metadata.Write(cg.logger, bdir, meta); err != nil {
			return false, ulid.ULID{}, errors.Wrap(err, "write planning meta file")
		}
	}

	// Plan against the written meta.json files.
	plan, err := comp.Plan(dir)
	if err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "plan compaction")
	}
	if len(plan) == 0 {
		// Nothing to do.
		return false, ulid.ULID{}, nil
	}

	// Due to #183 we verify that none of the blocks in the plan have overlapping sources.
	// This is one potential source of how we could end up with duplicated chunks.
	uniqueSources := map[ulid.ULID]struct{}{}

	// Once we have a plan we need to download the actual data.
	begin := time.Now()

	for _, pdir := range plan {
		meta, err := metadata.Read(pdir)
		if err != nil {
			return false, ulid.ULID{}, errors.Wrapf(err, "read meta from %s", pdir)
		}

		cgKey, groupKey := cg.Key(), GroupKey(meta.Thanos)
		if cgKey != groupKey {
			return false, ulid.ULID{}, halt(errors.Wrapf(err, "compact planned compaction for mixed groups. group: %s, planned block's group: %s", cgKey, groupKey))
		}

		for _, s := range meta.Compaction.Sources {
			if _, ok := uniqueSources[s]; ok {
				return false, ulid.ULID{}, halt(errors.Errorf("overlapping sources detected for plan %v", plan))
			}
			uniqueSources[s] = struct{}{}
		}

		id, err := ulid.Parse(filepath.Base(pdir))
		if err != nil {
			return false, ulid.ULID{}, errors.Wrapf(err, "plan dir %s", pdir)
		}

		if meta.ULID.Compare(id) != 0 {
			return false, ulid.ULID{}, errors.Errorf("mismatch between meta %s and dir %s", meta.ULID, id)
		}

		if err := block.Download(ctx, cg.logger, cg.bkt, id, pdir); err != nil {
			return false, ulid.ULID{}, retry(errors.Wrapf(err, "download block %s", id))
		}

		// Ensure all input blocks are valid.
		stats, err := block.GatherIndexIssueStats(cg.logger, filepath.Join(pdir, block.IndexFilename), meta.MinTime, meta.MaxTime)
		if err != nil {
			return false, ulid.ULID{}, errors.Wrapf(err, "gather index issues for block %s", pdir)
		}

		if err := stats.CriticalErr(); err != nil {
			return false, ulid.ULID{}, halt(errors.Wrapf(err, "block with not healthy index found %s; Compaction level %v; Labels: %v", pdir, meta.Compaction.Level, meta.Thanos.Labels))
		}

		if err := stats.Issue347OutsideChunksErr(); err != nil {
			return false, ulid.ULID{}, issue347Error(errors.Wrapf(err, "invalid, but reparable block %s", pdir), meta.ULID)
		}

		if err := stats.PrometheusIssue5372Err(); !cg.acceptMalformedIndex && err != nil {
			return false, ulid.ULID{}, errors.Wrapf(err,
				"block id %s, try running with --debug.accept-malformed-index", id)
		}
	}
	level.Debug(cg.logger).Log("msg", "downloaded and verified blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	begin = time.Now()

	compID, err = comp.Compact(dir, plan, nil)
	if err != nil {
		return false, ulid.ULID{}, halt(errors.Wrapf(err, "compact blocks %v", plan))
	}
	if compID == (ulid.ULID{}) {
		// Prometheus compactor found that the compacted block would have no samples.
		level.Info(cg.logger).Log("msg", "compacted block would have no samples, deleting source blocks", "blocks", fmt.Sprintf("%v", plan))
		for _, block := range plan {
			meta, err := metadata.Read(block)
			if err != nil {
				level.Warn(cg.logger).Log("msg", "failed to read meta for block", "block", block)
				continue
			}
			if meta.Stats.NumSamples == 0 {
				if err := cg.deleteBlock(block); err != nil {
					level.Warn(cg.logger).Log("msg", "failed to delete empty block found during compaction", "block", block)
				}
			}
		}
		// Even though this block was empty, there may be more work to do.
		return true, ulid.ULID{}, nil
	}
	cg.compactions.Inc()
	if overlappingBlocks {
		cg.verticalCompactions.Inc()
	}
	level.Info(cg.logger).Log("msg", "compacted blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin), "overlapping_blocks", overlappingBlocks)

	bdir := filepath.Join(dir, compID.String())
	index := filepath.Join(bdir, block.IndexFilename)
	indexCache := filepath.Join(bdir, block.IndexCacheFilename)

	newMeta, err := metadata.InjectThanos(cg.logger, bdir, metadata.Thanos{
		Labels:     cg.labels.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: cg.resolution},
		Source:     metadata.CompactorSource,
	}, nil)
	if err != nil {
		return false, ulid.ULID{}, errors.Wrapf(err, "failed to finalize the block %s", bdir)
	}

	if err = os.Remove(filepath.Join(bdir, "tombstones")); err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "remove tombstones")
	}

	// Ensure the output block is valid.
	if err := block.VerifyIndex(cg.logger, index, newMeta.MinTime, newMeta.MaxTime); !cg.acceptMalformedIndex && err != nil {
		return false, ulid.ULID{}, halt(errors.Wrapf(err, "invalid result block %s", bdir))
	}

	// Ensure the output block is not overlapping with anything else,
	// unless vertical compaction is enabled.
	if !cg.enableVerticalCompaction {
		if err := cg.areBlocksOverlapping(newMeta, plan...); err != nil {
			return false, ulid.ULID{}, halt(errors.Wrapf(err, "resulted compacted block %s overlaps with something", bdir))
		}
	}

	if err := indexheader.WriteJSON(cg.logger, index, indexCache); err != nil {
		return false, ulid.ULID{}, errors.Wrap(err, "write index cache")
	}

	begin = time.Now()

	if err := block.Upload(ctx, cg.logger, cg.bkt, bdir); err != nil {
		return false, ulid.ULID{}, retry(errors.Wrapf(err, "upload of %s failed", compID))
	}
	level.Info(cg.logger).Log("msg", "uploaded block", "result_block", compID, "duration", time.Since(begin))

	// Delete the blocks we just compacted from the group and bucket so they do not get included
	// into the next planning cycle.
	// Eventually the block we just uploaded should get synced into the group again (including sync-delay).
	for _, b := range plan {
		if err := cg.deleteBlock(b); err != nil {
			return false, ulid.ULID{}, retry(errors.Wrapf(err, "delete old block from bucket"))
		}
		cg.groupGarbageCollectedBlocks.Inc()
	}

	return true, compID, nil
}

func (cg *Group) deleteBlock(b string) error {
	id, err := ulid.Parse(filepath.Base(b))
	if err != nil {
		return errors.Wrapf(err, "plan dir %s", b)
	}

	if err := os.RemoveAll(b); err != nil {
		return errors.Wrapf(err, "remove old block dir %s", id)
	}

	// Spawn a new context so we always delete a block in full on shutdown.
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	level.Info(cg.logger).Log("msg", "marking compacted block for deletion", "old_block", id)
	if err := block.MarkForDeletion(delCtx, cg.logger, cg.bkt, id); err != nil {
		return errors.Wrapf(err, "delete block %s from bucket", id)
	}
	cg.blocksMarkedForDeletion.Inc()
	return nil
}

// BucketCompactor compacts blocks in a bucket.
type BucketCompactor struct {
	logger      log.Logger
	sy          *Syncer
	comp        tsdb.Compactor
	compactDir  string
	bkt         objstore.Bucket
	concurrency int
}

// NewBucketCompactor creates a new bucket compactor.
func NewBucketCompactor(
	logger log.Logger,
	sy *Syncer,
	comp tsdb.Compactor,
	compactDir string,
	bkt objstore.Bucket,
	concurrency int,
) (*BucketCompactor, error) {
	if concurrency <= 0 {
		return nil, errors.Errorf("invalid concurrency level (%d), concurrency level must be > 0", concurrency)
	}
	return &BucketCompactor{
		logger:      logger,
		sy:          sy,
		comp:        comp,
		compactDir:  compactDir,
		bkt:         bkt,
		concurrency: concurrency,
	}, nil
}

// Compact runs compaction over bucket.
func (c *BucketCompactor) Compact(ctx context.Context) error {
	defer func() {
		if err := os.RemoveAll(c.compactDir); err != nil {
			level.Error(c.logger).Log("msg", "failed to remove compaction work directory", "path", c.compactDir, "err", err)
		}
	}()

	// Loop over bucket and compact until there's no work left.
	for {
		var (
			wg                     sync.WaitGroup
			workCtx, workCtxCancel = context.WithCancel(ctx)
			groupChan              = make(chan *Group)
			errChan                = make(chan error, c.concurrency)
			finishedAllGroups      = true
			mtx                    sync.Mutex
		)
		defer workCtxCancel()

		// Set up workers who will compact the groups when the groups are ready.
		// They will compact available groups until they encounter an error, after which they will stop.
		for i := 0; i < c.concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for g := range groupChan {
					shouldRerunGroup, _, err := g.Compact(workCtx, c.compactDir, c.comp)
					if err == nil {
						if shouldRerunGroup {
							mtx.Lock()
							finishedAllGroups = false
							mtx.Unlock()
						}
						continue
					}

					if IsIssue347Error(err) {
						if err := RepairIssue347(workCtx, c.logger, c.bkt, c.sy.metrics.blocksMarkedForDeletion, err); err == nil {
							mtx.Lock()
							finishedAllGroups = false
							mtx.Unlock()
							continue
						}
					}
					errChan <- errors.Wrap(err, fmt.Sprintf("compaction failed for group %s", g.Key()))
					return
				}
			}()
		}

		// Clean up the compaction temporary directory at the beginning of every compaction loop.
		if err := os.RemoveAll(c.compactDir); err != nil {
			return errors.Wrap(err, "clean up the compaction temporary directory")
		}

		level.Info(c.logger).Log("msg", "start sync of metas")

		if err := c.sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync")
		}

		level.Info(c.logger).Log("msg", "start of GC")

		// Blocks that were compacted are garbage collected after each Compaction.
		// However if compactor crashes we need to resolve those on startup.
		if err := c.sy.GarbageCollect(ctx); err != nil {
			return errors.Wrap(err, "garbage")
		}

		level.Info(c.logger).Log("msg", "start of compaction")

		groups, err := c.sy.Groups()
		if err != nil {
			return errors.Wrap(err, "build compaction groups")
		}

		// Send all groups found during this pass to the compaction workers.
		var groupErrs terrors.MultiError

	groupLoop:
		for _, g := range groups {
			select {
			case groupErr := <-errChan:
				groupErrs.Add(groupErr)
				break groupLoop
			case groupChan <- g:
			}
		}
		close(groupChan)
		wg.Wait()

		// Collect any other error reported by the workers, or any error reported
		// while we were waiting for the last batch of groups to run the compaction.
		close(errChan)
		for groupErr := range errChan {
			groupErrs.Add(groupErr)
		}

		workCtxCancel()
		if len(groupErrs) > 0 {
			return groupErrs
		}

		if finishedAllGroups {
			break
		}
	}
	level.Info(c.logger).Log("msg", "compaction iterations done")
	return nil
}
