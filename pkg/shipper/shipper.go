// Package shipper detects directories on the local file system and uploads
// them to a block storage.
package shipper

import (
	"context"
	"os"
	"path/filepath"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"

	"math"

	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

// Bucket represents a writable bucket of data objects.
type Bucket interface {
	// Exists checks if the given directory exists at the remote site (and contains at least one element).
	Exists(ctx context.Context, dir string) (bool, error)

	// Upload writes the file specified in src to remote location specified as target.
	Upload(ctx context.Context, src, target string) error

	// Delete removes all data prefixed with the dir.
	Delete(ctx context.Context, dir string) error
}

type metrics struct {
	dirSyncs        prometheus.Counter
	dirSyncFailures prometheus.Counter
	uploads         prometheus.Counter
	uploadFailures  prometheus.Counter
}

func newMetrics(r prometheus.Registerer) *metrics {
	var m metrics

	m.dirSyncs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_syncs_total",
		Help: "Total dir sync attempts",
	})
	m.dirSyncFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_sync_failures_total",
		Help: "Total number of failed dir syncs",
	})
	m.uploads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_uploads_total",
		Help: "Total object upload attempts",
	})
	m.uploadFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_upload_failures_total",
		Help: "Total number of failed object uploads",
	})

	if r != nil {
		prometheus.MustRegister(
			m.dirSyncs,
			m.dirSyncFailures,
			m.uploads,
			m.uploadFailures,
		)
	}
	return &m
}

// Shipper watches a directory for matching files and directories and uploads
// them to a remote data store.
type Shipper struct {
	logger log.Logger
	dir    string
	bucket Bucket
	match  func(os.FileInfo) bool
	labels func() labels.Labels
	// MaxTime timestamp does not make sense for sidecar, so we need to gossip minTime only. We always have freshest data.
	gossipMinTimeFn func(mint int64)

	metrics *metrics
}

// New creates a new shipper that detects new TSDB blocks in dir and uploads them
// to remote if necessary. It attaches the return value of the labels getter to uploaded data.
func New(
	logger log.Logger,
	r prometheus.Registerer,
	dir string,
	bucket Bucket,
	lbls func() labels.Labels,
	gossipMinTimeFn func(mint int64),
) *Shipper {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if lbls == nil {
		lbls = func() labels.Labels { return nil }
	}
	if gossipMinTimeFn == nil {
		gossipMinTimeFn = func(mint int64) {}
	}
	return &Shipper{
		logger:          logger,
		dir:             dir,
		bucket:          bucket,
		labels:          lbls,
		gossipMinTimeFn: gossipMinTimeFn,
		metrics:         newMetrics(r),
	}
}

// Sync performs a single synchronization if the local block data with the remote end.
func (s *Shipper) Sync(ctx context.Context) {
	names, err := fileutil.ReadDir(s.dir)
	if err != nil {
		level.Warn(s.logger).Log("msg", "read dir failed", "err", err)
	}

	var oldestBlockMinTime int64 = math.MaxInt64
	for _, fn := range names {
		id, err := ulid.Parse(fn)
		if err != nil {
			continue
		}
		dir := filepath.Join(s.dir, fn)

		fi, err := os.Stat(dir)
		if err != nil {
			level.Warn(s.logger).Log("msg", "open file failed", "err", err)
			continue
		}
		if !fi.IsDir() {
			continue
		}
		minTime, err := s.sync(ctx, id, dir)
		if err != nil {
			level.Error(s.logger).Log("msg", "shipping failed", "dir", dir, "err", err)
			continue
		}

		if minTime < oldestBlockMinTime || oldestBlockMinTime == math.MaxInt64 {
			oldestBlockMinTime = minTime
		}
	}

	if oldestBlockMinTime != math.MaxInt64 {
		s.gossipMinTimeFn(oldestBlockMinTime)
	}
}

func (s *Shipper) sync(ctx context.Context, id ulid.ULID, dir string) (minTime int64, err error) {
	meta, err := block.ReadMetaFile(dir)
	if err != nil {
		return 0, errors.Wrap(err, "read meta file")
	}
	// We only ship of the first compacted block level.
	if meta.Compaction.Level > 1 {
		return meta.MinTime, nil
	}
	ok, err := s.bucket.Exists(ctx, id.String())
	if err != nil {
		return 0, errors.Wrap(err, "check exists")
	}
	if ok {
		return meta.MinTime, nil
	}

	level.Info(s.logger).Log("msg", "upload new block", "id", id)

	// We hard-link the files into a temporary upload directory so we are not affected
	// by other operations happening against the TSDB directory.
	updir := filepath.Join(s.dir, "thanos", "upload")

	if err := os.RemoveAll(updir); err != nil {
		return 0, errors.Wrap(err, "clean upload directory")
	}
	if err := os.MkdirAll(updir, 0777); err != nil {
		return 0, errors.Wrap(err, "create upload dir")
	}
	defer os.RemoveAll(updir)

	if err := hardlinkBlock(dir, updir); err != nil {
		return 0, errors.Wrap(err, "hard link block")
	}
	// Attach current labels and write a new meta file with Thanos extensions.
	if lset := s.labels(); lset != nil {
		meta.Thanos.Labels = lset.Map()
	}
	if err := block.WriteMetaFile(updir, meta); err != nil {
		return 0, errors.Wrap(err, "write meta file")
	}
	return meta.MinTime, s.uploadDir(ctx, id, updir)
}

// uploadDir uploads the given directory to the remote site.
func (s *Shipper) uploadDir(ctx context.Context, id ulid.ULID, dir string) error {
	s.metrics.dirSyncs.Inc()

	err := filepath.Walk(dir, func(src string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}

		target := filepath.Join(id.String(), strings.TrimPrefix(src, dir))
		level.Debug(s.logger).Log("msg", "upload file", "src", src, "dst", target)
		s.metrics.uploads.Inc()
		err = s.bucket.Upload(ctx, src, target)
		if err != nil {
			s.metrics.uploadFailures.Inc()
		}

		return err
	})
	if err == nil {
		return nil
	}
	s.metrics.dirSyncFailures.Inc()
	level.Error(s.logger).Log("msg", "upload failed; remove partial data", "dir", dir, "err", err)

	// We don't want to leave partially uploaded directories behind. Cleanup everything related to it
	// and use a uncanceled context.
	if err2 := s.bucket.Delete(ctx, dir); err2 != nil {
		level.Error(s.logger).Log(
			"msg", "cleanup failed; partial data may be left behind", "dir", dir, "err", err2)
	}
	return err
}

func hardlinkBlock(src, dst string) error {
	chunkDir := filepath.Join(dst, "chunks")

	if err := os.MkdirAll(chunkDir, 0777); err != nil {
		return errors.Wrap(err, "create chunks dir")
	}

	files, err := fileutil.ReadDir(filepath.Join(src, "chunks"))
	if err != nil {
		return errors.Wrap(err, "read chunk dir")
	}
	for i, fn := range files {
		files[i] = filepath.Join("chunks", fn)
	}
	files = append(files, "meta.json", "index")

	for _, fn := range files {
		if err := os.Link(filepath.Join(src, fn), filepath.Join(dst, fn)); err != nil {
			return errors.Wrapf(err, "hard link file %s", fn)
		}
	}
	return nil
}
