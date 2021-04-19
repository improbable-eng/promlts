// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/units"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/tsdb"
	blocksAPI "github.com/thanos-io/thanos/pkg/api/blocks"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/ui"
)

var (
	compactions = compactionSet{
		1 * time.Hour,
		2 * time.Hour,
		8 * time.Hour,
		2 * 24 * time.Hour,
		14 * 24 * time.Hour,
	}
)

type compactionSet []time.Duration

func (cs compactionSet) String() string {
	result := make([]string, len(cs))
	for i, c := range cs {
		result[i] = fmt.Sprintf("%d=%dh", i, int(c.Hours()))
	}
	return strings.Join(result, ", ")
}

// levels returns set of compaction levels not higher than specified max compaction level.
func (cs compactionSet) levels(maxLevel int) ([]int64, error) {
	if maxLevel >= len(cs) {
		return nil, errors.Errorf("level is bigger then default set of %d", len(cs))
	}

	levels := make([]int64, maxLevel+1)
	for i, c := range cs[:maxLevel+1] {
		levels[i] = int64(c / time.Millisecond)
	}
	return levels, nil
}

// maxLevel returns max available compaction level.
func (cs compactionSet) maxLevel() int {
	return len(cs) - 1
}

func registerCompact(app *extkingpin.App) {
	cmd := app.Command(component.Compact.String(), "Continuously compacts blocks in an object store bucket.")
	conf := &compactConfig{}
	conf.registerFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		return runCompact(g, logger, tracer, reg, component.Compact, *conf, getFlagsMap(cmd.Flags()))
	})
}

type compactMetrics struct {
	halted                      prometheus.Gauge
	retried                     prometheus.Counter
	iterations                  prometheus.Counter
	cleanups                    prometheus.Counter
	partialUploadDeleteAttempts prometheus.Counter
	blocksCleaned               prometheus.Counter
	blockCleanupFailures        prometheus.Counter
	blocksMarked                *prometheus.CounterVec
	garbageCollectedBlocks      prometheus.Counter
}

func newCompactMetrics(reg *prometheus.Registry, deleteDelay time.Duration) *compactMetrics {
	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_delete_delay_seconds",
		Help: "Configured delete delay in seconds.",
	}, func() float64 {
		return deleteDelay.Seconds()
	})

	m := &compactMetrics{}

	m.halted = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "thanos_compact_halted",
		Help: "Set to 1 if the compactor halted due to an unexpected error.",
	})
	m.halted.Set(0)
	m.retried = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_retries_total",
		Help: "Total number of retries after retriable compactor error.",
	})
	m.iterations = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_iterations_total",
		Help: "Total number of iterations that were executed successfully.",
	})
	m.cleanups = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_block_cleanup_loops_total",
		Help: "Total number of concurrent cleanup loops of partially uploaded blocks and marked blocks that were executed successfully.",
	})
	m.partialUploadDeleteAttempts = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_aborted_partial_uploads_deletion_attempts_total",
		Help: "Total number of started deletions of blocks that are assumed aborted and only partially uploaded.",
	})
	m.blocksCleaned = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_blocks_cleaned_total",
		Help: "Total number of blocks deleted in compactor.",
	})
	m.blockCleanupFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_block_cleanup_failures_total",
		Help: "Failures encountered while deleting blocks in compactor.",
	})
	m.blocksMarked = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_blocks_marked_total",
		Help: "Total number of blocks marked in compactor.",
	}, []string{"marker"})
	m.blocksMarked.WithLabelValues(metadata.NoCompactMarkFilename)
	m.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename)

	m.garbageCollectedBlocks = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collected_blocks_total",
		Help: "Total number of blocks marked for deletion by compactor.",
	})
	return m
}

func runCompact(
	g *run.Group,
	logger log.Logger,
	tracer opentracing.Tracer,
	reg *prometheus.Registry,
	component component.Component,
	conf compactConfig,
	flagsMap map[string]string,
) (rerr error) {
	deleteDelay := time.Duration(conf.deleteDelay)
	compactMetrics := newCompactMetrics(reg, deleteDelay)
	downsampleMetrics := newDownsampleMetrics(reg)

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(component, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	srv := httpserver.New(logger, reg, component, httpProbe,
		httpserver.WithListen(conf.http.bindAddress),
		httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	confContentYaml, err := conf.objStore.Content()
	if err != nil {
		return err
	}
	confPathYaml, err := conf.objStore.GetPath()

	bkt, err := client.NewBucket(logger, confContentYaml, reg, component.String(), confPathYaml...)
	if err != nil {
		return err
	}

	relabelContentYaml, err := conf.selectorRelabelConf.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}

	relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
	if err != nil {
		return err
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
	}()

	// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
	// The delay of deleteDelay/2 is added to ensure we fetch blocks that are meant to be deleted but do not have a replacement yet.
	// This is to make sure compactor will not accidentally perform compactions with gap instead.
	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, deleteDelay/2, conf.blockMetaFetchConcurrency)
	duplicateBlocksFilter := block.NewDeduplicateFilter()
	noCompactMarkerFilter := compact.NewGatherNoCompactionMarkFilter(logger, bkt)
	labelShardedMetaFilter := block.NewLabelShardedMetaFilter(relabelConfig)
	consistencyDelayMetaFilter := block.NewConsistencyDelayMetaFilter(logger, conf.consistencyDelay, extprom.WrapRegistererWithPrefix("thanos_", reg))

	baseMetaFetcher, err := block.NewBaseFetcher(logger, conf.blockMetaFetchConcurrency, bkt, "", extprom.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return errors.Wrap(err, "create meta fetcher")
	}

	enableVerticalCompaction := conf.enableVerticalCompaction
	if len(conf.dedupReplicaLabels) > 0 {
		enableVerticalCompaction = true
		level.Info(logger).Log(
			"msg", "deduplication.replica-label specified, enabling vertical compaction", "dedupReplicaLabels", strings.Join(conf.dedupReplicaLabels, ","),
		)
	}
	if enableVerticalCompaction {
		level.Info(logger).Log(
			"msg", "vertical compaction is enabled", "compact.enable-vertical-compaction", fmt.Sprintf("%v", conf.enableVerticalCompaction),
		)
	}
	var (
		compactorView = ui.NewBucketUI(
			logger,
			conf.label,
			conf.webConf.externalPrefix,
			conf.webConf.prefixHeaderName,
			"/loaded",
			component,
		)
		api = blocksAPI.NewBlocksAPI(logger, conf.webConf.disableCORS, conf.label, flagsMap)
		sy  *compact.Syncer
	)
	{
		// Make sure all compactor meta syncs are done through Syncer.SyncMeta for readability.
		cf := baseMetaFetcher.NewMetaFetcher(
			extprom.WrapRegistererWithPrefix("thanos_", reg), []block.MetadataFilter{
				labelShardedMetaFilter,
				consistencyDelayMetaFilter,
				ignoreDeletionMarkFilter,
				duplicateBlocksFilter,
				noCompactMarkerFilter,
			}, []block.MetadataModifier{block.NewReplicaLabelRemover(logger, conf.dedupReplicaLabels)},
		)
		cf.UpdateOnChange(func(blocks []metadata.Meta, err error) {
			compactorView.Set(blocks, err)
			api.SetLoaded(blocks, err)
		})
		sy, err = compact.NewMetaSyncer(
			logger,
			reg,
			bkt,
			cf,
			duplicateBlocksFilter,
			ignoreDeletionMarkFilter,
			compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename),
			compactMetrics.garbageCollectedBlocks,
			conf.blockSyncConcurrency)
		if err != nil {
			return errors.Wrap(err, "create syncer")
		}
	}

	levels, err := compactions.levels(conf.maxCompactionLevel)
	if err != nil {
		return errors.Wrap(err, "get compaction levels")
	}

	if conf.maxCompactionLevel < compactions.maxLevel() {
		level.Warn(logger).Log("msg", "Max compaction level is lower than should be", "current", conf.maxCompactionLevel, "default", compactions.maxLevel())
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		if rerr != nil {
			cancel()
		}
	}()
	// Instantiate the compactor with different time slices. Timestamps in TSDB
	// are in milliseconds.
	comp, err := tsdb.NewLeveledCompactor(ctx, reg, logger, levels, downsample.NewPool())
	if err != nil {
		return errors.Wrap(err, "create compactor")
	}

	var (
		compactDir      = path.Join(conf.dataDir, "compact")
		downsamplingDir = path.Join(conf.dataDir, "downsample")
	)

	if err := os.MkdirAll(compactDir, os.ModePerm); err != nil {
		return errors.Wrap(err, "create working compact directory")
	}

	if err := os.MkdirAll(downsamplingDir, os.ModePerm); err != nil {
		return errors.Wrap(err, "create working downsample directory")
	}

	grouper := compact.NewDefaultGrouper(
		logger,
		bkt,
		conf.acceptMalformedIndex,
		enableVerticalCompaction,
		reg,
		compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename),
		compactMetrics.garbageCollectedBlocks,
		metadata.HashFunc(conf.hashFunc),
	)
	planner := compact.WithLargeTotalIndexSizeFilter(
		compact.NewPlanner(logger, levels, noCompactMarkerFilter),
		bkt,
		int64(conf.maxBlockIndexSize),
		compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename),
	)
	blocksCleaner := compact.NewBlocksCleaner(logger, bkt, ignoreDeletionMarkFilter, deleteDelay, compactMetrics.blocksCleaned, compactMetrics.blockCleanupFailures)
	compactor, err := compact.NewBucketCompactor(
		logger,
		sy,
		grouper,
		planner,
		comp,
		compactDir,
		bkt,
		conf.compactionConcurrency,
	)
	if err != nil {
		return errors.Wrap(err, "create bucket compactor")
	}

	retentionByResolution := map[compact.ResolutionLevel]time.Duration{
		compact.ResolutionLevelRaw: time.Duration(conf.retentionRaw),
		compact.ResolutionLevel5m:  time.Duration(conf.retentionFiveMin),
		compact.ResolutionLevel1h:  time.Duration(conf.retentionOneHr),
	}

	if retentionByResolution[compact.ResolutionLevelRaw].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of raw samples is enabled", "duration", retentionByResolution[compact.ResolutionLevelRaw])
	}
	if retentionByResolution[compact.ResolutionLevel5m].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 5 min aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel5m])
	}
	if retentionByResolution[compact.ResolutionLevel1h].Seconds() != 0 {
		level.Info(logger).Log("msg", "retention policy of 1 hour aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel1h])
	}

	var cleanMtx sync.Mutex
	// TODO(GiedriusS): we could also apply retention policies here but the logic would be a bit more complex.
	cleanPartialMarked := func() error {
		cleanMtx.Lock()
		defer cleanMtx.Unlock()

		if err := sy.SyncMetas(ctx); err != nil {
			cancel()
			return errors.Wrap(err, "syncing metas")
		}

		compact.BestEffortCleanAbortedPartialUploads(ctx, logger, sy.Partial(), bkt, compactMetrics.partialUploadDeleteAttempts, compactMetrics.blocksCleaned, compactMetrics.blockCleanupFailures)
		if err := blocksCleaner.DeleteMarkedBlocks(ctx); err != nil {
			return errors.Wrap(err, "cleaning marked blocks")
		}
		compactMetrics.cleanups.Inc()

		return nil
	}

	compactMainFn := func() error {
		if err := compactor.Compact(ctx); err != nil {
			return errors.Wrap(err, "compaction")
		}

		if !conf.disableDownsampling {
			// After all compactions are done, work down the downsampling backlog.
			// We run two passes of this to ensure that the 1h downsampling is generated
			// for 5m downsamplings created in the first run.
			level.Info(logger).Log("msg", "start first pass of downsampling")
			if err := sy.SyncMetas(ctx); err != nil {
				return errors.Wrap(err, "sync before first pass of downsampling")
			}

			for _, meta := range sy.Metas() {
				groupKey := compact.DefaultGroupKey(meta.Thanos)
				downsampleMetrics.downsamples.WithLabelValues(groupKey)
				downsampleMetrics.downsampleFailures.WithLabelValues(groupKey)
			}
			if err := downsampleBucket(ctx, logger, downsampleMetrics, bkt, sy.Metas(), downsamplingDir, metadata.HashFunc(conf.hashFunc)); err != nil {
				return errors.Wrap(err, "first pass of downsampling failed")
			}

			level.Info(logger).Log("msg", "start second pass of downsampling")
			if err := sy.SyncMetas(ctx); err != nil {
				return errors.Wrap(err, "sync before second pass of downsampling")
			}
			if err := downsampleBucket(ctx, logger, downsampleMetrics, bkt, sy.Metas(), downsamplingDir, metadata.HashFunc(conf.hashFunc)); err != nil {
				return errors.Wrap(err, "second pass of downsampling failed")
			}
			level.Info(logger).Log("msg", "downsampling iterations done")
		} else {
			level.Info(logger).Log("msg", "downsampling was explicitly disabled")
		}

		// TODO(bwplotka): Find a way to avoid syncing if no op was done.
		if err := sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync before first pass of downsampling")
		}

		if err := compact.ApplyRetentionPolicyByResolution(ctx, logger, bkt, sy.Metas(), retentionByResolution, compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename)); err != nil {
			return errors.Wrap(err, "retention failed")
		}

		return cleanPartialMarked()
	}

	g.Add(func() error {
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		if !conf.wait {
			return compactMainFn()
		}

		// --wait=true is specified.
		return runutil.Repeat(conf.waitInterval, ctx.Done(), func() error {
			err := compactMainFn()
			if err == nil {
				compactMetrics.iterations.Inc()
				return nil
			}

			// The HaltError type signals that we hit a critical bug and should block
			// for investigation. You should alert on this being halted.
			if compact.IsHaltError(err) {
				if conf.haltOnError {
					level.Error(logger).Log("msg", "critical error detected; halting", "err", err)
					compactMetrics.halted.Set(1)
					select {}
				} else {
					return errors.Wrap(err, "critical error detected")
				}
			}

			// The RetryError signals that we hit an retriable error (transient error, no connection).
			// You should alert on this being triggered too frequently.
			if compact.IsRetryError(err) {
				level.Error(logger).Log("msg", "retriable error", "err", err)
				compactMetrics.retried.Inc()
				// TODO(bplotka): use actual "retry()" here instead of waiting 5 minutes?
				return nil
			}

			return errors.Wrap(err, "error executing compaction")
		})
	}, func(error) {
		cancel()
	})

	if conf.wait {
		r := route.New()

		ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)
		compactorView.Register(r, true, ins)

		global := ui.NewBucketUI(logger, conf.label, conf.webConf.externalPrefix, conf.webConf.prefixHeaderName, "/global", component)
		global.Register(r, false, ins)

		// Configure Request Logging for HTTP calls.
		opts := []logging.Option{logging.WithDecider(func(_ string, _ error) logging.Decision {
			return logging.NoLogCall
		})}
		logMiddleware := logging.NewHTTPServerMiddleware(logger, opts...)
		api.Register(r.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

		// Separate fetcher for global view.
		// TODO(bwplotka): Allow Bucket UI to visualize the state of the block as well.
		f := baseMetaFetcher.NewMetaFetcher(extprom.WrapRegistererWithPrefix("thanos_bucket_ui", reg), nil, nil, "component", "globalBucketUI")
		f.UpdateOnChange(func(blocks []metadata.Meta, err error) {
			global.Set(blocks, err)
			api.SetGlobal(blocks, err)
		})

		srv.Handle("/", r)

		// Periodically remove partial blocks and blocks marked for deletion
		// since one iteration potentially could take a long time.
		if conf.cleanupBlocksInterval > 0 {
			g.Add(func() error {
				return runutil.Repeat(conf.cleanupBlocksInterval, ctx.Done(), cleanPartialMarked)
			}, func(error) {
				cancel()
			})
		}

		g.Add(func() error {
			iterCtx, iterCancel := context.WithTimeout(ctx, conf.waitInterval)
			_, _, _ = f.Fetch(iterCtx)
			iterCancel()

			// For /global state make sure to fetch periodically.
			return runutil.Repeat(conf.blockViewerSyncBlockInterval, ctx.Done(), func() error {
				return runutil.RetryWithLog(logger, time.Minute, ctx.Done(), func() error {
					iterCtx, iterCancel := context.WithTimeout(ctx, conf.waitInterval)
					defer iterCancel()

					_, _, err := f.Fetch(iterCtx)
					if err != nil {
						return err
					}
					return nil
				})
			})
		}, func(error) {
			cancel()
		})
	}

	level.Info(logger).Log("msg", "starting compact node")
	statusProber.Ready()
	return nil
}

type compactConfig struct {
	haltOnError                                    bool
	acceptMalformedIndex                           bool
	maxCompactionLevel                             int
	http                                           httpConfig
	dataDir                                        string
	objStore                                       extflag.PathOrContent
	consistencyDelay                               time.Duration
	retentionRaw, retentionFiveMin, retentionOneHr model.Duration
	wait                                           bool
	waitInterval                                   time.Duration
	disableDownsampling                            bool
	blockSyncConcurrency                           int
	blockMetaFetchConcurrency                      int
	blockViewerSyncBlockInterval                   time.Duration
	cleanupBlocksInterval                          time.Duration
	compactionConcurrency                          int
	deleteDelay                                    model.Duration
	dedupReplicaLabels                             []string
	selectorRelabelConf                            extflag.PathOrContent
	webConf                                        webConfig
	label                                          string
	maxBlockIndexSize                              units.Base2Bytes
	hashFunc                                       string
	enableVerticalCompaction                       bool
}

func (cc *compactConfig) registerFlag(cmd extkingpin.FlagClause) {
	cmd.Flag("debug.halt-on-error", "Halt the process if a critical compaction error is detected.").
		Hidden().Default("true").BoolVar(&cc.haltOnError)
	cmd.Flag("debug.accept-malformed-index",
		"Compaction index verification will ignore out of order label names.").
		Hidden().Default("false").BoolVar(&cc.acceptMalformedIndex)
	cmd.Flag("debug.max-compaction-level", fmt.Sprintf("Maximum compaction level, default is %d: %s", compactions.maxLevel(), compactions.String())).
		Hidden().Default(strconv.Itoa(compactions.maxLevel())).IntVar(&cc.maxCompactionLevel)

	cc.http.registerFlag(cmd)

	cmd.Flag("data-dir", "Data directory in which to cache blocks and process compactions.").
		Default("./data").StringVar(&cc.dataDir)

	cc.objStore = *extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	cmd.Flag("consistency-delay", fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %v will be removed.", compact.PartialUploadThresholdAge)).
		Default("30m").DurationVar(&cc.consistencyDelay)

	cmd.Flag("retention.resolution-raw",
		"How long to retain raw samples in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&cc.retentionRaw)
	cmd.Flag("retention.resolution-5m", "How long to retain samples of resolution 1 (5 minutes) in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&cc.retentionFiveMin)
	cmd.Flag("retention.resolution-1h", "How long to retain samples of resolution 2 (1 hour) in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&cc.retentionOneHr)

	// TODO(kakkoyun, pgough): https://github.com/thanos-io/thanos/issues/2266.
	cmd.Flag("wait", "Do not exit after all compactions have been processed and wait for new work.").
		Short('w').BoolVar(&cc.wait)
	cmd.Flag("wait-interval", "Wait interval between consecutive compaction runs and bucket refreshes. Only works when --wait flag specified.").
		Default("5m").DurationVar(&cc.waitInterval)

	cmd.Flag("downsampling.disable", "Disables downsampling. This is not recommended "+
		"as querying long time ranges without non-downsampled data is not efficient and useful e.g it is not possible to render all samples for a human eye anyway").
		Default("false").BoolVar(&cc.disableDownsampling)

	cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing block metadata from object storage.").
		Default("20").IntVar(&cc.blockSyncConcurrency)
	cmd.Flag("block-meta-fetch-concurrency", "Number of goroutines to use when fetching block metadata from object storage.").
		Default("32").IntVar(&cc.blockMetaFetchConcurrency)
	cmd.Flag("block-viewer.global.sync-block-interval", "Repeat interval for syncing the blocks between local and remote view for /global Block Viewer UI.").
		Default("1m").DurationVar(&cc.blockViewerSyncBlockInterval)
	cmd.Flag("compact.cleanup-interval", "How often we should clean up partially uploaded blocks and blocks with deletion mark in the background when --wait has been enabled. Setting it to \"0s\" disables it - the cleaning will only happen at the end of an iteration.").
		Default("5m").DurationVar(&cc.cleanupBlocksInterval)

	cmd.Flag("compact.concurrency", "Number of goroutines to use when compacting groups.").
		Default("1").IntVar(&cc.compactionConcurrency)

	cmd.Flag("delete-delay", "Time before a block marked for deletion is deleted from bucket. "+
		"If delete-delay is non zero, blocks will be marked for deletion and compactor component will delete blocks marked for deletion from the bucket. "+
		"If delete-delay is 0, blocks will be deleted straight away. "+
		"Note that deleting blocks immediately can cause query failures, if store gateway still has the block loaded, "+
		"or compactor is ignoring the deletion because it's compacting the block at the same time.").
		Default("48h").SetValue(&cc.deleteDelay)

	cmd.Flag("compact.enable-vertical-compaction", "Experimental. When set to true, compactor will allow overlaps and perform **irreversible** vertical compaction. See https://thanos.io/tip/components/compact.md/#vertical-compactions to read more."+
		"Please note that this uses a NAIVE algorithm for merging (no smart replica deduplication, just chaining samples together)."+
		"NOTE: This flag is ignored and (enabled) when --deduplication.replica-label flag is set.").
		Hidden().Default("false").BoolVar(&cc.enableVerticalCompaction)

	cmd.Flag("deduplication.replica-label", "Label to treat as a replica indicator of blocks that can be deduplicated (repeated flag). This will merge multiple replica blocks into one. This process is irreversible."+
		"Experimental. When it is set to true, compactor will ignore the given labels so that vertical compaction can merge the blocks."+
		"Please note that this uses a NAIVE algorithm for merging (no smart replica deduplication, just chaining samples together)."+
		"This works well for deduplication of blocks with **precisely the same samples** like produced by Receiver replication.").
		Hidden().StringsVar(&cc.dedupReplicaLabels)

	// TODO(bwplotka): This is short term fix for https://github.com/thanos-io/thanos/issues/1424, replace with vertical block sharding https://github.com/thanos-io/thanos/pull/3390.
	cmd.Flag("compact.block-max-index-size", "Maximum index size for the resulted block during any compaction. Note that"+
		"total size is approximated in worst case. If the block that would be resulted from compaction is estimated to exceed this number, biggest source"+
		"block is marked for no compaction (no-compact-mark.json is uploaded) which causes this block to be excluded from any compaction. "+
		"Default is due to https://github.com/thanos-io/thanos/issues/1424, but it's overall recommended to keeps block size to some reasonable size.").
		Hidden().Default("64GB").BytesVar(&cc.maxBlockIndexSize)

	cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").EnumVar(&cc.hashFunc, "SHA256", "")

	cc.selectorRelabelConf = *extkingpin.RegisterSelectorRelabelFlags(cmd)

	cc.webConf.registerFlag(cmd)

	cmd.Flag("bucket-web-label", "Prometheus label to use as timeline title in the bucket web UI").StringVar(&cc.label)
}
