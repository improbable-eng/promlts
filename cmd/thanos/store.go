// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"

	blocksAPI "github.com/thanos-io/thanos/pkg/api/blocks"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/ui"
	"github.com/thanos-io/thanos/pkg/ui/config"
)

const fetcherConcurrency = 32

// registerStore registers a store command.
func registerStore(app *extkingpin.App) {
	cmd := app.Command(component.Store.String(), "store node giving access to blocks in a bucket provider. Now supported GCS, S3, Azure, Swift and Tencent COS.")

	httpBindAddr, httpGracePeriod := extkingpin.RegisterHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := extkingpin.RegisterGRPCFlags(cmd)

	dataDir := cmd.Flag("data-dir", "Local data directory used for caching purposes (index-header, in-mem cache items and meta.jsons). If removed, no data will be lost, just store will have to rebuild the cache. NOTE: Putting raw blocks here will not cause the store to read them. For such use cases use Prometheus + sidecar.").
		Default("./data").String()

	indexCacheSize := cmd.Flag("index-cache-size", "Maximum size of items held in the in-memory index cache. Ignored if --index-cache.config or --index-cache.config-file option is specified.").
		Default("250MB").Bytes()

	indexCacheConfig := extflag.RegisterPathOrContent(cmd, "index-cache.config",
		"YAML file that contains index cache configuration. See format details: https://thanos.io/tip/components/store.md/#index-cache",
		false)

	cachingBucketConfig := extflag.RegisterPathOrContent(extflag.HiddenCmdClause(cmd), "store.caching-bucket.config",
		"YAML that contains configuration for caching bucket. Experimental feature, with high risk of changes. See format details: https://thanos.io/tip/components/store.md/#caching-bucket",
		false)

	chunkPoolSize := cmd.Flag("chunk-pool-size", "Maximum size of concurrently allocatable bytes reserved strictly to reuse for chunks in memory.").
		Default("2GB").Bytes()

	maxSampleCount := cmd.Flag("store.grpc.series-sample-limit",
		"Maximum amount of samples returned via a single Series call. The Series call fails if this limit is exceeded. 0 means no limit. NOTE: For efficiency the limit is internally implemented as 'chunks limit' considering each chunk contains 120 samples (it's the max number of samples each chunk can contain), so the actual number of samples might be lower, even though the maximum could be hit.").
		Default("0").Uint()

	maxConcurrent := cmd.Flag("store.grpc.series-max-concurrency", "Maximum number of concurrent Series calls.").Default("20").Int()

	objStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "", true)

	syncInterval := cmd.Flag("sync-block-duration", "Repeat interval for syncing the blocks between local and remote view.").
		Default("3m").Duration()

	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when constructing index-cache.json blocks from object storage.").
		Default("20").Int()

	minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to serve. Thanos Store will serve only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))

	maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to serve. Thanos Store will serve only blocks, which happened earlier than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z"))

	advertiseCompatibilityLabel := cmd.Flag("debug.advertise-compatibility-label", "If true, Store Gateway in addition to other labels, will advertise special \"@thanos_compatibility_store_type=store\" label set. This makes store Gateway compatible with Querier before 0.8.0").
		Hidden().Default("true").Bool()

	selectorRelabelConf := extkingpin.RegisterSelectorRelabelFlags(cmd)

	postingOffsetsInMemSampling := cmd.Flag("store.index-header-posting-offsets-in-mem-sampling", "Controls what is the ratio of postings offsets store will hold in memory. "+
		"Larger value will keep less offsets, which will increase CPU cycles needed for query touching those postings. It's meant for setups that want low baseline memory pressure and where less traffic is expected. "+
		"On the contrary, smaller value will increase baseline memory usage, but improve latency slightly. 1 will keep all in memory. Default value is the same as in Prometheus which gives a good balance.").
		Hidden().Default(fmt.Sprintf("%v", store.DefaultPostingOffsetInMemorySampling)).Int()

	consistencyDelay := extkingpin.ModelDuration(cmd.Flag("consistency-delay", "Minimum age of all blocks before they are being read. Set it to safe value (e.g 30m) if your object storage is eventually consistent. GCS and S3 are (roughly) strongly consistent.").
		Default("0s"))

	ignoreDeletionMarksDelay := extkingpin.ModelDuration(cmd.Flag("ignore-deletion-marks-delay", "Duration after which the blocks marked for deletion will be filtered out while fetching blocks. "+
		"The idea of ignore-deletion-marks-delay is to ignore blocks that are marked for deletion with some delay. This ensures store can still serve blocks that are meant to be deleted but do not have a replacement yet. "+
		"If delete-delay duration is provided to compactor or bucket verify component, it will upload deletion-mark.json file to mark after what duration the block should be deleted rather than deleting the block straight away. "+
		"If delete-delay is non-zero for compactor or bucket verify component, ignore-deletion-marks-delay should be set to (delete-delay)/2 so that blocks marked for deletion are filtered out while fetching blocks before being deleted from bucket. "+
		"Default is 24h, half of the default value for --delete-delay on compactor.").
		Default("24h"))

	lazyIndexReaderEnabled := cmd.Flag("store.enable-index-header-lazy-reader", "If true, Store Gateway will lazy memory map index-header only once the block is required by a query.").
		Default("false").Bool()

	lazyIndexReaderIdleTimeout := cmd.Flag("store.index-header-lazy-reader-idle-timeout", "If index-header lazy reader is enabled and this idle timeout setting is > 0, memory map-ed index-headers will be automatically released after 'idle timeout' inactivity.").
		Hidden().Default("5m").Duration()

	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the bucket web UI interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos bucket web UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, debugLogging bool) error {
		if minTime.PrometheusTimestamp() > maxTime.PrometheusTimestamp() {
			return errors.Errorf("invalid argument: --min-time '%s' can't be greater than --max-time '%s'",
				minTime, maxTime)
		}

		return runStore(g,
			logger,
			reg,
			tracer,
			indexCacheConfig,
			objStoreConfig,
			*dataDir,
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			uint64(*indexCacheSize),
			uint64(*chunkPoolSize),
			uint64(*maxSampleCount),
			*maxConcurrent,
			component.Store,
			debugLogging,
			*syncInterval,
			*blockSyncConcurrency,
			&store.FilterConfig{
				MinTime: *minTime,
				MaxTime: *maxTime,
			},
			selectorRelabelConf,
			*advertiseCompatibilityLabel,
			time.Duration(*consistencyDelay),
			time.Duration(*ignoreDeletionMarksDelay),
			*webExternalPrefix,
			*webPrefixHeaderName,
			*postingOffsetsInMemSampling,
			cachingBucketConfig,
			getFlagsMap(cmd.Flags()),
			*lazyIndexReaderEnabled,
			*lazyIndexReaderIdleTimeout,
		)
	})
}

// runStore starts a daemon that serves queries to cluster peers using data from an object store.
func runStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	indexCacheConfig *extflag.PathOrContent,
	objStoreConfig *extflag.PathOrContent,
	dataDir string,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert, grpcKey, grpcClientCA, httpBindAddr string,
	httpGracePeriod time.Duration,
	indexCacheSizeBytes, chunkPoolSizeBytes, maxSampleCount uint64,
	maxConcurrency int,
	component component.Component,
	verbose bool,
	syncInterval time.Duration,
	blockSyncConcurrency int,
	filterConf *store.FilterConfig,
	selectorRelabelConf *extflag.PathOrContent,
	advertiseCompatibilityLabel bool,
	consistencyDelay time.Duration,
	ignoreDeletionMarksDelay time.Duration,
	externalPrefix, prefixHeader string,
	postingOffsetsInMemSampling int,
	cachingBucketConfig *extflag.PathOrContent,
	flagsMap map[string]string,
	lazyIndexReaderEnabled bool,
	lazyIndexReaderIdleTimeout time.Duration,
) error {
	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(component, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	srv := httpserver.New(logger, reg, component, httpProbe,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}
	confContentYamlStr, err := config.ReplaceSecret(confContentYaml)
	if err != nil {
		return err
	}
	bkt, err := client.NewBucket(logger, confContentYaml, reg, component.String())
	if err != nil {
		return errors.Wrap(err, "create bucket client")
	}

	cachingBucketConfigYaml, err := cachingBucketConfig.Content()
	if err != nil {
		return errors.Wrap(err, "get caching bucket configuration")
	}
	if len(cachingBucketConfigYaml) > 0 {
		bkt, err = storecache.NewCachingBucketFromYaml(cachingBucketConfigYaml, bkt, logger, reg)
		if err != nil {
			return errors.Wrap(err, "create caching bucket")
		}
	}

	relabelContentYaml, err := selectorRelabelConf.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}

	relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
	if err != nil {
		return err
	}

	indexCacheContentYaml, err := indexCacheConfig.Content()
	if err != nil {
		return errors.Wrap(err, "get content of index cache configuration")
	}

	// Add config files content to config files map.
	configFilesMap := map[string]string{
		"Object Store Configuration":     string(confContentYamlStr),
		"Index Cache Configuration":      string(indexCacheContentYaml),
		"Caching Bucket Configuration":   string(cachingBucketConfigYaml),
		"Selector Relabel Configuration": string(relabelContentYaml),
	}

	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
		}
	}()

	// Create the index cache loading its config from config file, while keeping
	// backward compatibility with the pre-config file era.
	var indexCache storecache.IndexCache
	if len(indexCacheContentYaml) > 0 {
		indexCache, err = storecache.NewIndexCache(logger, indexCacheContentYaml, reg)
	} else {
		indexCache, err = storecache.NewInMemoryIndexCacheWithConfig(logger, reg, storecache.InMemoryIndexCacheConfig{
			MaxSize:     model.Bytes(indexCacheSizeBytes),
			MaxItemSize: storecache.DefaultInMemoryIndexCacheConfig.MaxItemSize,
		})
	}
	if err != nil {
		return errors.Wrap(err, "create index cache")
	}

	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, ignoreDeletionMarksDelay, fetcherConcurrency)
	metaFetcher, err := block.NewMetaFetcher(logger, fetcherConcurrency, bkt, dataDir, extprom.WrapRegistererWithPrefix("thanos_", reg),
		[]block.MetadataFilter{
			block.NewTimePartitionMetaFilter(filterConf.MinTime, filterConf.MaxTime),
			block.NewLabelShardedMetaFilter(relabelConfig),
			block.NewConsistencyDelayMetaFilter(logger, consistencyDelay, extprom.WrapRegistererWithPrefix("thanos_", reg)),
			ignoreDeletionMarkFilter,
			block.NewDeduplicateFilter(),
		}, nil)
	if err != nil {
		return errors.Wrap(err, "meta fetcher")
	}

	// Limit the concurrency on queries against the Thanos store.
	if maxConcurrency < 0 {
		return errors.Errorf("max concurrency value cannot be lower than 0 (got %v)", maxConcurrency)
	}

	queriesGate := gate.New(extprom.WrapRegistererWithPrefix("thanos_bucket_store_series_", reg), maxConcurrency)

	bs, err := store.NewBucketStore(
		logger,
		reg,
		bkt,
		metaFetcher,
		dataDir,
		indexCache,
		queriesGate,
		chunkPoolSizeBytes,
		store.NewChunksLimiterFactory(maxSampleCount/store.MaxSamplesPerChunk), // The samples limit is an approximation based on the max number of samples per chunk.
		verbose,
		blockSyncConcurrency,
		filterConf,
		advertiseCompatibilityLabel,
		postingOffsetsInMemSampling,
		false,
		lazyIndexReaderEnabled,
		lazyIndexReaderIdleTimeout,
	)
	if err != nil {
		return errors.Wrap(err, "create object storage store")
	}

	// bucketStoreReady signals when bucket store is ready.
	bucketStoreReady := make(chan struct{})
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			level.Info(logger).Log("msg", "initializing bucket store")
			begin := time.Now()
			if err := bs.InitialSync(ctx); err != nil {
				close(bucketStoreReady)
				return errors.Wrap(err, "bucket store initial sync")
			}
			level.Info(logger).Log("msg", "bucket store ready", "init_duration", time.Since(begin).String())
			close(bucketStoreReady)

			err := runutil.Repeat(syncInterval, ctx.Done(), func() error {
				if err := bs.SyncBlocks(ctx); err != nil {
					level.Warn(logger).Log("msg", "syncing blocks failed", "err", err)
				}
				return nil
			})

			runutil.CloseWithLogOnErr(logger, bs, "bucket store")
			return err
		}, func(error) {
			cancel()
		})
	}
	// Start query (proxy) gRPC StoreAPI.
	{
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, component, grpcProbe,
			grpcserver.WithServer(store.RegisterStoreServer(bs)),
			grpcserver.WithListen(grpcBindAddr),
			grpcserver.WithGracePeriod(grpcGracePeriod),
			grpcserver.WithTLSConfig(tlsCfg),
		)

		g.Add(func() error {
			<-bucketStoreReady
			statusProber.Ready()
			return s.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			s.Shutdown(err)
		})
	}
	// Add bucket UI for loaded blocks.
	{
		r := route.New()
		ins := extpromhttp.NewInstrumentationMiddleware(reg)

		compactorView := ui.NewBucketUI(logger, "", externalPrefix, prefixHeader, "/loaded", component)
		compactorView.Register(r, true, ins)

		// Configure Request Logging for HTTP calls.
		opts := []logging.Option{logging.WithDecider(func() logging.Decision {
			return logging.NoLogCall
		})}
		logMiddleware := logging.NewHTTPServerMiddleware(logger, opts...)
		api := blocksAPI.NewBlocksAPI(logger, "", flagsMap, configFilesMap)
		api.Register(r.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

		metaFetcher.UpdateOnChange(func(blocks []metadata.Meta, err error) {
			compactorView.Set(blocks, err)
			api.SetLoaded(blocks, err)
		})
		srv.Handle("/", r)
	}

	level.Info(logger).Log("msg", "starting store node")
	return nil
}
