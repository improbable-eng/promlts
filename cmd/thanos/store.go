// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/alecthomas/units"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpclogging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"

	commonmodel "github.com/prometheus/common/model"

	extflag "github.com/efficientgo/tools/extkingpin"
	blocksAPI "github.com/thanos-io/thanos/pkg/api/blocks"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	hidden "github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/ui"
)

type storeConfig struct {
	indexCacheConfigs           extflag.PathOrContent
	objStoreConfig              extflag.PathOrContent
	dataDir                     string
	grpcConfig                  grpcConfig
	httpConfig                  httpConfig
	indexCacheSizeBytes         units.Base2Bytes
	chunkPoolSize               units.Base2Bytes
	maxSampleCount              uint64
	maxTouchedSeriesCount       uint64
	maxConcurrency              int
	component                   component.StoreAPI
	debugLogging                bool
	syncInterval                time.Duration
	blockSyncConcurrency        int
	blockMetaFetchConcurrency   int
	filterConf                  *store.FilterConfig
	selectorRelabelConf         extflag.PathOrContent
	advertiseCompatibilityLabel bool
	consistencyDelay            commonmodel.Duration
	ignoreDeletionMarksDelay    commonmodel.Duration
	webConfig                   webConfig
	postingOffsetsInMemSampling int
	cachingBucketConfig         extflag.PathOrContent
	reqLogConfig                *extflag.PathOrContent
	lazyIndexReaderEnabled      bool
	lazyIndexReaderIdleTimeout  time.Duration
}

func (sc *storeConfig) registerFlag(cmd extkingpin.FlagClause) {
	sc.httpConfig = *sc.httpConfig.registerFlag(cmd)
	sc.grpcConfig = *sc.grpcConfig.registerFlag(cmd)

	cmd.Flag("data-dir", "Local data directory used for caching purposes (index-header, in-mem cache items and meta.jsons). If removed, no data will be lost, just store will have to rebuild the cache. NOTE: Putting raw blocks here will not cause the store to read them. For such use cases use Prometheus + sidecar.").
		Default("./data").StringVar(&sc.dataDir)

	cmd.Flag("index-cache-size", "Maximum size of items held in the in-memory index cache. Ignored if --index-cache.config or --index-cache.config-file option is specified.").
		Default("250MB").BytesVar(&sc.indexCacheSizeBytes)

	sc.indexCacheConfigs = *extflag.RegisterPathOrContent(cmd, "index-cache.config",
		"YAML file that contains index cache configuration. See format details: https://thanos.io/tip/components/store.md/#index-cache",
		extflag.WithEnvSubstitution(),
	)

	sc.cachingBucketConfig = *extflag.RegisterPathOrContent(hidden.HiddenCmdClause(cmd), "store.caching-bucket.config",
		"YAML that contains configuration for caching bucket. Experimental feature, with high risk of changes. See format details: https://thanos.io/tip/components/store.md/#caching-bucket",
		extflag.WithEnvSubstitution(),
	)

	cmd.Flag("chunk-pool-size", "Maximum size of concurrently allocatable bytes reserved strictly to reuse for chunks in memory.").
		Default("2GB").BytesVar(&sc.chunkPoolSize)

	cmd.Flag("store.grpc.series-sample-limit",
		"Maximum amount of samples returned via a single Series call. The Series call fails if this limit is exceeded. 0 means no limit. NOTE: For efficiency the limit is internally implemented as 'chunks limit' considering each chunk contains 120 samples (it's the max number of samples each chunk can contain), so the actual number of samples might be lower, even though the maximum could be hit.").
		Default("0").Uint64Var(&sc.maxSampleCount)

	cmd.Flag("store.grpc.touched-series-limit",
		"Maximum amount of touched series returned via a single Series call. The Series call fails if this limit is exceeded. 0 means no limit.").
		Default("0").Uint64Var(&sc.maxTouchedSeriesCount)

	cmd.Flag("store.grpc.series-max-concurrency", "Maximum number of concurrent Series calls.").Default("20").IntVar(&sc.maxConcurrency)

	sc.component = component.Store

	sc.objStoreConfig = *extkingpin.RegisterCommonObjStoreFlags(cmd, "", true)

	cmd.Flag("sync-block-duration", "Repeat interval for syncing the blocks between local and remote view.").
		Default("3m").DurationVar(&sc.syncInterval)

	cmd.Flag("block-sync-concurrency", "Number of goroutines to use when constructing index-cache.json blocks from object storage.").
		Default("20").IntVar(&sc.blockSyncConcurrency)

	cmd.Flag("block-meta-fetch-concurrency", "Number of goroutines to use when fetching block metadata from object storage.").
		Default("32").IntVar(&sc.blockMetaFetchConcurrency)

	sc.filterConf = &store.FilterConfig{}

	cmd.Flag("min-time", "Start of time range limit to serve. Thanos Store will serve only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z").SetValue(&sc.filterConf.MinTime)

	cmd.Flag("max-time", "End of time range limit to serve. Thanos Store will serve only blocks, which happened earlier than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z").SetValue(&sc.filterConf.MaxTime)

	cmd.Flag("debug.advertise-compatibility-label", "If true, Store Gateway in addition to other labels, will advertise special \"@thanos_compatibility_store_type=store\" label set. This makes store Gateway compatible with Querier before 0.8.0").
		Hidden().Default("true").BoolVar(&sc.advertiseCompatibilityLabel)

	sc.selectorRelabelConf = *extkingpin.RegisterSelectorRelabelFlags(cmd)

	cmd.Flag("store.index-header-posting-offsets-in-mem-sampling", "Controls what is the ratio of postings offsets store will hold in memory. "+
		"Larger value will keep less offsets, which will increase CPU cycles needed for query touching those postings. It's meant for setups that want low baseline memory pressure and where less traffic is expected. "+
		"On the contrary, smaller value will increase baseline memory usage, but improve latency slightly. 1 will keep all in memory. Default value is the same as in Prometheus which gives a good balance.").
		Hidden().Default(fmt.Sprintf("%v", store.DefaultPostingOffsetInMemorySampling)).IntVar(&sc.postingOffsetsInMemSampling)

	cmd.Flag("consistency-delay", "Minimum age of all blocks before they are being read. Set it to safe value (e.g 30m) if your object storage is eventually consistent. GCS and S3 are (roughly) strongly consistent.").
		Default("0s").SetValue(&sc.consistencyDelay)

	cmd.Flag("ignore-deletion-marks-delay", "Duration after which the blocks marked for deletion will be filtered out while fetching blocks. "+
		"The idea of ignore-deletion-marks-delay is to ignore blocks that are marked for deletion with some delay. This ensures store can still serve blocks that are meant to be deleted but do not have a replacement yet. "+
		"If delete-delay duration is provided to compactor or bucket verify component, it will upload deletion-mark.json file to mark after what duration the block should be deleted rather than deleting the block straight away. "+
		"If delete-delay is non-zero for compactor or bucket verify component, ignore-deletion-marks-delay should be set to (delete-delay)/2 so that blocks marked for deletion are filtered out while fetching blocks before being deleted from bucket. "+
		"Default is 24h, half of the default value for --delete-delay on compactor.").
		Default("24h").SetValue(&sc.ignoreDeletionMarksDelay)

	cmd.Flag("store.enable-index-header-lazy-reader", "If true, Store Gateway will lazy memory map index-header only once the block is required by a query.").
		Default("false").BoolVar(&sc.lazyIndexReaderEnabled)

	cmd.Flag("store.index-header-lazy-reader-idle-timeout", "If index-header lazy reader is enabled and this idle timeout setting is > 0, memory map-ed index-headers will be automatically released after 'idle timeout' inactivity.").
		Hidden().Default("5m").DurationVar(&sc.lazyIndexReaderIdleTimeout)

	cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the bucket web UI interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos bucket web UI to be served behind a reverse proxy that strips a URL sub-path.").
		Default("").StringVar(&sc.webConfig.externalPrefix)

	cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").
		Default("").StringVar(&sc.webConfig.prefixHeaderName)

	cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").
		Default("false").BoolVar(&sc.webConfig.disableCORS)

	sc.reqLogConfig = extkingpin.RegisterRequestLoggingFlags(cmd)
}

// registerStore registers a store command.
func registerStore(app *extkingpin.App) {
	cmd := app.Command(component.Store.String(), "Store node giving access to blocks in a bucket provider. Now supported GCS, S3, Azure, Swift, Tencent COS and Aliyun OSS.")

	conf := &storeConfig{}
	conf.registerFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, debugLogging bool) error {
		if conf.filterConf.MinTime.PrometheusTimestamp() > conf.filterConf.MaxTime.PrometheusTimestamp() {
			return errors.Errorf("invalid argument: --min-time '%s' can't be greater than --max-time '%s'",
				conf.filterConf.MinTime, conf.filterConf.MaxTime)
		}

		httpLogOpts, err := logging.ParseHTTPOptions("", conf.reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions("", conf.reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		return runStore(g,
			logger,
			reg,
			tracer,
			httpLogOpts,
			grpcLogOpts,
			tagOpts,
			*conf,
			getFlagsMap(cmd.Flags()),
		)
	})
}

// runStore starts a daemon that serves queries to cluster peers using data from an object store.
func runStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	httpLogOpts []logging.Option,
	grpcLogOpts []grpclogging.Option,
	tagOpts []tags.Option,
	conf storeConfig,
	flagsMap map[string]string,
) error {
	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(conf.component, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	srv := httpserver.New(logger, reg, conf.component, httpProbe,
		httpserver.WithListen(conf.httpConfig.bindAddress),
		httpserver.WithGracePeriod(time.Duration(conf.httpConfig.gracePeriod)),
		httpserver.WithTLSConfig(conf.httpConfig.tlsConfig),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	confContentYaml, err := conf.objStoreConfig.Content()
	if err != nil {
		return err
	}

	bkt, err := client.NewBucket(logger, confContentYaml, reg, conf.component.String())
	if err != nil {
		return errors.Wrap(err, "create bucket client")
	}

	cachingBucketConfigYaml, err := conf.cachingBucketConfig.Content()
	if err != nil {
		return errors.Wrap(err, "get caching bucket configuration")
	}
	if len(cachingBucketConfigYaml) > 0 {
		bkt, err = storecache.NewCachingBucketFromYaml(cachingBucketConfigYaml, bkt, logger, reg)
		if err != nil {
			return errors.Wrap(err, "create caching bucket")
		}
	}

	relabelContentYaml, err := conf.selectorRelabelConf.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}

	relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
	if err != nil {
		return err
	}

	indexCacheContentYaml, err := conf.indexCacheConfigs.Content()
	if err != nil {
		return errors.Wrap(err, "get content of index cache configuration")
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
			MaxSize:     model.Bytes(conf.indexCacheSizeBytes),
			MaxItemSize: storecache.DefaultInMemoryIndexCacheConfig.MaxItemSize,
		})
	}
	if err != nil {
		return errors.Wrap(err, "create index cache")
	}

	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, time.Duration(conf.ignoreDeletionMarksDelay), conf.blockMetaFetchConcurrency)
	metaFetcher, err := block.NewMetaFetcher(logger, conf.blockMetaFetchConcurrency, bkt, conf.dataDir, extprom.WrapRegistererWithPrefix("thanos_", reg),
		[]block.MetadataFilter{
			block.NewTimePartitionMetaFilter(conf.filterConf.MinTime, conf.filterConf.MaxTime),
			block.NewLabelShardedMetaFilter(relabelConfig),
			block.NewConsistencyDelayMetaFilter(logger, time.Duration(conf.consistencyDelay), extprom.WrapRegistererWithPrefix("thanos_", reg)),
			ignoreDeletionMarkFilter,
			block.NewDeduplicateFilter(),
		}, nil)
	if err != nil {
		return errors.Wrap(err, "meta fetcher")
	}

	// Limit the concurrency on queries against the Thanos store.
	if conf.maxConcurrency < 0 {
		return errors.Errorf("max concurrency value cannot be lower than 0 (got %v)", conf.maxConcurrency)
	}

	queriesGate := gate.New(extprom.WrapRegistererWithPrefix("thanos_bucket_store_series_", reg), int(conf.maxConcurrency))

	chunkPool, err := store.NewDefaultChunkBytesPool(uint64(conf.chunkPoolSize))
	if err != nil {
		return errors.Wrap(err, "create chunk pool")
	}

	options := []store.BucketStoreOption{
		store.WithLogger(logger),
		store.WithRegistry(reg),
		store.WithIndexCache(indexCache),
		store.WithQueryGate(queriesGate),
		store.WithChunkPool(chunkPool),
		store.WithFilterConfig(conf.filterConf),
	}

	if conf.debugLogging {
		options = append(options, store.WithDebugLogging())
	}

	bs, err := store.NewBucketStore(
		bkt,
		metaFetcher,
		conf.dataDir,
		store.NewChunksLimiterFactory(conf.maxSampleCount/store.MaxSamplesPerChunk), // The samples limit is an approximation based on the max number of samples per chunk.
		store.NewSeriesLimiterFactory(conf.maxTouchedSeriesCount),
		store.NewGapBasedPartitioner(store.PartitionerMaxGapSize),
		conf.blockSyncConcurrency,
		conf.advertiseCompatibilityLabel,
		conf.postingOffsetsInMemSampling,
		false,
		conf.lazyIndexReaderEnabled,
		conf.lazyIndexReaderIdleTimeout,
		options...,
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

			err := runutil.Repeat(conf.syncInterval, ctx.Done(), func() error {
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

	infoSrv := info.NewInfoServer(
		component.Store.String(),
		func() []labelpb.ZLabelSet {
			return bs.LabelSet()
		},
		func() *infopb.StoreInfo {
			mint, maxt := bs.TimeRange()
			return &infopb.StoreInfo{
				MinTime: mint,
				MaxTime: maxt,
			}
		},
		nil,
		nil,
		nil,
		nil,
	)

	// Start query (proxy) gRPC StoreAPI.
	{
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), conf.grpcConfig.tlsSrvCert, conf.grpcConfig.tlsSrvKey, conf.grpcConfig.tlsSrvClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, grpcLogOpts, tagOpts, conf.component, grpcProbe,
			grpcserver.WithServer(store.RegisterStoreServer(bs)),
			grpcserver.WithServer(info.RegisterInfoServer(infoSrv)),
			grpcserver.WithListen(conf.grpcConfig.bindAddress),
			grpcserver.WithGracePeriod(time.Duration(conf.grpcConfig.gracePeriod)),
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
		ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)

		compactorView := ui.NewBucketUI(logger, "", conf.webConfig.externalPrefix, conf.webConfig.prefixHeaderName, "/loaded", conf.component)
		compactorView.Register(r, true, ins)

		// Configure Request Logging for HTTP calls.
		logMiddleware := logging.NewHTTPServerMiddleware(logger, httpLogOpts...)
		api := blocksAPI.NewBlocksAPI(logger, conf.webConfig.disableCORS, "", flagsMap)
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
