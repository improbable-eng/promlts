package main

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tls"
	"gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

// registerStore registers a store command.
func registerStore(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command(component.Store.String(), "store node giving access to blocks in a bucket provider. Now supported GCS, S3, Azure, Swift and Tencent COS.")

	httpBindAddr, httpGracePeriod := regHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := regGRPCFlags(cmd)

	dataDir := cmd.Flag("data-dir", "Data directory in which to cache remote blocks.").
		Default("./data").String()

	indexCacheSize := cmd.Flag("index-cache-size", "Maximum size of items held in the in-memory index cache. Ignored if --index-cache.config option is specified.").
		Default("250MB").Bytes()

	indexCacheConfig := extflag.RegisterPathOrContent(cmd, "index-cache.config",
		"YAML file that contains index cache configuration. See format details: https://thanos.io/caching.md/",
		false)

	chunkPoolSize := cmd.Flag("chunk-pool-size", "Maximum size of concurrently allocatable bytes for chunks.").
		Default("2GB").Bytes()

	maxSampleCount := cmd.Flag("store.grpc.series-sample-limit",
		"Maximum amount of samples returned via a single Series call. 0 means no limit. NOTE: for efficiency we take 120 as the number of samples in chunk (it cannot be bigger than that), so the actual number of samples might be lower, even though the maximum could be hit.").
		Default("0").Uint()

	maxConcurrent := cmd.Flag("store.grpc.series-max-concurrency", "Maximum number of concurrent Series calls.").Default("20").Int()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", true)

	syncInterval := cmd.Flag("sync-block-duration", "Repeat interval for syncing the blocks between local and remote view.").
		Default("3m").Duration()

	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing blocks from object storage.").
		Default("20").Int()

	minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to serve. Thanos Store will serve only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))

	maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to serve. Thanos Store will serve only blocks, which happened eariler than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z"))

	advertiseCompatibilityLabel := cmd.Flag("debug.advertise-compatibility-label", "If true, Store Gateway in addition to other labels, will advertise special \"@thanos_compatibility_store_type=store\" label set. This makes store Gateway compatible with Querier before 0.8.0").
		Hidden().Default("true").Bool()

	selectorRelabelConf := regSelectorRelabelFlags(cmd)

	m[component.Store.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, debugLogging bool) error {
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
			int(*maxConcurrent),
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
		)
	}
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
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	indexCacheSizeBytes uint64,
	chunkPoolSizeBytes uint64,
	maxSampleCount uint64,
	maxConcurrent int,
	component component.Component,
	verbose bool,
	syncInterval time.Duration,
	blockSyncConcurrency int,
	filterConf *store.FilterConfig,
	selectorRelabelConf *extflag.PathOrContent,
	advertiseCompatibilityLabel bool,
) error {
	// Initiate HTTP listener providing metrics endpoint and readiness/liveness probes.
	statusProber := prober.New(component, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	srv := httpserver.New(logger, reg, component, statusProber,
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

	bkt, err := client.NewBucket(logger, confContentYaml, reg, component.String())
	if err != nil {
		return errors.Wrap(err, "create bucket client")
	}

	relabelContentYaml, err := selectorRelabelConf.Content()
	if err != nil {
		return errors.Wrap(err, "get content of relabel configuration")
	}

	relabelConfig, err := parseRelabelConfig(relabelContentYaml)
	if err != nil {
		return err
	}

	indexCacheContentYaml, err := indexCacheConfig.Content()
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
			MaxSizeBytes:     indexCacheSizeBytes,
			MaxItemSizeBytes: storecache.DefaultInMemoryIndexCacheConfig.MaxItemSizeBytes,
		})
	}
	if err != nil {
		return errors.Wrap(err, "create index cache")
	}

	bs, err := store.NewBucketStore(
		logger,
		reg,
		bkt,
		dataDir,
		indexCache,
		chunkPoolSizeBytes,
		maxSampleCount,
		maxConcurrent,
		verbose,
		blockSyncConcurrency,
		filterConf,
		relabelConfig,
		advertiseCompatibilityLabel,
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

		s := grpcserver.New(logger, reg, tracer, component, bs,
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

	level.Info(logger).Log("msg", "starting store node")
	return nil
}

func parseRelabelConfig(contentYaml []byte) ([]*relabel.Config, error) {
	var relabelConfig []*relabel.Config
	if err := yaml.Unmarshal(contentYaml, &relabelConfig); err != nil {
		return nil, errors.Wrap(err, "parsing relabel configuration")
	}

	return relabelConfig, nil
}
