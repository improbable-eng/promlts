// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/exthttp"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	thanoshttp "github.com/thanos-io/thanos/pkg/http"
	"github.com/thanos-io/thanos/pkg/logging"
	meta "github.com/thanos-io/thanos/pkg/metadata"
	thanosmodel "github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/reloader"
	"github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/targets"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"
)

func registerSidecar(app *extkingpin.App) {
	cmd := app.Command(component.Sidecar.String(), "Sidecar for Prometheus server.")
	conf := &sidecarConfig{}
	conf.registerFlag(cmd)
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions("", conf.reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		rl := reloader.New(log.With(logger, "component", "reloader"),
			extprom.WrapRegistererWithPrefix("thanos_sidecar_", reg),
			&reloader.Options{
				ReloadURL:     reloader.ReloadURLFromBase(conf.prometheus.url),
				CfgFile:       conf.reloader.confFile,
				CfgOutputFile: conf.reloader.envVarConfFile,
				WatchedDirs:   conf.reloader.ruleDirectories,
				WatchInterval: conf.reloader.watchInterval,
				RetryInterval: conf.reloader.retryInterval,
			})

		return runSidecar(g, logger, reg, tracer, rl, component.Sidecar, *conf, grpcLogOpts, tagOpts)
	})
}

func runSidecar(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	reloader *reloader.Reloader,
	comp component.Component,
	conf sidecarConfig,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
) error {
	var m = &promMetadata{
		promURL: conf.prometheus.url,

		// Start out with the full time range. The shipper will constrain it later.
		// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
		mint: conf.limitMinTime.PrometheusTimestamp(),
		maxt: math.MaxInt64,

		limitMinTime: conf.limitMinTime,
		client:       promclient.NewWithTracingClient(logger, "thanos-sidecar"),
	}

	confContentYaml, err := conf.objStore.Content()
	if err != nil {
		return errors.Wrap(err, "getting object store config")
	}
	confPathYaml, err := conf.objStore.GetPath()
	var uploads = true
	if len(confContentYaml) == 0 {
		level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
		uploads = false
	}

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	srv := httpserver.New(logger, reg, comp, httpProbe,
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

	// Setup all the concurrent groups.
	{
		promUp := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_prometheus_up",
			Help: "Boolean indicator whether the sidecar can reach its Prometheus peer.",
		})
		lastHeartbeat := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_last_heartbeat_success_time_seconds",
			Help: "Timestamp of the last successful heartbeat in seconds.",
		})

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// Only check Prometheus's flags when upload is enabled.
			if uploads {
				// Check prometheus's flags to ensure sane sidecar flags.
				if err := validatePrometheus(ctx, m.client, logger, conf.shipper.ignoreBlockSize, m); err != nil {
					return errors.Wrap(err, "validate Prometheus flags")
				}
			}

			// Blocking query of external labels before joining as a Source Peer into gossip.
			// We retry infinitely until we reach and fetch labels from our Prometheus.
			err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
				if err := m.UpdateLabels(ctx); err != nil {
					level.Warn(logger).Log(
						"msg", "failed to fetch initial external labels. Is Prometheus running? Retrying",
						"err", err,
					)
					promUp.Set(0)
					statusProber.NotReady(err)
					return err
				}

				level.Info(logger).Log(
					"msg", "successfully loaded prometheus external labels",
					"external_labels", m.Labels().String(),
				)
				promUp.Set(1)
				statusProber.Ready()
				lastHeartbeat.SetToCurrentTime()
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			if len(m.Labels()) == 0 {
				return errors.New("no external labels configured on Prometheus server, uniquely identifying external labels must be configured; see https://thanos.io/tip/thanos/storage.md#external-labels for details.")
			}

			// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
			// the external labels we apply.
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer iterCancel()

				if err := m.UpdateLabels(iterCtx); err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					promUp.Set(1)
					lastHeartbeat.SetToCurrentTime()
				}

				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return reloader.Watch(ctx)
		}, func(error) {
			cancel()
		})
	}

	{
		t := exthttp.NewTransport()
		t.MaxIdleConnsPerHost = conf.connection.maxIdleConnsPerHost
		t.MaxIdleConns = conf.connection.maxIdleConns
		c := promclient.NewClient(&http.Client{Transport: tracing.HTTPTripperware(logger, t)}, logger, thanoshttp.ThanosUserAgent)

		promStore, err := store.NewPrometheusStore(logger, reg, c, conf.prometheus.url, component.Sidecar, m.Labels, m.Timestamps)
		if err != nil {
			return errors.Wrap(err, "create Prometheus store")
		}

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"),
			conf.grpc.tlsSrvCert, conf.grpc.tlsSrvKey, conf.grpc.tlsSrvClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, grpcLogOpts, tagOpts, comp, grpcProbe,
			grpcserver.WithServer(store.RegisterStoreServer(promStore)),
			grpcserver.WithServer(rules.RegisterRulesServer(rules.NewPrometheus(conf.prometheus.url, c, m.Labels))),
			grpcserver.WithServer(targets.RegisterTargetsServer(targets.NewPrometheus(conf.prometheus.url, c, m.Labels))),
			grpcserver.WithServer(meta.RegisterMetadataServer(meta.NewPrometheus(conf.prometheus.url, c))),
			grpcserver.WithServer(exemplars.RegisterExemplarsServer(exemplars.NewPrometheus(conf.prometheus.url, c, m.Labels))),
			grpcserver.WithListen(conf.grpc.bindAddress),
			grpcserver.WithGracePeriod(time.Duration(conf.grpc.gracePeriod)),
			grpcserver.WithTLSConfig(tlsCfg),
		)
		g.Add(func() error {
			statusProber.Ready()
			return s.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			s.Shutdown(err)
		})
	}

	if uploads {
		// The background shipper continuously scans the data directory and uploads
		// new blocks to Google Cloud Storage or an S3-compatible storage service.
		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Sidecar.String(), confPathYaml...)
		if err != nil {
			return err
		}

		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			}
		}()

		if err := promclient.IsWALDirAccessible(conf.tsdb.path); err != nil {
			level.Error(logger).Log("err", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			promReadyTimeout := conf.prometheus.readyTimeout
			extLabelsCtx, cancel := context.WithTimeout(ctx, promReadyTimeout)
			defer cancel()

			if err := runutil.Retry(2*time.Second, extLabelsCtx.Done(), func() error {
				if len(m.Labels()) == 0 {
					return errors.New("not uploading as no external labels are configured yet - is Prometheus healthy/reachable?")
				}
				return nil
			}); err != nil {
				return errors.Wrapf(err, "aborting as no external labels found after waiting %s", promReadyTimeout)
			}

			s := shipper.New(logger, reg, conf.tsdb.path, bkt, m.Labels, metadata.SidecarSource,
				conf.shipper.uploadCompacted, conf.shipper.allowOutOfOrderUpload, metadata.HashFunc(conf.shipper.hashFunc))

			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if uploaded, err := s.Sync(ctx); err != nil {
					level.Warn(logger).Log("err", err, "uploaded", uploaded)
				}

				minTime, _, err := s.Timestamps()
				if err != nil {
					level.Warn(logger).Log("msg", "reading timestamps failed", "err", err)
					return nil
				}
				m.UpdateTimestamps(minTime, math.MaxInt64)
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	level.Info(logger).Log("msg", "starting sidecar")
	return nil
}

func validatePrometheus(ctx context.Context, client *promclient.Client, logger log.Logger, ignoreBlockSize bool, m *promMetadata) error {
	var (
		flagErr error
		flags   promclient.Flags
	)

	if err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
		if flags, flagErr = client.ConfiguredFlags(ctx, m.promURL); flagErr != nil && flagErr != promclient.ErrFlagEndpointNotFound {
			level.Warn(logger).Log("msg", "failed to get Prometheus flags. Is Prometheus running? Retrying", "err", flagErr)
			return errors.Wrapf(flagErr, "fetch Prometheus flags")
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "fetch Prometheus flags")
	}

	if flagErr != nil {
		level.Warn(logger).Log("msg", "failed to check Prometheus flags, due to potentially older Prometheus. No extra validation is done.", "err", flagErr)
		return nil
	}

	// Check if compaction is disabled.
	if flags.TSDBMinTime != flags.TSDBMaxTime {
		if !ignoreBlockSize {
			return errors.Errorf("found that TSDB Max time is %s and Min time is %s. "+
				"Compaction needs to be disabled (storage.tsdb.min-block-duration = storage.tsdb.max-block-duration)", flags.TSDBMaxTime, flags.TSDBMinTime)
		}
		level.Warn(logger).Log("msg", "flag to ignore Prometheus min/max block duration flags differing is being used. If the upload of a 2h block fails and a Prometheus compaction happens that block may be missing from your Thanos bucket storage.")
	}
	// Check if block time is 2h.
	if flags.TSDBMinTime != model.Duration(2*time.Hour) {
		level.Warn(logger).Log("msg", "found that TSDB block time is not 2h. Only 2h block time is recommended.", "block-time", flags.TSDBMinTime)
	}

	return nil
}

type promMetadata struct {
	promURL *url.URL

	mtx    sync.Mutex
	mint   int64
	maxt   int64
	labels labels.Labels

	limitMinTime thanosmodel.TimeOrDurationValue

	client *promclient.Client
}

func (s *promMetadata) UpdateLabels(ctx context.Context) error {
	elset, err := s.client.ExternalLabels(ctx, s.promURL)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.labels = elset
	return nil
}

func (s *promMetadata) UpdateTimestamps(mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if mint < s.limitMinTime.PrometheusTimestamp() {
		mint = s.limitMinTime.PrometheusTimestamp()
	}

	s.mint = mint
	s.maxt = maxt
}

func (s *promMetadata) Labels() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.labels
}

func (s *promMetadata) Timestamps() (mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.mint, s.maxt
}

type sidecarConfig struct {
	http         httpConfig
	grpc         grpcConfig
	prometheus   prometheusConfig
	connection   connConfig
	tsdb         tsdbConfig
	reloader     reloaderConfig
	reqLogConfig *extflag.PathOrContent
	objStore     extflag.PathOrContent
	shipper      shipperConfig
	limitMinTime thanosmodel.TimeOrDurationValue
}

func (sc *sidecarConfig) registerFlag(cmd extkingpin.FlagClause) {
	sc.http.registerFlag(cmd)
	sc.grpc.registerFlag(cmd)
	sc.prometheus.registerFlag(cmd)
	sc.connection.registerFlag(cmd)
	sc.tsdb.registerFlag(cmd)
	sc.reloader.registerFlag(cmd)
	sc.reqLogConfig = extkingpin.RegisterRequestLoggingFlags(cmd)
	sc.objStore = *extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)
	sc.shipper.registerFlag(cmd)
	cmd.Flag("min-time", "Start of time range limit to serve. Thanos sidecar will serve only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z").SetValue(&sc.limitMinTime)
}
