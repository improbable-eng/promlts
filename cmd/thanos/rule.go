// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/rules/remotewrite"

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
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/strutil"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extkingpin"

	"github.com/thanos-io/thanos/pkg/alert"
	v1 "github.com/thanos-io/thanos/pkg/api/rule"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	http_util "github.com/thanos-io/thanos/pkg/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/query"
	thanosrules "github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/thanos-io/thanos/pkg/ui"
)

type ruleConfig struct {
	http    httpConfig
	grpc    grpcConfig
	web     webConfig
	shipper shipperConfig

	query           queryConfig
	queryConfigYAML []byte

	alertmgr               alertMgrConfig
	alertmgrsConfigYAML    []byte
	alertQueryURL          *url.URL
	alertRelabelConfigYAML []byte

	rwConfig     ruleRWConfig
	rwConfigYAML []byte

	resendDelay    time.Duration
	evalInterval   time.Duration
	ruleFiles      []string
	objStoreConfig *extflag.PathOrContent
	dataDir        string
	reloadSignal   <-chan struct{}
	lset           labels.Labels
}

func (rc *ruleConfig) registerFlag(cmd extkingpin.FlagClause) {
	rc.http.registerFlag(cmd)
	rc.grpc.registerFlag(cmd)
	rc.web.registerFlag(cmd)
	rc.shipper.registerFlag(cmd)
	rc.query.registerFlag(cmd)
	rc.alertmgr.registerFlag(cmd)
	rc.rwConfig.registerFlag(cmd)
}

// registerRule registers a rule command.
func registerRule(app *extkingpin.App) {
	comp := component.Rule
	cmd := app.Command(comp.String(), "Ruler evaluating Prometheus rules against given Query nodes, exposing Store API and storing old blocks in bucket.")

	conf := &ruleConfig{}
	conf.registerFlag(cmd)

	labelStrs := cmd.Flag("label", "Labels to be applied to all generated metrics (repeated). Similar to external labels for Prometheus, used to identify ruler and its blocks as unique source.").
		PlaceHolder("<name>=\"<value>\"").Strings()
	tsdbBlockDuration := extkingpin.ModelDuration(cmd.Flag("tsdb.block-duration", "Block duration for TSDB block.").
		Default("2h"))
	tsdbRetention := extkingpin.ModelDuration(cmd.Flag("tsdb.retention", "Block retention time on local disk.").
		Default("48h"))
	noLockFile := cmd.Flag("tsdb.no-lockfile", "Do not create lockfile in TSDB data directory. In any case, the lockfiles will be deleted on next startup.").Default("false").Bool()
	walCompression := cmd.Flag("tsdb.wal-compression", "Compress the tsdb WAL.").Default("true").Bool()

	cmd.Flag("data-dir", "data directory").Default("data/").StringVar(&conf.dataDir)
	cmd.Flag("rule-file", "Rule files that should be used by rule manager. Can be in glob format (repeated).").
		Default("rules/").StringsVar(&conf.ruleFiles)
	cmd.Flag("resend-delay", "Minimum amount of time to wait before resending an alert to Alertmanager.").
		Default("1m").DurationVar(&conf.resendDelay)
	cmd.Flag("eval-interval", "The default evaluation interval to use.").
		Default("30s").DurationVar(&conf.evalInterval)

	reqLogDecision := cmd.Flag("log.request.decision", "Deprecation Warning - This flag would be soon deprecated, and replaced with `request.logging-config`. Request Logging for logging the start and end of requests. By default this flag is disabled. LogFinishCall: Logs the finish call of the requests. LogStartAndFinishCall: Logs the start and finish call of the requests. NoLogCall: Disable request logging.").Default("").Enum("NoLogCall", "LogFinishCall", "LogStartAndFinishCall", "")

	conf.objStoreConfig = extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	var err error
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, reload <-chan struct{}, _ bool) error {
		conf.lset, err = parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		conf.alertQueryURL, err = url.Parse(*conf.alertmgr.alertQueryURL)
		if err != nil {
			return errors.Wrap(err, "parse alert query url")
		}

		tsdbOpts := &tsdb.Options{
			MinBlockDuration:  int64(time.Duration(*tsdbBlockDuration) / time.Millisecond),
			MaxBlockDuration:  int64(time.Duration(*tsdbBlockDuration) / time.Millisecond),
			RetentionDuration: int64(time.Duration(*tsdbRetention) / time.Millisecond),
			NoLockfile:        *noLockFile,
			WALCompression:    *walCompression,
		}

		// Parse and check query configuration.
		lookupQueries := map[string]struct{}{}
		for _, q := range conf.query.addrs {
			if _, ok := lookupQueries[q]; ok {
				return errors.Errorf("Address %s is duplicated for --query flag.", q)
			}

			lookupQueries[q] = struct{}{}
		}

		conf.queryConfigYAML, err = conf.query.configPath.Content()
		if err != nil {
			return err
		}
		if len(conf.query.sdFiles) == 0 && len(conf.query.addrs) == 0 && len(conf.queryConfigYAML) == 0 {
			return errors.New("no --query parameter was given")
		}
		if (len(conf.query.sdFiles) != 0 || len(conf.query.addrs) != 0) && len(conf.queryConfigYAML) != 0 {
			return errors.New("--query/--query.sd-files and --query.config* parameters cannot be defined at the same time")
		}

		// Parse and check remote-write config and enable stateless mode for ruler.
		if conf.rwConfig.configPath != nil {
			conf.rwConfigYAML, err = conf.rwConfig.configPath.Content()
			if err != nil {
				return err
			}
		}

		// Parse and check alerting configuration.
		conf.alertmgrsConfigYAML, err = conf.alertmgr.configPath.Content()
		if err != nil {
			return err
		}
		if len(conf.alertmgrsConfigYAML) != 0 && len(conf.alertmgr.alertmgrURLs) != 0 {
			return errors.New("--alertmanagers.url and --alertmanagers.config* parameters cannot be defined at the same time")
		}

		conf.alertRelabelConfigYAML, err = conf.alertmgr.alertRelabelConfigPath.Content()
		if err != nil {
			return err
		}

		httpLogOpts, err := logging.ParseHTTPOptions(*reqLogDecision, reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions(*reqLogDecision, reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		return runRule(g,
			logger,
			reg,
			tracer,
			comp,
			*conf,
			getFlagsMap(cmd.Flags()),
			httpLogOpts,
			grpcLogOpts,
			tagOpts,
			tsdbOpts,
		)
	})
}

// RuleMetrics defines Thanos Ruler metrics.
type RuleMetrics struct {
	configSuccess     prometheus.Gauge
	configSuccessTime prometheus.Gauge
	duplicatedQuery   prometheus.Counter
	rulesLoaded       *prometheus.GaugeVec
	ruleEvalWarnings  *prometheus.CounterVec
}

func newRuleMetrics(reg *prometheus.Registry) *RuleMetrics {
	m := new(RuleMetrics)

	factory := promauto.With(reg)
	m.configSuccess = factory.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_rule_config_last_reload_successful",
		Help: "Whether the last configuration reload attempt was successful.",
	})
	m.configSuccessTime = factory.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_rule_config_last_reload_success_timestamp_seconds",
		Help: "Timestamp of the last successful configuration reload.",
	})
	m.duplicatedQuery = factory.NewCounter(prometheus.CounterOpts{
		Name: "thanos_rule_duplicated_query_addresses_total",
		Help: "The number of times a duplicated query addresses is detected from the different configs in rule.",
	})
	m.rulesLoaded = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "thanos_rule_loaded_rules",
			Help: "Loaded rules partitioned by file and group.",
		},
		[]string{"strategy", "file", "group"},
	)
	m.ruleEvalWarnings = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "thanos_rule_evaluation_with_warnings_total",
			Help: "The total number of rule evaluation that were successful but had warnings which can indicate partial error.",
		}, []string{"strategy"},
	)
	m.ruleEvalWarnings.WithLabelValues(strings.ToLower(storepb.PartialResponseStrategy_ABORT.String()))
	m.ruleEvalWarnings.WithLabelValues(strings.ToLower(storepb.PartialResponseStrategy_WARN.String()))

	return m
}

// runRule runs a rule evaluation component that continuously evaluates alerting and recording
// rules. It sends alert notifications and writes TSDB data for results like a regular Prometheus server.
func runRule(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	comp component.Component,
	conf ruleConfig,
	flagsMap map[string]string,
	httpLogOpts []logging.Option,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
	tsdbOpts *tsdb.Options,
) error {
	metrics := newRuleMetrics(reg)

	var queryCfg []query.Config
	var err error
	if len(conf.queryConfigYAML) > 0 {
		queryCfg, err = query.LoadConfigs(conf.queryConfigYAML)
		if err != nil {
			return err
		}
	} else {
		queryCfg, err = query.BuildQueryConfig(conf.query.addrs)
		if err != nil {
			return err
		}

		// Build the query configuration from the legacy query flags.
		var fileSDConfigs []http_util.FileSDConfig
		if len(conf.query.sdFiles) > 0 {
			fileSDConfigs = append(fileSDConfigs, http_util.FileSDConfig{
				Files:           conf.query.sdFiles,
				RefreshInterval: model.Duration(conf.query.sdInterval),
			})
			queryCfg = append(queryCfg,
				query.Config{
					EndpointsConfig: http_util.EndpointsConfig{
						Scheme:        "http",
						FileSDConfigs: fileSDConfigs,
					},
				},
			)
		}
	}

	queryProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_rule_query_apis_", reg),
		dns.ResolverType(conf.query.dnsSDResolver),
	)
	var queryClients []*http_util.Client
	for _, cfg := range queryCfg {
		c, err := http_util.NewHTTPClient(cfg.HTTPClientConfig, "query")
		if err != nil {
			return err
		}
		c.Transport = tracing.HTTPTripperware(logger, c.Transport)
		queryClient, err := http_util.NewClient(logger, cfg.EndpointsConfig, c, queryProvider.Clone())
		if err != nil {
			return err
		}
		queryClients = append(queryClients, queryClient)
		// Discover and resolve query addresses.
		addDiscoveryGroups(g, queryClient, conf.query.dnsSDInterval)
	}
	var (
		appendable storage.Appendable
		queryable  storage.Queryable
		db         *tsdb.DB
	)

	if conf.rwConfig.configPath != nil {
		conf.rwConfigYAML, err = conf.rwConfig.configPath.Content()
		if err != nil {
			return err
		}
		var rwCfg remotewrite.Config
		if len(conf.rwConfigYAML) == 0 {
			return errors.New("no --remote-write.config was given")
		}
		rwCfg, err = remotewrite.LoadRemoteWriteConfig(conf.rwConfigYAML)
		if err != nil {
			return err
		}
		walDir := filepath.Join(conf.dataDir, rwCfg.Name)
		remoteStore, err := remotewrite.NewFanoutStorage(logger, reg, walDir, rwCfg)
		if err != nil {
			return errors.Wrap(err, "set up remote-write store for ruler")
		}
		appendable = remoteStore
		queryable = remoteStore
	} else {
		db, err = tsdb.Open(conf.dataDir, log.With(logger, "component", "tsdb"), reg, tsdbOpts)
		if err != nil {
			return errors.Wrap(err, "open TSDB")
		}

		level.Debug(logger).Log("msg", "removing storage lock file if any")
		if err := removeLockfileIfAny(logger, conf.dataDir); err != nil {
			return errors.Wrap(err, "remove storage lock files")
		}

		{
			done := make(chan struct{})
			g.Add(func() error {
				<-done
				return db.Close()
			}, func(error) {
				close(done)
			})
		}
		appendable = db
		queryable = db
	}

	// Build the Alertmanager clients.
	var alertingCfg alert.AlertingConfig
	if len(conf.alertmgrsConfigYAML) > 0 {
		alertingCfg, err = alert.LoadAlertingConfig(conf.alertmgrsConfigYAML)
		if err != nil {
			return err
		}
	} else {
		// Build the Alertmanager configuration from the legacy flags.
		for _, addr := range conf.alertmgr.alertmgrURLs {
			cfg, err := alert.BuildAlertmanagerConfig(addr, conf.alertmgr.alertmgrsTimeout)
			if err != nil {
				return err
			}
			alertingCfg.Alertmanagers = append(alertingCfg.Alertmanagers, cfg)
		}
	}

	if len(alertingCfg.Alertmanagers) == 0 {
		level.Warn(logger).Log("msg", "no alertmanager configured")
	}

	var alertRelabelConfigs []*relabel.Config
	if len(conf.alertRelabelConfigYAML) > 0 {
		alertRelabelConfigs, err = alert.LoadRelabelConfigs(conf.alertRelabelConfigYAML)
		if err != nil {
			return err
		}
	}

	amProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_rule_alertmanagers_", reg),
		dns.ResolverType(conf.query.dnsSDResolver),
	)
	var alertmgrs []*alert.Alertmanager
	for _, cfg := range alertingCfg.Alertmanagers {
		c, err := http_util.NewHTTPClient(cfg.HTTPClientConfig, "alertmanager")
		if err != nil {
			return err
		}
		c.Transport = tracing.HTTPTripperware(logger, c.Transport)
		// Each Alertmanager client has a different list of targets thus each needs its own DNS provider.
		amClient, err := http_util.NewClient(logger, cfg.EndpointsConfig, c, amProvider.Clone())
		if err != nil {
			return err
		}
		// Discover and resolve Alertmanager addresses.
		addDiscoveryGroups(g, amClient, conf.alertmgr.alertmgrsDNSSDInterval)

		alertmgrs = append(alertmgrs, alert.NewAlertmanager(logger, amClient, time.Duration(cfg.Timeout), cfg.APIVersion))
	}

	var (
		ruleMgr *thanosrules.Manager
		alertQ  = alert.NewQueue(logger, reg, 10000, 100, labelsTSDBToProm(conf.lset), conf.alertmgr.alertExcludeLabels, alertRelabelConfigs)
	)
	{
		// Run rule evaluation and alert notifications.
		notifyFunc := func(ctx context.Context, expr string, alerts ...*rules.Alert) {
			res := make([]*alert.Alert, 0, len(alerts))
			for _, alrt := range alerts {
				// Only send actually firing alerts.
				if alrt.State == rules.StatePending {
					continue
				}
				a := &alert.Alert{
					StartsAt:     alrt.FiredAt,
					Labels:       alrt.Labels,
					Annotations:  alrt.Annotations,
					GeneratorURL: conf.alertQueryURL.String() + strutil.TableLinkForExpression(expr),
				}
				if !alrt.ResolvedAt.IsZero() {
					a.EndsAt = alrt.ResolvedAt
				} else {
					a.EndsAt = alrt.ValidUntil
				}
				res = append(res, a)
			}
			alertQ.Push(res)
		}

		ctx, cancel := context.WithCancel(context.Background())
		logger = log.With(logger, "component", "rules")
		ruleMgr = thanosrules.NewManager(
			tracing.ContextWithTracer(ctx, tracer),
			reg,
			conf.dataDir,
			rules.ManagerOptions{
				NotifyFunc:  notifyFunc,
				Logger:      logger,
				Appendable:  appendable,
				ExternalURL: nil,
				Queryable:   queryable,
				ResendDelay: conf.resendDelay,
			},
			queryFuncCreator(logger, queryClients, metrics.duplicatedQuery, metrics.ruleEvalWarnings, conf.query.httpMethod),
			conf.lset,
		)

		// Schedule rule manager that evaluates rules.
		g.Add(func() error {
			ruleMgr.Run()
			<-ctx.Done()

			return nil
		}, func(err error) {
			cancel()
			ruleMgr.Stop()
		})
	}
	// Run the alert sender.
	{
		sdr := alert.NewSender(logger, reg, alertmgrs)
		ctx, cancel := context.WithCancel(context.Background())
		ctx = tracing.ContextWithTracer(ctx, tracer)

		g.Add(func() error {
			for {
				tracing.DoInSpan(ctx, "/send_alerts", func(ctx context.Context) {
					sdr.Send(ctx, alertQ.Pop(ctx.Done()))
				})

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
		}, func(error) {
			cancel()
		})
	}

	// Handle reload and termination interrupts.
	reloadWebhandler := make(chan chan error)
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// Initialize rules.
			if err := reloadRules(logger, conf.ruleFiles, ruleMgr, conf.evalInterval, metrics); err != nil {
				level.Error(logger).Log("msg", "initialize rules failed", "err", err)
				return err
			}
			for {
				select {
				case <-conf.reloadSignal:
					if err := reloadRules(logger, conf.ruleFiles, ruleMgr, conf.evalInterval, metrics); err != nil {
						level.Error(logger).Log("msg", "reload rules by sighup failed", "err", err)
					}
				case reloadMsg := <-reloadWebhandler:
					err := reloadRules(logger, conf.ruleFiles, ruleMgr, conf.evalInterval, metrics)
					if err != nil {
						level.Error(logger).Log("msg", "reload rules by webhandler failed", "err", err)
					}
					reloadMsg <- err
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}, func(error) {
			cancel()
		})
	}

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Start gRPC server.
	{
		tsdbStore := store.NewTSDBStore(logger, db, component.Rule, conf.lset)

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), conf.grpc.tlsSrvCert, conf.grpc.tlsSrvKey, conf.grpc.tlsSrvClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		// TODO: Add rules API implementation when ready.
		s := grpcserver.New(logger, reg, tracer, grpcLogOpts, tagOpts, comp, grpcProbe,
			grpcserver.WithServer(store.RegisterStoreServer(tsdbStore)),
			grpcserver.WithServer(thanosrules.RegisterRulesServer(ruleMgr)),
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
	// Start UI & metrics HTTP server.
	{
		router := route.New()

		// RoutePrefix must always start with '/'.
		conf.web.routePrefix = "/" + strings.Trim(conf.web.routePrefix, "/")

		// Redirect from / to /webRoutePrefix.
		if conf.web.routePrefix != "/" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, conf.web.routePrefix, http.StatusFound)
			})
			router = router.WithPrefix(conf.web.routePrefix)
		}

		router.Post("/-/reload", func(w http.ResponseWriter, r *http.Request) {
			reloadMsg := make(chan error)
			reloadWebhandler <- reloadMsg
			if err := <-reloadMsg; err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		})

		ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)

		// Configure Request Logging for HTTP calls.
		logMiddleware := logging.NewHTTPServerMiddleware(logger, httpLogOpts...)

		// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
		ui.NewRuleUI(logger, reg, ruleMgr, conf.alertQueryURL.String(), conf.web.externalPrefix, conf.web.prefixHeaderName).Register(router, ins)

		api := v1.NewRuleAPI(logger, reg, thanosrules.NewGRPCClient(ruleMgr), ruleMgr, conf.web.disableCORS, flagsMap)
		api.Register(router.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(conf.http.bindAddress),
			httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
			httpserver.WithTLSConfig(conf.http.tlsConfig),
		)
		srv.Handle("/", router)

		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})
	}

	confContentYaml, err := conf.objStoreConfig.Content()
	if err != nil {
		return err
	}

	if len(confContentYaml) > 0 {
		// The background shipper continuously scans the data directory and uploads
		// new blocks to Google Cloud Storage or an S3-compatible storage service.
		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Rule.String())
		if err != nil {
			return err
		}

		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			}
		}()

		s := shipper.New(logger, reg, conf.dataDir, bkt, func() labels.Labels { return conf.lset }, metadata.RulerSource, false, conf.shipper.allowOutOfOrderUpload, metadata.HashFunc(conf.shipper.hashFunc))

		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if _, err := s.Sync(ctx); err != nil {
					level.Warn(logger).Log("err", err)
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	} else {
		level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
	}

	level.Info(logger).Log("msg", "starting rule node")
	return nil
}

func removeLockfileIfAny(logger log.Logger, dataDir string) error {
	absdir, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(absdir, "lock")); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	level.Info(logger).Log("msg", "a leftover lockfile found and removed")
	return nil
}

func parseFlagLabels(s []string) (labels.Labels, error) {
	var lset labels.Labels
	for _, l := range s {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return nil, errors.Errorf("unrecognized label %q", l)
		}
		if !model.LabelName.IsValid(model.LabelName(string(parts[0]))) {
			return nil, errors.Errorf("unsupported format for label %s", l)
		}
		val, err := strconv.Unquote(parts[1])
		if err != nil {
			return nil, errors.Wrap(err, "unquote label value")
		}
		lset = append(lset, labels.Label{Name: parts[0], Value: val})
	}
	sort.Sort(lset)
	return lset, nil
}

func labelsTSDBToProm(lset labels.Labels) (res labels.Labels) {
	for _, l := range lset {
		res = append(res, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res
}

func removeDuplicateQueryEndpoints(logger log.Logger, duplicatedQueriers prometheus.Counter, urls []*url.URL) []*url.URL {
	set := make(map[string]struct{})
	deduplicated := make([]*url.URL, 0, len(urls))
	for _, u := range urls {
		if _, ok := set[u.String()]; ok {
			level.Warn(logger).Log("msg", "duplicate query address is provided", "addr", u.String())
			duplicatedQueriers.Inc()
			continue
		}
		deduplicated = append(deduplicated, u)
		set[u.String()] = struct{}{}
	}
	return deduplicated
}

func queryFuncCreator(
	logger log.Logger,
	queriers []*http_util.Client,
	duplicatedQuery prometheus.Counter,
	ruleEvalWarnings *prometheus.CounterVec,
	httpMethod string,
) func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {

	// queryFunc returns query function that hits the HTTP query API of query peers in randomized order until we get a result
	// back or the context get canceled.
	return func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
		var spanID string

		switch partialResponseStrategy {
		case storepb.PartialResponseStrategy_WARN:
			spanID = "/rule_instant_query HTTP[client]"
		case storepb.PartialResponseStrategy_ABORT:
			spanID = "/rule_instant_query_part_resp_abort HTTP[client]"
		default:
			// Programming error will be caught by tests.
			panic(errors.Errorf("unknown partial response strategy %v", partialResponseStrategy).Error())
		}

		promClients := make([]*promclient.Client, 0, len(queriers))
		for _, q := range queriers {
			promClients = append(promClients, promclient.NewClient(q, logger, "thanos-rule"))
		}

		return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
			for _, i := range rand.Perm(len(queriers)) {
				promClient := promClients[i]
				endpoints := removeDuplicateQueryEndpoints(logger, duplicatedQuery, queriers[i].Endpoints())
				for _, i := range rand.Perm(len(endpoints)) {
					span, ctx := tracing.StartSpan(ctx, spanID)
					v, warns, err := promClient.PromqlQueryInstant(ctx, endpoints[i], q, t, promclient.QueryOptions{
						Deduplicate:             true,
						PartialResponseStrategy: partialResponseStrategy,
						Method:                  httpMethod,
					})
					span.Finish()

					if err != nil {
						level.Error(logger).Log("err", err, "query", q)
						continue
					}
					if len(warns) > 0 {
						ruleEvalWarnings.WithLabelValues(strings.ToLower(partialResponseStrategy.String())).Inc()
						// TODO(bwplotka): Propagate those to UI, probably requires changing rule manager code ):
						level.Warn(logger).Log("warnings", strings.Join(warns, ", "), "query", q)
					}
					return v, nil
				}
			}
			return nil, errors.Errorf("no query API server reachable")
		}
	}
}

func addDiscoveryGroups(g *run.Group, c *http_util.Client, interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		c.Discover(ctx)
		return nil
	}, func(error) {
		cancel()
	})

	g.Add(func() error {
		return runutil.Repeat(interval, ctx.Done(), func() error {
			return c.Resolve(ctx)
		})
	}, func(error) {
		cancel()
	})
}

func reloadRules(logger log.Logger,
	ruleFiles []string,
	ruleMgr *thanosrules.Manager,
	evalInterval time.Duration,
	metrics *RuleMetrics) error {
	level.Debug(logger).Log("msg", "configured rule files", "files", strings.Join(ruleFiles, ","))
	var (
		errs      errutil.MultiError
		files     []string
		seenFiles = make(map[string]struct{})
	)
	for _, pat := range ruleFiles {
		fs, err := filepath.Glob(pat)
		if err != nil {
			// The only error can be a bad pattern.
			errs.Add(errors.Wrapf(err, "retrieving rule files failed. Ignoring file. pattern %s", pat))
			continue
		}

		for _, fp := range fs {
			if _, ok := seenFiles[fp]; ok {
				continue
			}
			files = append(files, fp)
			seenFiles[fp] = struct{}{}
		}
	}

	level.Info(logger).Log("msg", "reload rule files", "numFiles", len(files))

	if err := ruleMgr.Update(evalInterval, files); err != nil {
		metrics.configSuccess.Set(0)
		errs.Add(errors.Wrap(err, "reloading rules failed"))
		return errs.Err()
	}

	metrics.configSuccess.Set(1)
	metrics.configSuccessTime.Set(float64(time.Now().UnixNano()) / 1e9)

	metrics.rulesLoaded.Reset()
	for _, group := range ruleMgr.RuleGroups() {
		metrics.rulesLoaded.WithLabelValues(group.PartialResponseStrategy.String(), group.OriginalFile, group.Name()).Set(float64(len(group.Rules())))
	}
	return errs.Err()
}
