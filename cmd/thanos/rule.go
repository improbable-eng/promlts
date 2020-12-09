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
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
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
	"github.com/thanos-io/thanos/pkg/ui/config"
)

// registerRule registers a rule command.
func registerRule(app *extkingpin.App) {
	comp := component.Rule
	cmd := app.Command(comp.String(), "ruler evaluating Prometheus rules against given Query nodes, exposing Store API and storing old blocks in bucket")

	httpBindAddr, httpGracePeriod := extkingpin.RegisterHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := extkingpin.RegisterGRPCFlags(cmd)

	labelStrs := cmd.Flag("label", "Labels to be applied to all generated metrics (repeated). Similar to external labels for Prometheus, used to identify ruler and its blocks as unique source.").
		PlaceHolder("<name>=\"<value>\"").Strings()

	dataDir := cmd.Flag("data-dir", "data directory").Default("data/").String()

	ruleFiles := cmd.Flag("rule-file", "Rule files that should be used by rule manager. Can be in glob format (repeated).").
		Default("rules/").Strings()
	resendDelay := extkingpin.ModelDuration(cmd.Flag("resend-delay", "Minimum amount of time to wait before resending an alert to Alertmanager.").
		Default("1m"))
	evalInterval := extkingpin.ModelDuration(cmd.Flag("eval-interval", "The default evaluation interval to use.").
		Default("30s"))
	tsdbBlockDuration := extkingpin.ModelDuration(cmd.Flag("tsdb.block-duration", "Block duration for TSDB block.").
		Default("2h"))
	tsdbRetention := extkingpin.ModelDuration(cmd.Flag("tsdb.retention", "Block retention time on local disk.").
		Default("48h"))
	noLockFile := cmd.Flag("tsdb.no-lockfile", "Do not create lockfile in TSDB data directory. In any case, the lockfiles will be deleted on next startup.").Default("false").Bool()
	walCompression := cmd.Flag("tsdb.wal-compression", "Compress the tsdb WAL.").Default("true").Bool()

	alertmgrs := cmd.Flag("alertmanagers.url", "Alertmanager replica URLs to push firing alerts. Ruler claims success if push to at least one alertmanager from discovered succeeds. The scheme should not be empty e.g `http` might be used. The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect Alertmanager IPs through respective DNS lookups. The port defaults to 9093 or the SRV record's value. The URL path is used as a prefix for the regular Alertmanager API path.").
		Strings()
	alertmgrsTimeout := cmd.Flag("alertmanagers.send-timeout", "Timeout for sending alerts to Alertmanager").Default("10s").Duration()
	alertmgrsConfig := extflag.RegisterPathOrContent(cmd, "alertmanagers.config", "YAML file that contains alerting configuration. See format details: https://thanos.io/tip/components/rule.md/#configuration. If defined, it takes precedence over the '--alertmanagers.url' and '--alertmanagers.send-timeout' flags.", false)
	alertmgrsDNSSDInterval := extkingpin.ModelDuration(cmd.Flag("alertmanagers.sd-dns-interval", "Interval between DNS resolutions of Alertmanager hosts.").
		Default("30s"))

	alertQueryURL := cmd.Flag("alert.query-url", "The external Thanos Query URL that would be set in all alerts 'Source' field").String()

	alertExcludeLabels := cmd.Flag("alert.label-drop", "Labels by name to drop before sending to alertmanager. This allows alert to be deduplicated on replica label (repeated). Similar Prometheus alert relabelling").
		Strings()
	webRoutePrefix := cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. This option is analogous to --web.route-prefix of Prometheus.").Default("").String()
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the UI query web interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()

	requestLoggingDecision := cmd.Flag("log.request.decision", "Request Logging for logging the start and end of requests. LogFinishCall is enabled by default. LogFinishCall : Logs the finish call of the requests. LogStartAndFinishCall : Logs the start and finish call of the requests. NoLogCall : Disable request logging.").Default("LogFinishCall").Enum("NoLogCall", "LogFinishCall", "LogStartAndFinishCall")

	objStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	queries := cmd.Flag("query", "Addresses of statically configured query API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect query API servers through respective DNS lookups.").
		PlaceHolder("<query>").Strings()

	queryConfig := extflag.RegisterPathOrContent(cmd, "query.config", "YAML file that contains query API servers configuration. See format details: https://thanos.io/tip/components/rule.md/#configuration. If defined, it takes precedence over the '--query' and '--query.sd-files' flags.", false)

	fileSDFiles := cmd.Flag("query.sd-files", "Path to file that contains addresses of query API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").Strings()

	fileSDInterval := extkingpin.ModelDuration(cmd.Flag("query.sd-interval", "Refresh interval to re-read file SD files. (used as a fallback)").
		Default("5m"))

	dnsSDInterval := extkingpin.ModelDuration(cmd.Flag("query.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s"))

	httpMethod := cmd.Flag("query.http-method", "HTTP method to use when sending queries. Possible options: [GET, POST]").
		Default("POST").Enum("GET", "POST")

	dnsSDResolver := cmd.Flag("query.sd-dns-resolver", "Resolver to use. Possible options: [golang, miekgdns]").
		Default("golang").Hidden().String()

	allowOutOfOrderUpload := cmd.Flag("shipper.allow-out-of-order-uploads",
		"If true, shipper will skip failed block uploads in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().Bool()

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, reload <-chan struct{}, _ bool) error {
		lset, err := parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}
		alertQueryURL, err := url.Parse(*alertQueryURL)
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
		for _, q := range *queries {
			if _, ok := lookupQueries[q]; ok {
				return errors.Errorf("Address %s is duplicated for --query flag.", q)
			}

			lookupQueries[q] = struct{}{}
		}

		queryConfigYAML, err := queryConfig.Content()
		if err != nil {
			return err
		}
		if len(*fileSDFiles) == 0 && len(*queries) == 0 && len(queryConfigYAML) == 0 {
			return errors.New("no --query parameter was given")
		}
		if (len(*fileSDFiles) != 0 || len(*queries) != 0) && len(queryConfigYAML) != 0 {
			return errors.New("--query/--query.sd-files and --query.config* parameters cannot be defined at the same time")
		}

		// Parse and check alerting configuration.
		alertmgrsConfigYAML, err := alertmgrsConfig.Content()
		if err != nil {
			return err
		}
		if len(alertmgrsConfigYAML) != 0 && len(*alertmgrs) != 0 {
			return errors.New("--alertmanagers.url and --alertmanagers.config* parameters cannot be defined at the same time")
		}

		return runRule(g,
			logger,
			reg,
			tracer,
			*requestLoggingDecision,
			reload,
			lset,
			*alertmgrs,
			*alertmgrsTimeout,
			alertmgrsConfigYAML,
			time.Duration(*alertmgrsDNSSDInterval),
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			*webRoutePrefix,
			*webExternalPrefix,
			*webPrefixHeaderName,
			time.Duration(*resendDelay),
			time.Duration(*evalInterval),
			*dataDir,
			*ruleFiles,
			objStoreConfig,
			tsdbOpts,
			alertQueryURL,
			*alertExcludeLabels,
			*queries,
			*fileSDFiles,
			time.Duration(*fileSDInterval),
			queryConfigYAML,
			time.Duration(*dnsSDInterval),
			*dnsSDResolver,
			comp,
			*allowOutOfOrderUpload,
			*httpMethod,
			getFlagsMap(cmd.Flags()),
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
	requestLoggingDecision string,
	reloadSignal <-chan struct{},
	lset labels.Labels,
	alertmgrURLs []string,
	alertmgrsTimeout time.Duration,
	alertmgrsConfigYAML []byte,
	alertmgrsDNSSDInterval time.Duration,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	webRoutePrefix string,
	webExternalPrefix string,
	webPrefixHeaderName string,
	resendDelay time.Duration,
	evalInterval time.Duration,
	dataDir string,
	ruleFiles []string,
	objStoreConfig *extflag.PathOrContent,
	tsdbOpts *tsdb.Options,
	alertQueryURL *url.URL,
	alertExcludeLabels []string,
	queryAddrs []string,
	querySDFiles []string,
	querySDInterval time.Duration,
	queryConfigYAML []byte,
	dnsSDInterval time.Duration,
	dnsSDResolver string,
	comp component.Component,
	allowOutOfOrderUpload bool,
	httpMethod string,
	flagsMap map[string]string,
) error {
	metrics := newRuleMetrics(reg)

	var queryCfg []query.Config
	var err error
	if len(queryConfigYAML) > 0 {
		queryCfg, err = query.LoadConfigs(queryConfigYAML)
		if err != nil {
			return err
		}
	} else {
		queryCfg, err = query.BuildQueryConfig(queryAddrs)
		if err != nil {
			return err
		}

		// Build the query configuration from the legacy query flags.
		var fileSDConfigs []http_util.FileSDConfig
		if len(querySDFiles) > 0 {
			fileSDConfigs = append(fileSDConfigs, http_util.FileSDConfig{
				Files:           querySDFiles,
				RefreshInterval: model.Duration(querySDInterval),
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
		dns.ResolverType(dnsSDResolver),
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
		addDiscoveryGroups(g, queryClient, dnsSDInterval)
	}

	db, err := tsdb.Open(dataDir, log.With(logger, "component", "tsdb"), reg, tsdbOpts)
	if err != nil {
		return errors.Wrap(err, "open TSDB")
	}

	level.Debug(logger).Log("msg", "removing storage lock file if any")
	if err := removeLockfileIfAny(logger, dataDir); err != nil {
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

	// Build the Alertmanager clients.
	var alertingCfg alert.AlertingConfig
	if len(alertmgrsConfigYAML) > 0 {
		alertingCfg, err = alert.LoadAlertingConfig(alertmgrsConfigYAML)
		if err != nil {
			return err
		}
	} else {
		// Build the Alertmanager configuration from the legacy flags.
		for _, addr := range alertmgrURLs {
			cfg, err := alert.BuildAlertmanagerConfig(addr, alertmgrsTimeout)
			if err != nil {
				return err
			}
			alertingCfg.Alertmanagers = append(alertingCfg.Alertmanagers, cfg)
		}
	}

	if len(alertingCfg.Alertmanagers) == 0 {
		level.Warn(logger).Log("msg", "no alertmanager configured")
	}

	amProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_rule_alertmanagers_", reg),
		dns.ResolverType(dnsSDResolver),
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
		addDiscoveryGroups(g, amClient, alertmgrsDNSSDInterval)

		alertmgrs = append(alertmgrs, alert.NewAlertmanager(logger, amClient, time.Duration(cfg.Timeout), cfg.APIVersion))
	}

	var (
		ruleMgr *thanosrules.Manager
		alertQ  = alert.NewQueue(logger, reg, 10000, 100, labelsTSDBToProm(lset), alertExcludeLabels)
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
					GeneratorURL: alertQueryURL.String() + strutil.TableLinkForExpression(expr),
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
			dataDir,
			rules.ManagerOptions{
				NotifyFunc:  notifyFunc,
				Logger:      logger,
				Appendable:  db,
				ExternalURL: nil,
				Queryable:   db,
				ResendDelay: resendDelay,
			},
			queryFuncCreator(logger, queryClients, metrics.duplicatedQuery, metrics.ruleEvalWarnings, httpMethod),
			lset,
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
			if err := reloadRules(logger, ruleFiles, ruleMgr, evalInterval, metrics); err != nil {
				level.Error(logger).Log("msg", "initialize rules failed", "err", err)
				return err
			}
			for {
				select {
				case <-reloadSignal:
					if err := reloadRules(logger, ruleFiles, ruleMgr, evalInterval, metrics); err != nil {
						level.Error(logger).Log("msg", "reload rules by sighup failed", "err", err)
					}
				case reloadMsg := <-reloadWebhandler:
					err := reloadRules(logger, ruleFiles, ruleMgr, evalInterval, metrics)
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
		tsdbStore := store.NewTSDBStore(logger, reg, db, component.Rule, lset)

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		// TODO: Add rules API implementation when ready.
		s := grpcserver.New(logger, reg, tracer, comp, grpcProbe,
			grpcserver.WithServer(store.RegisterStoreServer(tsdbStore)),
			grpcserver.WithServer(thanosrules.RegisterRulesServer(ruleMgr)),
			grpcserver.WithListen(grpcBindAddr),
			grpcserver.WithGracePeriod(grpcGracePeriod),
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

	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	confContentYamlStr, err := config.ReplaceSecret(confContentYaml)
	if err != nil {
		return err
	}

	configFilesMap := map[string]string{
		"Query Config":          string(queryConfigYAML),
		"Alert Managers Config": string(alertmgrsConfigYAML),
		"Object Storage Config": string(confContentYamlStr),
	}

	// Start UI & metrics HTTP server.
	{
		router := route.New()

		// RoutePrefix must always start with '/'.
		webRoutePrefix = "/" + strings.Trim(webRoutePrefix, "/")

		// Redirect from / to /webRoutePrefix.
		if webRoutePrefix != "/" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, webRoutePrefix, http.StatusFound)
			})
			router = router.WithPrefix(webRoutePrefix)
		}

		router.Post("/-/reload", func(w http.ResponseWriter, r *http.Request) {
			reloadMsg := make(chan error)
			reloadWebhandler <- reloadMsg
			if err := <-reloadMsg; err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		})

		ins := extpromhttp.NewInstrumentationMiddleware(reg)

		// Configure Request Logging for HTTP calls.
		opts := []logging.Option{logging.WithDecider(func() logging.Decision {
			return logging.LogDecision[requestLoggingDecision]
		})}
		logMiddleware := logging.NewHTTPServerMiddleware(logger, opts...)

		// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
		ui.NewRuleUI(logger, reg, ruleMgr, alertQueryURL.String(), webExternalPrefix, webPrefixHeaderName).Register(router, ins)

		api := v1.NewRuleAPI(logger, reg, thanosrules.NewGRPCClient(ruleMgr), ruleMgr, flagsMap, configFilesMap)
		api.Register(router.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(httpBindAddr),
			httpserver.WithGracePeriod(httpGracePeriod),
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

		s := shipper.New(logger, reg, dataDir, bkt, func() labels.Labels { return lset }, metadata.RulerSource, false, allowOutOfOrderUpload)

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
			level.Warn(logger).Log("msg", "duplicate query address is provided - %v", u.String())
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
