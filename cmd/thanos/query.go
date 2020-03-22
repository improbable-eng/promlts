// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/cache"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/query"
	v1 "github.com/thanos-io/thanos/pkg/query/api"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/ui"
)

// registerQuery registers a query command.
func registerQuery(m map[string]setupFunc, app *kingpin.Application) {
	comp := component.Query
	cmd := app.Command(comp.String(), "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	httpBindAddr, httpGracePeriod := regHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := regGRPCFlags(cmd)

	secure := cmd.Flag("grpc-client-tls-secure", "Use TLS when talking to the gRPC server").Default("false").Bool()
	cert := cmd.Flag("grpc-client-tls-cert", "TLS Certificates to use to identify this client to the server").Default("").String()
	key := cmd.Flag("grpc-client-tls-key", "TLS Key for the client's certificate").Default("").String()
	caCert := cmd.Flag("grpc-client-tls-ca", "TLS CA Certificates to use to verify gRPC servers").Default("").String()
	serverName := cmd.Flag("grpc-client-server-name", "Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").String()

	webRoutePrefix := cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. This option is analogous to --web.route-prefix of Promethus.").Default("").String()
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the UI query web interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()

	queryTimeout := modelDuration(cmd.Flag("query.timeout", "Maximum time to process query by query node.").
		Default("2m"))

	maxConcurrentQueries := cmd.Flag("query.max-concurrent", "Maximum number of queries processed concurrently by query node.").
		Default("20").Int()

	replicaLabels := cmd.Flag("query.replica-label", "Labels to treat as a replica indicator along which data is deduplicated. Still you will be able to query without deduplication using 'dedup=false' parameter.").
		Strings()

	instantDefaultMaxSourceResolution := modelDuration(cmd.Flag("query.instant.default.max_source_resolution", "default value for max_source_resolution for instant queries. If not set, defaults to 0s only taking raw resolution into account. 1h can be a good value if you use instant queries over time ranges that incorporate times outside of your raw-retention.").Default("0s").Hidden())

	selectorLabels := cmd.Flag("selector-label", "Query selector labels that will be exposed in info endpoint (repeated).").
		PlaceHolder("<name>=\"<value>\"").Strings()

	stores := cmd.Flag("store", "Addresses of statically configured store API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect store API servers through respective DNS lookups.").
		PlaceHolder("<store>").Strings()

	storeConfig := extflag.RegisterPathOrContent(cmd, "store.config", "YAML file that contains store API servers configuration. See format details: https://thanos.io/components/query.md/#configuration. If defined, it takes precedence over the '--store' and '--store.sd-files' flags.", false)

	fileSDFiles := cmd.Flag("store.sd-files", "Path to files that contain addresses of store API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").Strings()

	fileSDInterval := modelDuration(cmd.Flag("store.sd-interval", "Refresh interval to re-read file SD files. It is used as a resync fallback.").
		Default("5m"))

	// TODO(bwplotka): Grab this from TTL at some point.
	dnsSDInterval := modelDuration(cmd.Flag("store.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s"))

	dnsSDResolver := cmd.Flag("store.sd-dns-resolver", fmt.Sprintf("Resolver to use. Possible options: [%s, %s]", dns.GolangResolverType, dns.MiekgdnsResolverType)).
		Default(string(dns.GolangResolverType)).Hidden().String()

	unhealthyStoreTimeout := modelDuration(cmd.Flag("store.unhealthy-timeout", "Timeout before an unhealthy store is cleaned from the store UI page.").Default("5m"))

	enableAutodownsampling := cmd.Flag("query.auto-downsampling", "Enable automatic adjustment (step / 5) to what source of data should be used in store gateways if no max_source_resolution param is specified.").
		Default("false").Bool()

	enablePartialResponse := cmd.Flag("query.partial-response", "Enable partial response for queries if no partial_response param is specified. --no-query.partial-response for disabling.").
		Default("true").Bool()

	defaultEvaluationInterval := modelDuration(cmd.Flag("query.default-evaluation-interval", "Set default evaluation interval for sub queries.").Default("1m"))

	storeResponseTimeout := modelDuration(cmd.Flag("store.response-timeout", "If a Store doesn't send any data in this specified duration then a Store will be ignored and partial data will be returned if it's enabled. 0 disables timeout.").Default("0ms"))

	m[comp.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		selectorLset, err := parseFlagLabels(*selectorLabels)
		if err != nil {
			return errors.Wrap(err, "parse federation labels")
		}

		lookupStores := map[string]struct{}{}
		for _, s := range *stores {
			if _, ok := lookupStores[s]; ok {
				return errors.Errorf("Address %s is duplicated for --store flag.", s)
			}

			lookupStores[s] = struct{}{}
		}

		storeConfigYAML, err := storeConfig.Content()
		if err != nil {
			return err
		}
		if len(*fileSDFiles) == 0 && len(*stores) == 0 && len(storeConfigYAML) == 0 {
			return errors.New("no --store parameter was given")
		}
		if (len(*fileSDFiles) != 0 || len(*stores) != 0) && len(storeConfigYAML) != 0 {
			return errors.New("--store/--store.sd-files and --store.config* parameters cannot be defined at the same time")
		}

		var fileSDConfig *file.SDConfig
		if len(*fileSDFiles) > 0 {
			fileSDConfig = &file.SDConfig{
				Files:           *fileSDFiles,
				RefreshInterval: *fileSDInterval,
			}
		}

		promql.SetDefaultEvaluationInterval(time.Duration(*defaultEvaluationInterval))

		return runQuery(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*secure,
			*cert,
			*key,
			*caCert,
			*serverName,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			*webRoutePrefix,
			*webExternalPrefix,
			*webPrefixHeaderName,
			*maxConcurrentQueries,
			time.Duration(*queryTimeout),
			time.Duration(*storeResponseTimeout),
			*replicaLabels,
			selectorLset,
			*stores,
			*enableAutodownsampling,
			*enablePartialResponse,
			fileSDConfig,
			storeConfigYAML,
			time.Duration(*dnsSDInterval),
			*dnsSDResolver,
			time.Duration(*unhealthyStoreTimeout),
			time.Duration(*instantDefaultMaxSourceResolution),
			component.Query,
		)
	}
}

// runQuery starts a server that exposes PromQL Query API. It is responsible for querying configured
// store nodes, merging and duplicating the data to satisfy user query.
func runQuery(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	secure bool,
	cert string,
	key string,
	caCert string,
	serverName string,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	webRoutePrefix string,
	webExternalPrefix string,
	webPrefixHeaderName string,
	maxConcurrentQueries int,
	queryTimeout time.Duration,
	storeResponseTimeout time.Duration,
	replicaLabels []string,
	selectorLset labels.Labels,
	storeAddrs []string,
	enableAutodownsampling bool,
	enablePartialResponse bool,
	fileSDConfig *file.SDConfig,
	storeConfigYAML []byte,
	dnsSDInterval time.Duration,
	dnsSDResolver string,
	unhealthyStoreTimeout time.Duration,
	instantDefaultMaxSourceResolution time.Duration,
	comp component.Component,
) error {
	// TODO(bplotka in PR #513 review): Move arguments into struct.
	duplicatedStores := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_query_duplicated_store_addresses_total",
		Help: "The number of times a duplicated store addresses is detected from the different configs in query",
	})

	var storesConfig []store.Config
	if len(storeConfigYAML) > 0 {
		var err error
		storesConfig, err = store.LoadConfig(storeConfigYAML)
		if err != nil {
			return errors.Wrap(err, "loading store config")
		}
	} else {
		storeConfig, err := store.NewConfig(storeAddrs, fileSDConfig, secure, cert, key, caCert, serverName)
		if err != nil {
			return errors.Wrap(err, "initialising stores config from individual flags")
		}
		storesConfig = []store.Config{storeConfig}
	}

	var storeSets []*query.StoreSet
	for _, config := range storesConfig {
		dialOpts, err := extgrpc.StoreClientGRPCOptsFromTlsConfig(logger, config.Name, reg, tracer, config.TlsConfig)
		if err != nil {
			return errors.Wrap(err, "building gRPC client")
		}

		fileSDCache := cache.New()
		dnsProvider := dns.NewProvider(
			logger,
			extprom.WrapRegistererWith(
				map[string]string{"config_name": config.Name},
				extprom.WrapRegistererWithPrefix("thanos_querier_store_apis_", reg),
			),
			dns.ResolverType(dnsSDResolver),
		)

		stores := query.NewStoreSet(
			logger,
			config.Name,
			reg,
			func() (specs []query.StoreSpec) {
				// Add DNS resolved addresses from static flags and file SD.
				for _, addr := range dnsProvider.Addresses() {
					specs = append(specs, query.NewGRPCStoreSpec(addr))
				}

				specs = removeDuplicateStoreSpecs(logger, duplicatedStores, specs)

				return specs
			},
			dialOpts,
			unhealthyStoreTimeout,
		)
		storeSets = append(storeSets, stores)

		// Periodically update the store set with the addresses we see in our cluster.
		{
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				return runutil.Repeat(5*time.Second, ctx.Done(), func() error {
					stores.Update(ctx)
					return nil
				})
			}, func(error) {
				cancel()
				stores.Close()
			})
		}
		// Run File Service Discovery and update the store set when the files are modified.
		if config.EndpointsConfig.FileSDConfigs != nil && len(config.EndpointsConfig.FileSDConfigs) > 0 {
			fileSDUpdates := make(chan []*targetgroup.Group)

			for _, fsdConfig := range config.EndpointsConfig.FileSDConfigs {
				ctxRun, cancelRun := context.WithCancel(context.Background())
				fileSD := file.NewDiscovery(&fsdConfig, logger)
				g.Add(func() error {
					fileSD.Run(ctxRun, fileSDUpdates)
					return nil
				}, func(error) {
					cancelRun()
				})
			}

			ctxUpdate, cancelUpdate := context.WithCancel(context.Background())
			staticAddresses := config.EndpointsConfig.StaticAddresses
			g.Add(func() error {
				for {
					select {
					case update := <-fileSDUpdates:
						// Discoverers sometimes send nil updates so need to check for it to avoid panics.
						if update == nil {
							continue
						}
						fileSDCache.Update(update)
						stores.Update(ctxUpdate)
						dnsProvider.Resolve(ctxUpdate, append(fileSDCache.Addresses(), staticAddresses...))
					case <-ctxUpdate.Done():
						return nil
					}
				}
			}, func(error) {
				cancelUpdate()
				close(fileSDUpdates)
			})
		}
		// Periodically update the addresses from static flags and file SD by resolving them using DNS SD if necessary.
		{
			ctx, cancel := context.WithCancel(context.Background())
			staticAddresses := config.EndpointsConfig.StaticAddresses
			g.Add(func() error {
				return runutil.Repeat(dnsSDInterval, ctx.Done(), func() error {
					dnsProvider.Resolve(ctx, append(fileSDCache.Addresses(), staticAddresses...))
					return nil
				})
			}, func(error) {
				cancel()
			})
		}
	}

	var (
		allClients = func() []store.Client {
			var ret []store.Client
			for _, ss := range storeSets {
				ret = append(ret, ss.Get()...)
			}
			return ret
		}
		proxy            = store.NewProxyStore(logger, reg, allClients, component.Query, selectorLset, storeResponseTimeout)
		queryableCreator = query.NewQueryableCreator(logger, proxy)
		engine           = promql.NewEngine(
			promql.EngineOpts{
				Logger:        logger,
				Reg:           reg,
				MaxConcurrent: maxConcurrentQueries,
				// TODO(bwplotka): Expose this as a flag: https://github.com/thanos-io/thanos/issues/703.
				MaxSamples: math.MaxInt32,
				Timeout:    queryTimeout,
			},
		)
		grpcProbe    = prober.NewGRPC()
		httpProbe    = prober.NewHTTP()
		statusProber = prober.Combine(
			httpProbe,
			grpcProbe,
			prober.NewInstrumentation(comp, logger, reg),
		)
	)
	// Start query API + UI HTTP server.
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

		flagsMap := map[string]string{
			// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
			"web.external-prefix": webExternalPrefix,
			"web.prefix-header":   webPrefixHeaderName,
		}

		getAllStoreStatuses := func() []query.StoreStatus {
			var ret []query.StoreStatus
			for _, stores := range storeSets {
				ret = append(ret, stores.GetStoreStatus()...)
			}
			return ret
		}
		ins := extpromhttp.NewInstrumentationMiddleware(reg)
		ui.NewQueryUI(logger, reg, getAllStoreStatuses, flagsMap).Register(router, ins)

		api := v1.NewAPI(logger, reg, engine, queryableCreator, enableAutodownsampling, enablePartialResponse, replicaLabels, instantDefaultMaxSourceResolution)

		api.Register(router.WithPrefix(path.Join(webRoutePrefix, "/api/v1")), tracer, logger, ins)

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
	// Start query (proxy) gRPC StoreAPI.
	{
		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, comp, grpcProbe, proxy,
			grpcserver.WithListen(grpcBindAddr),
			grpcserver.WithGracePeriod(grpcGracePeriod),
			grpcserver.WithTLSConfig(tlsCfg),
		)

		g.Add(func() error {
			statusProber.Ready()
			return s.ListenAndServe()
		}, func(error) {
			statusProber.NotReady(err)
			s.Shutdown(err)
		})
	}

	level.Info(logger).Log("msg", "starting query node")
	return nil
}

func removeDuplicateStoreSpecs(logger log.Logger, duplicatedStores prometheus.Counter, specs []query.StoreSpec) []query.StoreSpec {
	set := make(map[string]query.StoreSpec)
	for _, spec := range specs {
		addr := spec.Addr()
		if _, ok := set[addr]; ok {
			level.Warn(logger).Log("msg", "Duplicate store address is provided - %v", addr)
			duplicatedStores.Inc()
		}
		set[addr] = spec
	}
	deduplicated := make([]query.StoreSpec, 0, len(set))
	for _, value := range set {
		deduplicated = append(deduplicated, value)
	}
	return deduplicated
}
