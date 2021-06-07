// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//nolint:unparam
// TODO(kakkoyun): Fix linter issues - The pattern we use makes linter unhappy (returning unused config pointers).
package main

import (
	"net/url"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"

	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/extkingpin"
)

type grpcConfig struct {
	bindAddress    string
	gracePeriod    model.Duration
	tlsSrvCert     string
	tlsSrvKey      string
	tlsSrvClientCA string
}

func (gc *grpcConfig) registerFlag(cmd extkingpin.FlagClause) *grpcConfig {
	cmd.Flag("grpc-address",
		"Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components.").
		Default("0.0.0.0:10901").StringVar(&gc.bindAddress)
	cmd.Flag("grpc-grace-period",
		"Time to wait after an interrupt received for GRPC Server.").
		Default("2m").SetValue(&gc.gracePeriod)
	cmd.Flag("grpc-server-tls-cert",
		"TLS Certificate for gRPC server, leave blank to disable TLS").
		Default("").StringVar(&gc.tlsSrvCert)
	cmd.Flag("grpc-server-tls-key",
		"TLS Key for the gRPC server, leave blank to disable TLS").
		Default("").StringVar(&gc.tlsSrvKey)
	cmd.Flag("grpc-server-tls-client-ca",
		"TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").
		Default("").StringVar(&gc.tlsSrvClientCA)
	return gc
}

type httpConfig struct {
	bindAddress string
	tlsConfig   string
	gracePeriod model.Duration
}

func (hc *httpConfig) registerFlag(cmd extkingpin.FlagClause) *httpConfig {
	cmd.Flag("http-address",
		"Listen host:port for HTTP endpoints.").
		Default("0.0.0.0:10902").StringVar(&hc.bindAddress)
	cmd.Flag("http-grace-period",
		"Time to wait after an interrupt received for HTTP Server.").
		Default("2m").SetValue(&hc.gracePeriod)
	cmd.Flag(
		"http.config",
		"[EXPERIMENTAL] Path to the configuration file that can enable TLS or authentication for all HTTP endpoints.",
	).Default("").StringVar(&hc.tlsConfig)
	return hc
}

type prometheusConfig struct {
	url          *url.URL
	readyTimeout time.Duration
}

func (pc *prometheusConfig) registerFlag(cmd extkingpin.FlagClause) *prometheusConfig {
	cmd.Flag("prometheus.url",
		"URL at which to reach Prometheus's API. For better performance use local network.").
		Default("http://localhost:9090").URLVar(&pc.url)
	cmd.Flag("prometheus.ready_timeout",
		"Maximum time to wait for the Prometheus instance to start up").
		Default("10m").DurationVar(&pc.readyTimeout)
	return pc
}

type connConfig struct {
	maxIdleConns        int
	maxIdleConnsPerHost int
}

func (cc *connConfig) registerFlag(cmd extkingpin.FlagClause) *connConfig {
	cmd.Flag("receive.connection-pool-size",
		"Controls the http MaxIdleConns. Default is 0, which is unlimited").
		IntVar(&cc.maxIdleConns)
	cmd.Flag("receive.connection-pool-size-per-host",
		"Controls the http MaxIdleConnsPerHost").
		Default("100").IntVar(&cc.maxIdleConnsPerHost)
	return cc
}

type tsdbConfig struct {
	path string
}

func (tc *tsdbConfig) registerFlag(cmd extkingpin.FlagClause) *tsdbConfig {
	cmd.Flag("tsdb.path", "Data directory of TSDB.").Default("./data").StringVar(&tc.path)
	return tc
}

type reloaderConfig struct {
	confFile        string
	envVarConfFile  string
	ruleDirectories []string
	watchInterval   time.Duration
	retryInterval   time.Duration
}

func (rc *reloaderConfig) registerFlag(cmd extkingpin.FlagClause) *reloaderConfig {
	cmd.Flag("reloader.config-file",
		"Config file watched by the reloader.").
		Default("").StringVar(&rc.confFile)
	cmd.Flag("reloader.config-envsubst-file",
		"Output file for environment variable substituted config file.").
		Default("").StringVar(&rc.envVarConfFile)
	cmd.Flag("reloader.rule-dir",
		"Rule directories for the reloader to refresh (repeated field).").
		StringsVar(&rc.ruleDirectories)
	cmd.Flag("reloader.watch-interval",
		"Controls how often reloader re-reads config and rules.").
		Default("3m").DurationVar(&rc.watchInterval)
	cmd.Flag("reloader.retry-interval",
		"Controls how often reloader retries config reload in case of error.").
		Default("5s").DurationVar(&rc.retryInterval)

	return rc
}

type shipperConfig struct {
	uploadCompacted       bool
	ignoreBlockSize       bool
	allowOutOfOrderUpload bool
	hashFunc              string
}

func (sc *shipperConfig) registerFlag(cmd extkingpin.FlagClause) *shipperConfig {
	cmd.Flag("shipper.upload-compacted",
		"If true shipper will try to upload compacted blocks as well. Useful for migration purposes. Works only if compaction is disabled on Prometheus. Do it once and then disable the flag when done.").
		Default("false").BoolVar(&sc.uploadCompacted)
	cmd.Flag("shipper.ignore-unequal-block-size",
		"If true shipper will not require prometheus min and max block size flags to be set to the same value. Only use this if you want to keep long retention and compaction enabled on your Prometheus instance, as in the worst case it can result in ~2h data loss for your Thanos bucket storage.").
		Default("false").Hidden().BoolVar(&sc.ignoreBlockSize)
	cmd.Flag("shipper.allow-out-of-order-uploads",
		"If true, shipper will skip failed block uploads in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().BoolVar(&sc.allowOutOfOrderUpload)
	cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").EnumVar(&sc.hashFunc, "SHA256", "")
	return sc
}

type webConfig struct {
	routePrefix      string
	externalPrefix   string
	prefixHeaderName string
	disableCORS      bool
}

func (wc *webConfig) registerFlag(cmd extkingpin.FlagClause) *webConfig {
	cmd.Flag("web.route-prefix",
		"Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. This option is analogous to --web.route-prefix of Prometheus.").
		Default("").StringVar(&wc.routePrefix)
	cmd.Flag("web.external-prefix",
		"Static prefix for all HTML links and redirect URLs in the bucket web UI interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos bucket web UI to be served behind a reverse proxy that strips a URL sub-path.").
		Default("").StringVar(&wc.externalPrefix)
	cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").
		Default("").StringVar(&wc.prefixHeaderName)
	cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").Default("false").BoolVar(&wc.disableCORS)
	return wc
}

type queryConfig struct {
	addrs         []string
	sdFiles       []string
	sdInterval    time.Duration
	configPath    *extflag.PathOrContent
	dnsSDInterval time.Duration
	httpMethod    string
	dnsSDResolver string
}

func (qc *queryConfig) registerFlag(cmd extkingpin.FlagClause) *queryConfig {
	cmd.Flag("query", "Addresses of statically configured query API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect query API servers through respective DNS lookups.").
		PlaceHolder("<query>").StringsVar(&qc.addrs)
	qc.configPath = extflag.RegisterPathOrContent(cmd, "query.config", "YAML file that contains query API servers configuration. See format details: https://thanos.io/tip/components/rule.md/#configuration. If defined, it takes precedence over the '--query' and '--query.sd-files' flags.", extflag.WithEnvSubstitution())
	cmd.Flag("query.sd-files", "Path to file that contains addresses of query API servers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").StringsVar(&qc.sdFiles)
	cmd.Flag("query.sd-interval", "Refresh interval to re-read file SD files. (used as a fallback)").
		Default("5m").DurationVar(&qc.sdInterval)
	cmd.Flag("query.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s").DurationVar(&qc.dnsSDInterval)
	cmd.Flag("query.http-method", "HTTP method to use when sending queries. Possible options: [GET, POST]").
		Default("POST").EnumVar(&qc.httpMethod, "GET", "POST")
	cmd.Flag("query.sd-dns-resolver", "Resolver to use. Possible options: [golang, miekgdns]").
		Default("golang").Hidden().StringVar(&qc.dnsSDResolver)
	return qc
}

type alertMgrConfig struct {
	configPath             *extflag.PathOrContent
	alertmgrURLs           []string
	alertmgrsTimeout       time.Duration
	alertmgrsDNSSDInterval time.Duration
	alertExcludeLabels     []string
	alertQueryURL          *string
	alertRelabelConfigPath *extflag.PathOrContent
}

func (ac *alertMgrConfig) registerFlag(cmd extflag.FlagClause) *alertMgrConfig {
	ac.configPath = extflag.RegisterPathOrContent(cmd, "alertmanagers.config", "YAML file that contains alerting configuration. See format details: https://thanos.io/tip/components/rule.md/#configuration. If defined, it takes precedence over the '--alertmanagers.url' and '--alertmanagers.send-timeout' flags.", extflag.WithEnvSubstitution())
	cmd.Flag("alertmanagers.url", "Alertmanager replica URLs to push firing alerts. Ruler claims success if push to at least one alertmanager from discovered succeeds. The scheme should not be empty e.g `http` might be used. The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect Alertmanager IPs through respective DNS lookups. The port defaults to 9093 or the SRV record's value. The URL path is used as a prefix for the regular Alertmanager API path.").
		StringsVar(&ac.alertmgrURLs)
	cmd.Flag("alertmanagers.send-timeout", "Timeout for sending alerts to Alertmanager").Default("10s").
		DurationVar(&ac.alertmgrsTimeout)
	cmd.Flag("alertmanagers.sd-dns-interval", "Interval between DNS resolutions of Alertmanager hosts.").
		Default("30s").DurationVar(&ac.alertmgrsDNSSDInterval)
	ac.alertQueryURL = cmd.Flag("alert.query-url", "The external Thanos Query URL that would be set in all alerts 'Source' field").String()
	cmd.Flag("alert.label-drop", "Labels by name to drop before sending to alertmanager. This allows alert to be deduplicated on replica label (repeated). Similar Prometheus alert relabelling").
		StringsVar(&ac.alertExcludeLabels)
	ac.alertRelabelConfigPath = extflag.RegisterPathOrContent(cmd, "alert.relabel-config", "YAML file that contains alert relabelling configuration.", extflag.WithEnvSubstitution())
	return ac
}
