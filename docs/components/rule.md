---
title: Rule
type: docs
menu: components
---

# Rule (aka Ruler)

_**NOTE:** It is recommended to keep deploying rules inside the relevant Prometheus servers locally. Use ruler only on specific cases. Read details [below](rule.md#risk) why._

_The rule component should in particular not be used to circumvent solving rule deployment properly at the configuration management level._

The `thanos rule` command evaluates Prometheus recording and alerting rules against chosen query API via repeated `--query` (or FileSD via `--query.sd`). If more than one query is passed, round robin balancing is performed.

Rule results are written back to disk in the Prometheus 2.0 storage format. Rule nodes at the same time participate in the system as source store nodes, which means that they expose StoreAPI and upload their generated TSDB blocks to an object store.

You can think of Rule as a simplified Prometheus that does not require a sidecar and does not scrape and do PromQL evaluation (no QueryAPI).

The data of each Rule node can be labeled to satisfy the clusters labeling scheme. High-availability pairs can be run in parallel and should be distinguished by the designated replica label, just like regular Prometheus servers.
Read more about Ruler in HA [here](rule.md#ruler-ha)

```bash
thanos rule \
    --data-dir             "/path/to/data" \
    --eval-interval        "30s" \
    --rule-file            "/path/to/rules/*.rules.yaml" \
    --alert.query-url      "http://0.0.0.0:9090" \ # This tells what query URL to link to in UI.
    --alertmanagers.url    "http://alert.thanos.io" \
    --query                "query.example.org" \
    --query                "query2.example.org" \
    --objstore.config-file "bucket.yml" \
    --label                'monitor_cluster="cluster1"'
    --label                'replica="A"'
```

## Risk

Ruler has conceptual tradeoffs that might not be favorable for most use cases. The main tradeoff is its dependence on
query reliability. For Prometheus it is unlikely to have alert/recording rule evaluation failure as evaluation is local.

For Ruler the read path is distributed, since most likely Ruler is querying Thanos Querier which gets data from remote Store APIs.

This means that **query failure** are more likely to happen, that's why clear strategy on what will happen to alert and during query
unavailability is the key.


## Configuring Rules


Rule files use YAML, the syntax of a rule file is:

```
groups:
  [ - <rule_group> ]
```

A simple example rules file would be:

```
groups:
  - name: example
    rules:
    - record: job:http_inprogress_requests:sum
      expr: sum(http_inprogress_requests) by (job)
```

<rule_group>

```
# The name of the group. Must be unique within a file.
name: <string>

# How often rules in the group are evaluated.
[ interval: <duration> | default = global.evaluation_interval ]

rules:
  [ - <rule> ... ]
```

Thanos supports two types of rules which may be configured and then evaluated at regular intervals: recording rules and alerting rules.

### Recording Rules

Recording rules allow you to precompute frequently needed or computationally expensive expressions and save their result as a new set of time series. Querying the precomputed result will then often be much faster than executing the original expression every time it is needed. This is especially useful for dashboards, which need to query the same expression repeatedly every time they refresh.

Recording and alerting rules exist in a rule group. Rules within a group are run sequentially at a regular interval.

The syntax for recording rules is:

```
# The name of the time series to output to. Must be a valid metric name.
record: <string>

# The PromQL expression to evaluate. Every evaluation cycle this is
# evaluated at the current time, and the result recorded as a new set of
# time series with the metric name as given by 'record'.
expr: <string>

# Labels to add or overwrite before storing the result.
labels:
  [ <labelname>: <labelvalue> ]
```

### Alerting Rules

The syntax for alerting rules is:

```
# The name of the alert. Must be a valid metric name.
alert: <string>

# The PromQL expression to evaluate. Every evaluation cycle this is
# evaluated at the current time, and all resultant time series become
# pending/firing alerts.
expr: <string>

# Alerts are considered firing once they have been returned for this long.
# Alerts which have not yet fired for long enough are considered pending.
[ for: <duration> | default = 0s ]

# Labels to add or overwrite for each alert.
labels:
  [ <labelname>: <tmpl_string> ]

# Annotations to add to each alert.
annotations:
  [ <labelname>: <tmpl_string> ]
```

## Partial Response

See [this](query.md#partial-response) on initial info.

Rule allows you to specify rule groups with additional fields that control PartialResponseStrategy e.g:

```yaml
groups:
- name: "warn strategy"
  partial_response_strategy: "warn"
  rules:
  - alert: "some"
    expr: "up"
- name: "abort strategy"
  partial_response_strategy: "abort"
  rules:
  - alert: "some"
    expr: "up"
- name: "by default strategy is abort"
  rules:
  - alert: "some"
    expr: "up"
```

It is recommended to keep partial response as `abort` for alerts and that is the default as well.

Essentially, for alerting, having partial response can result in symptoms being missed by Rule's alert.

## Must have: essential Ruler alerts!

To be sure that alerting works it is essential to monitor Ruler and alert from another **Scraper (Prometheus + sidecar)** that sits in same cluster.

The most important metrics to alert on are:

* `thanos_alert_sender_alerts_dropped_total`. If greater than 0, it means that alerts triggered by Rule are not being sent to alertmanager which might
indicate connection, incompatibility or misconfiguration problems.

* `prometheus_rule_evaluation_failures_total`. If greater than 0, it means that that rule failed to be evaluated, which results in
either gap in rule or potentially ignored alert. This metric might indicate problems on the queryAPI endpoint you use. Alert heavily on this if this happens for longer than your alert thresholds.
`strategy` label will tell you if failures comes from rules that tolerate [partial response](rule.md#partial-response) or not.

* `prometheus_rule_group_last_duration_seconds < prometheus_rule_group_interval_seconds`  If the difference is large, it means
that rule evaluation took more time than the scheduled interval. It can indicate that your query backend (e.g Querier) takes too much time
to evaluate the query, i.e. that it is not fast enough to fill the rule. This might indicate other problems like slow StoreAPis or
too complex query expression in rule.

* `thanos_rule_evaluation_with_warnings_total`. If you choose to use Rules and Alerts with [partial response strategy's](rule.md#partial-response)
value as "warn", this metric will tell you how many evaluation ended up with some kind of warning. To see the actual warnings
see WARN log level. This might suggest that those evaluations return partial response and might not be accurate.

Those metrics are important for vanilla Prometheus as well, but even more important when we rely on (sometimes WAN) network.

// TODO(bwplotka): Rereview them after recent changes in metrics.

See [alerts](/examples/alerts/alerts.md#Ruler) for more example alerts for ruler.

NOTE: It is also recommended to set a mocked Alert on Ruler that checks if Query is up. This might be something simple like `vector(1)` query, just
to check if Querier is live.

## Performance.

As rule nodes outsource query processing to query nodes, they should generally experience little load. If necessary, functional sharding can be applied by splitting up the sets of rules between HA pairs.
Rules are processed with deduplicated data according to the replica label configured on query nodes.

## External labels

It is *mandatory* to add certain external labels to indicate the ruler origin (e.g `label='replica="A"'` or for `cluster`).
Otherwise running multiple ruler replicas will be not possible, resulting in clash during compaction.

NOTE: It is advised to put different external labels than labels given by other sources we are recording or alerting against.

For example:

* Ruler is in cluster `mon1` and we have Prometheus in cluster `eu1`
* By default we could try having consistent labels so we have `cluster=eu1` for Prometheus and `cluster=mon1` for Ruler.
* We configure `ScraperIsDown` alert that monitors service from `work1` cluster.
* When triggered this alert results in `ScraperIsDown{cluster=mon1}` since external labels always *replace* source labels.

This effectively drops the important metadata and makes it impossible to tell in what exactly `cluster` the `ScraperIsDown` alert found problem
without falling back to manual query.

## Ruler UI

On HTTP address Ruler exposes its UI that shows mainly Alerts and Rules page (similar to Prometheus Alerts page).
Each alert is linked to the query that the alert is performing, which you can click to navigate to the configured `alert.query-url`.

## Ruler HA

Ruler aims to use a similar approach to the one that Prometheus has. You can configure external labels, as well as simple relabelling.

In case of Ruler in HA you need to make sure you have the following labelling setup:

* Labels that identify the HA group ruler and replica label with different value for each ruler instance, e.g:
`cluster="eu1", replica="A"` and `cluster=eu1, replica="B"` by using `--label` flag.
* Labels that need to be dropped just before sending to alermanager in order for alertmanager to deduplicate alerts e.g
`--alert.label-drop="replica"`.

Full relabelling is planned to be done in future and is tracked here: https://github.com/thanos-io/thanos/issues/660

## Flags

[embedmd]:# (flags/rule.txt $)
```$
usage: thanos rule [<flags>]

Ruler evaluating Prometheus rules against given Query nodes, exposing Store API
and storing old blocks in bucket.

Flags:
      --alert.label-drop=ALERT.LABEL-DROP ...
                                 Labels by name to drop before sending to
                                 alertmanager. This allows alert to be
                                 deduplicated on replica label (repeated).
                                 Similar Prometheus alert relabelling
      --alert.query-url=ALERT.QUERY-URL
                                 The external Thanos Query URL that would be set
                                 in all alerts 'Source' field
      --alert.relabel-config=<content>
                                 Alternative to 'alert.relabel-config-file' flag
                                 (mutually exclusive). Content of YAML file that
                                 contains alert relabelling configuration.
      --alert.relabel-config-file=<file-path>
                                 Path to YAML file that contains alert
                                 relabelling configuration.
      --alertmanagers.config=<content>
                                 Alternative to 'alertmanagers.config-file' flag
                                 (mutually exclusive). Content of YAML file that
                                 contains alerting configuration. See format
                                 details:
                                 https://thanos.io/tip/components/rule.md/#configuration.
                                 If defined, it takes precedence over the
                                 '--alertmanagers.url' and
                                 '--alertmanagers.send-timeout' flags.
      --alertmanagers.config-file=<file-path>
                                 Path to YAML file that contains alerting
                                 configuration. See format details:
                                 https://thanos.io/tip/components/rule.md/#configuration.
                                 If defined, it takes precedence over the
                                 '--alertmanagers.url' and
                                 '--alertmanagers.send-timeout' flags.
      --alertmanagers.sd-dns-interval=30s
                                 Interval between DNS resolutions of
                                 Alertmanager hosts.
      --alertmanagers.send-timeout=10s
                                 Timeout for sending alerts to Alertmanager
      --alertmanagers.url=ALERTMANAGERS.URL ...
                                 Alertmanager replica URLs to push firing
                                 alerts. Ruler claims success if push to at
                                 least one alertmanager from discovered
                                 succeeds. The scheme should not be empty e.g
                                 `http` might be used. The scheme may be
                                 prefixed with 'dns+' or 'dnssrv+' to detect
                                 Alertmanager IPs through respective DNS
                                 lookups. The port defaults to 9093 or the SRV
                                 record's value. The URL path is used as a
                                 prefix for the regular Alertmanager API path.
      --data-dir="data/"         data directory
      --eval-interval=30s        The default evaluation interval to use.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components.
      --grpc-grace-period=2m     Time to wait after an interrupt received for
                                 GRPC Server.
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no client
                                 CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
      --hash-func=               Specify which hash function to use when
                                 calculating the hashes of produced files. If no
                                 function has been specified, it does not
                                 happen. This permits avoiding downloading some
                                 files twice albeit at some performance cost.
                                 Possible values are: "", "SHA256".
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --http.config=""           [EXPERIMENTAL] Path to the configuration file
                                 that can enable TLS or authentication for all
                                 HTTP endpoints.
      --label=<name>="<value>" ...
                                 Labels to be applied to all generated metrics
                                 (repeated). Similar to external labels for
                                 Prometheus, used to identify ruler and its
                                 blocks as unique source.
      --log.format=logfmt        Log format to use. Possible options: logfmt or
                                 json.
      --log.level=info           Log filtering level.
      --log.request.decision=    Deprecation Warning - This flag would be soon
                                 deprecated, and replaced with
                                 `request.logging-config`. Request Logging for
                                 logging the start and end of requests. By
                                 default this flag is disabled. LogFinishCall:
                                 Logs the finish call of the requests.
                                 LogStartAndFinishCall: Logs the start and
                                 finish call of the requests. NoLogCall: Disable
                                 request logging.
      --objstore.config=<content>
                                 Alternative to 'objstore.config-file' flag
                                 (mutually exclusive). Content of YAML file that
                                 contains object store configuration. See format
                                 details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config-file=<file-path>
                                 Path to YAML file that contains object store
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
      --query=<query> ...        Addresses of statically configured query API
                                 servers (repeatable). The scheme may be
                                 prefixed with 'dns+' or 'dnssrv+' to detect
                                 query API servers through respective DNS
                                 lookups.
      --query.config=<content>   Alternative to 'query.config-file' flag
                                 (mutually exclusive). Content of YAML file that
                                 contains query API servers configuration. See
                                 format details:
                                 https://thanos.io/tip/components/rule.md/#configuration.
                                 If defined, it takes precedence over the
                                 '--query' and '--query.sd-files' flags.
      --query.config-file=<file-path>
                                 Path to YAML file that contains query API
                                 servers configuration. See format details:
                                 https://thanos.io/tip/components/rule.md/#configuration.
                                 If defined, it takes precedence over the
                                 '--query' and '--query.sd-files' flags.
      --query.http-method=POST   HTTP method to use when sending queries.
                                 Possible options: [GET, POST]
      --query.sd-dns-interval=30s
                                 Interval between DNS resolutions.
      --query.sd-files=<path> ...
                                 Path to file that contains addresses of query
                                 API servers. The path can be a glob pattern
                                 (repeatable).
      --query.sd-interval=5m     Refresh interval to re-read file SD files.
                                 (used as a fallback)
      --remote-write.config=<content>
                                 Alternative to 'remote-write.config-file' flag
                                 (mutually exclusive). Content of YAML config
                                 for the remote-write server where samples
                                 should be sent to. This automatically enables
                                 stateless mode for ruler and no series will be
                                 stored in the ruler's TSDB. If an empty config
                                 (or file) is provided, the flag is ignored and
                                 ruler is run with its own TSDB.
      --remote-write.config-file=<file-path>
                                 Path to YAML config for the remote-write server
                                 where samples should be sent to. This
                                 automatically enables stateless mode for ruler
                                 and no series will be stored in the ruler's
                                 TSDB. If an empty config (or file) is provided,
                                 the flag is ignored and ruler is run with its
                                 own TSDB.
      --request.logging-config=<content>
                                 Alternative to 'request.logging-config-file'
                                 flag (mutually exclusive). Content of YAML file
                                 with request logging configuration. See format
                                 details:
                                 https://gist.github.com/yashrsharma44/02f5765c5710dd09ce5d14e854f22825
      --request.logging-config-file=<file-path>
                                 Path to YAML file with request logging
                                 configuration. See format details:
                                 https://gist.github.com/yashrsharma44/02f5765c5710dd09ce5d14e854f22825
      --resend-delay=1m          Minimum amount of time to wait before resending
                                 an alert to Alertmanager.
      --rule-file=rules/ ...     Rule files that should be used by rule manager.
                                 Can be in glob format (repeated).
      --shipper.upload-compacted
                                 If true shipper will try to upload compacted
                                 blocks as well. Useful for migration purposes.
                                 Works only if compaction is disabled on
                                 Prometheus. Do it once and then disable the
                                 flag when done.
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag
                                 (mutually exclusive). Content of YAML file with
                                 tracing configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing configuration.
                                 See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tsdb.block-duration=2h   Block duration for TSDB block.
      --tsdb.no-lockfile         Do not create lockfile in TSDB data directory.
                                 In any case, the lockfiles will be deleted on
                                 next startup.
      --tsdb.retention=48h       Block retention time on local disk.
      --tsdb.wal-compression     Compress the tsdb WAL.
      --version                  Show application version.
      --web.disable-cors         Whether to disable CORS headers to be set by
                                 Thanos. By default Thanos sets CORS headers to
                                 be allowed by all.
      --web.external-prefix=""   Static prefix for all HTML links and redirect
                                 URLs in the bucket web UI interface. Actual
                                 endpoints are still served on / or the
                                 web.route-prefix. This allows thanos bucket web
                                 UI to be served behind a reverse proxy that
                                 strips a URL sub-path.
      --web.prefix-header=""     Name of HTTP request header used for dynamic
                                 prefixing of UI links and redirects. This
                                 option is ignored if web.external-prefix
                                 argument is set. Security risk: enable this
                                 option only if a reverse proxy in front of
                                 thanos is resetting the header. The
                                 --web.prefix-header=X-Forwarded-Prefix option
                                 can be useful, for example, if Thanos UI is
                                 served via Traefik reverse proxy with
                                 PathPrefixStrip option enabled, which sends the
                                 stripped prefix value in X-Forwarded-Prefix
                                 header. This allows thanos UI to be served on a
                                 sub-path.
      --web.route-prefix=""      Prefix for API and UI endpoints. This allows
                                 thanos UI to be served on a sub-path. This
                                 option is analogous to --web.route-prefix of
                                 Prometheus.

```

## Configuration

### Alertmanager

The `--alertmanagers.config` and `--alertmanagers.config-file` flags allow specifying multiple Alertmanagers. Those entries are treated as a single HA group. This means that alert send failure is claimed only if the Ruler fails to send to all instances.

The configuration format is the following:

[embedmd]:# (../flags/config_rule_alerting.txt yaml)
```yaml
alertmanagers:
- http_config:
    basic_auth:
      username: ""
      password: ""
      password_file: ""
    bearer_token: ""
    bearer_token_file: ""
    proxy_url: ""
    tls_config:
      ca_file: ""
      cert_file: ""
      key_file: ""
      server_name: ""
      insecure_skip_verify: false
  static_configs: []
  file_sd_configs:
  - files: []
    refresh_interval: 0s
  scheme: http
  path_prefix: ""
  timeout: 10s
  api_version: v1
```

Supported values for `api_version` are `v1` or `v2`.

### Query API

The `--query.config` and `--query.config-file` flags allow specifying multiple query endpoints. Those entries are treated as a single HA group. This means that query failure is claimed only if the Ruler fails to query all instances.

The configuration format is the following:

[embedmd]:# (../flags/config_rule_query.txt yaml)
```yaml
- http_config:
    basic_auth:
      username: ""
      password: ""
      password_file: ""
    bearer_token: ""
    bearer_token_file: ""
    proxy_url: ""
    tls_config:
      ca_file: ""
      cert_file: ""
      key_file: ""
      server_name: ""
      insecure_skip_verify: false
  static_configs: []
  file_sd_configs:
  - files: []
    refresh_interval: 0s
  scheme: http
  path_prefix: ""
```
