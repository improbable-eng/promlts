---
title: Query
type: docs
menu: components
---

# Querier/Query

The `thanos query` command (also known as "Querier") implements the [Prometheus HTTP v1 API](https://prometheus.io/docs/prometheus/latest/querying/api/) to query data in a Thanos cluster via PromQL.

In short, it gathers the data needed to evaluate the query from underlying [StoreAPIs](https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/rpc.proto), evaluates the query and returns the result.

Querier is fully stateless and horizontally scalable.

Example command to run Querier:

```bash
thanos query \
    --http-address     "0.0.0.0:9090" \
    --store            "<store-api>:<grpc-port>" \
    --store            "<store-api2>:<grpc-port>"
```
## Querier use cases, why do I need this component?

Thanos Querier essentially allows to aggregate and optionally deduplicate multiple metrics backends under single Prometheus Query endpoint.

### Global View

Since for Querier "a backend" is anything that implements gRPC StoreAPI we can aggregate data from any number of the different storages like:

* Prometheus (see [Sidecar](./sidecar.md))
* Object Storage (see [Store Gateway](./store.md))
* Global alerting/recording rules evaluations (see [Ruler](./rule.md))
* Metrics received from Prometheus remote write streams (see [Receiver](./receive.md))
* Another Querier (you can stack Queriers on top of each other)
* Non-Prometheus systems!
    * e.g [OpenTSDB](../integrations.md#opentsdb)

Thanks to that, you can run queries (manually, from Grafana or via Alerting rule) that aggregate metrics from mix of those sources.

Some examples:

* `sum(cpu_used{cluster=~"cluster-(eu1|eu2|eu3|us1|us2|us3)", job="service1"})` that will give you sum of CPU used inside all listed clusters for service `service1`. This will work
even if those clusters runs multiple Prometheus servers each. Querier will know which data sources to query.

* In single cluster you shard Prometheus functionally or have different Prometheus instances for different tenants. You can spin up Querier to have access to both within single Query evaluation.

### Run-time deduplication of HA groups

Prometheus is stateful and does not allow replicating its database. This means that increasing high availability by running multiple Prometheus replicas is not very easy to use.
Simple load balancing will not work as for example after some crash, replica might be up but querying such replica will result in small gap during the period it was down. You have a
 second replica that maybe was up, but it could be down in other moment (e.g rolling restart), so load balancing on top of those is not working well.

Thanos Querier instead pulls the data from both replicas, and deduplicate those signals, filling the gaps if any, transparently to the Querier consumer.

## Metric Query Flow Overview

<img src="../img/querier.svg" class="img-fluid" alt="querier-steps" />

Overall QueryAPI exposed by Thanos is guaranteed to be compatible with [Prometheus 2.x. API](https://prometheus.io/docs/prometheus/latest/querying/api/).
The above diagram shows what Querier does for each Prometheus query request.

See [here](https://thanos.io/tip/thanos/service-discovery.md/) on how to connect Querier with desired StoreAPIs.

<!--- TODO explain steps  --->

###  Deduplication

The query layer can deduplicate series that were collected from high-availability pairs of data sources such as Prometheus.
A fixed single or multiple replica labels must be chosen for the entire cluster and can then be passed to query nodes on startup.

Two or more series that are only distinguished by the given replica label, will be merged into a single time series.
This also hides gaps in collection of a single data source.

### An example with a single replica labels:

* Prometheus + sidecar "A": `cluster=1,env=2,replica=A`
* Prometheus + sidecar "B": `cluster=1,env=2,replica=B`
* Prometheus + sidecar "A" in different cluster: `cluster=2,env=2,replica=A`

If we configure Querier like this:

```
thanos query \
    --http-address        "0.0.0.0:9090" \
    --query.replica-label "replica" \
    --store               "<store-api>:<grpc-port>" \
    --store               "<store-api2>:<grpc-port>" \
```

And we query for metric `up{job="prometheus",env="2"}` with this option we will get 2 results:

  * `up{job="prometheus",env="2",cluster="1"} 1`
  * `up{job="prometheus",env="2",cluster="2"} 1`

WITHOUT this replica flag (deduplication turned off), we will get 3 results:

  * `up{job="prometheus",env="2",cluster="1",replica="A"} 1`
  * `up{job="prometheus",env="2",cluster="1",replica="B"} 1`
  * `up{job="prometheus",env="2",cluster="2",replica="A"} 1`

### The same output will be present for this example with multiple replica labels:

* Prometheus + sidecar "A": `cluster=1,env=2,replica=A,replicaX=A`
* Prometheus + sidecar "B": `cluster=1,env=2,replica=B,replicaX=B`
* Prometheus + sidecar "A" in different cluster: `cluster=2,env=2,replica=A,replicaX=A`

```
thanos query \
    --http-address        "0.0.0.0:9090" \
    --query.replica-label "replica" \
    --query.replica-label "replicaX" \
    --store               "<store-api>:<grpc-port>" \
    --store               "<store-api2>:<grpc-port>" \
```


This logic can also be controlled via parameter on QueryAPI. More details below.

## Query API Overview

As mentioned, Query API exposed by Thanos is guaranteed to be compatible with [Prometheus 2.x. API](https://prometheus.io/docs/prometheus/latest/querying/api/).
However for additional Thanos features on top of Prometheus, Thanos adds:

* partial response behaviour
* several additional parameters listed below
* custom response fields.

Let's walk through all of those extensions:

### Partial Response

QueryAPI and StoreAPI has additional behaviour controlled via query parameter called [PartialResponseStrategy](/pkg/store/storepb/rpc.pb.go).

This parameter controls tradeoff between accuracy and availability.

Partial response is a potentially missed result within query against QueryAPI or StoreAPI. This can happen if one
of StoreAPIs is returning error or timeout whereas couple of others returns success. It does not mean you are missing data,
you might lucky enough that you actually get the correct data as the broken StoreAPI did not have anything for your query.

If partial response happen QueryAPI returns human readable warnings explained [here](query.md#custom-response-fields).

Now it supports two strategies:
* "warn"
* "abort" (default)

NOTE: Having a warning does not necessarily mean partial response (e.g no store matched query warning).

Querier also allows to configure different timeouts:

* `--query.timeout`
* `--store.response-timeout`

If you prefer availability over accuracy you can set tighter timeout to underlying StoreAPI than overall query timeout. If partial response
strategy is NOT `abort`, this will "ignore" slower StoreAPIs producing just warning with 200 status code response.

### Deduplication replica labels.

| HTTP URL/FORM parameter | Type | Default | Example |
|----|----|----|----|
| `replicaLabels` | `[]string` | `query.replica-label` flag (default: empty). | `replicaLabels=replicaA&replicaLabels=replicaB` |
|  |  |  |  |

This overwrites the `query.replica-label` cli flag to allow dynamic replica labels at query time.

### Deduplication Enabled

| HTTP URL/FORM parameter | Type | Default | Example |
|----|----|----|----|
| `dedup` | `Boolean` | True, but effect depends on `query.replica` configuration flag. | `1, t, T, TRUE, true, True` for "True" |
|  |  |  |  |

This controls if query results should be deduplicated using the replica labels.

### Auto downsampling

| HTTP URL/FORM parameter | Type | Default | Example |
|----|----|----|----|
| `max_source_resolution` | `Float64/time.Duration/model.Duration` | `step / 5` or `0` if `query.auto-downsampling` is false (default: False) | `5m` |
|  |  |  |  |

Max source resolution is max resolution in seconds we want to use for data we query for. This means that for value:

* 0 -> we will use only raw data.
* 5m -> we will use max 5m downsampling.
* 1h -> we will use max 1h downsampling.

### Partial Response Strategy

// TODO(bwplotka): Update. This will change to "strategy" soon as [PartialResponseStrategy enum here](/pkg/store/storepb/rpc.proto)

| HTTP URL/FORM parameter | Type | Default | Example |
|----|----|----|----|
| `partial_response` | `Boolean` | `query.partial-response` flag (default: True) | `1, t, T, TRUE, true, True` for "True" |
|  |  |  |  |

If true, then all storeAPIs that will be unavailable (and thus return no data) will not cause query to fail, but instead
return warning.

### Custom Response Fields

Any additional field does not break compatibility, however there is no guarantee that Grafana or any other client will understand those.

Currently Thanos UI exposed by Thanos understands

```go
type queryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`

	// Additional Thanos Response field.
	Warnings   []error          `json:"warnings,omitempty"`
}
```

Additional field is `Warnings` that contains every error that occurred that is assumed non critical. `partial_response`
option controls if storeAPI unavailability is considered critical.

### Concurrent Selects

Thanos Querier has the ability to perform concurrent select request per query. It dissects given PromQL statement and executes selectors concurrently against the discovered StoreAPIs.
The maximum number of concurrent requests are being made per query is controlled by `query.max-concurrent-select` flag.
Keep in mind that the maximum number of concurrent queries that are handled by querier is controlled by `query.max-concurrent`. Please consider implications of combined value while tuning the querier.

### Store filtering

It's possible to provide a set of matchers to the Querier api to select specific stores to be used during the query using the `storeMatch[]` parameter. It is useful when debugging a slow/broken store.
It uses the same format as the matcher of [Prometheus' federate api](https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers).
Note that at the moment the querier only supports the `__address__` which contain the address of the store as it is shown on the `/stores` endpoint of the UI.

Example:
```
- targets:
  - prometheus-foo.thanos-sidecar:10901
  - prometheus-bar.thanos-sidecar:10901
```

```
http://localhost:10901/api/v1/query?query=up&dedup=true&partial_response=true&storeMatch[]={__address__=~"prometheus-foo.*"}
```

Will only return metrics from `prometheus-foo.thanos-sidecar:10901`


## Expose UI on a sub-path

It is possible to expose thanos-query UI and optionally API on a sub-path.
The sub-path can be defined either statically or dynamically via an HTTP header.
Static path prefix definition follows the pattern used in Prometheus,
where `web.route-prefix` option defines HTTP request path prefix (endpoints prefix)
and `web.external-prefix` prefixes the URLs in HTML code and the HTTP redirect responses.

Additionally, Thanos supports dynamic prefix configuration, which
[is not yet implemented by Prometheus](https://github.com/prometheus/prometheus/issues/3156).
Dynamic prefixing simplifies setup when `thanos query` is exposed on a sub-path behind
a reverse proxy, for example, via a Kubernetes ingress controller
[Traefik](https://docs.traefik.io/routing/routers/)
or [nginx](https://github.com/kubernetes/ingress-nginx/pull/1805).
If `PathPrefixStrip: /some-path` option or `traefik.frontend.rule.type: PathPrefixStrip`
Kubernetes Ingress annotation is set, then `Traefik` writes the stripped prefix into X-Forwarded-Prefix header.
Then, `thanos query --web.prefix-header=X-Forwarded-Prefix` will serve correct HTTP redirects and links prefixed by the stripped path.

## File SD

`--store.sd-file` flag provides a path to a JSON or YAML formatted file, which contains a list of targets in [Prometheus target format](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config).

Example file SD file in YAML:

```
- targets:
  - prometheus-0.thanos-sidecar:10901
  - prometheus-1.thanos-sidecar:10901
  - thanos-store:10901
  - thanos-short-store:10901
  - thanos-rule:10901
- targets:
  - prometheus-0.thanos-sidecar.infra:10901
  - prometheus-1.thanos-sidecar.infra:10901
  - thanos-store.infra:10901
```


## Flags

[embedmd]:# (flags/query.txt $)
```$
usage: thanos query [<flags>]

Query node exposing PromQL enabled Query API with data retrieved from multiple
store nodes.

Flags:
      --endpoint=<endpoint> ...  Addresses of statically configured Thanos API
                                 servers (repeatable). The scheme may be
                                 prefixed with 'dns+' or 'dnssrv+' to detect
                                 Thanos API servers through respective DNS
                                 lookups.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components.
      --grpc-client-server-name=""
                                 Server name to verify the hostname on the
                                 returned gRPC certificates. See
                                 https://tools.ietf.org/html/rfc4366#section-3.1
      --grpc-client-tls-ca=""    TLS CA Certificates to use to verify gRPC
                                 servers
      --grpc-client-tls-cert=""  TLS Certificates to use to identify this client
                                 to the server
      --grpc-client-tls-key=""   TLS Key for the client's certificate
      --grpc-client-tls-secure   Use TLS when talking to the gRPC server
      --grpc-client-tls-skip-verify
                                 Disable TLS certificate verification i.e self
                                 signed, signed by fake CA
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
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --http.config=""           [EXPERIMENTAL] Path to the configuration file
                                 that can enable TLS or authentication for all
                                 HTTP endpoints.
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
      --query.auto-downsampling  Enable automatic adjustment (step / 5) to what
                                 source of data should be used in store gateways
                                 if no max_source_resolution param is specified.
      --query.default-evaluation-interval=1m
                                 Set default evaluation interval for sub
                                 queries.
      --query.default-step=1s    Set default step for range queries. Default
                                 step is only used when step is not set in UI.
                                 In such cases, Thanos UI will use default step
                                 to calculate resolution (resolution =
                                 max(rangeSeconds / 250, defaultStep)). This
                                 will not work from Grafana, but Grafana has
                                 __step variable which can be used.
      --query.lookback-delta=QUERY.LOOKBACK-DELTA
                                 The maximum lookback duration for retrieving
                                 metrics during expression evaluations. PromQL
                                 always evaluates the query for the certain
                                 timestamp (query range timestamps are deduced
                                 by step). Since scrape intervals might be
                                 different, PromQL looks back for given amount
                                 of time to get latest sample. If it exceeds the
                                 maximum lookback delta it assumes series is
                                 stale and returns none (a gap). This is why
                                 lookback delta should be set to at least 2
                                 times of the slowest scrape interval. If unset
                                 it will use the promql default of 5m.
      --query.max-concurrent=20  Maximum number of queries processed
                                 concurrently by query node.
      --query.max-concurrent-select=4
                                 Maximum number of select requests made
                                 concurrently per a query.
      --query.metadata.default-time-range=0s
                                 The default metadata time range duration for
                                 retrieving labels through Labels and Series API
                                 when the range parameters are not specified.
                                 The zero value means range covers the time
                                 since the beginning.
      --query.partial-response   Enable partial response for queries if no
                                 partial_response param is specified.
                                 --no-query.partial-response for disabling.
      --query.replica-label=QUERY.REPLICA-LABEL ...
                                 Labels to treat as a replica indicator along
                                 which data is deduplicated. Still you will be
                                 able to query without deduplication using
                                 'dedup=false' parameter. Data includes time
                                 series, recording rules, and alerting rules.
      --query.timeout=2m         Maximum time to process query by query node.
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
      --selector-label=<name>="<value>" ...
                                 Query selector labels that will be exposed in
                                 info endpoint (repeated).
      --store=<store> ...        Addresses of statically configured store API
                                 servers (repeatable). The scheme may be
                                 prefixed with 'dns+' or 'dnssrv+' to detect
                                 store API servers through respective DNS
                                 lookups.
      --store-strict=<staticstore> ...
                                 Addresses of only statically configured store
                                 API servers that are always used, even if the
                                 health check fails. Useful if you have a
                                 caching layer on top.
      --store.response-timeout=0ms
                                 If a Store doesn't send any data in this
                                 specified duration then a Store will be ignored
                                 and partial data will be returned if it's
                                 enabled. 0 disables timeout.
      --store.sd-dns-interval=30s
                                 Interval between DNS resolutions.
      --store.sd-files=<path> ...
                                 Path to files that contain addresses of store
                                 API servers. The path can be a glob pattern
                                 (repeatable).
      --store.sd-interval=5m     Refresh interval to re-read file SD files. It
                                 is used as a resync fallback.
      --store.unhealthy-timeout=5m
                                 Timeout before an unhealthy store is cleaned
                                 from the store UI page.
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag
                                 (mutually exclusive). Content of YAML file with
                                 tracing configuration. See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing configuration.
                                 See format details:
                                 https://thanos.io/tip/thanos/tracing.md/#configuration
      --version                  Show application version.
      --web.disable-cors         Whether to disable CORS headers to be set by
                                 Thanos. By default Thanos sets CORS headers to
                                 be allowed by all.
      --web.external-prefix=""   Static prefix for all HTML links and redirect
                                 URLs in the UI query web interface. Actual
                                 endpoints are still served on / or the
                                 web.route-prefix. This allows thanos UI to be
                                 served behind a reverse proxy that strips a URL
                                 sub-path.
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
                                 thanos UI to be served on a sub-path. Defaults
                                 to the value of --web.external-prefix. This
                                 option is analogous to --web.route-prefix of
                                 Prometheus.

```
