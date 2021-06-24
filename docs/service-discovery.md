# Service Discovery

Service Discovery (SD) is a vital part of several Thanos components. It allows Thanos to discover API targets required to perform certain operations.

SD is currently used in the following places within Thanos:

* `Thanos Querier` needs to know about [StoreAPI](https://github.com/thanos-io/thanos/blob/d3fb337da94d11c78151504b1fccb1d7e036f394/pkg/store/storepb/rpc.proto#L14) servers in order to query metrics from them.
* `Thanos Ruler` needs to know about `QueryAPI` servers in order to evaluate recording and alerting rules.
* `Thanos Ruler` needs to know about `Alertmanagers` HA replicas in order to send alerts.

There are currently several ways to configure SD, described below in more detail:

* Static Flags
* File SD
* DNS SD

## Static Flags

The simplest way to tell a component about a peer is to use a static flag.

### Thanos Querier

The repeatable flag `--store=<store>` can be used to specify a `StoreAPI` that `Thanos Querier` should use.

### Thanos Ruler

`Thanos Ruler` supports the configuration of `QueryAPI` endpoints using YAML with the `--query.config=<content>` and `--query.config-file=<path>` flags in the `static_configs` section.

`Thanos Ruler` also supports the configuration of Alertmanager endpoints using YAML with the `--alertmanagers.config=<content>` and `--alertmanagers.config-file=<path>` flags in the `static_configs` section.

## File Service Discovery

File Service Discovery is another mechanism for configuring components. With File SD, a list of files can be watched for updates, and the new configuration will be dynamically loaded when a change occurs. The list of files to watch is passed to a component via a flag shown in the component-specific sections below.

The format of the configuration file is the same as the one used in [Prometheus' File SD](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config). Both YAML and JSON files can be used. The format of the files is as follows:

* JSON:

```json
[
  {
    "targets": ["localhost:9090", "example.org:443"]
  }
]
```

* YAML:

```yaml
- targets: ['localhost:9090', 'example.org:443']
```

As a fallback, the file contents are periodically re-read at an interval that can be set using a flag specific to the component as shown below. The default value for all File SD re-read intervals is 5 minutes.

### Thanos Querier

The repeatable flag `--store.sd-files=<path>` can be used to specify the path to files that contain addresses of `StoreAPI` servers. The `<path>` can be a glob pattern so you can specify several files using a single flag.

The flag `--store.sd-interval=<5m>` can be used to change the fallback re-read interval from the default 5 minutes.

### Thanos Ruler

`Thanos Ruler` supports the configuration of `QueryAPI` endpoints using YAML with the `--query.config=<content>` and `--query.config-file=<path>` flags in the `file_sd_configs` section.

`Thanos Ruler` also supports the configuration of Alertmanager endpoints using YAML with the `--alertmanagers.config=<content>` and `--alertmanagers.config-file=<path>` flags in the `file_sd_configs` section.

## DNS Service Discovery

DNS Service Discovery is another mechanism for finding components that can be used in conjunction with Static Flags or File SD. With DNS SD, a domain name can be specified and it will be periodically queried to discover a list of IPs.

To use DNS SD, just add one of the following prefixes to the domain name in your configuration:

* `dns+` - the domain name after this prefix will be looked up as an A/AAAA query. *A port is required for this query type*. An example using this lookup with a static flag:

```
--store=dns+stores.thanos.mycompany.org:9090
```

* `dnssrv+` - the domain name after this prefix will be looked up as a SRV query, and then each SRV record will be looked up as an A/AAAA query. You do not need to specify a port as the one from the query results will be used. For example:

```
--store=dnssrv+_thanosstores._tcp.mycompany.org
```

DNS SRV record discovery also work well within Kubernetes. Consider the following example:

```
--store=dnssrv+_grpc._tcp.thanos-store.monitoring.svc
```

This configuration will instruct Thanos to discover all endpoints within the `thanos-store` service in the `monitoring` namespace and use the declared port named `grpc`.

* `dnssrvnoa+` - the domain name after this prefix will be looked up as a SRV query, with no A/AAAA lookup made after that. Similar to the `dnssrv+` case, you do not need to specify a port. For example:

```
--store=dnssrvnoa+_thanosstores._tcp.mycompany.org
```

The default interval between DNS lookups is 30s. This interval can be changed using the `store.sd-dns-interval` flag for `StoreAPI` configuration in `Thanos Querier`, or `query.sd-dns-interval` for `QueryAPI` configuration in `Thanos Ruler`.

## Other

Currently, there are no plans of adding other Service Discovery mechanisms like Consul SD, Kubernetes SD, etc. However, we welcome people implementing their preferred Service Discovery by writing the results to File SD, which can be consumed by the different Thanos components.
