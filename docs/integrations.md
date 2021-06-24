# Integrations

## StoreAPI

[StoreAPI](https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/rpc.proto) is a common proto interface for gRPC component that can connect to [Querier](components/query.md) in order to fetch the metric series. Natively Thanos implements [Sidecar](components/sidecar.md) (local Prometheus data), [Ruler](components/rule.md) and [Store gateway](components/store.md). This solves fetching series from Prometheus or Prometheus TSDB format, however same interface can be used to fetch metrics from other storages.

Below you can find some public integrations with other systems through StoreAPI:

### OpenTSDB as StoreAPI

[Geras](https://github.com/G-Research/geras) is an OpenTSDB integration service which can connect your OpenTSDB cluster to Thanos. Geras exposes the Thanos Storage API, thus other Thanos components can query OpenTSDB via Geras, providing a unified query interface over OpenTSDB and Prometheus.

Although OpenTSDB is able to aggregate the data, it's not supported by Geras at the moment.

### StoreAPI as Prometheus Remote Read

[thanos-remote-read](https://github.com/G-Research/thanos-remote-read) is another StoreAPI integration from our friends at G-Research.

It's a proxy, that allows exposing any Thanos service (or anything that exposes gRPC StoreAPI e.g. [Querier](components/query.md)) via Prometheus [remote read](https://github.com/prometheus/prometheus/blob/38d32e06862f6b72700f67043ce574508b5697f0/prompb/remote.proto#L27) protocol. This means that you can connect Thanos and expose its data to any remote-read compatible applications including [Prometheus itself.](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read)
