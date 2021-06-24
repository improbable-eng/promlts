# Multi-Tenancy

Thanos supports multi-tenancy by using external labels. For such use cases, the [Thanos Sidecar](../components/sidecar.md) based approach with layered [Thanos Queriers](../components/query.md) is recommended.

You can also use the [Thanos Receiver](../components/receive.md) however, we don't recommend it to achieve a global view of data of a single-tenant. Also note that, multi-tenancy may also be achievable if ingestion is not user-controlled, as then enforcing of labels, for example using the [prom-label-proxy](https://github.com/openshift/prom-label-proxy) (please thoroughly understand the mechanism if intending to employ this mechanism, as the wrong configuration could leak data).
