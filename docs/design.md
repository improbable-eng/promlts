# Design

Thanos is a set of components that can be composed into a highly available Prometheus setup with long term storage capabilities. Its main goals are operation simplicity and retaining of Prometheus's reliability properties.

The Prometheus metric data model and the 2.0 storage format ([spec](https://github.com/prometheus/prometheus/tree/master/tsdb/docs/format), [slides](https://www.slideshare.net/FabianReinartz/storing-16-bytes-at-scale-81282712)) are the foundational layers of all components in the system.

## Architecture

Thanos is a clustered system of components with distinct and decoupled purposes. Clustered components can be categorized as follows:

- Metric sources
- Stores
- Queriers

### Metric Sources

A data source is a very generalized definition of a component that produces or collects metric data. Source advertise this data in the cluster to potential clients. Metric data can be retrieved via a well-known gRPC service.

Thanos provides two components that act as data sources: the Prometheus sidecar and rule nodes.

The sidecar implements the gRPC service on top of Prometheus' [HTTP and remote-read APIs](https://prometheus.io/docs/querying/api/). The rule node directly implements it on top of the Prometheus storage engine it is running.

#### Metric Data Backup

Data sources that persist their data for long-term storage do so via the Prometheus 2.0 storage engine. The storage engine periodically produces immutable blocks of data for a fixed time range. A block is a directory with a handful of larger files containing all sample data and persisted indices that are required to retrieve the data:

```bash
01BX6V6TY06G5MFQ0GPH7EMXRH
├── chunks
│   ├── 000001
│   ├── 000002
│   └── 000003
├── index
└── meta.json
```

A blocks top-level directory is a ULID (like UUID but lexicographically sortable and encoding the creation time).

- [Chunk files](#chunk-file) hold a few hundred MB worth of chunks each. Chunks for the same series are sequentially aligned. Series in return are aligned by their metric name. This becomes relevant further down.
- The index file holds all information needed to look up specific series by their labels and the positions of their [chunks](#chunk).
- `meta.json` holds meta-information about a block like stats, time range, and compaction level.

Those block files can be backed up to object storage and later be queried by another component (see below). All data is uploaded as it is created by the Prometheus server/storage engine. The `meta.json` file may be extended by a `thanos` section, to which Thanos-specific metadata can be added. Currently, this includes the "external labels" the producer of the block has assigned. This later helps in filtering blocks for querying without accessing their data files. The meta.json is updated during upload time on sidecars.

```
┌────────────┬─────────┐         ┌────────────┬─────────┐     ┌─────────┐
│ Prometheus │ Sidecar │   ...   │ Prometheus │ Sidecar │     │   Rule  │
└────────────┴────┬────┘         └────────────┴────┬────┘     └┬────────┘
                  │                                │           │
                Blocks                           Blocks      Blocks
                  │                                │           │
                  v                                v           v
              ┌──────────────────────────────────────────────────┐
              │                   Object Storage                 │
              └──────────────────────────────────────────────────┘
```

#### Chunk

A chunk is part of the data structure of Prometheus TSDB, holding up to 120 samples for a single time series.

#### Chunk file

A chunk file is a file in a TSDB block that contains up to 0.5 GB worth of chunk entries in binary format.

### Stores

A store node acts as a gateway to block data that is stored in an object storage bucket. It implements the same gRPC API as data sources to provide access to all metric data found in the bucket.

It continuously synchronizes which blocks exist in the bucket and translates requests for metric data into object storage requests. It implements various strategies to minimize the number of requests to the object storage such as filtering relevant blocks by their metadata (e.g. time range and labels) and caching frequent index lookups.

The Prometheus 2.0 storage layout is optimized for minimal read amplification. For example, sample data for the same time series is sequentially aligned in a [chunk file](#chunk-file). Similarly, series for the same metric name is sequentially aligned as well. The store node is aware of the files' layout and translates data requests into a plan of a minimum amount of object storage request. Each request may fetch up to hundreds of thousands of [chunks](#chunk) at once. This is essential to satisfy even big queries with a limited amount of requests to the object storage.

Currently, only index data is cached. [Chunk](#chunk) data could be cached but is orders of magnitude larger in size. In the current state, fetching chunk data from the object storage already only accounts for a small fraction of end-to-end latency. Thus, there's currently no incentive to increase the store nodes resource requirements/limit its scalability by adding chunk caching.

### Stores & Data Sources - It's all the same

Since store nodes and data sources expose the same gRPC Store API, clients can largely treat them as equivalent and don't have to be concerned with which specific component they are querying. Each implementer of the Store API advertises meta-information about the data they provide. This allows clients to minimize the set of nodes they have to fan out, to satisfy a particular data query.

In its essence, the Store API allows to look up data by a set of label matches (as known from PromQL), and a time range. It returns compressed [chunks](#chunk) of samples as they are found in the block data. It is purely a data retrieval API and does *not* provide complex query execution.

```
┌──────────────────────┐  ┌────────────┬─────────┐   ┌────────────┐
│ Google Cloud Storage │  │ Prometheus │ Sidecar │   │    Rule    │
└─────────────────┬────┘  └────────────┴────┬────┘   └─┬──────────┘
                  │                         │          │
         Block File Ranges                  │          │
                  │                     Store API      │
                  v                         │          │
                ┌──────────────┐            │          │
                │     Store    │            │      Store API
                └────────┬─────┘            │          │
                         │                  │          │
                     Store API              │          │
                         │                  │          │
                         v                  v          v
                       ┌──────────────────────────────────┐
                       │              Client              │
                       └──────────────────────────────────┘

```

### Query Layer

Queriers are stateless and horizontally scalable instances that implement PromQL on top of the Store APIs exposed in the cluster. Queriers participate in the cluster to be able to resiliently discover all data sources and store nodes. Rule nodes in return can discover query nodes to evaluate recording and alerting rules.

Based on the metadata of store and source nodes, they attempt to minimize the request fanout to fetch data for a particular query.

```
┌──────────────────┐  ┌────────────┬─────────┐   ┌────────────┐
│    Store Node    │  │ Prometheus │ Sidecar │   │    Rule    │
└─────────────┬────┘  └────────────┴────┬────┘   └─┬──────────┘
              │                         │          │
              │                         │          │
              │                         │          │
              v                         v          v
        ┌─────────────────────────────────────────────────────┐
        │                      Query layer                    │
        └─────────────────────────────────────────────────────┘
                ^                  ^                  ^
                │                  │                  │
       ┌────────┴────────┐  ┌──────┴─────┐       ┌────┴───┐
       │ Alert Component │  │ Dashboards │  ...  │ Web UI │
       └─────────────────┘  └────────────┘       └────────┘

```

### Compactor

The compactor is a singleton process that does not participate in the Thanos cluster. Instead, it is only pointed at an object storage bucket and continuously consolidates multiple smaller blocks into larger ones. This significantly reduces the total storage size in the bucket, the load on store nodes and the number of requests required to fetch data for a query from the bucket.

The compactor also does additional batch processing such as down-sampling and applying retention policies.

## Scaling

None of the Thanos components provides any means of sharding. The only explicitly scalable component are query nodes, which are stateless and can be scaled out arbitrarily. Scaling of storage capacity is ensured by relying on an external object storage system.

Store, rule, and compactor nodes are all expected to scale significantly within a single instance or high availability pair. Similar to Prometheus, functional sharding can be applied for rare cases in which this does not hold.

For example, rule sets can be divided across multiple HA pairs of rule nodes. Store nodes likely are subject to functional sharding regardless by assigning dedicated buckets per region/datacenter.

Overall, first-class horizontal sharding is possible but will not be considered for the time being since there's no evidence that it is required in practical setups.

## Cost

The only extra cost Thanos adds to an existing Prometheus setup is essentially the price of storing and querying data from the object storage and running of the `store` node.

Queriers, compactors and rule nodes require approximately as many compute resources as they save by not doing the same work directly on Prometheus servers.

Data that is just accessed locally in conventional Prometheus setups have to be transferred over the network in Thanos. We generally expect this data shuffling to typically happen in unmetered networks and thus not causing any additional cost.

Typical object storage prices per GB are at about $0.02. The number of retrievals (typically priced at $0.004 per 10,000) by the store nodes strongly depends on the individual querying pattern. Adding 20% to the total storage cost to account for retrievals and running of store nodes seems like a conservative estimate.

Suppose we want to store 100TB of metric data. At about 1.07 bytes/sample in total data size, this is equivalent to:

- storing 48.88 years of data across an average of 1 million active time series with default 15s scrape interval.
- storing 3.25 years of data across an average of 1 million active time series with 1s scrape interval.

<details>
<summary>Calculations</summary>
<br>
Storing 100TB with 1.07 bytes/sample.
100 (TB) / 1.07 (bytes/sample) = 1.027580961×10¹⁴ samples.
We assume avg of 1mln time series, so 102758096.1 samples &#34;available&#34; for single series to fit into 100TB overall.

With 15s scrape interval (4 samples/min): 102758096.1 (samples) / 4 (samples/min) = 25689524.025 min = ~48.88 years

With 1s scrape interval (60 samples/min): 102758096.1 (samples) / 60 (samples/min) = 1712634.935 min = ~3.25 years

</details>

The cost for this amount of metric data would cost approximately $2400/month on top of the baseline Prometheus setup. In return, being able to reduce the retention time of Prometheus instances from weeks to hours will provide cost savings for local SSD or network block storage (typically $0.17/GB) and reduce memory consumption. This calculation does not yet account for shorter retention spans of low-priority data and downsampling.
