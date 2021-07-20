# Sharding

# Background

Currently, all components that read from object store assume that all the operations and functionality should be done based on **all** the available blocks that are present in the certain bucket's root directory.

This is in most cases totally fine, however with time and allowance of storing blocks from multiple `Sources` into the same bucket, the number of objects in a bucket can grow drastically.

This means that with time you might want to scale out certain components e.g:

## Compactor

Larger number of objects does not matter much, however compactor has to scale (CPU, network, disk, memory) with number of Sources pushing blocks to the object storage.

## Store Gateway

Queries against store gateway which are touching large number of blocks (no matter from what Sources) might be expensive, so it can scale with number of blocks.

# Relabelling

Similar to [promtail](https://grafana.com/docs/loki/latest/clients/promtail/configuration/#relabel_configs) this config will follow native [Prometheus relabel-config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config) syntax.

Now, thanos only support following relabel actions:

* keep

* drop

* hashmod
  * `external labels` for all components

  * `__block_id` for store gateway, see this [example](https://github.com/observatorium/configuration/blob/bf1304b0d7bce2ae3fefa80412bb358f9aa176fb/environments/openshift/manifests/observatorium-template.yaml#L1514-L1521)

The relabel config defines filtering process done on **every** synchronization with object storage.

We will allow potentially manipulating with several of inputs:

* External labels:
  * `<name>`

Output:

* If output is empty, drop block.

By default, on empty relabel-config, all external labels are assumed.

Example usages would be:

* Drop blocks which contains external labels cluster=A

```yaml
- action: drop
  regex: "A"
  source_labels:
  - cluster
```

* Keep only blocks which contains external labels cluster=A

```yaml
- action: keep
  regex: "A"
  source_labels:
  - cluster
```

We can shard by adjusting which labels should be included in the blocks.

# Time Partitioning

For store gateway, we can specify `--min-time` and `--max-time` flags to filter for what blocks store gateway should be responsible for.

More details can refer to "Time based partitioning" chapter in [Store gateway](components/store.md).
