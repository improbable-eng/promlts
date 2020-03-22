# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

NOTE: As semantic versioning states all 0.y.z releases can contain breaking changes in API (flags, grpc API, any backward compatibility)

We use *breaking* word for marking changes that are not backward compatible (relates only to v0.y.z releases.)

## Unreleased

### Fixed

- [#2238](https://github.com/thanos-io/thanos/pull/2238) Ruler: Fixed Issue #2204 bug in alert queue signalling filled up queue and alerts were dropped
- [#2231](https://github.com/thanos-io/thanos/pull/2231) Bucket Web - Sort chunks by thanos.downsample.resolution for better grouping
- [#2254](https://github.com/thanos-io/thanos/pull/2254) Bucket: Fix metrics registered multiple times in bucket replicate
- [#2271](https://github.com/thanos-io/thanos/pull/2271) Bucket Web: Fixed Issue #2260 bucket passes null when storage is empty

### Added

- [#2265](https://github.com/thanos-io/thanos/pull/2265) Compactor: Add `--wait-interval` to specify compaction wait interval between consecutive compact runs when `--wait` enabled.

### Changed

- [#2136](https://github.com/thanos-io/thanos/pull/2136) store, compact, bucket: schedule block deletion by adding deletion-mark.json. This adds a consistent way for multiple readers and writers to access object storage.
Since there are no consistency guarantees provided by some Object Storage providers, this PR adds a consistent lock-free way of dealing with Object Storage irrespective of the choice of object storage. In order to achieve this co-ordination, blocks are not deleted directly. Instead, blocks are marked for deletion by uploading `deletion-mark.json` file for the block that was chosen to be deleted. This file contains unix time of when the block was marked for deletion.

- [#2090](https://github.com/thanos-io/thanos/issues/2090) *breaking* Downsample command: the `downsample` command has moved as the `thanos bucket` sub-command, and cannot be called via `thanos downsample` any more.

## [v0.11.0](https://github.com/thanos-io/thanos/releases/tag/v0.11.0) - 2020.03.02

### Fixed

- [#2033](https://github.com/thanos-io/thanos/pull/2033) Minio-go: Fixed Issue #1494 support Web Identity providers for IAM credentials for AWS EKS.
- [#1985](https://github.com/thanos-io/thanos/pull/1985) Store Gateway: Fixed case where series entry is larger than 64KB in index.
- [#2051](https://github.com/thanos-io/thanos/pull/2051) Ruler: Fixed issue where ruler does not expose shipper metrics.
- [#2101](https://github.com/thanos-io/thanos/pull/2101) Ruler: Fixed bug where thanos_alert_sender_errors_total was not registered.
- [#1789](https://github.com/thanos-io/thanos/pull/1789) Store Gateway: Improve timeouts.
- [#2139](https://github.com/thanos-io/thanos/pull/2139) Properly handle SIGHUP for reloading.
- [#2040](https://github.com/thanos-io/thanos/pull/2040) UI: Fix URL of alerts in Ruler
- [#2033](https://github.com/thanos-io/thanos/pull/1978) Ruler: Fix tracing in Thanos Ruler

### Added

- [#2003](https://github.com/thanos-io/thanos/pull/2003) Query: Support downsampling for /series.
- [#1952](https://github.com/thanos-io/thanos/pull/1952) Store Gateway: Implemented [binary index header](https://thanos.io/proposals/201912_thanos_binary_index_header.md/). This significantly reduces resource consumption (memory, CPU, net bandwidth) for startup and data loading processes as well as baseline memory. This means that adding more blocks into object storage, without querying them will use almost no resources. This, however, **still means that querying large amounts of data** will result in high spikes of memory and CPU use as before, due to simply fetching large amounts of metrics data. Since we fixed baseline, we are now focusing on query performance optimizations in separate initiatives. To enable experimental `index-header` mode run store with hidden `experimental.enable-index-header` flag.
- [#2009](https://github.com/thanos-io/thanos/pull/2009) Store Gateway: Minimum age of all blocks before they are being read. Set it to a safe value (e.g 30m) if your object storage is eventually consistent. GCS and S3 are (roughly) strongly consistent.
- [#1963](https://github.com/thanos-io/thanos/pull/1963) Mixin: Add Thanos Ruler alerts.
- [#1984](https://github.com/thanos-io/thanos/pull/1984) Query: Add cache-control header to not cache on error.
- [#1870](https://github.com/thanos-io/thanos/pull/1870) UI: Persist settings in query.
- [#1969](https://github.com/thanos-io/thanos/pull/1969) Sidecar: allow setting http connection pool size via flags.
- [#1967](https://github.com/thanos-io/thanos/issues/1967) Receive: Allow local TSDB compaction.
- [#1939](https://github.com/thanos-io/thanos/pull/1939) Ruler: Add TLS and authentication support for query endpoints with the `--query.config` and `--query.config-file` CLI flags. See [documentation](docs/components/rule.md/#configuration) for further information.
- [#1982](https://github.com/thanos-io/thanos/pull/1982) Ruler: Add support for Alertmanager v2 API endpoints.
- [#2030](https://github.com/thanos-io/thanos/pull/2030) Query: Add `thanos_proxy_store_empty_stream_responses_total` metric for number of empty responses from stores.
- [#2049](https://github.com/thanos-io/thanos/pull/2049) Tracing: Support sampling on Elastic APM with new sample_rate setting.
- [#2008](https://github.com/thanos-io/thanos/pull/2008) Querier, Receiver, Sidecar, Store: Add gRPC [health check](https://github.com/grpc/grpc/blob/master/doc/health-checking.md) endpoints.
- [#2145](https://github.com/thanos-io/thanos/pull/2145) Tracing: track query sent to prometheus via remote read api.
- [#1848](https://github.com/thanos-io/thanos/pull/1848) Ruler: Return error messages when trigger reload with http.
- [#2113](https://github.com/thanos-io/thanos/pull/2113) Bucket: Added `thanos bucket replicate`.
- [#2226](https://github.com/thanos-io/thanos/pull/2226) Query: Add ability to mix Store TLS configuration with the `--store.config` amd `--store.config-file` CLI flags. See [documentation](docs/components/query.md/#configuration) for further information.

### Changed

- [#1970](https://github.com/thanos-io/thanos/issues/1970) *breaking* Receive: Use gRPC for forwarding requests between peers. Note that existing values for the `--receive.local-endpoint` flag and the endpoints in the hashring configuration file must now specify the receive gRPC port and must be updated to be a simple `host:port` combination, e.g. `127.0.0.1:10901`, rather than a full HTTP URL, e.g. `http://127.0.0.1:10902/api/v1/receive`.
- [#1933](https://github.com/thanos-io/thanos/pull/1933) Add a flag `--tsdb.wal-compression` to configure whether to enable tsdb wal compression in ruler and receiver.
- [#2021](https://github.com/thanos-io/thanos/pull/2021) Rename metric `thanos_query_duplicated_store_address` to `thanos_query_duplicated_store_addresses_total` and `thanos_rule_duplicated_query_address` to `thanos_rule_duplicated_query_addresses_total`.
- [#2166](https://github.com/thanos-io/thanos/pull/2166) Improve tooltip for bucket web UI.

## [v0.10.1](https://github.com/thanos-io/thanos/releases/tag/v0.10.1) - 2020.01.24

### Fixed

- [#2015](https://github.com/thanos-io/thanos/pull/2015) Sidecar: Querier /api/v1/series bug fixed when time range was ignored inside sidecar.
The bug was noticeable for example when using Grafana template variables.
- [#2120](https://github.com/thanos-io/thanos/pull/2120) Bucket Web: Set state of status prober properly.

## [v0.10.0](https://github.com/thanos-io/thanos/releases/tag/v0.10.0) - 2020.01.13

### Fixed

- [#1919](https://github.com/thanos-io/thanos/issues/1919) Compactor: Fixed potential data loss when uploading older blocks, or upload taking long time while compactor is
running.
- [#1937](https://github.com/thanos-io/thanos/pull/1937) Compactor: Improved synchronization of meta JSON files.
Compactor now properly handles partial block uploads for all operation like retention apply, downsampling and compaction. Additionally:

  * Removed `thanos_compact_sync_meta_*` metrics. Use `thanos_blocks_meta_*` metrics instead.
  * Added `thanos_consistency_delay_seconds` and `thanos_compactor_aborted_partial_uploads_deletion_attempts_total` metrics.

- [#1936](https://github.com/thanos-io/thanos/pull/1936) Store: Improved synchronization of meta JSON files. Store now properly handles corrupted disk cache. Added meta.json sync metrics.
- [#1856](https://github.com/thanos-io/thanos/pull/1856) Receive: close DBReadOnly after flushing to fix a memory leak.
- [#1882](https://github.com/thanos-io/thanos/pull/1882) Receive: upload to object storage as 'receive' rather than 'sidecar'.
- [#1907](https://github.com/thanos-io/thanos/pull/1907) Store: Fixed the duration unit for the metric `thanos_bucket_store_series_gate_duration_seconds`.
- [#1931](https://github.com/thanos-io/thanos/pull/1931) Compact: Fixed the compactor successfully exiting when actually an error occurred while compacting a blocks group.
- [#1872](https://github.com/thanos-io/thanos/pull/1872) Ruler: `/api/v1/rules` now shows a properly formatted value
- [#1945](https://github.com/thanos-io/thanos/pull/1945) `master` container images are now built with Go 1.13
- [#1956](https://github.com/thanos-io/thanos/pull/1956) Ruler: now properly ignores duplicated query addresses
- [#1975](https://github.com/thanos-io/thanos/pull/1975) Store Gateway: fixed panic caused by memcached servers selector when there's 1 memcached node

### Added

- [#1852](https://github.com/thanos-io/thanos/pull/1852) Add support for `AWS_CONTAINER_CREDENTIALS_FULL_URI` by upgrading to minio-go v6.0.44
- [#1854](https://github.com/thanos-io/thanos/pull/1854) Update Rule UI to support alerts count displaying and filtering.
- [#1838](https://github.com/thanos-io/thanos/pull/1838) Ruler: Add TLS and authentication support for Alertmanager with the `--alertmanagers.config` and `--alertmanagers.config-file` CLI flags. See [documentation](docs/components/rule.md/#configuration) for further information.
- [#1838](https://github.com/thanos-io/thanos/pull/1838) Ruler: Add a new `--alertmanagers.sd-dns-interval` CLI option to specify the interval between DNS resolutions of Alertmanager hosts.
- [#1881](https://github.com/thanos-io/thanos/pull/1881) Store Gateway: memcached support for index cache. See [documentation](docs/components/store.md/#index-cache) for further information.
- [#1904](https://github.com/thanos-io/thanos/pull/1904) Add a skip-chunks option in Store Series API to improve the response time of `/api/v1/series` endpoint.
- [#1910](https://github.com/thanos-io/thanos/pull/1910) Query: `/api/v1/labels` now understands `POST` - useful for sending bigger requests

### Changed

- [#1947](https://github.com/thanos-io/thanos/pull/1947) Upgraded Prometheus dependencies to v2.15.2. This includes:

  * Compactor: Significant reduction of memory footprint for compaction and downsampling process.
  * Querier: Accepting spaces between time range and square bracket. e.g `[ 5m]`
  * Querier: Improved PromQL parser performance.

- [#1833](https://github.com/thanos-io/thanos/pull/1833) `--shipper.upload-compacted` flag has been promoted to non hidden, non experimental state. More info available [here](docs/quick-tutorial.md#uploading-old-metrics).
- [#1867](https://github.com/thanos-io/thanos/pull/1867) Ruler: now sets a `Thanos/$version` `User-Agent` in requests
- [#1887](https://github.com/thanos-io/thanos/pull/1887) Service discovery now deduplicates targets between different target groups

## [v0.9.0](https://github.com/thanos-io/thanos/releases/tag/v0.9.0) - 2019.12.03

### Added

- [#1678](https://github.com/thanos-io/thanos/pull/1678) Add Lightstep as a tracing provider.
- [#1687](https://github.com/thanos-io/thanos/pull/1687) Add a new `--grpc-grace-period` CLI option to components which serve gRPC to set how long to wait until gRPC Server shuts down.
- [#1660](https://github.com/thanos-io/thanos/pull/1660) Sidecar: Add a new `--prometheus.ready_timeout` CLI option to the sidecar to set how long to wait until Prometheus starts up.
- [#1573](https://github.com/thanos-io/thanos/pull/1573) `AliYun OSS` object storage, see [documents](docs/storage.md#aliyun-oss) for further information.
- [#1680](https://github.com/thanos-io/thanos/pull/1680) Add a new `--http-grace-period` CLI option to components which serve HTTP to set how long to wait until HTTP Server shuts down.
- [#1712](https://github.com/thanos-io/thanos/pull/1712) Bucket: Rename flag on bucket web component from `--listen` to `--http-address` to match other components.
- [#1733](https://github.com/thanos-io/thanos/pull/1733) Compactor: New metric `thanos_compactor_iterations_total` on Thanos Compactor which shows the number of successful iterations.
- [#1758](https://github.com/thanos-io/thanos/pull/1758) Bucket: `thanos bucket web` now supports `--web.external-prefix` for proxying on a subpath.
- [#1770](https://github.com/thanos-io/thanos/pull/1770) Bucket: Add `--web.prefix-header` flags to allow for bucket UI to be accessible behind a reverse proxy.
- [#1668](https://github.com/thanos-io/thanos/pull/1668) Receiver: Added TLS options for both server and client remote write.

### Fixed

- [#1656](https://github.com/thanos-io/thanos/pull/1656) Store Gateway: Store now starts metric and status probe HTTP server earlier in its start-up sequence. `/-/healthy` endpoint now starts to respond with success earlier. `/metrics` endpoint starts serving metrics earlier as well. Make sure to point your readiness probes to the `/-/ready` endpoint rather than `/metrics`.
- [#1669](https://github.com/thanos-io/thanos/pull/1669) Store Gateway: Fixed store sharding. Now it does not load excluded meta.jsons and load/fetch index-cache.json files.
- [#1670](https://github.com/thanos-io/thanos/pull/1670) Sidecar: Fixed un-ordered blocks upload. Sidecar now uploads the oldest blocks first.
- [#1568](https://github.com/thanos-io/thanos/pull/1709) Store Gateway: Store now retains the first raw value of a chunk during downsampling to avoid losing some counter resets that occur on an aggregation boundary.
- [#1751](https://github.com/thanos-io/thanos/pull/1751) Querier: Fixed labels for StoreUI
- [#1773](https://github.com/thanos-io/thanos/pull/1773) Ruler: Fixed the /api/v1/rules endpoint that returned 500 status code with `failed to assert type of rule ...` message.
- [#1770](https://github.com/thanos-io/thanos/pull/1770) Querier: Fixed `--web.external-prefix` 404s for static resources.
- [#1785](https://github.com/thanos-io/thanos/pull/1785) Ruler: The /api/v1/rules endpoints now returns the original rule filenames.
- [#1791](https://github.com/thanos-io/thanos/pull/1791) Ruler: Ruler now supports identical rule filenames in different directories.
- [#1562](https://github.com/thanos-io/thanos/pull/1562) Querier: Downsampling option now carries through URL.
- [#1675](https://github.com/thanos-io/thanos/pull/1675) Querier: Reduced resource usage while using certain queries like `offset`.
- [#1725](https://github.com/thanos-io/thanos/pull/1725) & [#1718](https://github.com/thanos-io/thanos/pull/1718) Store Gateway: Per request memory improvements.

### Changed

- [#1666](https://github.com/thanos-io/thanos/pull/1666) Compact: `thanos_compact_group_compactions_total` now counts block compactions, so operations that resulted in a compacted block. The old behaviour
is now exposed by new metric: `thanos_compact_group_compaction_runs_started_total` and `thanos_compact_group_compaction_runs_completed_total` which counts compaction runs overall.
- [#1748](https://github.com/thanos-io/thanos/pull/1748) Updated all dependencies.
- [#1694](https://github.com/thanos-io/thanos/pull/1694) `prober_ready` and `prober_healthy` metrics are removed, for sake of `status`. Now `status` exposes same metric with a label, `check`. `check` can have "healty" or "ready" depending on status of the probe.
- [#1790](https://github.com/thanos-io/thanos/pull/1790) Ruler: Fixes subqueries support for ruler.
- [#1769](https://github.com/thanos-io/thanos/pull/1769) & [#1545](https://github.com/thanos-io/thanos/pull/1545) Adjusted most of the metrics histogram buckets.

## [v0.8.1](https://github.com/thanos-io/thanos/releases/tag/v0.8.1) - 2019.10.14

### Fixed

- [#1632](https://github.com/thanos-io/thanos/issues/1632) Removes the duplicated external labels detection on Thanos Querier; warning only; Made Store Gateway compatible with older Querier versions.
  * NOTE: `thanos_store_nodes_grpc_connections` metric is now per `external_labels` and `store_type`. It is a recommended  metric for Querier storeAPIs. `thanos_store_node_info` is marked as obsolete and will be removed in next release.
  * NOTE2: Store Gateway is now advertising artificial: `"@thanos_compatibility_store_type=store"` label. This is to have the current Store Gateway compatible with Querier pre v0.8.0.
  This label can be disabled by hidden `debug.advertise-compatibility-label=false` flag on Store Gateway.

## [v0.8.0](https://github.com/thanos-io/thanos/releases/tag/v0.8.0) - 2019.10.10

Lot's of improvements this release! Noteworthy items:
- First Katacoda tutorial! 🐱
- Fixed Deletion order causing Compactor to produce not needed 👻 blocks with missing random files.
- Store GW memory improvements (more to come!).
- Querier allows multiple deduplication labels.
- Both Compactor and Store Gateway can be **sharded** within the same bucket using relabelling!
- Sidecar exposed data from Prometheus can be now limited to given `min-time` (e.g 3h only).
- Numerous Thanos Receive improvements.

Make sure you check out Prometheus 2.13.0 as well. New release drastically improves usage and resource consumption of
both Prometheus and sidecar with Thanos: https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/

### Added

- [#1619](https://github.com/thanos-io/thanos/pull/1619) Thanos sidecar allows to limit min time range for data it exposes from Prometheus.
- [#1583](https://github.com/thanos-io/thanos/pull/1583) Thanos sharding:
  - Add relabel config (`--selector.relabel-config-file` and `selector.relabel-config`) into Thanos Store and Compact components.
Selecting blocks to serve depends on the result of block labels relabeling.
  - For store gateway, advertise labels from "approved" blocks.
- [#1540](https://github.com/thanos-io/thanos/pull/1540) Thanos Downsample added `/-/ready` and `/-/healthy` endpoints.
- [#1538](https://github.com/thanos-io/thanos/pull/1538) Thanos Rule added `/-/ready` and `/-/healthy` endpoints.
- [#1537](https://github.com/thanos-io/thanos/pull/1537) Thanos Receive added `/-/ready` and `/-/healthy` endpoints.
- [#1460](https://github.com/thanos-io/thanos/pull/1460) Thanos Store Added `/-/ready` and `/-/healthy` endpoints.
- [#1534](https://github.com/thanos-io/thanos/pull/1534) Thanos Query Added `/-/ready` and `/-/healthy` endpoints.
- [#1533](https://github.com/thanos-io/thanos/pull/1533) Thanos inspect now supports the timeout flag.
- [#1496](https://github.com/thanos-io/thanos/pull/1496) Thanos Receive now supports setting block duration.
- [#1362](https://github.com/thanos-io/thanos/pull/1362) Optional `replicaLabels` param for `/query` and
`/query_range` querier endpoints. When provided overwrite the `query.replica-label` cli flags.
- [#1482](https://github.com/thanos-io/thanos/pull/1482) Thanos now supports Elastic APM as tracing provider.
- [#1612](https://github.com/thanos-io/thanos/pull/1612) Thanos Rule added `resendDelay` flag.
- [#1480](https://github.com/thanos-io/thanos/pull/1480) Thanos Receive flushes storage on hashring change.
- [#1613](https://github.com/thanos-io/thanos/pull/1613) Thanos Receive now traces forwarded requests.

### Changed

- [#1362](https://github.com/thanos-io/thanos/pull/1362) `query.replica-label` configuration can be provided more than
once for multiple deduplication labels like: `--query.replica-label=prometheus_replica --query.replica-label=service`.
- [#1581](https://github.com/thanos-io/thanos/pull/1581) Thanos Store now can use smaller buffer sizes for Bytes pool; reducing memory for some requests.
- [#1622](https://github.com/thanos-io/thanos/pull/1622) & [#1590](https://github.com/thanos-io/thanos/pull/1590) Upgraded to Go 1.13.1
- [#1498](https://github.com/thanos-io/thanos/pull/1498) Thanos Receive change flag `labels` to `label` to be consistent with other commands.

### Fixed

- [#1525](https://github.com/thanos-io/thanos/pull/1525) Thanos now deletes block's file in correct order allowing to detect partial blocks without problems.
- [#1505](https://github.com/thanos-io/thanos/pull/1505) Thanos Store now removes invalid local cache blocks.
- [#1587](https://github.com/thanos-io/thanos/pull/1587) Thanos Sidecar cleanups all cache dirs after each compaction run.
- [#1582](https://github.com/thanos-io/thanos/pull/1582) Thanos Rule correctly parses Alertmanager URL if there is more `+` in it.
- [#1544](https://github.com/thanos-io/thanos/pull/1544) Iterating over object store is resilient to the edge case for some providers.
- [#1469](https://github.com/thanos-io/thanos/pull/1469) Fixed Azure potential failures (EOF) when requesting more data then blob has.
- [#1512](https://github.com/thanos-io/thanos/pull/1512) Thanos Store fixed memory leak for chunk pool.
- [#1488](https://github.com/thanos-io/thanos/pull/1488) Thanos Rule now now correctly links to query URL from rules and alerts.

## [v0.7.0](https://github.com/thanos-io/thanos/releases/tag/v0.7.0) - 2019.09.02

Accepted into CNCF:
- Thanos moved to new repository <https://github.com/thanos-io/thanos>
- Docker images moved to <https://quay.io/thanos/thanos> and mirrored at <https://hub.docker.com/r/thanosio/thanos>
- Slack moved to <https://slack.cncf.io> `#thanos`/`#thanos-dev`/`#thanos-prs`

### Added

- [#1478](https://github.com/thanos-io/thanos/pull/1478) Thanos components now exposes gRPC server metrics as soon as server starts, to provide more reliable data for instrumentation.
- [#1378](https://github.com/thanos-io/thanos/pull/1378) Thanos Receive now exposes `thanos_receive_config_hash`, `thanos_receive_config_last_reload_successful` and `thanos_receive_config_last_reload_success_timestamp_seconds` metrics to track latest configuration change
- [#1268](https://github.com/thanos-io/thanos/pull/1268) Thanos Sidecar added support for newest Prometheus streaming remote read added [here](https://github.com/prometheus/prometheus/pull/5703). This massively improves memory required by single
  request for both Prometheus and sidecar. Single requests now should take constant amount of memory on sidecar, so resource consumption prediction is now straightforward. This will be used if you have Prometheus `2.13` or `2.12-master`.
- [#1358](https://github.com/thanos-io/thanos/pull/1358) Added `part_size` configuration option for HTTP multipart requests minimum part size for S3 storage type
- [#1363](https://github.com/thanos-io/thanos/pull/1363) Thanos Receive now exposes `thanos_receive_hashring_nodes` and `thanos_receive_hashring_tenants` metrics to monitor status of hash-rings
- [#1395](https://github.com/thanos-io/thanos/pull/1395) Thanos Sidecar added `/-/ready` and `/-/healthy` endpoints to Thanos sidecar.
- [#1297](https://github.com/thanos-io/thanos/pull/1297) Thanos Compact added `/-/ready` and `/-/healthy` endpoints to Thanos compact.
- [#1431](https://github.com/thanos-io/thanos/pull/1431) Thanos Query added hidden flag to allow the use of downsampled resolution data for instant queries.
- [#1408](https://github.com/thanos-io/thanos/pull/1408) Thanos Store Gateway can now allow the specifying of supported time ranges it will serve (time sharding). Flags: `min-time` & `max-time`

### Changed

- [#1414](https://github.com/thanos-io/thanos/pull/1413) Upgraded important dependencies: Prometheus to 2.12-rc.0. TSDB is now part of Prometheus.
- [#1380](https://github.com/thanos-io/thanos/pull/1380) Upgraded important dependencies: Prometheus to 2.11.1 and TSDB to 0.9.1. Some changes affecting Querier:
  - [ENHANCEMENT] Query performance improvement: Efficient iteration and search in HashForLabels and HashWithoutLabels. #5707
  - [ENHANCEMENT] Optimize queries using regexp for set lookups. tsdb#602
  - [BUGFIX] prometheus_tsdb_compactions_failed_total is now incremented on any compaction failure. tsdb#613
  - [BUGFIX] PromQL: Correctly display {__name__="a"}.
- [#1338](https://github.com/thanos-io/thanos/pull/1338) Thanos Query still warns on store API duplicate, but allows a single one from duplicated set. This is gracefully warn about the problematic logic and not disrupt immediately.
- [#1385](https://github.com/thanos-io/thanos/pull/1385) Thanos Compact exposes flag to disable downsampling `downsampling.disable`.

### Fixed

- [#1327](https://github.com/thanos-io/thanos/pull/1327) Thanos Query `/series` API end-point now properly returns an empty array just like Prometheus if there are no results
- [#1302](https://github.com/thanos-io/thanos/pull/1302) Thanos now efficiently reuses HTTP keep-alive connections
- [#1371](https://github.com/thanos-io/thanos/pull/1371) Thanos Receive fixed race condition in hashring
- [#1430](https://github.com/thanos-io/thanos/pull/1430) Thanos fixed value of GOMAXPROCS inside container.
- [#1410](https://github.com/thanos-io/thanos/pull/1410) Fix for CVE-2019-10215

### Deprecated

- [#1458](https://github.com/thanos-io/thanos/pull/1458) Thanos Query and Receive now use common instrumentation middleware. As as result, for sake of `http_requests_total` and `http_request_duration_seconds_bucket`; Thanos Query no longer exposes `thanos_query_api_instant_query_duration_seconds`, `thanos_query_api_range_query_duration_second` metrics and Thanos Receive no longer exposes `thanos_http_request_duration_seconds`, `thanos_http_requests_total`, `thanos_http_response_size_bytes`.
- [#1423](https://github.com/thanos-io/thanos/pull/1423) Thanos Bench deprecated.

## [v0.6.0](https://github.com/thanos-io/thanos/releases/tag/v0.6.0) - 2019.07.18

### Added

- [#1097](https://github.com/thanos-io/thanos/pull/1097) Added `thanos check rules` linter for Thanos rule rules files.

- [#1253](https://github.com/thanos-io/thanos/pull/1253) Add support for specifying a maximum amount of retries when using Azure Blob storage (default: no retries).

- [#1244](https://github.com/thanos-io/thanos/pull/1244) Thanos Compact now exposes new metrics `thanos_compact_downsample_total` and `thanos_compact_downsample_failures_total` which are useful to catch when errors happen

- [#1260](https://github.com/thanos-io/thanos/pull/1260) Thanos Query/Rule now exposes metrics `thanos_querier_store_apis_dns_provider_results` and `thanos_ruler_query_apis_dns_provider_results` which tell how many addresses were configured and how many were actually discovered respectively

- [#1248](https://github.com/thanos-io/thanos/pull/1248) Add a web UI to show the state of remote storage.

- [#1217](https://github.com/thanos-io/thanos/pull/1217) Thanos Receive gained basic hashring support

- [#1262](https://github.com/thanos-io/thanos/pull/1262) Thanos Receive got a new metric `thanos_http_requests_total` which shows how many requests were handled by it

- [#1243](https://github.com/thanos-io/thanos/pull/1243) Thanos Receive got an ability to forward time series data between nodes. Now you can pass the hashring configuration via `--receive.hashrings-file`; the refresh interval `--receive.hashrings-file-refresh-interval`; the name of the local node's name `--receive.local-endpoint`; and finally the header's name which is used to determine the tenant `--receive.tenant-header`.

- [#1147](https://github.com/thanos-io/thanos/pull/1147) Support for the Jaeger tracer has been added!

*breaking* New common flags were added for configuring tracing: `--tracing.config-file` and `--tracing.config`. You can either pass a file to Thanos with the tracing configuration or pass it in the command line itself. Old `--gcloudtrace.*` flags were removed :warning:

To migrate over the old `--gcloudtrace.*` configuration, your tracing configuration should look like this:

```yaml

---
type: STACKDRIVER
config:
- service_name: 'foo'
  project_id: '123'
  sample_factor: 123
```

The other `type` you can use is `JAEGER` now. The `config` keys and values are Jaeger specific and you can find all of the information [here](https://github.com/jaegertracing/jaeger-client-go#environment-variables).

### Changed

- [#1284](https://github.com/thanos-io/thanos/pull/1284) Add support for multiple label-sets in Info gRPC service.
This deprecates the single `Labels` slice of the `InfoResponse`, in a future release backward compatible handling for the single set of Labels will be removed. Upgrading to v0.6.0 or higher is advised.
*breaking* If you run have duplicate queries in your Querier configuration with hierarchical federation of multiple Queries this PR makes Thanos Querier to detect this case and block all duplicates. Refer to 0.6.1 which at least allows for single replica to work.

- [#1314](https://github.com/thanos-io/thanos/pull/1314) Removes `http_request_duration_microseconds` (Summary) and adds `http_request_duration_seconds` (Histogram) from http server instrumentation used in Thanos APIs and UIs.

- [#1287](https://github.com/thanos-io/thanos/pull/1287) Sidecar now waits on Prometheus' external labels before starting the uploading process

- [#1261](https://github.com/thanos-io/thanos/pull/1261) Thanos Receive now exposes metrics `thanos_http_request_duration_seconds` and `thanos_http_response_size_bytes` properly of each handler

- [#1274](https://github.com/thanos-io/thanos/pull/1274) Iteration limit has been lifted from the LRU cache so there should be no more spam of error messages as they were harmless

- [#1321](https://github.com/thanos-io/thanos/pull/1321) Thanos Query now fails early on a query which only uses external labels - this improves clarity in certain situations

### Fixed

- [#1227](https://github.com/thanos-io/thanos/pull/1227) Some context handling issues were fixed in Thanos Compact; some unnecessary memory allocations were removed in the hot path of Thanos Store.

- [#1183](https://github.com/thanos-io/thanos/pull/1183) Compactor now correctly propagates retriable/haltable errors which means that it will not unnecessarily restart if such an error occurs

- [#1231](https://github.com/thanos-io/thanos/pull/1231) Receive now correctly handles SIGINT and closes without deadlocking

- [#1278](https://github.com/thanos-io/thanos/pull/1278) Fixed inflated values problem with `sum()` on Thanos Query

- [#1280](https://github.com/thanos-io/thanos/pull/1280) Fixed a problem with concurrent writes to a `map` in Thanos Query while rendering the UI

- [#1311](https://github.com/thanos-io/thanos/pull/1311) Fixed occasional panics in Compact and Store when using Azure Blob cloud storage caused by lack of error checking in client library.

- [#1322](https://github.com/thanos-io/thanos/pull/1322) Removed duplicated closing of the gRPC listener - this gets rid of harmless messages like `store gRPC listener: close tcp 0.0.0.0:10901: use of closed network connection` when those programs are being closed

### Deprecated

- [#1216](https://github.com/thanos-io/thanos/pull/1216) the old "Command-line flags" has been removed from Thanos Query UI since it was not populated and because we are striving for consistency

## [v0.5.0](https://github.com/thanos-io/thanos/releases/tag/v0.5.0) - 2019.06.05

TL;DR: Store LRU cache is no longer leaking, Upgraded Thanos UI to Prometheus 2.9, Fixed auto-downsampling, Moved to Go 1.12.5 and more.

This version moved tarballs to Golang 1.12.5 from 1.11 as well, so same warning applies if you use `container_memory_usage_bytes` from cadvisor. Use `container_memory_working_set_bytes` instead.

*breaking* As announced couple of times this release also removes gossip with all configuration flags (`--cluster.*`).

### Fixed

- [#1142](https://github.com/thanos-io/thanos/pull/1142) fixed major leak on store LRU cache for index items (postings and series).
- [#1163](https://github.com/thanos-io/thanos/pull/1163) sidecar is no longer blocking for custom Prometheus versions/builds. It only checks if flags return non 404, then it performs optional checks.
- [#1146](https://github.com/thanos-io/thanos/pull/1146) store/bucket: make getFor() work with interleaved resolutions.
- [#1157](https://github.com/thanos-io/thanos/pull/1157) querier correctly handles duplicated stores when some store changes external labels in place.

### Added

- [#1094](https://github.com/thanos-io/thanos/pull/1094) Allow configuring the response header timeout for the S3 client.

### Changed

- [#1118](https://github.com/thanos-io/thanos/pull/1118) *breaking* swift: Added support for cross-domain authentication by introducing `userDomainID`, `userDomainName`, `projectDomainID`, `projectDomainName`.
  The outdated terms `tenantID`, `tenantName` are deprecated and have been replaced by `projectID`, `projectName`.

- [#1066](https://github.com/thanos-io/thanos/pull/1066) Upgrade Thanos ui to Prometheus v2.9.1.

  Changes from the upstream:
  * query:
    - [ENHANCEMENT] Update moment.js and moment-timezone.js [PR #4679](https://github.com/prometheus/prometheus/pull/4679)
    - [ENHANCEMENT] Support to query elements by a specific time [PR #4764](https://github.com/prometheus/prometheus/pull/4764)
    - [ENHANCEMENT] Update to Bootstrap 4.1.3 [PR #5192](https://github.com/prometheus/prometheus/pull/5192)
    - [BUGFIX] Limit number of merics in prometheus UI [PR #5139](https://github.com/prometheus/prometheus/pull/5139)
    - [BUGFIX] Web interface Quality of Life improvements [PR #5201](https://github.com/prometheus/prometheus/pull/5201)
  * rule:
    - [ENHANCEMENT] Improve rule views by wrapping lines [PR #4702](https://github.com/prometheus/prometheus/pull/4702)
    - [ENHANCEMENT] Show rule evaluation errors on rules page [PR #4457](https://github.com/prometheus/prometheus/pull/4457)

- [#1156](https://github.com/thanos-io/thanos/pull/1156) Moved CI and docker multistage to Golang 1.12.5 for latest mem alloc improvements.
- [#1103](https://github.com/thanos-io/thanos/pull/1103) Updated go-cos deps. (COS bucket client).
- [#1149](https://github.com/thanos-io/thanos/pull/1149) Updated google Golang API deps (GCS bucket client).
- [#1190](https://github.com/thanos-io/thanos/pull/1190) Updated minio deps (S3 bucket client). This fixes minio retries.

- [#1133](https://github.com/thanos-io/thanos/pull/1133) Use prometheus v2.9.2, common v0.4.0 & tsdb v0.8.0.

  Changes from the upstreams:
  * store gateway:
    - [ENHANCEMENT] Fast path for EmptyPostings cases in Merge, Intersect and Without.
  * store gateway & compactor:
    - [BUGFIX] Fix fd and vm_area leak on error path in chunks.NewDirReader.
    - [BUGFIX] Fix fd and vm_area leak on error path in index.NewFileReader.
  * query:
    - [BUGFIX] Make sure subquery range is taken into account for selection #5467
    - [ENHANCEMENT] Check for cancellation on every step of a range evaluation. #5131
    - [BUGFIX] Exponentation operator to drop metric name in result of operation. #5329
    - [BUGFIX] Fix output sample values for scalar-to-vector comparison operations. #5454
  * rule:
    - [BUGFIX] Reload rules: copy state on both name and labels. #5368

## Deprecated

- [#1008](https://github.com/thanos-io/thanos/pull/1008) *breaking* Removed Gossip implementation. All `--cluster.*` flags removed and Thanos will error out if any is provided.

## [v0.4.0](https://github.com/thanos-io/thanos/releases/tag/v0.4.0) - 2019.05.3

:warning: **IMPORTANT** :warning: This is the last release that supports gossip. From Thanos v0.5.0, gossip will be completely removed.

This release also disables gossip mode by default for all components.
See [this](docs/proposals/201809_gossip-removal.md) for more details.

:warning: This release moves Thanos docker images (NOT artifacts by accident) to Golang 1.12. This release includes change in GC's memory release which gives following effect (source: https://golang.org/doc/go1.12):

> On Linux, the runtime now uses MADV_FREE to release unused memory. This is more efficient but may result in higher reported RSS. The kernel will reclaim the unused data when it is needed. To revert to the Go 1.11 behavior (MADV_DONTNEED), set the environment variable GODEBUG=madvdontneed=1.

If you want to see exact memory allocation of Thanos process:
* Use `go_memstats_heap_alloc_bytes` metric exposed by Golang or `container_memory_working_set_bytes` exposed by cadvisor.
* Add `GODEBUG=madvdontneed=1` before running Thanos binary to revert to memory releasing to pre 1.12 logic.

Using cadvisor `container_memory_usage_bytes` metric could be misleading e.g: https://github.com/google/cadvisor/issues/2242

### Added

- [thanos.io](https://thanos.io) website & automation :tada:
- [#1053](https://github.com/thanos-io/thanos/pull/1053) compactor: Compactor & store gateway now handles incomplete uploads gracefully. Added hard limit on how long block upload can take (30m).
- [#811](https://github.com/thanos-io/thanos/pull/811) Remote write receiver component :heart: :heart: thanks to RedHat (@brancz) contribution.
- [#910](https://github.com/thanos-io/thanos/pull/910) Query's stores UI page is now sorted by type and old DNS or File SD stores are removed after 5 minutes (configurable via the new `--store.unhealthy-timeout=5m` flag).
- [#905](https://github.com/thanos-io/thanos/pull/905) Thanos support for Query API: /api/v1/labels. Notice that the API was added in Prometheus v2.6.
- [#798](https://github.com/thanos-io/thanos/pull/798) Ability to limit the maximum number of concurrent request to Series() calls in Thanos Store and the maximum amount of samples we handle.
- [#1060](https://github.com/thanos-io/thanos/pull/1060) Allow specifying region attribute in S3 storage configuration

:warning: **WARNING** :warning: #798 adds a new default limit to Thanos Store: `--store.grpc.series-max-concurrency`. Most likely you will want to make it the same as `--query.max-concurrent` on Thanos Query.

New options:

  New Store flags:

    * `--store.grpc.series-sample-limit` limits the amount of samples that might be retrieved on a single Series() call. By default it is 0. Consider enabling it by setting it to more than 0 if you are running on limited resources.
    * `--store.grpc.series-max-concurrency` limits the number of concurrent Series() calls in Thanos Store. By default it is 20. Considering making it lower or bigger depending on the scale of your deployment.

  New Store metrics:

    * `thanos_bucket_store_queries_dropped_total` shows how many queries were dropped due to the samples limit;
    * `thanos_bucket_store_queries_concurrent_max` is a constant metric which shows how many Series() calls can concurrently be executed by Thanos Store;
    * `thanos_bucket_store_queries_in_flight` shows how many queries are currently "in flight" i.e. they are being executed;
    * `thanos_bucket_store_gate_duration_seconds` shows how many seconds it took for queries to pass through the gate in both cases - when that fails and when it does not.

  New Store tracing span:
    * `store_query_gate_ismyturn` shows how long it took for a query to pass (or not) through the gate.

- [#1016](https://github.com/thanos-io/thanos/pull/1016) Added option for another DNS resolver (miekg/dns client).
Note that this is required to have SRV resolution working on [Golang 1.11+ with KubeDNS below v1.14](https://github.com/golang/go/issues/27546)

   New Querier and Ruler flag: `-- store.sd-dns-resolver` which allows to specify resolver to use. Either `golang` or `miekgdns`

- [#986](https://github.com/thanos-io/thanos/pull/986) Allow to save some startup & sync time in store gateway as it is no longer needed to compute index-cache from block index on its own for larger blocks.
  The store Gateway still can do it, but it first checks bucket if there is index-cached uploaded already.
  In the same time, compactor precomputes the index cache file on every compaction.

  New Compactor flag: `--index.generate-missing-cache-file` was added to allow quicker addition of index cache files. If enabled it precomputes missing files on compactor startup. Note that it will take time and it's only one-off step per bucket.

- [#887](https://github.com/thanos-io/thanos/pull/887) Compact: Added new `--block-sync-concurrency` flag, which allows you to configure number of goroutines to use when syncing block metadata from object storage.
- [#928](https://github.com/thanos-io/thanos/pull/928) Query: Added `--store.response-timeout` flag. If a Store doesn't send any data in this specified duration then a Store will be ignored and partial data will be returned if it's enabled. 0 disables timeout.
- [#893](https://github.com/thanos-io/thanos/pull/893) S3 storage backend has graduated to `stable` maturity level.
- [#936](https://github.com/thanos-io/thanos/pull/936) Azure storage backend has graduated to `stable` maturity level.
- [#937](https://github.com/thanos-io/thanos/pull/937) S3: added trace functionality. You can add `trace.enable: true` to enable the minio client's verbose logging.
- [#953](https://github.com/thanos-io/thanos/pull/953) Compact: now has a hidden flag `--debug.accept-malformed-index`. Compaction index verification will ignore out of order label names.
- [#963](https://github.com/thanos-io/thanos/pull/963) GCS: added possibility to inline ServiceAccount into GCS config.
- [#1010](https://github.com/thanos-io/thanos/pull/1010) Compact: added new flag `--compact.concurrency`. Number of goroutines to use when compacting groups.
- [#1028](https://github.com/thanos-io/thanos/pull/1028) Query: added `--query.default-evaluation-interval`, which sets default evaluation interval for sub queries.
- [#980](https://github.com/thanos-io/thanos/pull/980) Ability to override Azure storage endpoint for other regions (China)
- [#1021](https://github.com/thanos-io/thanos/pull/1021) Query API `series` now supports POST method.
- [#939](https://github.com/thanos-io/thanos/pull/939) Query API `query_range` now supports POST method.

### Changed

- [#970](https://github.com/thanos-io/thanos/pull/970) Deprecated `partial_response_disabled` proto field. Added `partial_response_strategy` instead. Both in gRPC and Query API.
  No `PartialResponseStrategy` field for `RuleGroups` by default means `abort` strategy (old PartialResponse disabled) as this is recommended option for Rules and alerts.

  Metrics:

    * Added `thanos_rule_evaluation_with_warnings_total` to Ruler.
    * DNS `thanos_ruler_query_apis*` are now `thanos_ruler_query_apis_*` for consistency.
    * DNS `thanos_querier_store_apis*` are now `thanos_querier_store_apis__*` for consistency.
    * Query Gate `thanos_bucket_store_series*` are now `thanos_bucket_store_series_*` for consistency.
    * Most of thanos ruler metris related to rule manager has `strategy` label.

  Ruler tracing spans:

    * `/rule_instant_query HTTP[client]` is now `/rule_instant_query_part_resp_abort HTTP[client]"` if request is for abort strategy.

- [#1009](https://github.com/thanos-io/thanos/pull/1009): Upgraded Prometheus (~v2.7.0-rc.0 to v2.8.1)  and TSDB (`v0.4.0` to `v0.6.1`) deps.

  Changes that affects Thanos:
   * query:
     * [ENHANCEMENT] In histogram_quantile merge buckets with equivalent le values. #5158.
     * [ENHANCEMENT] Show list of offending labels in the error message in many-to-many scenarios. #5189
     * [BUGFIX] Fix panic when aggregator param is not a literal. #5290
   * ruler:
     * [ENHANCEMENT] Reduce time that Alertmanagers are in flux when reloaded. #5126
     * [BUGFIX] prometheus_rule_group_last_evaluation_timestamp_seconds is now a unix timestamp. #5186
     * [BUGFIX] prometheus_rule_group_last_duration_seconds now reports seconds instead of nanoseconds. Fixes our [issue #1027](https://github.com/thanos-io/thanos/issues/1027)
     * [BUGFIX] Fix sorting of rule groups. #5260
   * store: [ENHANCEMENT] Fast path for EmptyPostings cases in Merge, Intersect and Without.
   * tooling: [FEATURE] New dump command to tsdb tool to dump all samples.
   * compactor:
      * [ENHANCEMENT] When closing the db any running compaction will be cancelled so it doesn't block.
      * [CHANGE] *breaking* Renamed flag `--sync-delay` to `--consistency-delay` [#1053](https://github.com/thanos-io/thanos/pull/1053)

  For ruler essentially whole TSDB CHANGELOG applies between v0.4.0-v0.6.1: https://github.com/prometheus/tsdb/blob/master/CHANGELOG.md

  Note that this was added on TSDB and Prometheus: [FEATURE] Time-ovelapping blocks are now allowed. #370
  Whoever due to nature of Thanos compaction (distributed systems), for safety reason this is disabled for Thanos compactor for now.

- [#868](https://github.com/thanos-io/thanos/pull/868) Go has been updated to 1.12.
- [#1055](https://github.com/thanos-io/thanos/pull/1055) Gossip flags are now disabled by default and deprecated.
- [#964](https://github.com/thanos-io/thanos/pull/964) repair: Repair process now sorts the series and labels within block.
- [#1073](https://github.com/thanos-io/thanos/pull/1073) Store: index cache for requests. It now calculates the size properly (includes slice header), has anti-deadlock safeguard and reports more metrics.

### Fixed

- [#921](https://github.com/thanos-io/thanos/pull/921) `thanos_objstore_bucket_last_successful_upload_time` now does not appear when no blocks have been uploaded so far.
- [#966](https://github.com/thanos-io/thanos/pull/966) Bucket: verify no longer warns about overlapping blocks, that overlap `0s`
- [#848](https://github.com/thanos-io/thanos/pull/848) Compact: now correctly works with time series with duplicate labels.
- [#894](https://github.com/thanos-io/thanos/pull/894) Thanos Rule: UI now correctly shows evaluation time.
- [#865](https://github.com/thanos-io/thanos/pull/865) Query: now properly parses DNS SRV Service Discovery.
- [#889](https://github.com/thanos-io/thanos/pull/889) Store: added safeguard against merging posting groups segfault
- [#941](https://github.com/thanos-io/thanos/pull/941) Sidecar: added better handling of intermediate restarts.
- [#933](https://github.com/thanos-io/thanos/pull/933) Query: Fixed 30 seconds lag of adding new store to query.
- [#962](https://github.com/thanos-io/thanos/pull/962) Sidecar: Make config reloader file writes atomic.
- [#982](https://github.com/thanos-io/thanos/pull/982) Query: now advertises Min & Max Time accordingly to the nodes.
- [#1041](https://github.com/thanos-io/thanos/issues/1038) Ruler is now able to return long time range queries.
- [#904](https://github.com/thanos-io/thanos/pull/904) Compact: Skip compaction for blocks with no samples.
- [#1070](https://github.com/thanos-io/thanos/pull/1070) Downsampling works back again. Deferred closer errors are now properly captured.

## [v0.3.2](https://github.com/thanos-io/thanos/releases/tag/v0.3.2) - 2019.03.04

### Added

- [#851](https://github.com/thanos-io/thanos/pull/851) New read API endpoint for api/v1/rules and api/v1/alerts.
- [#873](https://github.com/thanos-io/thanos/pull/873) Store: fix set index cache LRU

:warning: **WARNING** :warning: #873 fix fixes actual handling of `index-cache-size`. Handling of limit for this cache was
broken so it was unbounded all the time. From this release actual value matters and is extremely low by default. To "revert"
the old behaviour (no boundary), use a large enough value.

### Fixed

- [#833](https://github.com/thanos-io/thanos/issues/833) Store Gateway matcher regression for intersecting with empty posting.
- [#867](https://github.com/thanos-io/thanos/pull/867) Fixed race condition in sidecare between reloader and shipper.

## [v0.3.1](https://github.com/thanos-io/thanos/releases/tag/v0.3.1) - 2019.02.18

### Fixed

- [#829](https://github.com/thanos-io/thanos/issues/829) Store Gateway crashing due to `slice bounds out of range`.
- [#834](https://github.com/thanos-io/thanos/issues/834) Store Gateway matcher regression for `<>` `!=`.


## [v0.3.0](https://github.com/thanos-io/thanos/releases/tag/v0.3.0) - 2019.02.08

### Added

- Support for gzip compressed configuration files before envvar substitution for reloader package.
- `bucket inspect` command for better insights on blocks in object storage.
- Support for [Tencent COS](docs/storage.md#tencent-cos-configuration) object storage.
- Partial Response disable option for StoreAPI and QueryAPI.
- Partial Response disable button on Thanos UI
- We have initial docs for goDoc documentation!
- Flags for Querier and Ruler UIs: `--web.route-prefix`, `--web.external-prefix`, `--web.prefix-header`. Details [here](docs/components/query.md#expose-ui-on-a-sub-path)

### Fixed

- [#649](https://github.com/thanos-io/thanos/issues/649) - Fixed store label values api to add also external label values.
- [#396](https://github.com/thanos-io/thanos/issues/396) - Fixed sidecar logic for proxying series that has more than 2^16 samples from Prometheus.
- [#732](https://github.com/thanos-io/thanos/pull/732) - Fixed S3 authentication sequence. You can see new sequence enumerated [here](https://github.com/thanos-io/thanos/blob/master/docs/storage.md#aws-s3-configuration)
- [#745](https://github.com/thanos-io/thanos/pull/745) - Fixed race conditions and edge cases for Thanos Querier fanout logic.
- [#651](https://github.com/thanos-io/thanos/issues/651) - Fixed index cache when asked buffer size is bigger than cache max size.

### Changed

- [#529](https://github.com/thanos-io/thanos/pull/529) Massive improvement for compactor. Downsampling memory consumption was reduce to only store labels and single chunks per each series.
- Qurerier UI: Store page now shows the store APIs per component type.
- Prometheus and TSDB deps are now up to date with ~2.7.0 Prometheus version. Lot's of things has changed. See details [here #704](https://github.com/thanos-io/thanos/pull/704) Known changes that affects us:
    - prometheus/prometheus/discovery/file
      - [ENHANCEMENT] Discovery: Improve performance of previously slow updates of changes of targets. #4526
      - [BUGFIX] Wait for service discovery to stop before exiting #4508 ??
    - prometheus/prometheus/promql:
      - **[ENHANCEMENT] Subqueries support. #4831**
      - [BUGFIX] PromQL: Fix a goroutine leak in the lexer/parser. #4858
      - [BUGFIX] Change max/min over_time to handle NaNs properly. #438
      - [BUGFIX] Check label name for `count_values` PromQL function. #4585
      - [BUGFIX] Ensure that vectors and matrices do not contain identical label-sets. #4589
      - [ENHANCEMENT] Optimize PromQL aggregations #4248
      - [BUGFIX] Only add LookbackDelta to vector selectors #4399
      - [BUGFIX] Reduce floating point errors in stddev and related functions #4533
    - prometheus/prometheus/rules:
      - New metrics exposed! (prometheus evaluation!)
      - [ENHANCEMENT] Rules: Error out at load time for invalid templates, rather than at evaluation time. #4537
    - prometheus/tsdb/index: Index reader optimizations.
- Thanos store gateway flag for sync concurrency (`block-sync-concurrency` with `20` default, so no change by default)
- S3 provider:
  - Added `put_user_metadata` option to config.
  - Added `insecure_skip_verify` option to config.

### Deprecated

- Tests against Prometheus below v2.2.1. This does not mean *lack* of support for those. Only that we don't tests the compatibility anymore. See [#758](https://github.com/thanos-io/thanos/issues/758) for details.

## [v0.2.1](https://github.com/thanos-io/thanos/releases/tag/v0.2.1) - 2018.12.27

### Added

- Relabel drop for Thanos Ruler to enable replica label drop and alert deduplication on AM side.
- Query: Stores UI page available at `/stores`.

![](./docs/img/query_ui_stores.png)

### Fixed

- Thanos Rule Alertmanager DNS SD bug.
- DNS SD bug when having SRV results with different ports.
- Move handling of HA alertmanagers to be the same as Prometheus.
- Azure iteration implementation flaw.

## [v0.2.0](https://github.com/thanos-io/thanos/releases/tag/v0.2.0) - 2018.12.10

Next Thanos release adding support to new discovery method, gRPC mTLS and two new object store providers (Swift and Azure).

Note lots of necessary breaking changes in flags that relates to bucket configuration.

### Deprecated

- *breaking*: Removed all bucket specific flags as we moved to config files:
    - --gcs-bucket=\<bucket\>
    - --s3.bucket=\<bucket\>
    - --s3.endpoint=\<api-url\>
    - --s3.access-key=\<key\>
    - --s3.insecure
    - --s3.signature-version2
    - --s3.encrypt-sse
    - --gcs-backup-bucket=\<bucket\>
    - --s3-backup-bucket=\<bucket\>
- *breaking*: Removed support of those environment variables for bucket:
    * S3_BUCKET
    * S3_ENDPOINT
    * S3_ACCESS_KEY
    * S3_INSECURE
    * S3_SIGNATURE_VERSION2
- *breaking*: Removed provider specific bucket metrics e.g `thanos_objstore_gcs_bucket_operations_total` in favor of of generic bucket operation metrics.

### Changed

- *breaking*: Added `thanos_` prefix to memberlist (gossip) metrics. Make sure to update your dashboards and rules.
- S3 provider:
  - Set `"X-Amz-Acl": "bucket-owner-full-control"` metadata for s3 upload operation.

### Added

- Support for heterogeneous secure gRPC on StoreAPI.
- Handling of scalar result in rule node evaluating rules.
- Flag `--objstore.config-file` to reference to the bucket configuration file in yaml format. Detailed information can be found in document [storage](docs/storage.md).
- File service discovery for StoreAPIs:
- In `thanos rule`, static configuration of query nodes via `--query`
- In `thanos rule`, file based discovery of query nodes using `--query.file-sd-config.files`
- In `thanos query`, file based discovery of store nodes using `--store.file-sd-config.files`
- `/-/healthy` endpoint to Querier.
- DNS service discovery to static and file based configurations using the `dns+` and `dnssrv+` prefixes for the respective lookup. Details [here](docs/service-discovery.md)
- `--cluster.disable` flag to disable gossip functionality completely.
- Hidden flag to configure max compaction level.
- Azure Storage.
- OpenStack Swift support.
- Thanos Ruler `thanos_rule_loaded_rules` metric.
- Option for JSON logger format.

### Fixed

- Issue whereby the Proxy Store could end up in a deadlock if there were more than 9 stores being queried and all returned an error.
- Ruler tracing causing panics.
- GatherIndexStats panics on duplicated chunks check.
- Clean up of old compact blocks on compact restart.
- Sidecar too frequent Prometheus reload.
- `thanos_compactor_retries_total` metric not being registered.

## [v0.1.0](https://github.com/thanos-io/thanos/releases/tag/v0.1.0) - 2018.09.14

Initial version to have a stable reference before [gossip protocol removal](/docs/proposals/201809_gossip-removal.md).

### Added

- Gossip layer for all components.
- StoreAPI gRPC proto.
- TSDB block upload logic for Sidecar.
- StoreAPI logic for Sidecar.
- Config and rule reloader logic for Sidecar.
- On-the fly result merge and deduplication logic for Querier.
- Custom Thanos UI (based mainly on Prometheus UI) for Querier.
- Optimized object storage fetch logic for Store.
- Index cache and chunk pool for Store for better memory usage.
- Stable support for Google Cloud Storage object storage.
- StoreAPI logic for Querier to support Thanos federation (experimental).
- Support for S3 minio-based AWS object storage (experimental).
- Compaction logic of blocks from multiple sources for Compactor.
- Optional Compaction fixed retention.
- Optional downsampling logic for Compactor (experimental).
- Rule (including alerts) evaluation logic for Ruler.
- Rule UI with hot rules reload.
- StoreAPI logic for Ruler.
- Basic metric orchestration for all components.
- Verify commands with potential fixes (experimental).
- Compact / Downsample offline commands.
- Bucket commands.
- Downsampling support for UI.
- Grafana dashboards for Thanos components.
