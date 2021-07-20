---
title: Instrumenting Thanos
type: docs
menu: thanos
---

# Instrumenting Thanos
We use the Prometheus [Go Client](https://github.com/prometheus/client_golang) library to instrument Thanos itself. Following are the metrics exposed by thanos:

```bash
TYPE                NAME                                                                 HELP
GAUGE               thanos_delete_delay_seconds                                          Configured delete delay in seconds.
GAUGE               thanos_compact_halted                                                Set to 1 if the compactor halted due to an unexpected error.
COUNTER             thanos_compact_retries_total                                         Total number of retries after retriable compactor error.
COUNTER             thanos_compact_iterations_total                                      Total number of iterations that were executed successfully.
COUNTER             thanos_compact_block_cleanup_loops_total                             Total number of concurrent cleanup loops of partially uploaded blocks and marked blocks that were executed successfully.
COUNTER             thanos_compact_aborted_partial_uploads_deletion_attempts_total       Total number of started deletions of blocks that are assumed aborted and only partially uploaded.
COUNTER             thanos_compact_blocks_cleaned_total                                  Total number of blocks deleted in compactor.
COUNTER             thanos_compact_block_cleanup_failures_total                          Failures encountered while deleting blocks in compactor.
COUNTER             thanos_compact_blocks_marked_total                                   Total number of blocks marked in compactor.
COUNTER             thanos_compact_garbage_collected_blocks_total                        Total number of blocks marked for deletion by compactor.
COUNTER             thanos_compact_downsample_total                                      Total number of downsampling attempts.
COUNTER             thanos_compact_downsample_failures_total                             Total number of failed downsampling attempts.
COUNTER             thanos_query_duplicated_store_addresses_total                        The number of times a duplicated store addresses is detected from the different configs in query
COUNTER             thanos_receive_multi_db_updates_attempted_total                      Number of Multi DB attempted reloads with flush and potential upload due to hashring changes
COUNTER             thanos_receive_multi_db_updates_completed_total                      Number of Multi DB completed reloads with flush and potential upload due to hashring changes
GAUGE               thanos_rule_config_last_reload_successful                            Whether the last configuration reload attempt was successful.
GAUGE               thanos_rule_config_last_reload_success_timestamp_seconds             Timestamp of the last successful configuration reload.
COUNTER             thanos_rule_duplicated_query_addresses_total                         The number of times a duplicated query addresses is detected from the different configs in rule.
GAUGE               thanos_rule_loaded_rules                                             Loaded rules partitioned by file and group.
COUNTER             thanos_rule_evaluation_with_warnings_total                           The total number of rule evaluation that were successful but had warnings which can indicate partial error.
GAUGE               thanos_sidecar_prometheus_up                                         Boolean indicator whether the sidecar can reach its Prometheus peer.
GAUGE               thanos_sidecar_last_heartbeat_success_time_seconds                   Timestamp of the last successful heartbeat in seconds.
COUNTER             thanos_alert_queue_alerts_dropped_total                              Total number of alerts that were dropped from the queue.
COUNTER             thanos_alert_queue_alerts_pushed_total                               Total number of alerts pushed to the queue.
COUNTER             thanos_alert_queue_alerts_popped_total                               Total number of alerts popped from the queue.
GAUGE               thanos_alert_queue_capacity                                          Capacity of the alert queue.
GAUGE               thanos_alert_queue_length                                            Length of the alert queue.
COUNTER             thanos_alert_sender_alerts_sent_total                                Total number of alerts sent by alertmanager.
COUNTER             thanos_alert_sender_errors_total                                     Total number of errors while sending alerts to alertmanager.
COUNTER             thanos_alert_sender_alerts_dropped_total                             Total number of alerts dropped in case of all sends to alertmanagers failed.
HISTOGRAM           thanos_alert_sender_latency_seconds                                  Latency for sending alert notifications (not including dropped notifications).
HISTOGRAM           thanos_query_range_requested_timespan_duration_seconds               A histogram of the query range window in seconds
HISTOGRAM           blocks_meta_sync_duration_seconds                                    Duration of the blocks metadata synchronization in seconds
COUNTER             blocks_meta_base_syncs_total                                         Total blocks metadata synchronization attempts by base Fetcher
GAUGE               consistency_delay_seconds                                            Configured consistency delay in seconds.
COUNTER             blocks_meta_syncs_total                                              Total blocks metadata synchronization attempts
COUNTER             blocks_meta_sync_failures_total                                      Total blocks metadata synchronization failures
COUNTER             indexheader_lazy_load_total                                          Total number of index-header lazy load operations.
COUNTER             indexheader_lazy_load_failed_total                                   Total number of failed index-header lazy load operations.
COUNTER             indexheader_lazy_unload_total                                        Total number of index-header lazy unload operations.
COUNTER             indexheader_lazy_unload_failed_total                                 Total number of failed index-header lazy unload operations.
HISTOGRAM           indexheader_lazy_load_duration_seconds                               Duration of the index-header lazy loading in seconds.
COUNTER             thanos_cache_inmemory_items_evicted_total                            Total number of items that were evicted from the inmemory cache.
COUNTER             thanos_cache_inmemory_items_added_total                              Total number of items that were added to the inmemory cache.
COUNTER             thanos_cache_inmemory_requests_total                                 Total number of requests to the inmemory cache.
COUNTER             thanos_cache_inmemory_hits_on_expired_data_total                     Total number of requests to the inmemory cache that were a hit but needed to be evicted due to TTL.
COUNTER             thanos_cache_inmemory_items_overflowed_total                         Total number of items that could not be added to the inmemory cache due to being too big.
COUNTER             thanos_cache_inmemory_hits_total                                     Total number of requests to the inmemory cache that were a hit.
GAUGE               thanos_cache_inmemory_items                                          Current number of items in the inmemory cache.
GAUGE               thanos_cache_inmemory_items_size_bytes                               Current byte size of items in the inmemory cache.
GAUGE               thanos_cache_inmemory_total_size_bytes                               Current byte size of items (both value and key) in the inmemory cache.
GAUGE               thanos_cache_inmemory_max_size_bytes                                 Maximum number of bytes to be held in the inmemory cache.
GAUGE               thanos_cache_inmemory_max_item_size_bytes                            Maximum number of bytes for single entry to be held in the inmemory cache.
COUNTER             thanos_cache_memcached_requests_total                                Total number of items requests to memcached.
COUNTER             thanos_cache_memcached_hits_total                                    Total number of items requests to the cache that were a hit.
GAUGE               thanos_memcached_client_info                                         A metric with a constant '1' value labeled by configuration options from which memcached client was configured.
COUNTER             thanos_memcached_operations_total                                    Total number of operations against memcached.
COUNTER             thanos_memcached_operation_failures_total                            Total number of operations against memcached that failed.
COUNTER             thanos_memcached_operation_skipped_total                             Total number of operations against memcached that have been skipped.
HISTOGRAM           thanos_memcached_operation_duration_seconds                          Duration of operations against memcached.
HISTOGRAM           thanos_memcached_operation_data_size_bytes                           Tracks the size of the data stored in and fetched from memcached.
COUNTER             thanos_compact_group_compactions_total                               Total number of group compaction attempts that resulted in a new block.
COUNTER             thanos_compact_group_compaction_runs_started_total                   Total number of group compaction attempts.
COUNTER             thanos_compact_group_compaction_runs_completed_total                 Total number of group completed compaction runs. This also includes compactor group runs that resulted with no compaction.
COUNTER             thanos_compact_group_compactions_failures_total                      Total number of failed group compactions.
COUNTER             thanos_compact_group_vertical_compactions_total                      Total number of group compaction attempts that resulted in a new block based on overlapping blocks.
COUNTER             thanos_compact_garbage_collection_total                              Total number of garbage collection operations.
COUNTER             thanos_compact_garbage_collection_failures_total                     Total number of failed garbage collection operations.
HISTOGRAM           thanos_compact_garbage_collection_duration_seconds                   Time it took to perform garbage collection iteration.
COUNTER             dns_lookups_total                                                    The number of DNS lookups resolutions attempts
COUNTER             dns_failures_total                                                   The number of DNS lookup failures
HISTOGRAM           http_request_duration_seconds                                        Tracks the latencies for HTTP requests.
SUMMARY             http_request_size_bytes                                              Tracks the size of HTTP requests.
COUNTER             http_requests_total                                                  Tracks the number of HTTP requests.
SUMMARY             http_response_size_bytes                                             Tracks the size of HTTP responses.
COUNTER             thanos_objstore_bucket_operations_total                              Total number of all attempted operations against a bucket.
COUNTER             thanos_objstore_bucket_operation_failures_total                      Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
HISTOGRAM           thanos_objstore_bucket_operation_duration_seconds                    Duration of successful operations against the bucket
GAUGE               thanos_objstore_bucket_last_successful_upload_time                   Second timestamp of the last successful upload to the bucket.
GAUGE               status                                                               Represents status (0 indicates failure, 1 indicates success) of the component.
GAUGE               thanos_store_node_info                                               Number of nodes with the same external labels identified by their hash. If any time-series is larger than 1, external label uniqueness is not true
GAUGE               thanos_store_nodes_grpc_connections                                  Number indicating current number of gRPC connection to store nodes. This indicates also to how many stores query node have access to.
COUNTER             thanos_frontend_downsampled_extra_queries_total                      Total number of additional queries for downsampled data
COUNTER             thanos_query_frontend_queries_total                                  Total queries passing through query frontend
COUNTER             thanos_frontend_split_queries_total                                  Total number of underlying query requests after the split by interval is applied
COUNTER             thanos_receive_hashrings_file_errors_total                           The number of errors watching the hashrings configuration file.
COUNTER             thanos_receive_hashrings_file_refreshes_total                        The number of refreshes of the hashrings configuration file.
GAUGE               thanos_receive_hashring_nodes                                        The number of nodes per hashring.
GAUGE               thanos_receive_hashring_tenants                                      The number of tenants per hashring.
GAUGE               thanos_receive_config_hash                                           Hash of the currently loaded hashring configuration file.
GAUGE               thanos_receive_config_last_reload_successful                         Whether the last hashring configuration file reload attempt was successful.
GAUGE               thanos_receive_config_last_reload_success_timestamp_seconds          Timestamp of the last successful hashring configuration file reload.
COUNTER             thanos_receive_hashrings_file_changes_total                          The number of times the hashrings configuration file has changed.
COUNTER             thanos_receive_forward_requests_total                                The number of forward requests.
COUNTER             thanos_receive_replications_total                                    The number of replication operations done by the receiver. The success of replication is fulfilled when a quorum is met.
GAUGE               thanos_receive_replication_factor                                    The number of times to replicate incoming write requests.
COUNTER             reloader_reloads_total                                               Total number of reload requests.
COUNTER             reloader_reloads_failed_total                                        Total number of reload requests that failed.
COUNTER             reloader_config_apply_operations_total                               Total number of config apply operations.
COUNTER             reloader_config_apply_operations_failed_total                        Total number of config apply operations that failed.
GAUGE               reloader_watches                                                     Number of resources watched by the reloader.
COUNTER             reloader_watch_events_total                                          Total number of events received by the reloader from the watcher.
COUNTER             reloader_watch_errors_total                                          Total number of errors received by the reloader from the watcher.
COUNTER             thanos_replicate_replication_runs_total                              The number of replication runs split by success and error.
HISTOGRAM           thanos_replicate_replication_run_duration_seconds                    The Duration of replication runs split by success and error.
COUNTER             thanos_replicate_blocks_already_replicated_total                     Total number of blocks skipped due to already being replicated.
COUNTER             thanos_replicate_blocks_replicated_total                             Total number of blocks replicated.
COUNTER             thanos_replicate_objects_replicated_total                            Total number of objects replicated.
COUNTER             grpc_req_panics_recovered_total                                      Total number of gRPC requests recovered from internal panic.
COUNTER             thanos_shipper_dir_syncs_total                                       Total number of dir syncs
COUNTER             thanos_shipper_dir_sync_failures_total                               Total number of failed dir syncs
COUNTER             thanos_shipper_uploads_total                                         Total number of uploaded blocks
COUNTER             thanos_shipper_upload_failures_total                                 Total number of block upload failures
GAUGE               thanos_shipper_upload_compacted_done                                 If 1 it means shipper uploaded all compacted blocks from the filesystem.
GAUGE               thanos_shipper_upload_compacted_done                                 If 1 it means shipper uploaded all compacted blocks from the filesystem.
COUNTER             thanos_bucket_store_block_loads_total                                Total number of remote block loading attempts.
COUNTER             thanos_bucket_store_block_load_failures_total                        Total number of failed remote block loading attempts.
COUNTER             thanos_bucket_store_block_drops_total                                Total number of local blocks that were dropped.
COUNTER             thanos_bucket_store_block_drop_failures_total                        Total number of local blocks that failed to be dropped.
GAUGE               thanos_bucket_store_blocks_loaded                                    Number of currently loaded blocks.
SUMMARY             thanos_bucket_store_series_data_touched                              How many items of a data type in a block were touched for a single series request.
SUMMARY             thanos_bucket_store_series_data_fetched                              How many items of a data type in a block were fetched for a single series request.
SUMMARY             thanos_bucket_store_series_data_size_touched_bytes                   Size of all items of a data type in a block were touched for a single series request.
SUMMARY             thanos_bucket_store_series_data_size_fetched_bytes                   Size of all items of a data type in a block were fetched for a single series request.
SUMMARY             thanos_bucket_store_series_blocks_queried                            Number of blocks in a bucket store that were touched to satisfy a query.
HISTOGRAM           thanos_bucket_store_series_get_all_duration_seconds                  Time it takes until all per-block prepares and loads for a query are finished.
HISTOGRAM           thanos_bucket_store_series_merge_duration_seconds                    Time it takes to merge sub-results from all queried blocks into a single result.
SUMMARY             thanos_bucket_store_series_result_series                             Number of series observed in the final result of a query.
HISTOGRAM           thanos_bucket_store_sent_chunk_size_bytes                            Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.
COUNTER             thanos_bucket_store_queries_dropped_total                            Number of queries that were dropped due to the limit.
COUNTER             thanos_bucket_store_cached_postings_compressions_total               Number of postings compressions before storing to index cache.
COUNTER             thanos_bucket_store_cached_postings_compression_errors_total         Number of postings compression errors.
COUNTER             thanos_bucket_store_cached_postings_compression_time_seconds_total   Time spent compressing postings before storing them into postings cache.
COUNTER             thanos_bucket_store_cached_postings_original_size_bytes_total        Original size of postings stored into cache.
COUNTER             thanos_bucket_store_cached_postings_compressed_size_bytes_total      Compressed size of postings stored into cache.
HISTOGRAM           thanos_bucket_store_cached_series_fetch_duration_seconds             The time it takes to fetch series to respond to a request sent to a store gateway. It includes both the time to fetch it from the cache and from storage in case of cache misses.
HISTOGRAM           thanos_bucket_store_cached_postings_fetch_duration_seconds           The time it takes to fetch postings to respond to a request sent to a store gateway. It includes both the time to fetch it from the cache and from storage in case of cache misses.
COUNTER             thanos_store_bucket_cache_getrange_requested_bytes_total             Total number of bytes requested via GetRange.
COUNTER             thanos_store_bucket_cache_getrange_fetched_bytes_total               Total number of bytes fetched because of GetRange operation. Data from bucket is then stored to cache.
COUNTER             thanos_store_bucket_cache_getrange_refetched_bytes_total             Total number of bytes re-fetched from storage because of GetRange operation, despite being in cache already.
COUNTER             thanos_store_bucket_cache_operation_requests_total                   Number of requested operations matching given config.
COUNTER             thanos_store_bucket_cache_operation_hits_total                       Number of operations served from cache for given config.
COUNTER             thanos_store_index_cache_items_added_total                           Total number of items that were added to the index cache.
COUNTER             thanos_store_index_cache_requests_total                              Total number of requests to the cache.
COUNTER             thanos_store_index_cache_items_overflowed_total                      Total number of items that could not be added to the cache due to being too big.
COUNTER             thanos_store_index_cache_hits_total                                  Total number of requests to the cache that were a hit.
GAUGE               thanos_store_index_cache_items                                       Current number of items in the index cache.
GAUGE               thanos_store_index_cache_items_size_bytes                            Current byte size of items in the index cache.
GAUGE               thanos_store_index_cache_total_size_bytes                            Current byte size of items (both value and key) in the index cache.
GAUGE               thanos_store_index_cache_max_size_bytes                              Maximum number of bytes to be held in the index cache.
GAUGE               thanos_store_index_cache_max_item_size_bytes                         Maximum number of bytes for single entry to be held in the index cache.
COUNTER             thanos_store_index_cache_items_evicted_total                         Total number of items that were evicted from the index cache.
COUNTER             thanos_store_index_cache_requests_total                              Total number of items requests to the cache.
COUNTER             thanos_store_index_cache_hits_total                                  Total number of items requests to the cache that were a hit.
HISTOGRAM           prometheus_store_received_frames                                     Number of frames received per streamed response.
COUNTER             thanos_proxy_store_empty_stream_responses_total                      Total number of empty responses received.
COUNTER             thanos_verify_blocks_marked_for_deletion_total                       Total number of blocks marked for deletion by verify.
```

You can checkout these metrics yourself using [promlinter](https://github.com/yeya24/promlinter)
