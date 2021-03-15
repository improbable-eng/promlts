{
  local thanos = self,
  store+:: {
    selector: error 'must provide selector for Thanos Store alerts',
    grpcErrorThreshold: 5,
    compactionErrorThreshold: 5,
    seriesGateErrorThreshold: 2,
    bucketOpsErrorThreshold: 5,
    bucketOpsP99LatencyThreshold: 2,
    aggregator: std.join(', ', std.objectFields(thanos.hierarcies) + ['job']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.store == null then [] else [
      local location = if std.length(std.objectFields(thanos.hierarcies)) > 0 then ' in ' + std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(thanos.hierarcies)]) else ' ';
      {
        name: 'thanos-store',
        rules: [
          {
            alert: 'ThanosStoreGrpcErrorRate',
            annotations: {
              description: 'Thanos Store {{$labels.job}}%sis failing to handle {{$value | humanize}}%% of requests.' % location,
              summary: 'Thanos Store is failing to handle qrpcd requests.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(grpc_server_started_total{%(selector)s}[5m]))
              * 100 > %(grpcErrorThreshold)s
              )
            ||| % thanos.store,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosStoreSeriesGateLatencyHigh',
            annotations: {
              description: 'Thanos Store {{$labels.job}}%shas a 99th percentile latency of {{$value}} seconds for store series gate requests.' % location,
              summary: 'Thanos Store has high latency for store series gate requests.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (%(aggregator)s, le) (rate(thanos_bucket_store_series_gate_duration_seconds_bucket{%(selector)s}[5m]))) > %(seriesGateErrorThreshold)s
              and
                sum by (%(aggregator)s) (rate(thanos_bucket_store_series_gate_duration_seconds_count{%(selector)s}[5m])) > 0
              )
            ||| % thanos.store,
            'for': '10m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosStoreBucketHighOperationFailures',
            annotations: {
              description: 'Thanos Store {{$labels.job}}%sBucket is failing to execute {{$value | humanize}}%% of operations.' % location,
              summary: 'Thanos Store Bucket is failing to execute operations.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(thanos_objstore_bucket_operations_total{%(selector)s}[5m]))
              * 100 > %(bucketOpsErrorThreshold)s
              )
            ||| % thanos.store,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosStoreObjstoreOperationLatencyHigh',
            annotations: {
              description: 'Thanos Store {{$labels.job}}%sBucket has a 99th percentile latency of {{$value}} seconds for the bucket operations.' % location,
              summary: 'Thanos Store is having high latency for bucket operations.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (%(aggregator)s, le) (rate(thanos_objstore_bucket_operation_duration_seconds_bucket{%(selector)s}[5m]))) > %(bucketOpsP99LatencyThreshold)s
              and
                sum by (%(aggregator)s) (rate(thanos_objstore_bucket_operation_duration_seconds_count{%(selector)s}[5m])) > 0
              )
            ||| % thanos.store,
            'for': '10m',
            labels: {
              severity: 'warning',
            },
          },
        ],
      },
    ],
  },
}
