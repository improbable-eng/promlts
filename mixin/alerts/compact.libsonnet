{
  local thanos = self,
  compact+:: {
    selector: error 'must provide selector for Thanos Compact alerts',
    compactionErrorThreshold: 5,
    bucketOpsErrorThreshold: 5,
    dimensions: std.join(', ', std.objectFields(thanos.targetGroups) + ['job']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.compact == null then [] else [
      local location = if std.length(std.objectFields(thanos.targetGroups)) > 0 then ' in %s' % std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(thanos.targetGroups)]) else '';
      {
        name: 'thanos-compact',
        rules: [
          {
            alert: 'ThanosCompactMultipleRunning',
            annotations: {
              description: 'No more than one Thanos Compact instance should be running at once. There are {{$value}}%s instances running.' % location,
              summary: 'Thanos Compact has multiple instances running.',
            },
            expr: 'sum by (%(dimensions)s) (up{%(selector)s}) > 1' % thanos.compact,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactHalted',
            annotations: {
              description: 'Thanos Compact {{$labels.job}}%s has failed to run and now is halted.' % location,
              summary: 'Thanos Compact has failed to run and is now halted.',
            },
            expr: 'thanos_compact_halted{%(selector)s} == 1' % thanos.compact,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactHighCompactionFailures',
            annotations: {
              description: 'Thanos Compact {{$labels.job}}%s is failing to execute {{$value | humanize}}%% of compactions.' % location,
              summary: 'Thanos Compact is failing to execute compactions.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_compact_group_compactions_failures_total{%(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(thanos_compact_group_compactions_total{%(selector)s}[5m]))
              * 100 > %(compactionErrorThreshold)s
              )
            ||| % thanos.compact,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactBucketHighOperationFailures',
            annotations: {
              description: 'Thanos Compact {{$labels.job}}%s Bucket is failing to execute {{$value | humanize}}%% of operations.' % location,
              summary: 'Thanos Compact Bucket is having a high number of operation failures.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(thanos_objstore_bucket_operations_total{%(selector)s}[5m]))
              * 100 > %(bucketOpsErrorThreshold)s
              )
            ||| % thanos.compact,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactHasNotRun',
            annotations: {
              description: 'Thanos Compact {{$labels.job}}%s has not uploaded anything for 24 hours.' % location,
              summary: 'Thanos Compact has not uploaded anything for last 24 hours.',
            },
            expr: '(time() - max by (%(dimensions)s) (max_over_time(thanos_objstore_bucket_last_successful_upload_time{%(selector)s}[24h]))) / 60 / 60 > 24' % thanos.compact,
            labels: {
              severity: 'warning',
            },
          },
        ],
      },
    ],
  },
}
