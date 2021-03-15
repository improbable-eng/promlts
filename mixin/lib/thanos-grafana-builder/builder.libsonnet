local grafana = import 'grafonnet/grafana.libsonnet';
local template = grafana.template;

(import 'grafana-builder/grafana.libsonnet') +
{
  collapse: {
    collapse: true,
  },

  panel(title, description=null)::
    super.panel(title) { [if description != null then 'description']: description },

  addDashboardLink(name): {
    links+: [
      {
        dashboard: name,
        includeVars: true,
        keepTime: true,
        title: name,
        type: 'dashboard',
      },
    ],
  },

  spanSize(size):: {
    span: size,
  },

  postfix(postfix):: {
    postfix: postfix,
  },

  sparkline:: {
    sparkline: {
      show: true,
      lineColor: 'rgb(31, 120, 193)',
      fillColor: 'rgba(31, 118, 189, 0.18)',
    },
  },

  latencyPanel(metricName, selector, aggregator, multiplier='1'):: {
    local aggregatedLabels = std.split(aggregator, ','),
    local aggregatorTemplate = std.join(' ', ['{{%s}}' % label for label in aggregatedLabels]),

    nullPointMode: 'null as zero',
    targets: [
      {
        expr: 'histogram_quantile(%.2f, sum by (%s, le) (rate(%s_bucket{%s}[$interval]))) * %s' % [percentile, aggregator, metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'p%d %s' % [100 * percentile, aggregatorTemplate],
        logBase: 10,
        min: null,
        max: null,
        refId: 'A',
        step: 10,
      }
      for percentile in [0.5, 0.9, 0.99]
    ],
    yaxes: $.yaxes('s'),
    seriesOverrides: [
      {
        alias: 'p99',
        color: '#FA6400',
        fill: 1,
        fillGradient: 1,
      },
      {
        alias: 'p90',
        color: '#E0B400',
        fill: 1,
        fillGradient: 1,
      },
      {
        alias: 'p50',
        color: '#37872D',
        fill: 10,
        fillGradient: 0,
      },
    ],
  },

  qpsErrTotalPanel(selectorErr, selectorTotal, aggregator):: {
    local expr(selector) = 'sum by (%s) (rate(%s[$interval]))' % [aggregator, selector],

    aliasColors: {
      'error': '#E24D42',
    },
    targets: [
      {
        expr: '%s / %s' % [expr(selectorErr), expr(selectorTotal)],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'error',
        refId: 'A',
        step: 10,
      },
    ],
    yaxes: $.yaxes({ format: 'percentunit' }),
  } + $.stack,

  qpsSuccErrRatePanel(selectorErr, selectorTotal, aggregator):: {
    local expr(selector) = 'sum by (%s) (rate(%s[$interval]))' % [aggregator, selector],

    aliasColors: {
      success: '#7EB26D',
      'error': '#E24D42',
    },
    targets: [
      {
        expr: '%s / %s' % [expr(selectorErr), expr(selectorTotal)],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'error',
        refId: 'A',
        step: 10,
      },
      {
        expr: '(%s - %s) / %s' % [expr(selectorTotal), expr(selectorErr), expr(selectorTotal)],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'success',
        refId: 'B',
        step: 10,
      },
    ],
    yaxes: $.yaxes({ format: 'percentunit', max: 1 }),
  } + $.stack,

  resourceUtilizationRow(selector, aggregator)::
    $.row('Resources')
    .addPanel(
      $.panel('Memory Used') +
      $.queryPanel(
        [
          'sum by (%s) (go_memstats_alloc_bytes{%s})' % [aggregator, selector],
          'sum by (%s) (go_memstats_heap_alloc_bytes{%s})' % [aggregator, selector],
          'sum by (%s) (rate(go_memstats_alloc_bytes_total{%s})[30s])' % [aggregator, selector],
          'sum by (%s) (rate(go_memstats_heap_alloc_bytes{%s})[30s])' % [aggregator, selector],
          'sum by (%s) (go_memstats_stack_inuse_bytes{%s})' % [aggregator, selector],
          'sum by (%s) (go_memstats_heap_inuse_bytes{%s})' % [aggregator, selector],
        ],
        [
          'alloc all {{job}}',
          'alloc heap {{job}}',
          'alloc rate all {{job}}',
          'alloc rate heap {{job}}',
          'inuse heap {{job}}',
          'inuse stack {{job}}',
        ]
      ) +
      { yaxes: $.yaxes('bytes') },
    )
    .addPanel(
      $.panel('Goroutines') +
      $.queryPanel(
        'sum by (%s) (go_goroutines{%s})' % [aggregator, selector],
        '{{job}}'
      )
    )
    .addPanel(
      $.panel('GC Time Quantiles') +
      $.queryPanel(
        'sum by (%s) (go_gc_duration_seconds{%s})' % [aggregator, selector],
        '{{quantile}} {{job}}'
      )
    ) +
    $.collapse,
} +
(import 'grpc.libsonnet') +
(import 'http.libsonnet') +
(import 'slo.libsonnet')
