{
  querier+:: {
    jobPrefix: 'thanos-querier',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sQuery' % $.dashboard.prefix,
  },
  store+:: {
    jobPrefix: 'thanos-store',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sStore' % $.dashboard.prefix,
  },
  receiver+:: {
    jobPrefix: 'thanos-receiver',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sReceiver' % $.dashboard.prefix,
  },
  rule+:: {
    jobPrefix: 'thanos-rule',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sRule' % $.dashboard.prefix,
  },
  compactor+:: {
    jobPrefix: 'thanos-compactor',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sCompact' % $.dashboard.prefix,
  },
  sidecar+:: {
    jobPrefix: 'thanos-sidecar',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    title: '%(prefix)sSidecar' % $.dashboard.prefix,
  },
  overview+:: {
    title: '%(prefix)sOverview' % $.dashboard.prefix,
  },
  dashboard+:: {
    prefix: 'Thanos / ',
    tags: ['thanos-mixin'],
    namespaceQuery: 'kube_pod_info',
  },
}
