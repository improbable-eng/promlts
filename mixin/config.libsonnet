{
  query+:: {
    jobPrefix: 'thanos-query',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    namespaceLabel: $.dashboard.namespaceLabel, 
    title: '%(prefix)sQuery' % $.dashboard.prefix,
  },
  store+:: {
    jobPrefix: 'thanos-store',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    namespaceLabel: $.dashboard.namespaceLabel, 
    title: '%(prefix)sStore' % $.dashboard.prefix,
  },
  receive+:: {
    jobPrefix: 'thanos-receive',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    namespaceLabel: $.dashboard.namespaceLabel, 
    title: '%(prefix)sReceive' % $.dashboard.prefix,
  },
  rule+:: {
    jobPrefix: 'thanos-rule',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    namespaceLabel: $.dashboard.namespaceLabel, 
    title: '%(prefix)sRule' % $.dashboard.prefix,
  },
  compact+:: {
    jobPrefix: 'thanos-compact',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    namespaceLabel: $.dashboard.namespaceLabel, 
    title: '%(prefix)sCompact' % $.dashboard.prefix,
  },
  sidecar+:: {
    jobPrefix: 'thanos-sidecar',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    namespaceLabel: $.dashboard.namespaceLabel, 
    title: '%(prefix)sSidecar' % $.dashboard.prefix,
  },
  bucket_replicate+:: {
    jobPrefix: 'thanos-bucket-replicate',
    selector: 'job=~"%s.*"' % self.jobPrefix,
    namespaceLabel: $.dashboard.namespaceLabel, 
    title: '%(prefix)sBucketReplicate' % $.dashboard.prefix,
  },
  overview+:: {
    title: '%(prefix)sOverview' % $.dashboard.prefix,
  },
  dashboard+:: {
    prefix: 'Thanos / ',
    tags: ['thanos-mixin'],
    namespaceMetric: 'kube_pod_info',
    namespaceLabel: 'namespace',
  },
}
