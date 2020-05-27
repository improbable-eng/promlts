module _ // Auto generated by https://github.com/bwplotka/bingo. DO NOT EDIT

go 1.14

replace (
	// Mitigation for: https://github.com/Azure/go-autorest/issues/414
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v12.3.0+incompatible
	k8s.io/api => k8s.io/api v0.17.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.5
	k8s.io/client-go => k8s.io/client-go v0.17.5
	k8s.io/klog => github.com/simonpasquier/klog-gokit v0.1.0
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30
)

require github.com/prometheus/prometheus v1.8.2-0.20200507164740-ecee9c8abfd1 // cmd/prometheus
