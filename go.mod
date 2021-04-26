module github.com/thanos-io/thanos

require (
	cloud.google.com/go v0.81.0
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20210208195552-ff826a37aa15
	github.com/aliyun/aliyun-oss-go-sdk v2.0.4+incompatible
	github.com/blang/semver/v4 v4.0.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/chromedp/cdproto v0.0.0-20200424080200-0de008e41fa0
	github.com/chromedp/chromedp v0.5.3
	github.com/cortexproject/cortex v1.8.1-0.20210422151339-cf1c444e0905
	github.com/davecgh/go-spew v1.1.1
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fatih/structtag v1.1.0
	github.com/felixge/fgprof v0.9.1
	github.com/fsnotify/fsnotify v1.4.9
	github.com/go-kit/kit v0.10.0
	github.com/go-openapi/strfmt v0.20.1
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.0.3
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e
	github.com/golang/snappy v0.0.3
	github.com/googleapis/gax-go v2.0.2+incompatible
	github.com/grpc-ecosystem/go-grpc-middleware/providers/kit/v2 v2.0.0-20201002093600-73cf2ae9d891
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.2.0.20201207153454-9f6bf00c00a7
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/jpillora/backoff v1.0.0
	github.com/leanovate/gopter v0.2.4
	github.com/lightstep/lightstep-tracer-go v0.18.1
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/miekg/dns v1.1.41
	github.com/minio/minio-go/v7 v7.0.10
	github.com/mozillazg/go-cos v0.13.0
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f
	github.com/ncw/swift v1.0.52
	github.com/oklog/run v1.1.0
	github.com/oklog/ulid v1.3.1
	github.com/olekukonko/tablewriter v0.0.2
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/alertmanager v0.21.1-0.20210422101724-8176f78a70e1
	github.com/prometheus/client_golang v1.10.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.23.0
	github.com/prometheus/exporter-toolkit v0.5.1
	github.com/prometheus/prometheus v1.8.2-0.20210519120135-d95b0972505f
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.28.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible
	github.com/weaveworks/common v0.0.0-20210419092856-009d1eebd624
	go.elastic.co/apm v1.11.0
	go.elastic.co/apm/module/apmot v1.11.0
	go.uber.org/atomic v1.7.0
	go.uber.org/automaxprocs v1.2.0
	go.uber.org/goleak v1.1.10
	golang.org/x/crypto v0.0.0-20210220033148-5ea612d1eb83
	golang.org/x/oauth2 v0.0.0-20210427180440-81ed05c6b58c
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/text v0.3.6
	google.golang.org/api v0.46.0
	google.golang.org/genproto v0.0.0-20210429181445-86c259c2b4ab
	google.golang.org/grpc v1.37.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/fsnotify.v1 v1.4.7
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

replace (
	// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86.
	// Required by Cortex https://github.com/cortexproject/cortex/pull/3051.
	github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab
	// Update to v1.1.1 to make sure windows CI pass.
	github.com/elastic/go-sysinfo => github.com/elastic/go-sysinfo v1.1.1

	// TODO: Remove this: https://github.com/thanos-io/thanos/issues/3967.
	github.com/minio/minio-go/v7 => github.com/bwplotka/minio-go/v7 v7.0.11-0.20210324165441-f9927e5255a6
	// Make sure Prometheus version is pinned as Prometheus semver does not include Go APIs.
	github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20210519120135-d95b0972505f
	github.com/sercand/kuberesolver => github.com/sercand/kuberesolver v2.4.0+incompatible
	google.golang.org/grpc => google.golang.org/grpc v1.29.1

	// Overriding to use latest commit
	gopkg.in/alecthomas/kingpin.v2 => github.com/alecthomas/kingpin v1.3.8-0.20210301060133-17f40c25f497

	// From Prometheus.
	k8s.io/klog => github.com/simonpasquier/klog-gokit v0.3.0
	k8s.io/klog/v2 => github.com/simonpasquier/klog-gokit/v2 v2.0.1
)

go 1.15
