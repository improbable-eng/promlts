# Use Cases

There are many great use cases of [Thanos](https://thanos.io/), we collect them here for others to refer to.

## Horizontal scalable Prometheus Scraping with Kvass

*Case add by* [@RayHuangCN](https://github.com/RayHuangCN)

If your want to build a horizontal scalable Prometheus for large kubernetes cluster monitoring, you can use [Thanos](https://github.com/thanos-io/thanos) + [Kvass](https://github.com/tkestack/kvass).

[Kvass](https://github.com/tkestack/kvass) is not another Prometheus compatible TSDB but a Prometheus sharding solution, which uses [Sidecar](https://github.com/tkestack/kvass#sidecar) to generate special config file only contains part of targets assigned from [Coordinator](https://github.com/tkestack/kvass#coordinator) for every Prometheus shard. [Thanos](https://github.com/thanos-io/thanos) is used for **global data view**, **rules processing** and **long term storage capabilities**. your can see usage detail [docs](https://github.com/tkestack/kvass#kvass--thanos).

[Thanos](https://github.com/thanos-io/thanos) + [Kvass](https://github.com/tkestack/kvass) is working well to scrape basic metrics of cluster (kubelet, cadvisor, kube-state-metrics and node-exporter...) with following cluster size. Just use one Prometheus config file, without any `federation` or `hashmod`.

- 1k+ Nodes
- 60k+ Pods
- 100k+ Containers
