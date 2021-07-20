---
weight: 1
type: docs
title: Getting Started
menu: thanos
---

# Getting started

Thanos provides a global query view, high availability, data backup with historical, cheap data access as its core features in a single binary.

Those features can be deployed independently of each other. This allows you to have a subset of Thanos features ready for immediate benefit or testing, while also making it flexible for gradual roll outs in more complex environments.

In this quick-start guide, we will explain:

* How to ask questions, build and contribute to Thanos.
* A few common ways of deploying Thanos.
* Links for further reading.

Thanos will work in cloud native environments as well as more traditional ones. Some users run Thanos in Kubernetes while others on the bare metal.

## Dependencies

Thanos aims for a simple deployment and maintenance model. The only dependencies are:

* One or more [Prometheus](https://prometheus.io) v2.2.1+ installations with persistent disk.
* Optional object storage
  * Thanos is able to use [many different storage providers](storage.md), with the ability to add more providers as necessary.

## Get Thanos!

You can find the latest Thanos release [here](https://github.com/thanos-io/thanos/releases).

Main should be stable and usable. Every commit to main builds docker image named `main-<data>-<sha>` in [quay.io/thanos/thanos](https://quay.io/repository/thanos/thanos) and [thanosio/thanos dockerhub (mirror)](https://hub.docker.com/r/thanosio/thanos)

We also perform minor releases every 6 weeks.

During that, we build tarballs for major platforms and release docker images.

See [release process docs](release-process.md) for details.

## Building from source:

Thanos is built purely in [Golang](https://golang.org/), thus allowing to run Thanos on various x64 operating systems.

If you want to build Thanos from source you would need a working installation of the Go 1.16+ [toolchain](https://github.com/golang/tools) (`GOPATH`, `PATH=${GOPATH}/bin:${PATH}`).

Thanos can be downloaded and built by running:

```bash
go get github.com/thanos-io/thanos/cmd/thanos
```

The `thanos` binary should now be in your `$PATH` and is the only thing required to deploy any of its components.

## Contributing

Contributions are very welcome! See our [CONTRIBUTING.md](https://github.com/thanos-io/thanos/tree/main/CONTRIBUTING.md) for more information.

## Community

Thanos is an open source project and we value and welcome new contributors and members of the community. Here are ways to get in touch with the community:

* Slack: [#thanos](https://slack.cncf.io/)
* Issue Tracker: [GitHub Issues](https://github.com/thanos-io/thanos/issues)

## Maintainers

See [MAINTAINERS.md](https://github.com/thanos-io/thanos/tree/main/MAINTAINERS.md).

## Community Thanos Kubernetes Applications

Thanos is **not** tied to Kubernetes. However, Kubernetes, Thanos and Prometheus are part of the CNCF so the most popular applications are on top of Kubernetes.

Our friendly community maintains a few different ways of installing Thanos on Kubernetes. See those below:

* [prometheus-operator](https://github.com/coreos/prometheus-operator): Prometheus operator has support for deploying Prometheus with Thanos
* [kube-thanos](https://github.com/thanos-io/kube-thanos): Jsonnet based Kubernetes templates.
* [Community Helm charts](https://artifacthub.io/packages/search?ts_query_web=thanos)

If you want to add yourself to this list, let us know!

## Deploying Thanos

* [WIP] Detailed, free, in-browser interactive tutorial [as Katacoda Thanos Course](https://katacoda.com/thanos/courses/thanos/1-globalview)
* [Quick Tutorial](quick-tutorial.md) on Thanos website.

## Operating

See up to date [jsonnet mixins](https://github.com/thanos-io/thanos/tree/main/mixin/README.md) We also have example Grafana dashboards [here](https://github.com/thanos-io/thanos/tree/main/examples/dashboards/dashboards.md) and some [alerts](https://github.com/thanos-io/thanos/tree/main/examples/alerts/alerts.md) to get you started.

## Talks

* 12.2020: [Absorbing Thanos Infinite Powers for Multi-Cluster Telemetry](https://www.youtube.com/watch?v=6Nx2BFyr7qQ)
* 12.2020: [Turn It Up to a Million: Ingesting Millions of Metrics with Thanos Receive](https://www.youtube.com/watch?v=5MJqdJq41Ms)
* 02.2018: [Very first Prometheus Meetup Slides](https://www.slideshare.net/BartomiejPotka/thanos-global-durable-prometheus-monitoring)
* 02.2019: [FOSDEM + demo](https://fosdem.org/2019/schedule/event/thanos_transforming_prometheus_to_a_global_scale_in_a_seven_simple_steps/)
* 03.2019: [Alibaba Cloud user story](https://www.youtube.com/watch?v=ZS6zMksfipc)
* 09.2019: [CloudNative Warsaw Slides](https://docs.google.com/presentation/d/1cKpbJY3jIAtr03M-zcNujwBA38_LDj7NqE4LjNfvglE/edit?usp=sharing)
* 11.2019: [CloudNative Deep Dive](https://www.youtube.com/watch?v=qQN0N14HXPM)
* 11.2019: [CloudNative Intro](https://www.youtube.com/watch?v=m0JgWlTc60Q)
* 2019: [Prometheus in Practice: HA with Thanos](https://www.slideshare.net/ThomasRiley45/prometheus-in-practice-high-availability-with-thanos-devopsdays-edinburgh-2019)

## Blog posts

* 2020:

  * [Banzai Cloud user story](https://banzaicloud.com/blog/multi-cluster-monitoring/)
  * [Monitoring the Beat microservices: A tale of evolution](https://build.thebeat.co/monitoring-the-beat-microservices-a-tale-of-evolution-4e246882606e)

* 2019:

  * [Metric monitoring architecture](https://improbable.io/blog/thanos-architecture-at-improbable)
  * [Red Hat user story](https://blog.openshift.com/federated-prometheus-with-thanos-receive/)
  * [HelloFresh blog posts part 1](https://engineering.hellofresh.com/monitoring-at-hellofresh-part-1-architecture-677b4bd6b728)
  * [HelloFresh blog posts part 2](https://engineering.hellofresh.com/monitoring-at-hellofresh-part-2-operating-the-monitoring-system-8175cd939c1d)
  * [Thanos deployment](https://www.metricfire.com/blog/ha-kubernetes-monitoring-using-prometheus-and-thanos)
  * [Taboola user story](https://engineering.taboola.com/monitoring-and-metering-scale/)
  * [Thanos via Prometheus Operator](https://kkc.github.io/2019/02/10/prometheus-operator-with-thanos/)

* 2018:

  * [Introduction blog post](https://improbable.io/games/blog/thanos-prometheus-at-scale)
  * [Monzo user story](https://monzo.com/blog/2018/07/27/how-we-monitor-monzo)
  * [Banzai Cloud hand's on](https://banzaicloud.com/blog/hands-on-thanos/)
  * [uSwitch user story](https://medium.com/uswitch-labs/making-prometheus-more-awesome-with-thanos-fbec8c6c28ad)
  * [Thanos usage](https://www.infracloud.io/blogs/thanos-ha-scalable-prometheus/)

## Integrations

See [Integrations page](integrations.md).

## Testing Thanos on Single Host

We don't recommend running Thanos on a single node on production. Thanos is designed and built to run as a distributed system. Vanilla Prometheus might be totally enough for small setups.

However, in case you want to play and run Thanos components on a single node, we recommend following the port layout:

| Component      | Interface               | Port  |
|----------------|-------------------------|-------|
| Sidecar        | gRPC                    | 10901 |
| Sidecar        | HTTP                    | 10902 |
| Query          | gRPC                    | 10903 |
| Query          | HTTP                    | 10904 |
| Store          | gRPC                    | 10905 |
| Store          | HTTP                    | 10906 |
| Receive        | gRPC (store API)        | 10907 |
| Receive        | HTTP (remote write API) | 10908 |
| Receive        | HTTP                    | 10909 |
| Rule           | gRPC                    | 10910 |
| Rule           | HTTP                    | 10911 |
| Compact        | HTTP                    | 10912 |
| Query Frontend | HTTP                    | 10913 |

You can see example one-node setup [here](https://github.com/thanos-io/thanos/tree/main/scripts/quickstart.sh).
