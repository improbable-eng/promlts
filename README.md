<p align="center"><img src="docs/img/Thanos-logo_fullmedium.png" alt="Thanos Logo"></p>

[![Latest Release](https://img.shields.io/github/release/thanos-io/thanos.svg?style=flat-square)](https://github.com/thanos-io/thanos/releases/latest) [![Go Report Card](https://goreportcard.com/badge/github.com/thanos-io/thanos)](https://goreportcard.com/report/github.com/thanos-io/thanos) [![Go Code reference](https://img.shields.io/badge/code%20reference-go.dev-darkblue.svg)](https://pkg.go.dev/github.com/thanos-io/thanos?tab=subdirectories) [![Slack](https://img.shields.io/badge/join%20slack-%23thanos-brightgreen.svg)](https://slack.cncf.io/) [![Netlify Status](https://api.netlify.com/api/v1/badges/664a5091-934c-4b0e-a7b6-bc12f822a590/deploy-status)](https://app.netlify.com/sites/thanos-io/deploys) [![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/3048/badge)](https://bestpractices.coreinfrastructure.org/projects/3048)

[![CI](https://github.com/thanos-io/thanos/workflows/CI/badge.svg)](https://github.com/thanos-io/thanos/actions?query=workflow%3ACI) [![CI](https://circleci.com/gh/thanos-io/thanos.svg?style=svg)](https://circleci.com/gh/thanos-io/thanos) [![go](https://github.com/thanos-io/thanos/workflows/go/badge.svg)](https://github.com/thanos-io/thanos/actions?query=workflow%3Ago) [![react](https://github.com/thanos-io/thanos/workflows/react/badge.svg)](https://github.com/thanos-io/thanos/actions?query=workflow%3Areact) [![docs](https://github.com/thanos-io/thanos/workflows/docs/badge.svg)](https://github.com/thanos-io/thanos/actions?query=workflow%3Adocs) [![Gitpod ready-to-code](https://img.shields.io/badge/Gitpod-ready--to--code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/thanos-io/thanos)

## Overview

Thanos is a set of components that can be composed into a highly available metric system with unlimited storage capacity, which can be added seamlessly on top of existing Prometheus deployments.

Thanos is a [CNCF](https://www.cncf.io/) Incubating project.

Thanos leverages the Prometheus 2.0 storage format to cost-efficiently store historical metric data in any object storage while retaining fast query latencies. Additionally, it provides a global query view across all Prometheus installations and can merge data from Prometheus HA pairs on the fly.

Concretely the aims of the project are:

1. Global query view of metrics.
2. Unlimited retention of metrics.
3. High availability of components, including Prometheus.

## Getting Started

* **[Getting Started](https://thanos.io/tip/thanos/getting-started.md/)**
* [Design](https://thanos.io/tip/thanos/design.md/)
* [Blog posts](docs/getting-started.md#blog-posts)
* [Talks](docs/getting-started.md#talks)
* [Proposals](docs/proposals-done)
* [Integrations](docs/integrations.md)

## Features

* Global querying view across all connected Prometheus servers
* Deduplication and merging of metrics collected from Prometheus HA pairs
* Seamless integration with existing Prometheus setups
* Any object storage as its only, optional dependency
* Downsampling historical data for massive query speedup
* Cross-cluster federation
* Fault-tolerant query routing
* Simple gRPC "Store API" for unified data access across all metric data
* Easy integration points for custom metric providers

## Architecture Overview

Deployment with Sidecar:

![Sidecar](https://docs.google.com/drawings/d/e/2PACX-1vTBFKKgf8YDInJyRakPE8eZZg9phTlOsBB2ogNkFvhNGbZ8YDvz_cGMbxWZBG1G6hpsQfSX145FpYcv/pub?w=960&h=720)

Deployment with Receive:

![Receive](https://docs.google.com/drawings/d/e/2PACX-1vTfko27YB_3ab7ZL8ODNG5uCcrpqKxhmqaz3lW-yhGN3_oNxkTrqXmwwlcZjaWf3cGgAJIM4CMwwkEV/pub?w=960&h=720)

## Thanos Philosophy

The philosophy of Thanos and our community is borrowing much from UNIX philosophy and the golang programming language.

* Each subcommand should do one thing and do it well
  * eg. thanos query proxies incoming calls to known store API endpoints merging the result
* Write components that work together
  * e.g. blocks should be stored in native prometheus format
* Make it easy to read, write, and, run components
  * e.g. reduce complexity in system design and implementation

## Releases

Main branch should be stable and usable. Every commit to main builds docker image named `main-<date>-<sha>` in [quay.io/thanos/thanos](https://quay.io/repository/thanos/thanos) and [thanosio/thanos dockerhub (mirror)](https://hub.docker.com/r/thanosio/thanos)

We also perform minor releases every 6 weeks.

During that, we build tarballs for major platforms and release docker images.

See [release process docs](docs/release-process.md) for details.

## Contributing

Contributions are very welcome! See our [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## Community

Thanos is an open source project and we value and welcome new contributors and members of the community. Here are ways to get in touch with the community:

* Slack: [#thanos](https://slack.cncf.io/)
* Issue Tracker: [GitHub Issues](https://github.com/thanos-io/thanos/issues)

## Adopters

See [`Adopters List`](website/data/adopters.yml).

## Maintainers

See [MAINTAINERS.md](MAINTAINERS.md)
