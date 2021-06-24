# Changing Golang version

Thanos build system is pinned to certain Golang version. This is to ensure that Golang version changes is done by us in controlled, traceable way.

To update Thanos build system to newer Golang:

1. Edit [.promu.yaml](../../.promu.yml) and edit `go: version: <go version>` in YAML to desired version. This will ensure that all artifacts are built with desired Golang version. How to verify? Download tarball, unpack and invoke `thanos --version`
2. Edit [.circleci/config.yaml](../../.circleci/config.yml) and update ` - image: cimg/go:<go version>-node` to desired Golang version. This will ensure that all docker images and go tests are using desired Golang version. How to verify? Invoke `docker pull quay.io/thanos/thanos:<version> --version`
3. Edit [.github/workflows/docs.yaml](../../.github/workflows/docs.yaml) [.github/workflows/go.yaml](../../.github/workflows/go.yaml) and update Go version.
