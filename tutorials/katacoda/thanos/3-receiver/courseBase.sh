#!/usr/bin/env bash

docker pull quay.io/prometheus/prometheus:v2.27.0
docker pull quay.io/thanos/thanos:v0.22.0-rc.0

mkdir /root/editor
