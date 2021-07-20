---
title: Q&A
type: docs
menu: operating
---

# Q&A

This is a curated list of "Questions and Answers". The goal of this page is to provide answers to frequently asked questions and give basic information about operating Thanos.

We encourage to asks questions in the appropriate channels and update this page to share the knowledge further.


## Can the Sidecar read the object store values it wrote, or is it purely an uploader and you have to use Store to read them?

The sidecar optionally upload metrics. It also exposes the storeAPI to query Prometheus data


## If i have 1 Observer cluster and 2 Remote (Observee) clusters, how many ObjectStore buckets I have to create?

You can use one bucket for multiple clusters, but a bucket per cluster works too.

##  How often does the receiver send data to object store?

Usually once every 2 hours same as sidecar but per [proposal](https://thanos.io/tip/proposals/201812_thanos-remote-receive.md/)  it might end up uploading under these conditions:
Receivers do not need to re-shard data on rollouts; instead, they must flush the write-ahead-log to a Prometheus TSDB block and upload it to object storage during shutdown in the case of rollouts and scaling events.

## what does validity 0s mean, that any cache will be invalidated after 0 seconds?

validity 0s for memcached will be set to 24h by default.

## For deployments to Kubernetes, is it recommended to have multiple Store statefulsets or 1 Store statefulset with multiple replicas?

A single store will just do fine. If needed you can create more instances where each instance takes a portion of the requests (sharding). Multiple replica's are only relevant for high availability. You'll have to wonder if a replica is required for a pod that starts in a few seconds though.

## Is it possible to only use thanos storage gateway, and not thanos query?

 you need Query to interface with the Sidecar(s), to query the data that hasn’t been moved to backend storage yet (i.e., that still resides on the Prometheus servers). Otherwise, your queries will be missing the last 2hrs of data.

## What is, and how to configure --block-meta-fetch-concurrency?

## What is, and how to configure --block-sync-concurrency?

## How to calculate/see --store.grpc.touched-series-limit?

## What is a TSDB Block?

See [storage](https://thanos.io/tip/thanos/storage.md/#tsdb-block)

## What is a chunk?

## How can we calculate k8s resource limits for the Thanos Sidecar?

## Is there an easy way to identify the size of --chunk-pool-size you need?

## How can the Store component be limited?

## How can the maximum memory usage of the Store component be calculated?

