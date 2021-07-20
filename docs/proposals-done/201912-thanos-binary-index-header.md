---
type: proposal
title: Binary Format for Index-cache; Renaming to Index-header.
status: complete
owner: bwplotka
menu: proposals-done
---

### Related Tickets

* https://github.com/thanos-io/thanos/issues/942 (format changes)
* https://github.com/thanos-io/thanos/issues/1711 (pulling index in smarter way)
* https://github.com/thanos-io/thanos/pull/1013 (initial investigation of different formats)
* https://github.com/thanos-io/thanos/issues/448 (OOM)
* https://github.com/thanos-io/thanos/issues/1705 (LTS umbrella issue)

## Summary

This short document describes the motivation and the design of a new format that is meant to replace `index-cache.json` we have currently.

We also propose renaming index-cache to *index-header* due to name collision with index cache for postings and series.

## Motivation

Currently the Store Gateway component has to be aware of all the blocks (modulo sharding & time partitioning) in the bucket. For each block that we want to serve metrics from, the Store Gateway has to have that block `synced` in order to:

* Know what postings are matching given query matchers.
* Know what label values are for names.
* Know how to un-intern symbols.

The `sync` process includes:

* Download meta.json or fetch from disk if cached.
* Check if index-cache.json is present on the disk. If not:
  * Check if index-cache.json is present in the bucket. If not:
    * Download the whole TSDB index file, mmap all of it.
    * Build index-cache.json
  * else:
    * Download index-cache.json from bucket
  * Delete downloaded TSDB index.
* Load whole index-cache.json to memory and keep it for lifetime of the block.

The current, mentioned [index-cache.json](https://github.com/thanos-io/thanos/blob/bd9aa1b4be3bb5d841cb7271c29d02ebb5eb5168/pkg/block/index.go#L40) holds block’s:
* [TOC](https://github.com/prometheus/prometheus/blob/master/tsdb/docs/format/index.md#toc)
* [Symbols](https://github.com/prometheus/prometheus/blob/master/tsdb/docs/format/index.md#symbol-table)
* LabelValues:
  * Calculated from all [LabelsIndicesTable](https://github.com/prometheus/prometheus/blob/master/tsdb/docs/format/index.md#label-offset-table), that each points to [LabelIndex](https://github.com/prometheus/prometheus/blob/master/tsdb/docs/format/index.md#label-index) entry.
  * Used for matching against request selectors and LabelValues/LabelNames calls.
* [Postings Offsets](https://github.com/prometheus/prometheus/blob/master/tsdb/docs/format/index.md#postings-offset-table)

There are few problems with this approach:

1. Downloading the whole TSDB index file (which can be from few MB up to 64GB or even more when limit will be increased soon) is expensive and might be slow.
2. Building such index-cache requires at least 2x `index-cache.json` size of memory.
3. Parsing large index-cache.json files (which can be from few KB to few GB) might be slow.
4. We keep the full content of index-cache file in memory for all blocks.
   * Some blocks are never queried.
   * Some posting offsets and label values within each block are never used.

1, 2 & 3 contributes to Store Gateway startup being slow and resource consuming: https://github.com/thanos-io/thanos/issues/448

1. causes unnecessary constant cache for unused blocks.

This design is trying to address those four problems.

## Goals

* Reduce confusion between index-cache.json and IndexCache for series and postings.
* Use constant amount of memory for building index-header/index-cache; download only required pieces of index from bucket and compute new TOC file.
* Leverage mmap and let OS unload unused pages from the physical memory as needed to satisfy the queries against the block. We can leverage mmap due to random access against label values and postings offsets.

## No Goals

* Removing initial startup for Thanos Store Gateway completely as designed in [Cortex, no initial block sync](https://github.com/thanos-io/thanos/issues/1813)
  * However this proposal might be a step towards that as we might be able to load and build index-cache/index quickly on demand from disk. See [Future Work](#future-work)
  * At the same time be able to load `index-header` at query time directly from bucket is not a goal of this proposal.
* Decreasing size. While it would be nice to use less space; our aim is latency of building/loading the block. That might be correlated with size, but not necessarily (e.g when extra considering compression)

## Verification

* Compare memory and latency usage for startup without index-header being generated on compactor
* Benchmark for loading the `index-header` vs `index-cache.json` into Store GW.

## Proposal

TSDB index is in binary [format](https://github.com/prometheus/prometheus/blob/master/tsdb/docs/format/index.md).

To allow reduced resource consumption and effort when building (1), (2), "index-header" for blocks we plan to reuse similar format for sections like symbols, label indices and posting offsets in separate the file called `index-header` that will replace currently existing `index-cache.json`.

The process for building this will be as follows:

* Thanks to https://github.com/thanos-io/thanos/pull/1792 we can check final size of index and scan for TOC file.
* With TOC:
  * Get the symbols table and copy it to the local file.
  * Get the posting table and copy it to the local file.
* Write new TOC file at the end of file.

With that effort building time and resources should be compared with downloading the prebuilt `index-header` from the bucket. This allows us to reduce complexity of the system as we don't need to cache that in object storage by compactor.

Thanks to this format we can reuse most of the [FileReader](https://github.com/prometheus/prometheus/blob/de0a772b8e7d27dc744810a1a693d97be027049a/tsdb/index/index.go#L664) code to load file.

Thanos will build/compose all index-headers on the startup for now, however in theory we can load and build those blocks on demand. Given the minimal memory that each loaded block should take now, this is described as [Future Work](#future-work)

### Length of Posting to fetch

While idea of combing different pieces of TSDB index as our index-header is great, unfortunately we heavily rely on information about size of each posting represented as `postingRange.End`.

We need to know apriori how to partition and how many bytes we need to fetch from the storage to get each posting: https://github.com/thanos-io/thanos/blob/7e11afe64af0c096743a3de8a594616abf52be45/pkg/store/bucket.go#L1567

To calculate those sizes we use [`indexr.PostingsRanges()`](https://github.com/thanos-io/thanos/blob/7e11afe64af0c096743a3de8a594616abf52be45/pkg/block/index.go#L156-L155) which scans through `posting` section of the TSDB index. Having to fetch whole postings section just to get size of each posting makes this proposal less valuable as we still need to download big part of index and traverse through it instead of what we propose in [#Proposal](#proposal)

For series we don't know the exact size either, however we estimate max size of each series to be 64*1024. It depends on sane number of label pairs and chunks per series. We have really only one potential case when this was too low: https://github.com/thanos-io/thanos/issues/552. Decision about series this was made here: https://github.com/thanos-io/thanos/issues/146

For postings it's more tricky as it depends on number of series in which given label pair exists. For worst case it can be even million label-pair for popular pairs like `__name__=http_request_http_duration_bucket` etc.

We have few options:

* Encode this posting size in TSDB index `PostingOffset`: Unlikely to happen as not needed by Prometheus.
* Scan postings to fetch, which is something we wanted to avoid when building `index-header` without downloading full TSDB index: This option invalidates this proposal.
* Estimate some large value (will overfetch): Too large to always overfetch.
* Estimate casual value and retry fetching remaining size on demand for data consistency.

However there is one that this proposal aims for:

* We can deduce the length of posting by the beginning of the next posting offset in posting table. This is thanks to the sorted postings.

## Risk

### Unexpected memory usage

Users care the most about surprising spikes in memory usage. Currently the Store Gateway caches the whole index-cache.json. While it's silly to do so for all blocks, this will happen anyway if query will span over large number of blocks and series. This means that while baseline memory will be reduced, baseline vs request memory difference will be even more noticeable.

This tradeoff is acceptable, due to total memory used for all operation should be much smaller. Additionally such query spanning all of the blocks and series are unlikely and should be blocked by simple `sample` limit.

### Benchmark

How to micro-benchmark such change? mmaping is outside of Go run time, which counts allocations etc.

### Do we need to mmap?

mmap adds a lot of complexity and confusion especially around monitoring it's memory usage as it does not appear on Go profiles.

While mmap is great for random access against a big file, in fact the current implementation of the [FileReader](https://github.com/prometheus/prometheus/blob/de0a772b8e7d27dc744810a1a693d97be027049a/tsdb/index/index.go#L664) reallocates symbols, offsets and label name=value pairs while reading. This really defies the purpose of mmap as we want to combine all info in a dense few sequential sections of index-header binary format, this file will be mostly read sequentially. Still, label values can be accessed randomly, that's why we propose to start with mmap straight away.

### Is initial long startup, really a problem?

After initial startup with persistent disk, next startup should be quick due to cached files on disk for old blocks. Only new one will be iterated on. However still initial startup and adhoc syncs can be problematic for example: auto scaling. To adapt to high load you want component to start up quickly.

### LabelNames and LabelValues.

Currently all of those methods are getting labels values and label names across all blocks in the system.

This will load **all** blocks to the system on every such call.

We have couple of options:

* Limit with time range: https://github.com/thanos-io/thanos/issues/1811
* Cache e.g popular label values queries like `__name__`

## Alternatives

* Introduce faster JSON parser like [json-iterator/go](https://github.com/json-iterator/go)
  * However, this does not contribute towards faster creation of such `index-header` from TSDB index.
* Design external memory compatible index instead of TSDB index.
  * This is being discussed in another threads. While we should look on this ASAP, it's rather a long term plan.
* Build `index-cache.json` on sidecar
  * This unfortunately may require some extra memory for sidecar which we want to avoid.

## Work Plan

Replace `index-cache.json` with `index-header`:
* Implement building `index-header` file from pieces of TSDB index.
* Allow Store Gateway to build it instead of `index-cache.json` to disk.
* Allow Store Gateway to use `index-header` instead of `index-cache.json` for queries.
* Load those files using mmap
* Remove building `index-cache.json` in compactor.

## Future Work

* Load on demand at query time:

We can maintain a pool with limited number of `index-header` files loaded at the time in Store Gateway. With an LRU logic we should be able to deduce which of blocks should be unloaded and left on the disk.

This proposes following algorithm:

* blocks newer then X (e.g 2w) being always loaded
* blocks older than X will be loaded on demand on query time and kept cached until evicted.
* background eviction process unloads blocks without pending reads to the amount of Y (e.g 100 blocks) in LRU fashion.

Both X and Y being configurable. From UX perspective it would be nice to set configurable memory limit for loaded blocks.

* Build on demand at query time:
  * Allow Store Gateway to build `index-header` on demand from bucket at query time.
  * Do not build `index-header` on startup at all, just lazy background job if needed.
