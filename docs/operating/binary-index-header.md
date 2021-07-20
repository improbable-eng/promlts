# Binary index-header

In order to query series inside blocks from object storage, [Store Gateway](../components/store.md) has to know certain initial info about each block such as:

- symbols table to unintern string values
- postings offset for posting lookup

In order to achieve so, on startup for each block `index-header` is built from pieces of original block's index and stored on disk. Such `index-header` file is then mmaped and used by Store Gateway, but never uploaded back to the object storage.

## Format (version 1)

The following describes the format of the `index-header` file found in each block store gateway local directory. It is terminated by a table of contents which serves as an entry point into the index.

```
┌─────────────────────────────┬───────────────────────────────┐
│    magic(0xBAAAD792) <4b>   │      version(1) <1 byte>      │
├─────────────────────────────┬───────────────────────────────┤
│  index version(2) <1 byte>  │ index PostingOffsetTable <8b> │
├─────────────────────────────┴───────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │      Symbol Table (exact copy from original index)      │ │
│ ├─────────────────────────────────────────────────────────┤ │
│ │      Posting Offset Table (exact copy from index)       │ │
│ ├─────────────────────────────────────────────────────────┤ │
│ │                          TOC                            │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

When the index is written, an arbitrary number of padding bytes may be added between the lined out main sections above. When sequentially scanning through the file, any zero bytes after a section's specified length must be skipped.

Most of the sections described below start with a `len` field. It always specifies the number of bytes just before the trailing CRC32 checksum. The checksum is always calculated over those `len` bytes.

### Symbol Table

See [Symbols](https://github.com/prometheus/prometheus/blob/d782387f814753b0118d402ec8cdbdef01bf9079/tsdb/docs/format/index.md#symbol-table)

### Postings Offset Table

See [Posting Offset Table](https://github.com/prometheus/prometheus/blob/d782387f814753b0118d402ec8cdbdef01bf9079/tsdb/docs/format/index.md#postings-offset-table)

### TOC

The table of contents serves as an entry point to the entire index and points to various sections in the file. If a reference is zero, it indicates the respective section does not exist and empty results should be returned upon lookup.

```
┌─────────────────────────────────────────┐
│ ref(symbols) <8b>                       │
├─────────────────────────────────────────┤
│ ref(postings offset table) <8b>         │
├─────────────────────────────────────────┤
│ CRC32 <4b>                              │
└─────────────────────────────────────────┘
```

## How the index-header is built

The [Store Gateway](../components/store.md) periodically scans the bucket to look for new and deleted blocks. For each new block found, the Gateway stores the index-header on the local disk, building it with specific sections of the block's index downloaded using GET byte range requests.

Since the index-header is built downloading specific segments of the original block's index and this is a computationally easy operation, the index-header is never uploaded back to the object storage and multiple Store Gateway instances (or the same instance after a rolling update without a persistent disk) will re-build the index-header from original block's index each time, if not already existing on local disk.

## Impact on number of open file descriptors

The Store Gateway stores each block's index-header on the local disk and loads it via mmap. This means that the Gateway keeps a file descriptor for each loaded block. If your Thanos setup has many blocks in the bucket, the Gateway may hit the `file-max` ulimit (maximum number of open file descriptions by a process); in such case, we recommend increasing the limit on your system.

## Impact on CPU, memory and disk

Given an index-header, the Store Gateway does **not** fully load the entire content in memory, but it only loads 1/32 of postings offsets. At query time, postings offsets are then looked up both from memory (1/32 offsets) and from the mmap-ed index-header files, which could hit the disk or not whether the requested file block has been cached by the OS.

The trade-off picked by this optimization technique allows to significantly reduce the Store Gateway memory utilization while increasing CPU and disk read OPS. The Store Gateway has a hidden CLI flag `--store.index-header-posting-offsets-in-mem-sampling` to control the ratio of postings offsets loaded into memory; larger values will lower memory usage and increase CPU and disk read OPS, while lower values will increase memory and lower CPU and disk read OPS. A value of `1` will keep all in memory.
