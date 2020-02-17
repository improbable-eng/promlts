// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestReaders(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block index version 2.
	id1, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "1"}, {Name: "longer-string", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124)
	testutil.Ok(t, err)

	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, id1.String())))

	// Copy block index version 1 for backward compatibility.
	/* The block here was produced at the commit
	    706602daed1487f7849990678b4ece4599745905 used in 2.0.0 with:
	   db, _ := Open("v1db", nil, nil, nil)
	   app := db.Appender()
	   app.Add(labels.FromStrings("foo", "bar"), 1, 2)
	   app.Add(labels.FromStrings("foo", "baz"), 3, 4)
	   app.Add(labels.FromStrings("foo", "meh"), 1000*3600*4, 4) // Not in the block.
	   // Make sure we've enough values for the lack of sorting of postings offsets to show up.
	   for i := 0; i < 100; i++ {
	     app.Add(labels.FromStrings("bar", strconv.FormatInt(int64(i), 10)), 0, 0)
	   }
	   app.Commit()
	   db.compact()
	   db.Close()
	*/

	m, err := metadata.Read("./testdata/index_format_v1")
	testutil.Ok(t, err)
	e2eutil.Copy(t, "./testdata/index_format_v1", filepath.Join(tmpDir, m.ULID.String()))

	bdir := filepath.Join(tmpDir, m.ULID.String())
	m.Thanos = metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}
	err = metadata.Write(log.NewNopLogger(), bdir, m)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, m.ULID.String())))

	for _, id := range []ulid.ULID{id1, m.ULID} {
		t.Run(id.String(), func(t *testing.T) {
			indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, id.String(), block.IndexFilename))
			testutil.Ok(t, err)
			defer func() { _ = indexFile.Close() }()

			b := realByteSlice(indexFile.Bytes())

			t.Run("binary", func(t *testing.T) {
				fn := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
				testutil.Ok(t, WriteBinary(ctx, bkt, id, fn))

				br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id)
				testutil.Ok(t, err)

				defer func() { testutil.Ok(t, br.Close()) }()

				if id == id1 {
					testutil.Equals(t, 1, br.version)
					testutil.Equals(t, 2, br.indexVersion)
					testutil.Equals(t, &BinaryTOC{Symbols: headerLen, PostingsOffsetTable: 50}, br.toc)
					testutil.Equals(t, int64(330), br.indexLastPostingEnd)
					testutil.Equals(t, 8, br.symbols.Size())
					testutil.Equals(t, 3, len(br.postings))
					testutil.Equals(t, 0, len(br.postingsV1))
					testutil.Equals(t, 2, len(br.nameSymbols))
				}

				compareIndexToHeader(t, b, br)
			})

			t.Run("json", func(t *testing.T) {
				fn := filepath.Join(tmpDir, id.String(), block.IndexCacheFilename)
				testutil.Ok(t, WriteJSON(log.NewNopLogger(), filepath.Join(tmpDir, id.String(), "index"), fn))

				jr, err := NewJSONReader(ctx, log.NewNopLogger(), nil, tmpDir, id)
				testutil.Ok(t, err)

				defer func() { testutil.Ok(t, jr.Close()) }()

				if id == id1 {
					testutil.Equals(t, 6, len(jr.symbols))
					testutil.Equals(t, 2, len(jr.lvals))
					testutil.Equals(t, 6, len(jr.postings))
				}

				compareIndexToHeader(t, b, jr)
			})
		})
	}

}

func compareIndexToHeader(t *testing.T, indexByteSlice index.ByteSlice, headerReader Reader) {
	indexReader, err := index.NewReader(indexByteSlice)
	testutil.Ok(t, err)
	defer func() { _ = indexReader.Close() }()

	testutil.Equals(t, indexReader.Version(), headerReader.IndexVersion())

	if indexReader.Version() == index.FormatV2 {
		// For v2 symbols ref sequential integers 0, 1, 2 etc.
		iter := indexReader.Symbols()
		i := 0
		for iter.Next() {
			r, err := headerReader.LookupSymbol(uint32(i))
			testutil.Ok(t, err)
			testutil.Equals(t, iter.At(), r)

			i++
		}
		testutil.Ok(t, iter.Err())
		_, err := headerReader.LookupSymbol(uint32(i))
		testutil.NotOk(t, err)

	} else {
		// For v1 symbols refs are actual offsets in the index.
		symbols, err := getSymbolTable(indexByteSlice)
		testutil.Ok(t, err)

		for refs, sym := range symbols {
			r, err := headerReader.LookupSymbol(refs)
			testutil.Ok(t, err)
			testutil.Equals(t, sym, r)
		}
		_, err = headerReader.LookupSymbol(200000)
		testutil.NotOk(t, err)
	}

	expLabelNames, err := indexReader.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, expLabelNames, headerReader.LabelNames())

	expRanges, err := indexReader.PostingsRanges()
	testutil.Ok(t, err)

	minStart := int64(math.MaxInt64)
	maxEnd := int64(math.MinInt64)
	for il, lname := range expLabelNames {
		expectedLabelVals, err := indexReader.LabelValues(lname)
		testutil.Ok(t, err)

		vals, err := headerReader.LabelValues(lname)
		testutil.Ok(t, err)
		testutil.Equals(t, expectedLabelVals, vals)

		for iv, v := range vals {
			if minStart > expRanges[labels.Label{Name: lname, Value: v}].Start {
				minStart = expRanges[labels.Label{Name: lname, Value: v}].Start
			}
			if maxEnd < expRanges[labels.Label{Name: lname, Value: v}].End {
				maxEnd = expRanges[labels.Label{Name: lname, Value: v}].End
			}

			ptr, err := headerReader.PostingsOffset(lname, v)
			testutil.Ok(t, err)

			// For index-cache those values are exact.
			//
			// For binary they are exact except last item posting offset. It's good enough if the value is larger than exact posting ending.
			if indexReader.Version() == index.FormatV2 {
				if iv == len(vals)-1 && il == len(expLabelNames)-1 {
					testutil.Equals(t, expRanges[labels.Label{Name: lname, Value: v}].Start, ptr.Start)
					testutil.Assert(t, expRanges[labels.Label{Name: lname, Value: v}].End <= ptr.End, "got offset %v earlier than actual posting end %v ", ptr.End, expRanges[labels.Label{Name: lname, Value: v}].End)
					continue
				}
			} else {
				// For index formatV1 the last one does not mean literally last value, as postings were not sorted.
				// Account for that. We know it's 40 label value.
				if v == "40" {
					testutil.Equals(t, expRanges[labels.Label{Name: lname, Value: v}].Start, ptr.Start)
					testutil.Assert(t, expRanges[labels.Label{Name: lname, Value: v}].End <= ptr.End, "got offset %v earlier than actual posting end %v ", ptr.End, expRanges[labels.Label{Name: lname, Value: v}].End)
					continue
				}
			}
			testutil.Equals(t, expRanges[labels.Label{Name: lname, Value: v}], ptr)
		}
	}

	ptr, err := headerReader.PostingsOffset(index.AllPostingsKey())
	testutil.Ok(t, err)
	testutil.Equals(t, expRanges[labels.Label{Name: "", Value: ""}].Start, ptr.Start)
	testutil.Equals(t, expRanges[labels.Label{Name: "", Value: ""}].End, ptr.End)

	vals, err := indexReader.LabelValues("not-existing")
	testutil.Ok(t, err)
	testutil.Equals(t, []string(nil), vals)

	_, err = headerReader.PostingsOffset("not-existing", "1")
	testutil.NotOk(t, err)
}

func prepareIndexV2Block(t testing.TB, tmpDir string, bkt objstore.Bucket) *metadata.Meta {
	/* Copy index 6MB block index version 2. It was generated via thanosbench. Meta.json:
		{
		"ulid": "01DRBP4RNVZ94135ZA6B10EMRR",
		"minTime": 1570766415000,
		"maxTime": 1570939215001,
		"stats": {
			"numSamples": 115210000,
			"numSeries": 10000,
			"numChunks": 990000
		},
		"compaction": {
			"level": 1,
			"sources": [
				"01DRBP4RNVZ94135ZA6B10EMRR"
			]
		},
		"version": 1,
		"thanos": {
			"labels": {
				"cluster": "one",
				"dataset": "continuous"
			},
			"downsample": {
				"resolution": 0
			},
			"source": "blockgen"
		}
	}
	*/

	m, err := metadata.Read("./testdata/index_format_v2")
	testutil.Ok(t, err)
	e2eutil.Copy(t, "./testdata/index_format_v2", filepath.Join(tmpDir, m.ULID.String()))

	m.Thanos = metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}
	bdir := filepath.Join(tmpDir, m.ULID.String())
	err = metadata.Write(log.NewNopLogger(), bdir, m)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(context.Background(), log.NewNopLogger(), bkt, filepath.Join(tmpDir, m.ULID.String())))

	return m
}

func BenchmarkJSONWrite(t *testing.B) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	tmpDir, err := ioutil.TempDir("", "bench-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	m := prepareIndexV2Block(t, tmpDir, bkt)
	testutil.Ok(t, os.MkdirAll(filepath.Join(tmpDir, "local", m.ULID.String()), os.ModePerm))
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexCacheFilename)
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		testutil.Ok(t, forceDownloadFile(
			ctx,
			logger,
			bkt,
			filepath.Join(m.ULID.String(), block.IndexFilename),
			filepath.Join(tmpDir, "local", m.ULID.String(), block.IndexFilename),
		))
		testutil.Ok(t, WriteJSON(logger, filepath.Join(tmpDir, "local", m.ULID.String(), block.IndexFilename), fn))
	}
}

func forceDownloadFile(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, src, dst string) (err error) {
	rc, err := bkt.Get(ctx, src)
	if err != nil {
		return errors.Wrapf(err, "get file %s", src)
	}
	defer runutil.CloseWithLogOnErr(logger, rc, "download block's file reader")

	f, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "create file")
	}

	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if rerr := os.Remove(dst); rerr != nil {
				level.Warn(logger).Log("msg", "failed to remove partially downloaded file", "file", dst, "err", rerr)
			}
		}
	}()
	defer runutil.CloseWithLogOnErr(logger, f, "download block's output file")

	if _, err = io.Copy(f, rc); err != nil {
		return errors.Wrap(err, "copy object to file")
	}
	return nil
}

func BenchmarkJSONReader(t *testing.B) {
	logger := log.NewNopLogger()
	tmpDir, err := ioutil.TempDir("", "bench-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	m := prepareIndexV2Block(t, tmpDir, bkt)
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexCacheFilename)
	testutil.Ok(t, WriteJSON(log.NewNopLogger(), filepath.Join(tmpDir, m.ULID.String(), block.IndexFilename), fn))

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		jr, err := newFileJSONReader(logger, fn)
		testutil.Ok(t, err)
		testutil.Ok(t, jr.Close())
	}
}

func BenchmarkBinaryWrite(t *testing.B) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "bench-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	m := prepareIndexV2Block(t, tmpDir, bkt)
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexHeaderFilename)

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		testutil.Ok(t, WriteBinary(ctx, bkt, m.ULID, fn))
	}
}

func BenchmarkBinaryReader(t *testing.B) {
	ctx := context.Background()
	tmpDir, err := ioutil.TempDir("", "bench-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)

	m := prepareIndexV2Block(t, tmpDir, bkt)
	fn := filepath.Join(tmpDir, m.ULID.String(), block.IndexHeaderFilename)
	testutil.Ok(t, WriteBinary(ctx, bkt, m.ULID, fn))

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		br, err := newFileBinaryReader(fn)
		testutil.Ok(t, err)
		testutil.Ok(t, br.Close())
	}
}
