// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package replicate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func testLogger(testName string) log.Logger {
	return log.With(
		level.NewFilter(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), level.AllowDebug()),
		"test", testName,
	)
}

func testULID(inc int64) ulid.ULID {
	timestamp := time.Unix(1000000+inc, 0)
	entropy := ulid.Monotonic(rand.New(rand.NewSource(timestamp.UnixNano())), 0)
	ulid := ulid.MustNew(ulid.Timestamp(timestamp), entropy)

	return ulid
}

func testMeta(ulid ulid.ULID) *metadata.Meta {
	return &metadata.Meta{
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"test-labelname": "test-labelvalue",
			},
			Downsample: metadata.ThanosDownsample{
				Resolution: int64(compact.ResolutionLevelRaw),
			},
		},
		BlockMeta: tsdb.BlockMeta{
			ULID: ulid,
			Compaction: tsdb.BlockMetaCompaction{
				Level: 1,
			},
			Version: metadata.TSDBVersion1,
		},
	}
}

func TestReplicationSchemeAll(t *testing.T) {
	testBlockID := testULID(0)
	var cases = []struct {
		name     string
		selector labels.Selector
		blockIDs []ulid.ULID
		prepare  func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket)
		assert   func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket)
	}{
		{
			name:    "EmptyOrigin",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {},
			assert:  func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {},
		},
		{
			name: "NoMeta",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				_ = originBucket.Upload(ctx, path.Join(testULID(0).String(), "chunks", "000001"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				if len(targetBucket.Objects()) != 0 {
					t.Fatal("TargetBucket should have been empty but is not.")
				}
			},
		},
		{
			name: "PartialMeta",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				_ = originBucket.Upload(ctx, path.Join(testULID(0).String(), "meta.json"), bytes.NewReader([]byte("{")))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				if len(targetBucket.Objects()) != 0 {
					t.Fatal("TargetBucket should have been empty but is not.")
				}
			},
		},
		{
			name: "FullBlock",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				ulid := testULID(0)
				meta := testMeta(ulid)

				b, err := json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				if len(targetBucket.Objects()) != 3 {
					t.Fatal("TargetBucket should have one block made up of three objects replicated.")
				}
			},
		},
		{
			name: "PreviousPartialUpload",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				ulid := testULID(0)
				meta := testMeta(ulid)

				b, err := json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))

				_ = targetBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), io.LimitReader(bytes.NewReader(b), int64(len(b)-10)))
				_ = targetBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = targetBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				for k := range originBucket.Objects() {
					if !bytes.Equal(originBucket.Objects()[k], targetBucket.Objects()[k]) {
						t.Fatalf("Object %s not equal in origin and target bucket.", k)
					}
				}
			},
		},
		{
			name: "OnlyUploadsRaw",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				ulid := testULID(0)
				meta := testMeta(ulid)

				b, err := json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))

				ulid = testULID(1)
				meta = testMeta(ulid)
				meta.Thanos.Downsample.Resolution = int64(compact.ResolutionLevel5m)

				b, err = json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				expected := 3
				got := len(targetBucket.Objects())
				if got != expected {
					t.Fatalf("TargetBucket should have one block made up of three objects replicated. Got %d but expected %d objects.", got, expected)
				}
			},
		},
		{
			name: "UploadMultipleCandidatesWhenPresent",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				ulid := testULID(0)
				meta := testMeta(ulid)

				b, err := json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))

				ulid = testULID(1)
				meta = testMeta(ulid)

				b, err = json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				expected := 6
				got := len(targetBucket.Objects())
				if got != expected {
					t.Fatalf("TargetBucket should have two blocks made up of three objects replicated. Got %d but expected %d objects.", got, expected)
				}
			},
		},
		{
			name: "LabelSelector",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				ulid := testULID(0)
				meta := testMeta(ulid)

				b, err := json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))

				ulid = testULID(1)
				meta = testMeta(ulid)
				meta.Thanos.Labels["test-labelname"] = "non-selected-value"

				b, err = json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				expected := 3
				got := len(targetBucket.Objects())
				if got != expected {
					t.Fatalf("TargetBucket should have one block made up of three objects replicated. Got %d but expected %d objects.", got, expected)
				}
			},
		},
		{
			name: "NonZeroCompaction",
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				ulid := testULID(0)
				meta := testMeta(ulid)
				meta.BlockMeta.Compaction.Level = 2

				b, err := json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				if len(targetBucket.Objects()) != 0 {
					t.Fatal("TargetBucket should have been empty but is not.")
				}
			},
		},
		{
			name:     "Regression",
			selector: labels.Selector{},
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				b := []byte(`{
        "ulid": "01DQYXMK8G108CEBQ79Y84DYVY",
        "minTime": 1571911200000,
        "maxTime": 1571918400000,
        "stats": {
                "numSamples": 90793,
                "numSeries": 3703,
                "numChunks": 3746
        },
        "compaction": {
                "level": 1,
                "sources": [
                        "01DQYXMK8G108CEBQ79Y84DYVY"
                ]
        },
        "version": 1,
        "thanos": {
                "labels": {
                        "receive": "true",
                        "replica": "thanos-receive-default-0"
                },
                "downsample": {
                        "resolution": 0
                },
                "source": "receive"
        }
}`)

				_ = originBucket.Upload(ctx, path.Join("01DQYXMK8G108CEBQ79Y84DYVY", "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join("01DQYXMK8G108CEBQ79Y84DYVY", "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join("01DQYXMK8G108CEBQ79Y84DYVY", "index"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				if len(targetBucket.Objects()) != 3 {
					t.Fatal("TargetBucket should have one block does not.")
				}

				expected := originBucket.Objects()["01DQYXMK8G108CEBQ79Y84DYVY/meta.json"]
				got := targetBucket.Objects()["01DQYXMK8G108CEBQ79Y84DYVY/meta.json"]
				testutil.Equals(t, expected, got)
			},
		},
		{
			name:     "BlockIDs",
			blockIDs: []ulid.ULID{testBlockID},
			prepare: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				meta := testMeta(testBlockID)

				b, err := json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(testBlockID.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(testBlockID.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(testBlockID.String(), "index"), bytes.NewReader(nil))

				ulid := testULID(1)
				meta = testMeta(ulid)

				b, err = json.Marshal(meta)
				testutil.Ok(t, err)
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "meta.json"), bytes.NewReader(b))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "chunks", "000001"), bytes.NewReader(nil))
				_ = originBucket.Upload(ctx, path.Join(ulid.String(), "index"), bytes.NewReader(nil))
			},
			assert: func(ctx context.Context, t *testing.T, originBucket, targetBucket *objstore.InMemBucket) {
				expected := 3
				got := len(targetBucket.Objects())
				if got != expected {
					t.Fatalf("TargetBucket should have one block made up of three objects replicated. Got %d but expected %d objects.", got, expected)
				}
			},
		},
	}

	for _, c := range cases {
		ctx := context.Background()
		originBucket := objstore.NewInMemBucket()
		targetBucket := objstore.NewInMemBucket()
		logger := testLogger(t.Name() + "/" + c.name)

		c.prepare(ctx, t, originBucket, targetBucket)

		matcher, err := labels.NewMatcher(labels.MatchEqual, "test-labelname", "test-labelvalue")
		testutil.Ok(t, err)

		selector := labels.Selector{
			matcher,
		}
		if c.selector != nil {
			selector = c.selector
		}

		filter := NewBlockFilter(logger, selector, []compact.ResolutionLevel{compact.ResolutionLevelRaw}, []int{1}, c.blockIDs).Filter
		fetcher, err := block.NewMetaFetcher(logger, 32, objstore.WithNoopInstr(originBucket), "", nil, nil, nil)
		testutil.Ok(t, err)

		r := newReplicationScheme(logger, newReplicationMetrics(nil), filter, fetcher, objstore.WithNoopInstr(originBucket), targetBucket, nil)

		err = r.execute(ctx, 5)
		testutil.Ok(t, err)

		c.assert(ctx, t, originBucket, targetBucket)
	}
}
