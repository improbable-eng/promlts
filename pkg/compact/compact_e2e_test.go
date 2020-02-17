// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/objtesting"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestSyncer_GarbageCollect_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		// Generate 10 source block metas and construct higher level blocks
		// that are higher compactions of them.
		var metas []*metadata.Meta
		var ids []ulid.ULID

		for i := 0; i < 10; i++ {
			var m metadata.Meta

			m.Version = 1
			m.ULID = ulid.MustNew(uint64(i), nil)
			m.Compaction.Sources = []ulid.ULID{m.ULID}
			m.Compaction.Level = 1

			ids = append(ids, m.ULID)
			metas = append(metas, &m)
		}

		var m1 metadata.Meta
		m1.Version = 1
		m1.ULID = ulid.MustNew(100, nil)
		m1.Compaction.Level = 2
		m1.Compaction.Sources = ids[:4]
		m1.Thanos.Downsample.Resolution = 0

		var m2 metadata.Meta
		m2.Version = 1
		m2.ULID = ulid.MustNew(200, nil)
		m2.Compaction.Level = 2
		m2.Compaction.Sources = ids[4:8] // last two source IDs is not part of a level 2 block.
		m2.Thanos.Downsample.Resolution = 0

		var m3 metadata.Meta
		m3.Version = 1
		m3.ULID = ulid.MustNew(300, nil)
		m3.Compaction.Level = 3
		m3.Compaction.Sources = ids[:9] // last source ID is not part of level 3 block.
		m3.Thanos.Downsample.Resolution = 0

		var m4 metadata.Meta
		m4.Version = 1
		m4.ULID = ulid.MustNew(400, nil)
		m4.Compaction.Level = 2
		m4.Compaction.Sources = ids[9:] // covers the last block but is a different resolution. Must not trigger deletion.
		m4.Thanos.Downsample.Resolution = 1000

		// Create all blocks in the bucket.
		for _, m := range append(metas, &m1, &m2, &m3, &m4) {
			fmt.Println("create", m.ULID)
			var buf bytes.Buffer
			testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
			testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ULID.String(), metadata.MetaFilename), &buf))
		}

		metaFetcher, err := block.NewMetaFetcher(nil, 32, bkt, "", nil)
		testutil.Ok(t, err)

		sy, err := NewSyncer(nil, nil, bkt, metaFetcher, 1, false, false)
		testutil.Ok(t, err)

		// Do one initial synchronization with the bucket.
		testutil.Ok(t, sy.SyncMetas(ctx))
		testutil.Ok(t, sy.GarbageCollect(ctx))

		var rem []ulid.ULID
		err = bkt.Iter(ctx, "", func(n string) error {
			rem = append(rem, ulid.MustParse(n[:len(n)-1]))
			return nil
		})
		testutil.Ok(t, err)

		sort.Slice(rem, func(i, j int) bool {
			return rem[i].Compare(rem[j]) < 0
		})

		// Only the level 3 block, the last source block in both resolutions should be left.
		testutil.Equals(t, []ulid.ULID{metas[9].ULID, m3.ULID, m4.ULID}, rem)

		// After another sync the changes should also be reflected in the local groups.
		testutil.Ok(t, sy.SyncMetas(ctx))

		// Only the level 3 block, the last source block in both resolutions should be left.
		groups, err := sy.Groups()
		testutil.Ok(t, err)

		testutil.Equals(t, "0@17241709254077376921", groups[0].Key())
		testutil.Equals(t, []ulid.ULID{metas[9].ULID, m3.ULID}, groups[0].IDs())
		testutil.Equals(t, "1000@17241709254077376921", groups[1].Key())
		testutil.Equals(t, []ulid.ULID{m4.ULID}, groups[1].IDs())
	})
}

func MetricCount(c prometheus.Collector) int {
	var (
		mCount int
		mChan  = make(chan prometheus.Metric)
		done   = make(chan struct{})
	)

	go func() {
		for range mChan {
			mCount++
		}
		close(done)
	}()

	c.Collect(mChan)
	close(mChan)
	<-done

	return mCount
}

func TestGroup_Compact_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		// Create fresh, empty directory for actual test.
		dir, err := ioutil.TempDir("", "test-compact")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

		logger := log.NewLogfmtLogger(os.Stderr)

		reg := prometheus.NewRegistry()

		metaFetcher, err := block.NewMetaFetcher(nil, 32, bkt, "", nil)
		testutil.Ok(t, err)

		sy, err := NewSyncer(nil, nil, bkt, metaFetcher, 5, false, false)
		testutil.Ok(t, err)

		comp, err := tsdb.NewLeveledCompactor(ctx, reg, logger, []int64{1000, 3000}, nil)
		testutil.Ok(t, err)

		bComp, err := NewBucketCompactor(logger, sy, comp, dir, bkt, 2)
		testutil.Ok(t, err)

		// Compaction on empty should not fail.
		testutil.Ok(t, bComp.Compact(ctx))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.garbageCollectedBlocks))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.garbageCollectionFailures))
		testutil.Equals(t, 0, MetricCount(sy.metrics.compactions))
		testutil.Equals(t, 0, MetricCount(sy.metrics.compactionRunsStarted))
		testutil.Equals(t, 0, MetricCount(sy.metrics.compactionRunsCompleted))
		testutil.Equals(t, 0, MetricCount(sy.metrics.compactionFailures))

		_, err = os.Stat(dir)
		testutil.Assert(t, os.IsNotExist(err), "dir %s should be remove after compaction.", dir)

		// Test label name with slash, regression: https://github.com/thanos-io/thanos/issues/1661.
		extLabels := labels.Labels{{Name: "e1", Value: "1/weird"}}
		extLabels2 := labels.Labels{{Name: "e1", Value: "1"}}
		metas := createAndUpload(t, bkt, []blockgenSpec{
			{
				numSamples: 100, mint: 0, maxt: 1000, extLset: extLabels, res: 124,
				series: []labels.Labels{
					{{Name: "a", Value: "1"}},
					{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
					{{Name: "a", Value: "3"}},
					{{Name: "a", Value: "4"}},
				},
			},
			{
				numSamples: 100, mint: 2000, maxt: 3000, extLset: extLabels, res: 124,
				series: []labels.Labels{
					{{Name: "a", Value: "3"}},
					{{Name: "a", Value: "4"}},
					{{Name: "a", Value: "5"}},
					{{Name: "a", Value: "6"}},
				},
			},
			// Mix order to make sure compact is able to deduct min time / max time.
			// Currently TSDB does not produces empty blocks (see: https://github.com/prometheus/tsdb/pull/374). However before v2.7.0 it was
			// so we still want to mimick this case as close as possible.
			{
				mint: 1000, maxt: 2000, extLset: extLabels, res: 124,
				// Empty block.
			},
			// Due to TSDB compaction delay (not compacting fresh block), we need one more block to be pushed to trigger compaction.
			{
				numSamples: 100, mint: 3000, maxt: 4000, extLset: extLabels, res: 124,
				series: []labels.Labels{
					{{Name: "a", Value: "7"}},
				},
			},
			// Extra block for "distraction" for different resolution and one for different labels.
			{
				numSamples: 100, mint: 5000, maxt: 6000, extLset: labels.Labels{{Name: "e1", Value: "2"}}, res: 124,
				series: []labels.Labels{
					{{Name: "a", Value: "7"}},
				},
			},
			// Extra block for "distraction" for different resolution and one for different labels.
			{
				numSamples: 100, mint: 4000, maxt: 5000, extLset: extLabels, res: 0,
				series: []labels.Labels{
					{{Name: "a", Value: "7"}},
				},
			},
			// Second group (extLabels2).
			{
				numSamples: 100, mint: 2000, maxt: 3000, extLset: extLabels2, res: 124,
				series: []labels.Labels{
					{{Name: "a", Value: "3"}},
					{{Name: "a", Value: "4"}},
					{{Name: "a", Value: "6"}},
				},
			},
			{
				numSamples: 100, mint: 0, maxt: 1000, extLset: extLabels2, res: 124,
				series: []labels.Labels{
					{{Name: "a", Value: "1"}},
					{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
					{{Name: "a", Value: "3"}},
					{{Name: "a", Value: "4"}},
				},
			},
			// Due to TSDB compaction delay (not compacting fresh block), we need one more block to be pushed to trigger compaction.
			{
				numSamples: 100, mint: 3000, maxt: 4000, extLset: extLabels2, res: 124,
				series: []labels.Labels{
					{{Name: "a", Value: "7"}},
				},
			},
		})

		testutil.Ok(t, bComp.Compact(ctx))
		testutil.Equals(t, 5.0, promtest.ToFloat64(sy.metrics.garbageCollectedBlocks))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.garbageCollectionFailures))
		testutil.Equals(t, 4, MetricCount(sy.metrics.compactions))
		testutil.Equals(t, 1.0, promtest.ToFloat64(sy.metrics.compactions.WithLabelValues(GroupKey(metas[0].Thanos))))
		testutil.Equals(t, 1.0, promtest.ToFloat64(sy.metrics.compactions.WithLabelValues(GroupKey(metas[7].Thanos))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.compactions.WithLabelValues(GroupKey(metas[4].Thanos))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.compactions.WithLabelValues(GroupKey(metas[5].Thanos))))
		testutil.Equals(t, 4, MetricCount(sy.metrics.compactionRunsStarted))
		testutil.Equals(t, 2.0, promtest.ToFloat64(sy.metrics.compactionRunsStarted.WithLabelValues(GroupKey(metas[0].Thanos))))
		testutil.Equals(t, 2.0, promtest.ToFloat64(sy.metrics.compactionRunsStarted.WithLabelValues(GroupKey(metas[7].Thanos))))
		// TODO(bwplotka): Looks like we do some unnecessary loops. Not a major problem but investigate.
		testutil.Equals(t, 2.0, promtest.ToFloat64(sy.metrics.compactionRunsStarted.WithLabelValues(GroupKey(metas[4].Thanos))))
		testutil.Equals(t, 2.0, promtest.ToFloat64(sy.metrics.compactionRunsStarted.WithLabelValues(GroupKey(metas[5].Thanos))))
		testutil.Equals(t, 4, MetricCount(sy.metrics.compactionRunsCompleted))
		testutil.Equals(t, 2.0, promtest.ToFloat64(sy.metrics.compactionRunsCompleted.WithLabelValues(GroupKey(metas[0].Thanos))))
		testutil.Equals(t, 2.0, promtest.ToFloat64(sy.metrics.compactionRunsCompleted.WithLabelValues(GroupKey(metas[7].Thanos))))
		// TODO(bwplotka): Looks like we do some unnecessary loops. Not a major problem but investigate.
		testutil.Equals(t, 2.0, promtest.ToFloat64(sy.metrics.compactionRunsCompleted.WithLabelValues(GroupKey(metas[4].Thanos))))
		testutil.Equals(t, 2.0, promtest.ToFloat64(sy.metrics.compactionRunsCompleted.WithLabelValues(GroupKey(metas[5].Thanos))))
		testutil.Equals(t, 4, MetricCount(sy.metrics.compactionFailures))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.compactionFailures.WithLabelValues(GroupKey(metas[0].Thanos))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.compactionFailures.WithLabelValues(GroupKey(metas[7].Thanos))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.compactionFailures.WithLabelValues(GroupKey(metas[4].Thanos))))
		testutil.Equals(t, 0.0, promtest.ToFloat64(sy.metrics.compactionFailures.WithLabelValues(GroupKey(metas[5].Thanos))))

		_, err = os.Stat(dir)
		testutil.Assert(t, os.IsNotExist(err), "dir %s should be remove after compaction.", dir)

		// Check object storage. All blocks that were included in new compacted one should be removed. New compacted ones
		// are present and looks as expected.
		nonCompactedExpected := map[ulid.ULID]bool{
			metas[3].ULID: false,
			metas[4].ULID: false,
			metas[5].ULID: false,
			metas[8].ULID: false,
		}
		others := map[string]metadata.Meta{}
		testutil.Ok(t, bkt.Iter(ctx, "", func(n string) error {
			id, ok := block.IsBlockDir(n)
			if !ok {
				return nil
			}

			if _, ok := nonCompactedExpected[id]; ok {
				nonCompactedExpected[id] = true
				return nil
			}

			meta, err := block.DownloadMeta(ctx, logger, bkt, id)
			if err != nil {
				return err
			}

			others[GroupKey(meta.Thanos)] = meta
			return nil
		}))

		for id, found := range nonCompactedExpected {
			testutil.Assert(t, found, "not found expected block %s", id.String())
		}

		// We expect two compacted blocks only outside of what we expected in `nonCompactedExpected`.
		testutil.Equals(t, 2, len(others))
		{
			meta, ok := others[groupKey(124, extLabels)]
			testutil.Assert(t, ok, "meta not found")

			testutil.Equals(t, int64(0), meta.MinTime)
			testutil.Equals(t, int64(3000), meta.MaxTime)
			testutil.Equals(t, uint64(6), meta.Stats.NumSeries)
			testutil.Equals(t, uint64(2*4*100), meta.Stats.NumSamples) // Only 2 times 4*100 because one block was empty.
			testutil.Equals(t, 2, meta.Compaction.Level)
			testutil.Equals(t, []ulid.ULID{metas[0].ULID, metas[1].ULID, metas[2].ULID}, meta.Compaction.Sources)

			// Check thanos meta.
			testutil.Assert(t, labels.Equal(extLabels, labels.FromMap(meta.Thanos.Labels)), "ext labels does not match")
			testutil.Equals(t, int64(124), meta.Thanos.Downsample.Resolution)
		}
		{
			meta, ok := others[groupKey(124, extLabels2)]
			testutil.Assert(t, ok, "meta not found")

			testutil.Equals(t, int64(0), meta.MinTime)
			testutil.Equals(t, int64(3000), meta.MaxTime)
			testutil.Equals(t, uint64(5), meta.Stats.NumSeries)
			testutil.Equals(t, uint64(2*4*100-100), meta.Stats.NumSamples)
			testutil.Equals(t, 2, meta.Compaction.Level)
			testutil.Equals(t, []ulid.ULID{metas[6].ULID, metas[7].ULID}, meta.Compaction.Sources)

			// Check thanos meta.
			testutil.Assert(t, labels.Equal(extLabels2, labels.FromMap(meta.Thanos.Labels)), "ext labels does not match")
			testutil.Equals(t, int64(124), meta.Thanos.Downsample.Resolution)
		}
	})
}

type blockgenSpec struct {
	mint, maxt int64
	series     []labels.Labels
	numSamples int
	extLset    labels.Labels
	res        int64
}

func createAndUpload(t testing.TB, bkt objstore.Bucket, blocks []blockgenSpec) (metas []*metadata.Meta) {
	prepareDir, err := ioutil.TempDir("", "test-compact-prepare")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(prepareDir)) }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for _, b := range blocks {
		var id ulid.ULID
		var err error
		if b.numSamples == 0 {
			id, err = createEmptyBlock(prepareDir, b.mint, b.maxt, b.extLset, b.res)
		} else {
			id, err = e2eutil.CreateBlock(ctx, prepareDir, b.series, b.numSamples, b.mint, b.maxt, b.extLset, b.res)
		}
		testutil.Ok(t, err)

		meta, err := metadata.Read(filepath.Join(prepareDir, id.String()))
		testutil.Ok(t, err)
		metas = append(metas, meta)

		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(prepareDir, id.String())))
	}
	return metas
}

// createEmptyBlock produces empty block like it was the case before fix: https://github.com/prometheus/tsdb/pull/374.
// (Prometheus pre v2.7.0).
func createEmptyBlock(dir string, mint int64, maxt int64, extLset labels.Labels, resolution int64) (ulid.ULID, error) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	if err := os.Mkdir(path.Join(dir, uid.String()), os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	if err := os.Mkdir(path.Join(dir, uid.String(), "chunks"), os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	w, err := index.NewWriter(context.Background(), path.Join(dir, uid.String(), "index"))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "new index")
	}

	if err := w.Close(); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	m := tsdb.BlockMeta{
		Version: 1,
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{uid},
		},
	}
	b, err := json.Marshal(&m)
	if err != nil {
		return ulid.ULID{}, err
	}

	if err := ioutil.WriteFile(path.Join(dir, uid.String(), "meta.json"), b, os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "saving meta.json")
	}

	bdir := filepath.Join(dir, uid.String())
	meta, err := metadata.Read(bdir)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "read meta")
	}
	meta.Thanos = metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     metadata.TestSource,
	}

	if err = metadata.Write(log.NewNopLogger(), bdir, meta); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "finalize block")
	}

	return uid, nil
}
