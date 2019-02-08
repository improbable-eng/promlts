package store

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/objtesting"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"
)

type storeSuite struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup

	store            *BucketStore
	minTime, maxTime int64
}

func (s *storeSuite) Close() {
	s.cancel()
	s.wg.Wait()
}

func prepareStoreWithTestBlocks(t testing.TB, dir string, bkt objstore.Bucket, maxSampleCount uint64) *storeSuite {
	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "2", "b", "1"),
		labels.FromStrings("a", "2", "b", "2"),
		labels.FromStrings("a", "1", "c", "1"),
		labels.FromStrings("a", "1", "c", "2"),
		labels.FromStrings("a", "2", "c", "1"),
		labels.FromStrings("a", "2", "c", "2"),
	}
	extLset := labels.FromStrings("ext1", "value1")

	start := time.Now()
	now := start

	ctx, cancel := context.WithCancel(context.Background())
	s := &storeSuite{cancel: cancel}
	blocks := 0
	for i := 0; i < 3; i++ {
		mint := timestamp.FromTime(now)
		now = now.Add(2 * time.Hour)
		maxt := timestamp.FromTime(now)

		if s.minTime == 0 {
			s.minTime = mint
		}
		s.maxTime = maxt

		// Create two blocks per time slot. Only add 10 samples each so only one chunk
		// gets created each. This way we can easily verify we got 10 chunks per series below.
		id1, err := testutil.CreateBlock(dir, series[:4], 10, mint, maxt, extLset, 0)
		testutil.Ok(t, err)
		id2, err := testutil.CreateBlock(dir, series[4:], 10, mint, maxt, extLset, 0)
		testutil.Ok(t, err)

		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Add labels to the meta of the second block.
		meta, err := metadata.Read(dir2)
		testutil.Ok(t, err)
		meta.Thanos.Labels = map[string]string{"ext2": "value2"}
		testutil.Ok(t, metadata.Write(log.NewNopLogger(), dir2, meta))

		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, dir1))
		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, dir2))
		blocks += 2

		testutil.Ok(t, os.RemoveAll(dir1))
		testutil.Ok(t, os.RemoveAll(dir2))
	}

	store, err := NewBucketStore(log.NewLogfmtLogger(os.Stderr), nil, bkt, dir, 100, 0, maxSampleCount, 20, false, 20)
	testutil.Ok(t, err)

	s.store = store

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		if err := runutil.Repeat(100*time.Millisecond, ctx.Done(), func() error {
			return store.SyncBlocks(ctx)
		}); err != nil && errors.Cause(err) != context.Canceled {
			t.Error(err)
			t.FailNow()
		}
	}()

	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()

	testutil.Ok(t, runutil.Retry(100*time.Millisecond, rctx.Done(), func() error {
		if store.numBlocks() < blocks {
			return errors.New("not all blocks loaded")
		}
		return nil
	}))

	return s
}

func TestBucketStore_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dir, err := ioutil.TempDir("", "test_bucketstore_e2e")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

		s := prepareStoreWithTestBlocks(t, dir, bkt, 0)
		defer s.Close()

		mint, maxt := s.store.TimeRange()
		testutil.Equals(t, s.minTime, mint)
		testutil.Equals(t, s.maxTime, maxt)

		vals, err := s.store.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "a"})
		testutil.Ok(t, err)
		testutil.Equals(t, []string{"1", "2"}, vals.Values)

		pbseries := [][]storepb.Label{
			{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
			{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
			{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
			{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
			{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}, {Name: "ext1", Value: "value1"}},
			{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
			{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
			{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
		}
		srv := newStoreSeriesServer(ctx)

		testutil.Ok(t, s.store.Series(&storepb.SeriesRequest{
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
			},
			MinTime: mint,
			MaxTime: maxt,
		}, srv))
		testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

		for i, s := range srv.SeriesSet {
			testutil.Equals(t, pbseries[i], s.Labels)
			testutil.Equals(t, 3, len(s.Chunks))
		}

		pbseries = [][]storepb.Label{
			{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
			{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}, {Name: "ext1", Value: "value1"}},
		}
		srv = newStoreSeriesServer(ctx)

		testutil.Ok(t, s.store.Series(&storepb.SeriesRequest{
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "2"},
			},
			MinTime: mint,
			MaxTime: maxt,
		}, srv))
		testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

		for i, s := range srv.SeriesSet {
			testutil.Equals(t, pbseries[i], s.Labels)
			testutil.Equals(t, 3, len(s.Chunks))
		}

		// Matching by external label should work as well.
		pbseries = [][]storepb.Label{
			{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext2", Value: "value2"}},
			{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext2", Value: "value2"}},
		}
		srv = newStoreSeriesServer(ctx)

		testutil.Ok(t, s.store.Series(&storepb.SeriesRequest{
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				{Type: storepb.LabelMatcher_EQ, Name: "ext2", Value: "value2"},
			},
			MinTime: mint,
			MaxTime: maxt,
		}, srv))
		testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

		for i, s := range srv.SeriesSet {
			testutil.Equals(t, pbseries[i], s.Labels)
			testutil.Equals(t, 3, len(s.Chunks))
		}

		srv = newStoreSeriesServer(ctx)
		testutil.Ok(t, s.store.Series(&storepb.SeriesRequest{
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				{Type: storepb.LabelMatcher_EQ, Name: "ext2", Value: "wrong-value"},
			},
			MinTime: mint,
			MaxTime: maxt,
		}, srv))
		testutil.Equals(t, 0, len(srv.SeriesSet))

		// Test the samples limit.
		testutil.Ok(t, os.RemoveAll(dir))
		s = prepareStoreWithTestBlocks(t, dir, bkt, 120)
		mint, maxt = s.store.TimeRange()
		defer s.Close()

		srv = newStoreSeriesServer(ctx)

		testutil.Ok(t, s.store.Series(&storepb.SeriesRequest{
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
			},
			MinTime: mint,
			MaxTime: maxt,
		}, srv))

		testutil.NotOk(t, s.store.Series(&storepb.SeriesRequest{
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
			},
			MinTime: mint,
			MaxTime: maxt,
		}, srv))

	})

}
