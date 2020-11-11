package indexheader

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestReaderPool_NewBinaryReader(t *testing.T) {
	tests := map[string]struct {
		lazyReaderEnabled     bool
		lazyReaderIdleTimeout time.Duration
	}{
		"lazy reader is disabled": {
			lazyReaderEnabled: false,
		},
		"lazy reader is enabled but close on idle timeout is disabled": {
			lazyReaderEnabled:     true,
			lazyReaderIdleTimeout: 0,
		},
		"lazy reader and close on idle timeout are both enabled": {
			lazyReaderEnabled:     true,
			lazyReaderIdleTimeout: time.Minute,
		},
	}

	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String())))

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pool := NewReaderPool(log.NewNopLogger(), testData.lazyReaderEnabled, testData.lazyReaderIdleTimeout)
			defer pool.Close()

			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3)
			testutil.Ok(t, err)
			defer func() { testutil.Ok(t, r.Close()) }()

			// Ensure it can read data.
			labelNames, err := r.LabelNames()
			testutil.Ok(t, err)
			testutil.Equals(t, []string{"a"}, labelNames)
		})
	}
}

func TestReaderPool_ShouldCloseIdleLazyReaders(t *testing.T) {
	const idleTimeout = time.Second

	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String())))

	pool := NewReaderPool(log.NewNopLogger(), true, idleTimeout)
	defer pool.Close()

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, r.Close()) }()

	// Ensure it can read data.
	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Assert(t, r.(*readerTracker).reader.(*LazyBinaryReader).reader != nil)

	// Wait enough time before checking it.
	time.Sleep(idleTimeout * 2)

	// We expect the reader has been closed, but not released from the pool.
	testutil.Assert(t, r.(*readerTracker).reader.(*LazyBinaryReader).reader == nil)
	testutil.Assert(t, pool.isTracking(r.(*readerTracker)))

	// Ensure it can still read data (will be re-opened).
	labelNames, err = r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Assert(t, r.(*readerTracker).reader.(*LazyBinaryReader).reader != nil)
	testutil.Assert(t, pool.isTracking(r.(*readerTracker)))

	// We expect an explicit call to Close() to close the reader and release it from the pool too.
	testutil.Ok(t, r.Close())
	testutil.Assert(t, r.(*readerTracker).reader.(*LazyBinaryReader).reader == nil)
	testutil.Assert(t, !pool.isTracking(r.(*readerTracker)))
}
