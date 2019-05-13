package indexcache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/labels"
)

func TestWriteReadBinaryCache(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-compact-prepare")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	b, err := testutil.CreateBlock(tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, nil, 124)
	testutil.Ok(t, err)

	l := log.NewNopLogger()
	bCache := BinaryCache{logger: l}

	fn := filepath.Join(tmpDir, "index.cache.dat")
	testutil.Ok(t, bCache.WriteIndexCache(filepath.Join(tmpDir, b.String(), "index"), fn))

	version, symbols, lvals, postings, err := bCache.ReadIndexCache(fn)
	testutil.Ok(t, err)

	testutil.Equals(t, 6, len(symbols))
	testutil.Equals(t, 2, len(lvals))
	testutil.Equals(t, 2, version)

	vals, ok := lvals["a"]
	testutil.Assert(t, ok, "")
	testutil.Equals(t, []string{"1", "2", "3", "4"}, vals)

	vals, ok = lvals["b"]
	testutil.Assert(t, ok, "")
	testutil.Equals(t, []string{"1"}, vals)
	testutil.Equals(t, 6, len(postings))
}
