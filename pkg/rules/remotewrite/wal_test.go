// This is copied from https://github.com/grafana/agent/blob/a23bd5cf27c2ac99695b7449d38fb12444941a1c/pkg/prom/wal/wal_test.go
// TODO(idoqo): Migrate to prometheus package when https://github.com/prometheus/prometheus/pull/8785 is ready.
package remotewrite

import (
	"context"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/stretchr/testify/require"
)

func TestStorage_InvalidSeries(t *testing.T) {
	walDir, err := ioutil.TempDir(os.TempDir(), "wal")
	require.NoError(t, err)
	defer os.RemoveAll(walDir)

	s, err := NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, s.Close())
	}()

	app := s.Appender(context.Background())

	_, err = app.Append(0, labels.Labels{}, 0, 0)
	require.Error(t, err, "should reject empty labels")

	_, err = app.Append(0, labels.Labels{{Name: "a", Value: "1"}, {Name: "a", Value: "2"}}, 0, 0)
	require.Error(t, err, "should reject duplicate labels")

	// Sanity check: valid series
	_, err = app.Append(0, labels.Labels{{Name: "a", Value: "1"}}, 0, 0)
	require.NoError(t, err, "should not reject valid series")
}

func TestStorage(t *testing.T) {
	walDir, err := ioutil.TempDir(os.TempDir(), "wal")
	require.NoError(t, err)
	defer os.RemoveAll(walDir)

	s, err := NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, s.Close())
	}()

	app := s.Appender(context.Background())

	// Write some samples
	payload := seriesList{
		{name: "foo", samples: []sample{{1, 10.0}, {10, 100.0}}},
		{name: "bar", samples: []sample{{2, 20.0}, {20, 200.0}}},
		{name: "baz", samples: []sample{{3, 30.0}, {30, 300.0}}},
	}
	for _, metric := range payload {
		metric.Write(t, app)
	}

	require.NoError(t, app.Commit())

	collector := walDataCollector{}
	replayer := walReplayer{w: &collector}
	require.NoError(t, replayer.Replay(s.wal.Dir()))

	names := []string{}
	for _, series := range collector.series {
		names = append(names, series.Labels.Get("__name__"))
	}
	require.Equal(t, payload.SeriesNames(), names)

	expectedSamples := payload.ExpectedSamples()
	actual := collector.samples
	sort.Sort(byRefSample(actual))
	require.Equal(t, expectedSamples, actual)
}

func TestStorage_ExistingWAL(t *testing.T) {
	walDir, err := ioutil.TempDir(os.TempDir(), "wal")
	require.NoError(t, err)
	defer os.RemoveAll(walDir)

	s, err := NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)

	app := s.Appender(context.Background())
	payload := seriesList{
		{name: "foo", samples: []sample{{1, 10.0}, {10, 100.0}}},
		{name: "bar", samples: []sample{{2, 20.0}, {20, 200.0}}},
		{name: "baz", samples: []sample{{3, 30.0}, {30, 300.0}}},
		{name: "blerg", samples: []sample{{4, 40.0}, {40, 400.0}}},
	}

	// Write half of the samples.
	for _, metric := range payload[0 : len(payload)/2] {
		metric.Write(t, app)
	}

	require.NoError(t, app.Commit())
	require.NoError(t, s.Close())

	// We need to wait a little bit for the previous store to finish
	// flushing.
	time.Sleep(time.Millisecond * 150)

	// Create a new storage, write the other half of samples.
	s, err = NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, s.Close())
	}()

	// Verify that the storage picked up existing series when it
	// replayed the WAL.
	for series := range s.series.iterator().Channel() {
		require.Greater(t, series.lastTs, int64(0), "series timestamp not updated")
	}

	app = s.Appender(context.Background())

	for _, metric := range payload[len(payload)/2:] {
		metric.Write(t, app)
	}

	require.NoError(t, app.Commit())

	collector := walDataCollector{}
	replayer := walReplayer{w: &collector}
	require.NoError(t, replayer.Replay(s.wal.Dir()))

	names := []string{}
	for _, series := range collector.series {
		names = append(names, series.Labels.Get("__name__"))
	}
	require.Equal(t, payload.SeriesNames(), names)

	expectedSamples := payload.ExpectedSamples()
	actual := collector.samples
	sort.Sort(byRefSample(actual))
	require.Equal(t, expectedSamples, actual)
}

func TestStorage_Truncate(t *testing.T) {
	// Same as before but now do the following:
	// after writing all the data, forcefully create 4 more segments,
	// then do a truncate of a timestamp for _some_ of the data.
	// then read data back in. Expect to only get the latter half of data.
	walDir, err := ioutil.TempDir(os.TempDir(), "wal")
	require.NoError(t, err)
	defer os.RemoveAll(walDir)

	s, err := NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, s.Close())
	}()

	app := s.Appender(context.Background())

	payload := seriesList{
		{name: "foo", samples: []sample{{1, 10.0}, {10, 100.0}}},
		{name: "bar", samples: []sample{{2, 20.0}, {20, 200.0}}},
		{name: "baz", samples: []sample{{3, 30.0}, {30, 300.0}}},
		{name: "blerg", samples: []sample{{4, 40.0}, {40, 400.0}}},
	}

	for _, metric := range payload {
		metric.Write(t, app)
	}

	require.NoError(t, app.Commit())

	// Forefully create a bunch of new segments so when we truncate
	// there's enough segments to be considered for truncation.
	for i := 0; i < 5; i++ {
		require.NoError(t, s.wal.NextSegment())
	}

	// Truncate half of the samples, keeping only the second sample
	// per series.
	keepTs := payload[len(payload)-1].samples[0].ts + 1
	err = s.Truncate(keepTs)
	require.NoError(t, err)

	payload = payload.Filter(func(s sample) bool {
		return s.ts >= keepTs
	})
	expectedSamples := payload.ExpectedSamples()

	// Read back the WAL, collect series and samples.
	collector := walDataCollector{}
	replayer := walReplayer{w: &collector}
	require.NoError(t, replayer.Replay(s.wal.Dir()))

	names := []string{}
	for _, series := range collector.series {
		names = append(names, series.Labels.Get("__name__"))
	}
	require.Equal(t, payload.SeriesNames(), names)

	actual := collector.samples
	sort.Sort(byRefSample(actual))
	require.Equal(t, expectedSamples, actual)
}

func TestStorage_WriteStalenessMarkers(t *testing.T) {
	walDir, err := ioutil.TempDir(os.TempDir(), "wal")
	require.NoError(t, err)
	defer os.RemoveAll(walDir)

	s, err := NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, s.Close())
	}()

	app := s.Appender(context.Background())

	// Write some samples
	payload := seriesList{
		{name: "foo", samples: []sample{{1, 10.0}, {10, 100.0}}},
		{name: "bar", samples: []sample{{2, 20.0}, {20, 200.0}}},
		{name: "baz", samples: []sample{{3, 30.0}, {30, 300.0}}},
	}
	for _, metric := range payload {
		metric.Write(t, app)
	}

	require.NoError(t, app.Commit())

	// Write staleness markers for every series
	require.NoError(t, s.WriteStalenessMarkers(func() int64 {
		// Pass math.MaxInt64 so it seems like everything was written already
		return math.MaxInt64
	}))

	// Read back the WAL, collect series and samples.
	collector := walDataCollector{}
	replayer := walReplayer{w: &collector}
	require.NoError(t, replayer.Replay(s.wal.Dir()))

	actual := collector.samples
	sort.Sort(byRefSample(actual))

	staleMap := map[uint64]bool{}
	for _, sample := range actual {
		if _, ok := staleMap[sample.Ref]; !ok {
			staleMap[sample.Ref] = false
		}
		if value.IsStaleNaN(sample.V) {
			staleMap[sample.Ref] = true
		}
	}

	for ref, v := range staleMap {
		require.True(t, v, "ref %d doesn't have stale marker", ref)
	}
}

func TestStoraeg_TruncateAfterClose(t *testing.T) {
	walDir, err := ioutil.TempDir(os.TempDir(), "wal")
	require.NoError(t, err)
	defer os.RemoveAll(walDir)

	s, err := NewStorage(log.NewNopLogger(), nil, walDir)
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.Error(t, ErrWALClosed, s.Truncate(0))
}

type sample struct {
	ts  int64
	val float64
}

type series struct {
	name    string
	samples []sample

	ref *uint64
}

func (s *series) Write(t *testing.T, app storage.Appender) {
	t.Helper()

	lbls := labels.FromMap(map[string]string{"__name__": s.name})

	offset := 0
	if s.ref == nil {
		// Write first sample to get ref ID
		ref, err := app.Append(0, lbls, s.samples[0].ts, s.samples[0].val)
		require.NoError(t, err)

		s.ref = &ref
		offset = 1
	}

	// Write other data points with AddFast
	for _, sample := range s.samples[offset:] {
		_, err := app.Append(*s.ref, lbls, sample.ts, sample.val)
		require.NoError(t, err)
	}
}

type seriesList []*series

// Filter creates a new seriesList with series filtered by a sample
// keep predicate function.
func (s seriesList) Filter(fn func(s sample) bool) seriesList {
	var ret seriesList

	for _, entry := range s {
		var samples []sample

		for _, sample := range entry.samples {
			if fn(sample) {
				samples = append(samples, sample)
			}
		}

		if len(samples) > 0 {
			ret = append(ret, &series{
				name:    entry.name,
				ref:     entry.ref,
				samples: samples,
			})
		}
	}

	return ret
}

func (s seriesList) SeriesNames() []string {
	names := make([]string, 0, len(s))
	for _, series := range s {
		names = append(names, series.name)
	}
	return names
}

// ExpectedSamples returns the list of expected samples, sorted by ref ID and timestamp
func (s seriesList) ExpectedSamples() []record.RefSample {
	expect := []record.RefSample{}
	for _, series := range s {
		for _, sample := range series.samples {
			expect = append(expect, record.RefSample{
				Ref: *series.ref,
				T:   sample.ts,
				V:   sample.val,
			})
		}
	}
	sort.Sort(byRefSample(expect))
	return expect
}

type byRefSample []record.RefSample

func (b byRefSample) Len() int      { return len(b) }
func (b byRefSample) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byRefSample) Less(i, j int) bool {
	if b[i].Ref == b[j].Ref {
		return b[i].T < b[j].T
	}
	return b[i].Ref < b[j].Ref
}
