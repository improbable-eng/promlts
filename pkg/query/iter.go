// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// promSeriesSet implements the SeriesSet interface of the Prometheus storage package on
// top of gRPC storepb.Store_SeriesClient.
// NOTE: This is a hot path. Keep it heavily efficient.
type promSeriesSet struct {
	client storepb.Store_SeriesClient
	// err stores last error or EOF if no more results.
	err error

	mint, maxt int64
	aggrs      []storepb.Aggr

	curr *storepb.Series
	at   *storepb.Series

	warns storage.Warnings
}

func newPromSeriesSet(client storepb.Store_SeriesClient, mint, maxt int64, aggrs []storepb.Aggr) *promSeriesSet {
	return &promSeriesSet{
		client: client,
		mint:   mint,
		maxt:   maxt,
		aggrs:  aggrs,
	}
}

func (s *promSeriesSet) Next() bool {
	if s.err != nil {
		return false
	}

	for {
		// We assume implementation is listening on context cancellation.
		resp, err := s.client.Recv()
		if err != nil {
			s.err = err
			if s.err != io.EOF || s.curr == nil {
				return false
			}
			s.at = s.curr
			return true
		}

		if resp.GetSeries() == nil {
			if w := resp.GetWarning(); w != "" {
				s.warns = append(s.warns, errors.New(w))
			}
			// Hint of other type of message. Ignore for forward compatibility.
			continue
		}

		// storage.Series are more strict then SeriesSet, it requires storage.Series to iterate over full series.
		// Yet, we use storepb.Proxy code which concatenates same series into one response. (See storepb.mergedSeriesSet).
		if s.curr == nil {
			s.curr = resp.GetSeries()
			continue
		}
		if labels.Compare(s.curr.PromLabels(), resp.GetSeries().PromLabels()) == 0 {
			s.err = errors.Errorf("detected failed invariant, the same series %v in different storepb frames.", s.curr.PromLabels())
			return false
		}

		s.at = s.curr
		s.curr = resp.GetSeries()
		return true
	}
}

func (s *promSeriesSet) At() storage.Series {
	if s.at == nil {
		return nil
	}
	return newChunkSeries(s.at.PromLabels(), s.at.Chunks, s.mint, s.maxt, s.aggrs)
}

func (s *promSeriesSet) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}

func (s *promSeriesSet) Warnings() storage.Warnings {
	return s.warns
}

// chunkSeries implements storage.Series for a series on storepb types.
type chunkSeries struct {
	lset       labels.Labels
	chunks     []storepb.AggrChunk
	mint, maxt int64
	aggrs      []storepb.Aggr
}

// newChunkSeries allows to iterate over samples for each sorted and non-overlapped chunks.
func newChunkSeries(lset labels.Labels, chunks []storepb.AggrChunk, mint, maxt int64, aggrs []storepb.Aggr) *chunkSeries {
	return &chunkSeries{
		lset:   lset,
		chunks: chunks,
		mint:   mint,
		maxt:   maxt,
		aggrs:  aggrs,
	}
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chunkSeries) Iterator() chunkenc.Iterator {
	var sit chunkenc.Iterator
	its := make([]chunkenc.Iterator, 0, len(s.chunks))

	if len(s.aggrs) == 1 {
		switch s.aggrs[0] {
		case storepb.Aggr_COUNT:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Count, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_SUM:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Sum, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_MIN:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Min, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_MAX:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Max, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case storepb.Aggr_COUNTER:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Counter, c.Raw))
			}
			// TODO(bwplotka): This breaks resets function. See https://github.com/thanos-io/thanos/issues/3644
			sit = downsample.NewApplyCounterResetsIterator(its...)
		default:
			return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
		}
		return dedup.NewBoundedSeriesIterator(sit, s.mint, s.maxt)
	}

	if len(s.aggrs) != 2 {
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}

	switch {
	case s.aggrs[0] == storepb.Aggr_SUM && s.aggrs[1] == storepb.Aggr_COUNT,
		s.aggrs[0] == storepb.Aggr_COUNT && s.aggrs[1] == storepb.Aggr_SUM:

		for _, c := range s.chunks {
			if c.Raw != nil {
				its = append(its, getFirstIterator(c.Raw))
			} else {
				sum, cnt := getFirstIterator(c.Sum), getFirstIterator(c.Count)
				its = append(its, downsample.NewAverageChunkIterator(cnt, sum))
			}
		}
		sit = newChunkSeriesIterator(its)
	default:
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}
	return dedup.NewBoundedSeriesIterator(sit, s.mint, s.maxt)
}

func getFirstIterator(cs ...*storepb.Chunk) chunkenc.Iterator {
	for _, c := range cs {
		if c == nil {
			continue
		}
		chk, err := chunkenc.FromData(chunkEncoding(c.Type), c.Data)
		if err != nil {
			return errSeriesIterator{err}
		}
		return chk.Iterator(nil)
	}
	return errSeriesIterator{errors.New("no valid chunk found")}
}

func chunkEncoding(e storepb.Chunk_Encoding) chunkenc.Encoding {
	switch e {
	case storepb.Chunk_XOR:
		return chunkenc.EncXOR
	}
	return 255 // Invalid.
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) bool      { return false }
func (errSeriesIterator) Next() bool           { return false }
func (errSeriesIterator) At() (int64, float64) { return 0, 0 }
func (it errSeriesIterator) Err() error        { return it.err }

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []chunkenc.Iterator
	i      int
}

func newChunkSeriesIterator(cs []chunkenc.Iterator) chunkenc.Iterator {
	if len(cs) == 0 {
		// This should not happen. StoreAPI implementations should not send empty results.
		return errSeriesIterator{err: errors.Errorf("store returned an empty result")}
	}
	return &chunkSeriesIterator{chunks: cs}
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
	// We generally expect the chunks already to be cut down
	// to the range we are interested in. There's not much to be gained from
	// hopping across chunks so we just call next until we reach t.
	for {
		ct, _ := it.At()
		if ct >= t {
			return true
		}
		if !it.Next() {
			return false
		}
	}
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.chunks[it.i].At()
}

func (it *chunkSeriesIterator) Next() bool {
	lastT, _ := it.At()

	if it.chunks[it.i].Next() {
		return true
	}
	if it.Err() != nil {
		return false
	}
	if it.i >= len(it.chunks)-1 {
		return false
	}
	// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
	// We must ensure to skip any overlapping range between adjacent chunks.
	it.i++
	return it.Seek(lastT + 1)
}

func (it *chunkSeriesIterator) Err() error {
	return it.chunks[it.i].Err()
}

type lazySeriesSet struct {
	create func() (s storage.SeriesSet, ok bool)

	set storage.SeriesSet
}

func (c *lazySeriesSet) Next() bool {
	if c.set != nil {
		return c.set.Next()
	}

	var ok bool
	c.set, ok = c.create()
	return ok
}

func (c *lazySeriesSet) Err() error {
	if c.set != nil {
		return c.set.Err()
	}
	return nil
}

func (c *lazySeriesSet) At() storage.Series {
	if c.set != nil {
		return c.set.At()
	}
	return nil
}

func (c *lazySeriesSet) Warnings() storage.Warnings {
	if c.set != nil {
		return c.set.Warnings()
	}
	return nil
}
