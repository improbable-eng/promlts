package query

import (
	"context"
	"io"
	"sync"

	"github.com/go-kit/kit/log"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/errgroup"
)

var _ promql.Queryable = (*Queryable)(nil)

// StoreInfo holds meta information about a store used by query.
type StoreInfo interface {
	// Client to access the store.
	Client() storepb.StoreClient

	// Labels returns store labels that should be appended to every metric returned by this store.
	Labels() []storepb.Label
}

// Queryable allows to open a querier against a dynamic set of stores.
type Queryable struct {
	logger log.Logger
	stores func() []StoreInfo
}

// NewQueryable creates implementation of promql.Queryable that fetches data from the given
// store API endpoints.
func NewQueryable(logger log.Logger, stores func() []StoreInfo) *Queryable {
	return &Queryable{
		logger: logger,
		stores: stores,
	}
}

func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(q.logger, ctx, q.stores(), mint, maxt), nil
}

type querier struct {
	logger     log.Logger
	ctx        context.Context
	cancel     func()
	mint, maxt int64
	stores     []StoreInfo
}

// newQuerier creates implementation of storage.Querier that fetches data from the given
// store API endpoints.
func newQuerier(logger log.Logger, ctx context.Context, stores []StoreInfo, mint, maxt int64) *querier {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &querier{
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
		mint:   mint,
		maxt:   maxt,
		stores: stores,
	}
}

// matchStore returns true iff the given store may hold data for the given label matchers.
func storeMatches(s StoreInfo, matchers ...*labels.Matcher) bool {
	for _, m := range matchers {
		for _, l := range s.Labels() {
			if l.Name != m.Name {
				continue
			}
			if !m.Matches(l.Value) {
				return false
			}
		}
	}
	return true
}

func (q *querier) Select(ms ...*labels.Matcher) storage.SeriesSet {
	var (
		mtx sync.Mutex
		all []chunkSeriesSet
		// TODO(fabxc): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)

	sms, err := translateMatchers(ms...)
	if err != nil {
		return promSeriesSet{set: errSeriesSet{err: err}}
	}
	for _, s := range q.stores {
		// We might be able to skip the store if its meta information indicates
		// it cannot have series matching our query.
		if !storeMatches(s, ms...) {
			continue
		}
		store := s

		g.Go(func() error {
			set, err := q.selectSingle(store.Client(), sms...)
			if err != nil {
				return err
			}
			mtx.Lock()
			all = append(all, set)
			mtx.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return promSeriesSet{set: errSeriesSet{err: err}}
	}
	return promSeriesSet{set: mergeAllSeriesSets(all...), mint: q.mint, maxt: q.maxt}
}

func (q *querier) selectSingle(client storepb.StoreClient, ms ...storepb.LabelMatcher) (chunkSeriesSet, error) {
	sc, err := client.Series(q.ctx, &storepb.SeriesRequest{
		MinTime:  q.mint,
		MaxTime:  q.maxt,
		Matchers: ms,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	res := &storeSeriesSet{i: -1}

	for {
		r, err := sc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		res.series = append(res.series, r.Series)
	}
	return res, nil
}

func (q *querier) LabelValues(name string) ([]string, error) {
	var (
		mtx sync.Mutex
		all []string
		// TODO(bplotka): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)

	for _, s := range q.stores {
		store := s

		g.Go(func() error {
			values, err := q.labelValuesSingle(store.Client(), name)
			if err != nil {
				return err
			}

			mtx.Lock()
			all = append(all, values...)
			mtx.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return dedupStrings(all), nil
}

func (q *querier) labelValuesSingle(client storepb.StoreClient, name string) ([]string, error) {
	resp, err := client.LabelValues(q.ctx, &storepb.LabelValuesRequest{
		Label: name,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	return resp.Values, nil
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}
