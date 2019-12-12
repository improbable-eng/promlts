package storecache

import (
	"errors"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMemcachedIndexCache_FetchMultiPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	label1 := labels.Label{Name: "instance", Value: "a"}
	label2 := labels.Label{Name: "instance", Value: "b"}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedPostings
		mockedErr      error
		fetchBlockID   ulid.ULID
		fetchLabels    []labels.Label
		expectedHits   map[labels.Label][]byte
		expectedMisses []labels.Label
	}{
		"should return no hits on empty cache": {
			setup:          []mockedPostings{},
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label1, label2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedPostings{
				{block: block1, label: label1, value: value1},
				{block: block1, label: label2, value: value2},
				{block: block2, label: label1, value: value3},
			},
			fetchBlockID: block1,
			fetchLabels:  []labels.Label{label1, label2},
			expectedHits: map[labels.Label][]byte{
				label1: value1,
				label2: value2,
			},
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setup: []mockedPostings{
				{block: block1, label: label1, value: value1},
				{block: block2, label: label1, value: value3},
			},
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   map[labels.Label][]byte{label1: value1},
			expectedMisses: []labels.Label{label2},
		},
		"should return no hits on memcached error": {
			setup: []mockedPostings{
				{block: block1, label: label1, value: value1},
				{block: block1, label: label2, value: value2},
				{block: block2, label: label1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label1, label2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewMemcachedIndexCache(log.NewNopLogger(), memcached, nil)
			testutil.Ok(t, err)

			// Store the postings expected before running the test.
			for _, p := range testData.setup {
				c.StorePostings(p.block, p.label, p.value)
			}

			// Fetch postings from cached and assert on it.
			hits, misses := c.FetchMultiPostings(testData.fetchBlockID, testData.fetchLabels)
			testutil.Equals(t, testData.expectedHits, hits)
			testutil.Equals(t, testData.expectedMisses, misses)
		})
	}
}

func TestMemcachedIndexCache_FetchMultiSeries(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedSeries
		mockedErr      error
		fetchBlockID   ulid.ULID
		fetchIds       []uint64
		expectedHits   map[uint64][]byte
		expectedMisses []uint64
	}{
		"should return no hits on empty cache": {
			setup:          []mockedSeries{},
			fetchBlockID:   block1,
			fetchIds:       []uint64{1, 2},
			expectedHits:   nil,
			expectedMisses: []uint64{1, 2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block1, id: 2, value: value2},
				{block: block2, id: 1, value: value3},
			},
			fetchBlockID: block1,
			fetchIds:     []uint64{1, 2},
			expectedHits: map[uint64][]byte{
				1: value1,
				2: value2,
			},
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block2, id: 1, value: value3},
			},
			fetchBlockID:   block1,
			fetchIds:       []uint64{1, 2},
			expectedHits:   map[uint64][]byte{1: value1},
			expectedMisses: []uint64{2},
		},
		"should return no hits on memcached error": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block1, id: 2, value: value2},
				{block: block2, id: 1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchBlockID:   block1,
			fetchIds:       []uint64{1, 2},
			expectedHits:   nil,
			expectedMisses: []uint64{1, 2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewMemcachedIndexCache(log.NewNopLogger(), memcached, nil)
			testutil.Ok(t, err)

			// Store the series expected before running the test.
			for _, p := range testData.setup {
				c.StoreSeries(p.block, p.id, p.value)
			}

			// Fetch series from cached and assert on it.
			hits, misses := c.FetchMultiSeries(testData.fetchBlockID, testData.fetchIds)
			testutil.Equals(t, testData.expectedHits, hits)
			testutil.Equals(t, testData.expectedMisses, misses)
		})
	}
}

type mockedPostings struct {
	block ulid.ULID
	label labels.Label
	value []byte
}

type mockedSeries struct {
	block ulid.ULID
	id    uint64
	value []byte
}

type mockedMemcachedClient struct {
	cache             map[string][]byte
	mockedGetMultiErr error
}

func newMockedMemcachedClient(mockedGetMultiErr error) *mockedMemcachedClient {
	return &mockedMemcachedClient{
		cache:             map[string][]byte{},
		mockedGetMultiErr: mockedGetMultiErr,
	}
}

func (c *mockedMemcachedClient) GetMulti(keys []string) (map[string][]byte, error) {
	if c.mockedGetMultiErr != nil {
		return nil, c.mockedGetMultiErr
	}

	hits := map[string][]byte{}

	for _, key := range keys {
		if value, ok := c.cache[key]; ok {
			hits[key] = value
		}
	}

	return hits, nil
}

func (c *mockedMemcachedClient) SetAsync(key string, value []byte, ttl time.Duration) error {
	c.cache[key] = value

	return nil
}

func (c *mockedMemcachedClient) Stop() {
	// Nothing to do.
}
