// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

// TestInmemorySingleflight tests whether the in flight mechanism works.
func TestInmemorySingleflight(t *testing.T) {
	t.Parallel()

	conf := []byte(`
max_size: 1MB
max_item_size: 2KB
single_flight: true
`)
	const testKey = "test"

	c, _ := NewInMemoryCache("test", log.NewNopLogger(), nil, conf)

	ctx := context.Background()

	// Miss! We only block on further Fetch() calls
	// due to the single-flight mode. Thus, we can continue.
	hits := c.Fetch(ctx, []string{testKey})
	testutil.Assert(t, len(hits) == 0)

	g := &errgroup.Group{}

	g.Go(func() error {
		// This blocks :(
		hits := c.Fetch(ctx, []string{testKey})
		if len(hits) == 0 {
			return errors.New("no hits")
		}
		return nil
	})

	time.Sleep(1 * time.Second)
	// This unblocks the other goroutine.
	c.Store(ctx, map[string][]byte{testKey: []byte("aa")}, 1*time.Minute)

	testutil.Ok(t, g.Wait())

	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(c.sfSaved))
}

// TestInmemorySingleflightMultipleKeys tests whether single-flight mechanism works
// when a multiple key request comes.
func TestInmemorySingleflightMultipleKeys(t *testing.T) {
	t.Parallel()

	conf := []byte(`
max_size: 1MB
max_item_size: 2KB
single_flight: true
`)

	c, _ := NewInMemoryCache("test", log.NewNopLogger(), nil, conf)

	ctx, cancel := context.WithCancel(context.Background())

	testKeys := []string{"test", "test2"}

	hits := c.Fetch(ctx, testKeys)
	testutil.Assert(t, len(hits) == 0)

	g := &errgroup.Group{}

	g.Go(func() error {
		// This blocks :(
		hits := c.Fetch(context.Background(), testKeys)
		if len(hits) != 1 {
			return errors.New("expected to have 1 hit")
		}
		return nil
	})

	time.Sleep(1 * time.Second)
	c.Store(context.Background(), map[string][]byte{testKeys[0]: []byte("foobar")}, 1*time.Minute)
	time.Sleep(1 * time.Second)
	cancel()

	testutil.Ok(t, g.Wait())
	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(c.sfSaved))
}

// TestInmemorySingleflightInterrupted tests whether single-flight mechanism still works
// properly when Store() never comes.
func TestInmemorySingleflightInterrupted(t *testing.T) {
	t.Parallel()

	conf := []byte(`
max_size: 1MB
max_item_size: 2KB
single_flight: true
`)

	c, _ := NewInMemoryCache("test", log.NewNopLogger(), nil, conf)

	ctx, cancel := context.WithCancel(context.Background())

	const testKey = "test"

	// Miss! We only block on further Fetch() calls
	// due to the single-flight mode. Thus, we can continue.
	hits := c.Fetch(ctx, []string{testKey})
	testutil.Assert(t, len(hits) == 0)

	g := &errgroup.Group{}

	g.Go(func() error {
		// This blocks :(
		hits := c.Fetch(ctx, []string{testKey})

		if len(hits) != 0 {
			return errors.New("got hits")
		}
		return nil
	})
	cancel()

	time.Sleep(1 * time.Second)
	testutil.Ok(t, g.Wait())
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(c.sfSaved))
}

func TestInmemoryCache(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later on.
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup        map[string][]byte
		ttl          time.Duration
		testTTL      bool
		mockedErr    error
		fetchKeys    []string
		expectedHits map[string][]byte
	}{
		"should return no hits on empty cache": {
			setup:        nil,
			ttl:          time.Duration(time.Hour),
			fetchKeys:    []string{key1, key2},
			expectedHits: map[string][]byte{},
		},
		"should return no misses on 100% hit ratio": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
				key3: value3,
			},
			ttl:       time.Duration(time.Hour),
			fetchKeys: []string{key1},
			expectedHits: map[string][]byte{
				key1: value1,
			},
		},
		"should return a miss due to TTL": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
				key3: value3,
			},
			// setting ttl for the values as 250 ms
			ttl:          time.Duration(250 * time.Millisecond),
			testTTL:      true,
			fetchKeys:    []string{key1},
			expectedHits: map[string][]byte{},
		},
		"should return hits and misses on partial hits": {
			setup: map[string][]byte{
				key1: value1,
				key2: value2,
			},
			ttl:          time.Duration(time.Hour),
			fetchKeys:    []string{key1, key3},
			expectedHits: map[string][]byte{key1: value1},
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			conf := []byte(`
max_size: 1MB
max_item_size: 2KB
`)
			c, _ := NewInMemoryCache("test", log.NewNopLogger(), nil, conf)

			// Store the postings expected before running the test.
			ctx := context.Background()
			c.Store(ctx, testData.setup, testData.ttl)

			// Add delay to test expiry functionality.
			if testData.testTTL {
				// The delay will be based on the TTL itself.
				time.Sleep(testData.ttl)
			}

			// Fetch postings from cached and assert on it.
			hits := c.Fetch(ctx, testData.fetchKeys)
			testutil.Equals(t, testData.expectedHits, hits)

			// Assert on metrics.
			testutil.Equals(t, float64(len(testData.fetchKeys)), prom_testutil.ToFloat64(c.requests))
			testutil.Equals(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits))
		})
	}
}

func TestNewInMemoryCache(t *testing.T) {
	// Should return error when the max size of the cache is smaller than the max size of
	conf := []byte(`
max_size: 2KB
max_item_size: 1MB
`)
	cache, err := NewInMemoryCache("test", log.NewNopLogger(), nil, conf)
	testutil.NotOk(t, err)
	testutil.Equals(t, (*InMemoryCache)(nil), cache)
}

func benchCacheGetSet(b *testing.B, cache *InMemoryCache, numKeys, concurrency int) {
	wg := &sync.WaitGroup{}

	for k := 0; k < numKeys; k++ {
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < b.N; j++ {
					cache.Fetch(context.Background(), []string{fmt.Sprintf("%d", k)})
					cache.Store(context.Background(), map[string][]byte{fmt.Sprintf("%d", k): {}}, 1*time.Minute)
				}
			}()
		}
		wg.Wait()
	}
}

func BenchmarkInmemorySingleflight(b *testing.B) {
	conf := []byte(`max_size: 2KB
max_item_size: 1KB`)
	cache, err := NewInMemoryCache("test", log.NewNopLogger(), nil, conf)
	testutil.Ok(b, err)

	singleflightConf := []byte(`
max_size: 2KB
max_item_size: 1KB
single_flight: true`)
	sfCache, err := NewInMemoryCache("testsf", log.NewNopLogger(), nil, singleflightConf)
	testutil.Ok(b, err)

	for _, numKeys := range []int{100, 1000, 10000} {
		for _, concurrency := range []int{1, 5, 10} {
			b.Run(fmt.Sprintf("inmemory_get_set_keys%d_c%d", numKeys, concurrency), func(b *testing.B) {
				benchCacheGetSet(b, cache, numKeys, concurrency)
			})

			b.Run(fmt.Sprintf("inmemory_singleflight_get_set_keys%d_conc%d", numKeys, concurrency), func(b *testing.B) {
				benchCacheGetSet(b, sfCache, numKeys, concurrency)
			})
		}
	}
}
