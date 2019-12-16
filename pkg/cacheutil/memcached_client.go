package cacheutil

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
)

const (
	defaultTimeout                     = 100 * time.Millisecond
	defaultMaxIdleConnections          = 100
	defaultMaxAsyncConcurrency         = 20
	defaultMaxAsyncBufferSize          = 10000
	defaultDNSProviderUpdateInterval   = 10 * time.Second
	defaultMaxGetMultiBatchConcurrency = 20
	defaultMaxGetMultiBatchSize        = 1024
)

var (
	errMemcachedAsyncBufferFull = errors.New("the async buffer is full")
	errMemcachedConfigNoAddrs   = errors.New("no memcached addrs provided")
)

// MemcachedClient is a high level client to interact with memcached.
type MemcachedClient interface {
	// GetMulti fetches multiple keys at once from memcached.
	GetMulti(keys []string) (map[string][]byte, error)

	// SetAsync enqueues an asynchronous operation to store a key into memcached.
	SetAsync(key string, value []byte, ttl time.Duration) error

	// Stop client and release underlying resources.
	Stop()
}

// memcachedClientBackend is an interface used to mock the underlying client in tests.
type memcachedClientBackend interface {
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Set(item *memcache.Item) error
}

// MemcachedClientConfig is the config accepted by MemcachedClient.
type MemcachedClientConfig struct {
	// Addrs specifies the list of memcached addresses. The addresses get
	// resolved with the DNS provider.
	Addrs []string

	// Timeout specifies the socket read/write timeout.
	Timeout time.Duration

	// MaxIdleConnections specifies the maximum number of idle connections that
	// will be maintained per address. For better performances, this should be
	// set to a number higher than your peak parallel requests.
	MaxIdleConnections int

	// MaxAsyncConcurrency specifies the maximum number of concurrent asynchronous
	// operations can occur.
	MaxAsyncConcurrency int

	// MaxAsyncBufferSize specifies the maximum number of enqueued asynchronous
	// operations allowed.
	MaxAsyncBufferSize int

	// MaxGetMultiBatchConcurrency specifies the maximum number of concurrent batch
	// executions by GetMulti().
	// TODO(pracucci) Should this be a global (per-client) limit or a per-single MultiGet()
	//                limit? The latter would allow us to avoid a single very large MultiGet()
	//                will slow down other requests.
	MaxGetMultiBatchConcurrency int

	// MaxGetMultiBatchSize specified the maximum number of keys a single underlying
	// GetMulti() should run. If more keys are specified, internally keys are splitted
	// into multiple batches and fetched concurrently up to MaxGetMultiBatchConcurrency
	// parallelism.
	MaxGetMultiBatchSize int

	// DNSProviderUpdateInterval specifies the DNS discovery update interval.
	DNSProviderUpdateInterval time.Duration
}

func (c *MemcachedClientConfig) applyDefaults() {
	if c.Timeout == 0 {
		c.Timeout = defaultTimeout
	}

	if c.MaxIdleConnections == 0 {
		c.MaxIdleConnections = defaultMaxIdleConnections
	}

	if c.MaxAsyncConcurrency == 0 {
		c.MaxAsyncConcurrency = defaultMaxAsyncConcurrency
	}

	if c.MaxAsyncBufferSize == 0 {
		c.MaxAsyncBufferSize = defaultMaxAsyncBufferSize
	}

	if c.DNSProviderUpdateInterval == 0 {
		c.DNSProviderUpdateInterval = defaultDNSProviderUpdateInterval
	}

	if c.MaxGetMultiBatchConcurrency == 0 {
		c.MaxGetMultiBatchConcurrency = defaultMaxGetMultiBatchConcurrency
	}

	if c.MaxGetMultiBatchSize == 0 {
		c.MaxGetMultiBatchSize = defaultMaxGetMultiBatchSize
	}
}

func (c *MemcachedClientConfig) validate() error {
	if len(c.Addrs) == 0 {
		return errMemcachedConfigNoAddrs
	}

	return nil
}

type memcachedClient struct {
	logger   log.Logger
	config   MemcachedClientConfig
	client   memcachedClientBackend
	selector *MemcachedJumpHashSelector

	// DNS provider used to keep the memcached servers list updated.
	provider *dns.Provider

	// Channel used to notify internal goroutines when they should quit.
	stop chan struct{}

	// Channel used to enqueue async operations.
	asyncQueue chan func()

	// Channel used to enqueue get multi operations.
	getMultiQueue chan *memcachedGetMultiBatch

	// Wait group used to wait all workers on stopping.
	workers sync.WaitGroup
}

type memcachedGetMultiBatch struct {
	keys    []string
	results chan<- *memcachedGetMultiResult
}

type memcachedGetMultiResult struct {
	items map[string]*memcache.Item
	err   error
}

// NewMemcachedClient makes a new MemcachedClient.
func NewMemcachedClient(logger log.Logger, provider *dns.Provider, config MemcachedClientConfig) (MemcachedClient, error) {
	// We use a custom servers selector in order to use a jump hash
	// for servers selection.
	selector := &MemcachedJumpHashSelector{}

	client := memcache.NewFromSelector(selector)
	client.Timeout = config.Timeout
	client.MaxIdleConns = config.MaxIdleConnections

	return newMemcachedClient(logger, client, selector, provider, config)
}

func newMemcachedClient(logger log.Logger, client memcachedClientBackend, selector *MemcachedJumpHashSelector, provider *dns.Provider, config MemcachedClientConfig) (MemcachedClient, error) {
	config.applyDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}

	c := &memcachedClient{
		logger:        logger,
		config:        config,
		client:        client,
		selector:      selector,
		provider:      provider,
		asyncQueue:    make(chan func(), config.MaxAsyncBufferSize),
		getMultiQueue: make(chan *memcachedGetMultiBatch),
		stop:          make(chan struct{}, 1),
	}

	// As soon as the client is created it must ensure that memcached server
	// addresses are resolved, so we're going to trigger an initial addresses
	// resolution here.
	if err := c.resolveAddrs(); err != nil {
		return nil, err
	}

	c.workers.Add(1)
	go c.resolveAddrsLoop()

	// Start a number of goroutines - processing async operations - equal
	// to the max concurrency we have.
	c.workers.Add(c.config.MaxAsyncConcurrency)
	for i := 0; i < c.config.MaxAsyncConcurrency; i++ {
		go c.asyncQueueProcessLoop()
	}

	// Start a number of goroutines - processing get multi batch operations - equal
	// to the max concurrency we have.
	c.workers.Add(c.config.MaxGetMultiBatchConcurrency)
	for i := 0; i < c.config.MaxGetMultiBatchConcurrency; i++ {
		go c.getMultiQueueProcessLoop()
	}

	return c, nil
}

func (c *memcachedClient) Stop() {
	close(c.stop)

	// Wait until all workers have terminated.
	c.workers.Wait()
}

func (c *memcachedClient) SetAsync(key string, value []byte, ttl time.Duration) error {
	return c.enqueueAsync(func() {
		err := c.client.Set(&memcache.Item{
			Key:        key,
			Value:      value,
			Expiration: int32(time.Now().Add(ttl).Unix()),
		})

		if err != nil {
			level.Warn(c.logger).Log("msg", fmt.Sprintf("failed to store item with key %s to memcached", key), "err", err)
		}
	})
}

func (c *memcachedClient) GetMulti(keys []string) (map[string][]byte, error) {
	batches, err := c.getMultiBatched(keys)
	if err != nil {
		if len(batches) == 0 {
			return nil, err
		}

		// In case we have both results and an error, it means some batch requests
		// failed and other succeeded. In this case we prefer to log it and move on,
		// given returning from results from the cache is better than returning
		// nothing.
		level.Warn(c.logger).Log("msg", "failed to fetch some keys batches from memcached", "err", err)
	}

	hits := map[string][]byte{}
	for _, items := range batches {
		for key, item := range items {
			hits[key] = item.Value
		}
	}

	return hits, nil
}

func (c *memcachedClient) getMultiBatched(keys []string) ([]map[string]*memcache.Item, error) {
	// Do not batch if the input keys are less then the max batch size.
	if len(keys) <= c.config.MaxGetMultiBatchSize {
		items, err := c.client.GetMulti(keys)
		if err != nil {
			return nil, err
		}

		return []map[string]*memcache.Item{items}, nil
	}

	// Calculate the number of expected results.
	batchSize := c.config.MaxGetMultiBatchSize
	numResults := len(keys) / batchSize
	if len(keys)%batchSize != 0 {
		numResults++
	}

	// Split input keys into batches and schedule a job for it.
	results := make(chan *memcachedGetMultiResult, numResults)
	defer close(results)

	go func() {
		for batchStart := 0; batchStart < len(keys); batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > len(keys) {
				batchEnd = len(keys)
			}

			c.getMultiQueue <- &memcachedGetMultiBatch{
				keys:    keys[batchStart:batchEnd],
				results: results,
			}
		}
	}()

	// Wait for all batch results. In case of error, we keep
	// track of the last error occurred.
	items := make([]map[string]*memcache.Item, 0, numResults)
	var lastErr error

	for i := 0; i < numResults; i++ {
		result := <-results
		if result.err != nil {
			lastErr = result.err
			continue
		}

		items = append(items, result.items)
	}

	return items, lastErr
}

func (c *memcachedClient) enqueueAsync(op func()) error {
	select {
	case c.asyncQueue <- op:
		return nil
	default:
		return errMemcachedAsyncBufferFull
	}
}

func (c *memcachedClient) asyncQueueProcessLoop() {
	defer c.workers.Done()

	for {
		select {
		case op := <-c.asyncQueue:
			op()
		case <-c.stop:
			return
		}
	}
}

func (c *memcachedClient) getMultiQueueProcessLoop() {
	defer c.workers.Done()

	for {
		select {
		case batch := <-c.getMultiQueue:
			res := &memcachedGetMultiResult{}
			res.items, res.err = c.client.GetMulti(batch.keys)

			batch.results <- res
		case <-c.stop:
			return
		}
	}
}

func (c *memcachedClient) resolveAddrsLoop() {
	defer c.workers.Done()

	ticker := time.NewTicker(c.config.DNSProviderUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := c.resolveAddrs()
			if err != nil {
				level.Warn(c.logger).Log("msg", "failed update memcached servers list", "err", err)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *memcachedClient) resolveAddrs() error {
	// Resolve configured addresses with a reasonable timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c.provider.Resolve(ctx, c.config.Addrs)

	// Fail in case no server address is resolved.
	servers := c.provider.Addresses()
	if len(servers) == 0 {
		return errors.New("no server address resolved")
	}

	return c.selector.SetServers(servers...)
}
