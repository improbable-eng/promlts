// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	originCache  = "cache"
	originBucket = "bucket"

	existsTrue  = "true"
	existsFalse = "false"

	opGet        = "get"
	opGetRange   = "getrange"
	opIter       = "iter"
	opExists     = "exists"
	opObjectSize = "objectsize"
)

var errObjNotFound = errors.Errorf("object not found")

// Bucket implementation that provides some caching features, based on passed configuration.
type CachingBucket struct {
	objstore.Bucket

	cfg    *CachingBucketConfig
	logger log.Logger

	requestedGetRangeBytes *prometheus.CounterVec
	fetchedGetRangeBytes   *prometheus.CounterVec
	refetchedGetRangeBytes *prometheus.CounterVec

	operationConfigs  map[string][]*operationConfig
	operationRequests *prometheus.CounterVec
	operationHits     *prometheus.CounterVec
}

// NewCachingBucket creates new caching bucket with provided configuration.
func NewCachingBucket(b objstore.Bucket, cfg *CachingBucketConfig, logger log.Logger, reg prometheus.Registerer) (*CachingBucket, error) {
	if b == nil {
		return nil, errors.New("bucket is nil")
	}

	cb := &CachingBucket{
		Bucket: b,
		cfg:    cfg,
		logger: logger,

		operationConfigs: map[string][]*operationConfig{},

		requestedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_getrange_requested_bytes_total",
			Help: "Total number of bytes requested via GetRange.",
		}, []string{"config"}),
		fetchedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_getrange_fetched_bytes_total",
			Help: "Total number of bytes fetched because of GetRange operation. Data from bucket is then stored to cache.",
		}, []string{"origin", "config"}),
		refetchedGetRangeBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_getrange_refetched_bytes_total",
			Help: "Total number of bytes re-fetched from storage because of GetRange operation, despite being in cache already.",
		}, []string{"origin", "config"}),

		operationRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_operation_requests_total",
			Help: "Number of requested operations matching given config.",
		}, []string{"operation", "config"}),
		operationHits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_store_bucket_cache_operation_hits_total",
			Help: "Number of operations served from cache for given config.",
		}, []string{"operation", "config"}),
	}

	for op, names := range cfg.allConfigNames() {
		for _, n := range names {
			cb.operationRequests.WithLabelValues(op, n)
			cb.operationHits.WithLabelValues(op, n)

			if op == opGetRange {
				cb.requestedGetRangeBytes.WithLabelValues(n)
				cb.fetchedGetRangeBytes.WithLabelValues(originCache, n)
				cb.fetchedGetRangeBytes.WithLabelValues(originBucket, n)
				cb.refetchedGetRangeBytes.WithLabelValues(originCache, n)
			}
		}
	}

	return cb, nil
}

func (cb *CachingBucket) Name() string {
	return "caching: " + cb.Bucket.Name()
}

func (cb *CachingBucket) WithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := cb.Bucket.(objstore.InstrumentedBucket); ok {
		// Make a copy, but replace bucket with instrumented one.
		res := &CachingBucket{}
		*res = *cb
		res.Bucket = ib.WithExpectedErrs(expectedFunc)
		return res
	}

	return cb
}

func (cb *CachingBucket) ReaderWithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return cb.WithExpectedErrs(expectedFunc)
}

func (cb *CachingBucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	cfgName, cfg := cb.cfg.findIterConfig(dir)
	if cfg == nil {
		return cb.Bucket.Iter(ctx, dir, f)
	}

	cb.operationRequests.WithLabelValues(opIter, cfgName).Inc()

	key := cachingKeyIter(dir)

	data := cfg.cache.Fetch(ctx, []string{key})
	if data[key] != nil {
		list, err := decodeIterResult(data[key])
		if err == nil {
			cb.operationHits.WithLabelValues(opIter, cfgName).Inc()

			for _, n := range list {
				err = f(n)
				if err != nil {
					return err
				}
			}
			return nil
		} else {
			// This should not happen.
			level.Warn(cb.logger).Log("msg", "failed to decode cached Iter result", "err", err)
		}
	}

	// Iteration can take a while (esp. since it calls function), and iterTTL is generally low.
	// We will compute TTL based time when iteration started.
	iterTime := time.Now()
	var list []string
	err := cb.Bucket.Iter(ctx, dir, func(s string) error {
		list = append(list, s)
		return f(s)
	})

	remainingTTL := cfg.ttl - time.Since(iterTime)
	if err == nil && remainingTTL > 0 {
		data := encodeIterResult(list)
		if data != nil {
			cfg.cache.Store(ctx, map[string][]byte{key: data}, remainingTTL)
		}
	}
	return err
}

// Iter results should compress nicely, especially in subdirectories.
func encodeIterResult(files []string) []byte {
	data, err := json.Marshal(files)
	if err != nil {
		return nil
	}

	return snappy.Encode(nil, data)
}

func decodeIterResult(data []byte) ([]string, error) {
	decoded, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, err
	}

	var list []string
	err = json.Unmarshal(decoded, &list)
	return list, err
}

func (cb *CachingBucket) Exists(ctx context.Context, name string) (bool, error) {
	cfgName, cfg := cb.cfg.findExistConfig(name)
	if cfg == nil {
		return cb.Bucket.Exists(ctx, name)
	}

	cb.operationRequests.WithLabelValues(opExists, cfgName).Inc()

	key := cachingKeyExists(name)
	hits := cfg.cache.Fetch(ctx, []string{key})

	if ex := hits[key]; ex != nil {
		switch string(ex) {
		case existsTrue:
			cb.operationHits.WithLabelValues(opExists, cfgName).Inc()
			return true, nil
		case existsFalse:
			cb.operationHits.WithLabelValues(opExists, cfgName).Inc()
			return false, nil
		default:
			level.Warn(cb.logger).Log("msg", "unexpected cached 'exists' value", "val", string(ex))
		}
	}

	existsTime := time.Now()
	ok, err := cb.Bucket.Exists(ctx, name)
	if err == nil {
		storeExistsCacheEntry(ctx, key, ok, existsTime, cfg.cache, cfg.existsTTL, cfg.doesntExistTTL)
	}

	return ok, err
}

func storeExistsCacheEntry(ctx context.Context, cachingKey string, exists bool, ts time.Time, cache cache.Cache, existsTTL, doesntExistTTL time.Duration) {
	var (
		data []byte
		ttl  time.Duration
	)
	if exists {
		ttl = existsTTL - time.Since(ts)
		data = []byte(existsTrue)
	} else {
		ttl = doesntExistTTL - time.Since(ts)
		data = []byte(existsFalse)
	}

	if ttl > 0 {
		cache.Store(ctx, map[string][]byte{cachingKey: data}, ttl)
	}
}

func (cb *CachingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	cfgName, cfg := cb.cfg.findGetConfig(name)
	if cfg == nil {
		return cb.Bucket.Get(ctx, name)
	}

	cb.operationRequests.WithLabelValues(opGet, cfgName).Inc()

	key := cachingKeyContent(name)
	existsKey := cachingKeyExists(name)

	hits := cfg.cache.Fetch(ctx, []string{key, existsKey})
	if hits[key] != nil {
		cb.operationHits.WithLabelValues(opGet, cfgName).Inc()
		return ioutil.NopCloser(bytes.NewReader(hits[key])), nil
	}

	// If we know that file doesn't exist, we can return that. Useful for deletion marks.
	if ex := hits[existsKey]; ex != nil && string(ex) == existsFalse {
		cb.operationHits.WithLabelValues(opGet, cfgName).Inc()
		return nil, errObjNotFound
	}

	getTime := time.Now()
	reader, err := cb.Bucket.Get(ctx, name)
	if err != nil {
		if cb.Bucket.IsObjNotFoundErr(err) {
			// Cache that object doesn't exist.
			storeExistsCacheEntry(ctx, existsKey, false, getTime, cfg.cache, cfg.existsTTL, cfg.doesntExistTTL)
		}

		return nil, err
	}
	defer runutil.CloseWithLogOnErr(cb.logger, reader, "CachingBucket.Get(%q)", name)

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	ttl := cfg.contentTTL - time.Since(getTime)
	if ttl > 0 {
		cfg.cache.Store(ctx, map[string][]byte{key: data}, ttl)
	}
	storeExistsCacheEntry(ctx, existsKey, true, getTime, cfg.cache, cfg.existsTTL, cfg.doesntExistTTL)

	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (cb *CachingBucket) IsObjNotFoundErr(err error) bool {
	return err == errObjNotFound || cb.Bucket.IsObjNotFoundErr(err)
}

func (cb *CachingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if off < 0 || length <= 0 {
		return cb.Bucket.GetRange(ctx, name, off, length)
	}

	cfgName, cfg := cb.cfg.findGetRangeConfig(name)
	if cfg == nil {
		return cb.Bucket.GetRange(ctx, name, off, length)
	}

	var (
		r   io.ReadCloser
		err error
	)
	tracing.DoInSpan(ctx, "cachingbucket_getrange", func(ctx context.Context) {
		r, err = cb.cachedGetRange(ctx, name, off, length, cfgName, cfg)
	})
	return r, err
}

func (cb *CachingBucket) ObjectSize(ctx context.Context, name string) (uint64, error) {
	cfgName, cfg := cb.cfg.findObjectSizeConfig(name)
	if cfg == nil {
		return cb.Bucket.ObjectSize(ctx, name)
	}

	return cb.cachedObjectSize(ctx, name, cfgName, cfg.cache, cfg.ttl)
}

func (cb *CachingBucket) cachedObjectSize(ctx context.Context, name string, cfgName string, cache cache.Cache, ttl time.Duration) (uint64, error) {
	key := cachingKeyObjectSize(name)

	cb.operationRequests.WithLabelValues(opObjectSize, cfgName).Inc()

	hits := cache.Fetch(ctx, []string{key})
	if s := hits[key]; len(s) == 8 {
		cb.operationHits.WithLabelValues(opObjectSize, cfgName).Inc()
		return binary.BigEndian.Uint64(s), nil
	}

	size, err := cb.Bucket.ObjectSize(ctx, name)
	if err != nil {
		return 0, err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], size)
	cache.Store(ctx, map[string][]byte{key: buf[:]}, ttl)

	return size, nil
}

func (cb *CachingBucket) cachedGetRange(ctx context.Context, name string, offset, length int64, cfgName string, cfg *getRangeConfig) (io.ReadCloser, error) {
	cb.operationRequests.WithLabelValues(opGetRange, cfgName)
	cb.requestedGetRangeBytes.WithLabelValues(cfgName).Add(float64(length))

	size, err := cb.cachedObjectSize(ctx, name, cfgName, cfg.cache, cfg.objectSizeTTL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get size of object: %s", name)
	}

	// If length goes over object size, adjust length. We use it later to limit number of read bytes.
	if uint64(offset+length) > size {
		length = int64(size - uint64(offset))
	}

	// Start and end range are subrange-aligned offsets into object, that we're going to read.
	startRange := (offset / cfg.subrangeSize) * cfg.subrangeSize
	endRange := ((offset + length) / cfg.subrangeSize) * cfg.subrangeSize
	if (offset+length)%cfg.subrangeSize > 0 {
		endRange += cfg.subrangeSize
	}

	// The very last subrange in the object may have length that is not divisible by subrange size.
	lastSubrangeOffset := endRange - cfg.subrangeSize
	lastSubrangeLength := int(cfg.subrangeSize)
	if uint64(endRange) > size {
		lastSubrangeOffset = (int64(size) / cfg.subrangeSize) * cfg.subrangeSize
		lastSubrangeLength = int(int64(size) - lastSubrangeOffset)
	}

	numSubranges := (endRange - startRange) / cfg.subrangeSize

	offsetKeys := make(map[int64]string, numSubranges)
	keys := make([]string, 0, numSubranges)

	totalRequestedBytes := int64(0)
	for off := startRange; off < endRange; off += cfg.subrangeSize {
		end := off + cfg.subrangeSize
		if end > int64(size) {
			end = int64(size)
		}
		totalRequestedBytes += (end - off)

		k := cachingKeyObjectSubrange(name, off, end)
		keys = append(keys, k)
		offsetKeys[off] = k
	}

	// Try to get all subranges from the cache.
	totalCachedBytes := int64(0)
	hits := cfg.cache.Fetch(ctx, keys)
	for _, b := range hits {
		totalCachedBytes += int64(len(b))
		cb.fetchedGetRangeBytes.WithLabelValues(originCache, cfgName).Add(float64(len(b)))
	}
	cb.operationHits.WithLabelValues(opGetRange, cfgName).Add(float64(totalCachedBytes) / float64(totalRequestedBytes))

	if len(hits) < len(keys) {
		if hits == nil {
			hits = map[string][]byte{}
		}

		err := cb.fetchMissingSubranges(ctx, name, startRange, endRange, offsetKeys, hits, lastSubrangeOffset, lastSubrangeLength, cfgName, cfg)
		if err != nil {
			return nil, err
		}
	}

	return ioutil.NopCloser(newSubrangesReader(cfg.subrangeSize, offsetKeys, hits, offset, length)), nil
}

type rng struct {
	start, end int64
}

// fetchMissingSubranges fetches missing subranges, stores them into "hits" map
// and into cache as well (using provided cacheKeys).
func (cb *CachingBucket) fetchMissingSubranges(ctx context.Context, name string, startRange, endRange int64, cacheKeys map[int64]string, hits map[string][]byte, lastSubrangeOffset int64, lastSubrangeLength int, cfgName string, cfg *getRangeConfig) error {
	// Ordered list of missing sub-ranges.
	var missing []rng

	for off := startRange; off < endRange; off += cfg.subrangeSize {
		if hits[cacheKeys[off]] == nil {
			missing = append(missing, rng{start: off, end: off + cfg.subrangeSize})
		}
	}

	missing = mergeRanges(missing, 0) // Merge adjacent ranges.
	// Keep merging until we have only max number of ranges (= requests).
	for limit := cfg.subrangeSize; cfg.maxSubRequests > 0 && len(missing) > cfg.maxSubRequests; limit = limit * 2 {
		missing = mergeRanges(missing, limit)
	}

	var hitsMutex sync.Mutex

	// Run parallel queries for each missing range. Fetched data is stored into 'hits' map, protected by hitsMutex.
	g, gctx := errgroup.WithContext(ctx)
	for _, m := range missing {
		m := m
		g.Go(func() error {
			r, err := cb.Bucket.GetRange(gctx, name, m.start, m.end-m.start)
			if err != nil {
				return errors.Wrapf(err, "fetching range [%d, %d]", m.start, m.end)
			}
			defer runutil.CloseWithLogOnErr(cb.logger, r, "fetching range [%d, %d]", m.start, m.end)

			for off := m.start; off < m.end && gctx.Err() == nil; off += cfg.subrangeSize {
				key := cacheKeys[off]
				if key == "" {
					return errors.Errorf("fetching range [%d, %d]: caching key for offset %d not found", m.start, m.end, off)
				}

				// We need a new buffer for each subrange, both for storing into hits, and also for caching.
				var subrangeData []byte
				if off == lastSubrangeOffset {
					// The very last subrange in the object may have different length,
					// if object length isn't divisible by subrange size.
					subrangeData = make([]byte, lastSubrangeLength)
				} else {
					subrangeData = make([]byte, cfg.subrangeSize)
				}
				_, err := io.ReadFull(r, subrangeData)
				if err != nil {
					return errors.Wrapf(err, "fetching range [%d, %d]", m.start, m.end)
				}

				storeToCache := false
				hitsMutex.Lock()
				if _, ok := hits[key]; !ok {
					storeToCache = true
					hits[key] = subrangeData
				}
				hitsMutex.Unlock()

				if storeToCache {
					cb.fetchedGetRangeBytes.WithLabelValues(originBucket, cfgName).Add(float64(len(subrangeData)))
					cfg.cache.Store(gctx, map[string][]byte{key: subrangeData}, cfg.subrangeTTL)
				} else {
					cb.refetchedGetRangeBytes.WithLabelValues(originCache, cfgName).Add(float64(len(subrangeData)))
				}
			}

			return gctx.Err()
		})
	}

	return g.Wait()
}

// Merges ranges that are close to each other. Modifies input.
func mergeRanges(input []rng, limit int64) []rng {
	if len(input) == 0 {
		return input
	}

	last := 0
	for ix := 1; ix < len(input); ix++ {
		if (input[ix].start - input[last].end) <= limit {
			input[last].end = input[ix].end
		} else {
			last++
			input[last] = input[ix]
		}
	}
	return input[:last+1]
}

func cachingKeyObjectSize(name string) string {
	return fmt.Sprintf("size:%s", name)
}

func cachingKeyObjectSubrange(name string, start int64, end int64) string {
	return fmt.Sprintf("subrange:%s:%d:%d", name, start, end)
}

func cachingKeyIter(name string) string {
	return fmt.Sprintf("iter:%s", name)
}

func cachingKeyExists(name string) string {
	return fmt.Sprintf("exists:%s", name)
}

func cachingKeyContent(name string) string {
	return fmt.Sprintf("content:%s", name)
}

// Reader implementation that uses in-memory subranges.
type subrangesReader struct {
	subrangeSize int64

	// Mapping of subrangeSize-aligned offsets to keys in hits.
	offsetsKeys map[int64]string
	subranges   map[string][]byte

	// Offset for next read, used to find correct subrange to return data from.
	readOffset int64

	// Remaining data to return from this reader. Once zero, this reader reports EOF.
	remaining int64
}

func newSubrangesReader(subrangeSize int64, offsetsKeys map[int64]string, subranges map[string][]byte, readOffset, remaining int64) *subrangesReader {
	return &subrangesReader{
		subrangeSize: subrangeSize,
		offsetsKeys:  offsetsKeys,
		subranges:    subranges,

		readOffset: readOffset,
		remaining:  remaining,
	}
}

func (c *subrangesReader) Read(p []byte) (n int, err error) {
	if c.remaining <= 0 {
		return 0, io.EOF
	}

	currentSubrangeOffset := (c.readOffset / c.subrangeSize) * c.subrangeSize
	currentSubrange, err := c.subrangeAt(currentSubrangeOffset)
	if err != nil {
		return 0, errors.Wrapf(err, "read position: %d", c.readOffset)
	}

	offsetInSubrange := int(c.readOffset - currentSubrangeOffset)
	toCopy := len(currentSubrange) - offsetInSubrange
	if toCopy <= 0 {
		// This can only happen if subrange's length is not subrangeSize, and reader is told to read more data.
		return 0, errors.Errorf("no more data left in subrange at position %d, subrange length %d, reading position %d", currentSubrangeOffset, len(currentSubrange), c.readOffset)
	}

	if len(p) < toCopy {
		toCopy = len(p)
	}
	if c.remaining < int64(toCopy) {
		toCopy = int(c.remaining) // Conversion is safe, c.remaining is small enough.
	}

	copy(p, currentSubrange[offsetInSubrange:offsetInSubrange+toCopy])
	c.readOffset += int64(toCopy)
	c.remaining -= int64(toCopy)

	return toCopy, nil
}

func (c *subrangesReader) subrangeAt(offset int64) ([]byte, error) {
	b := c.subranges[c.offsetsKeys[offset]]
	if b == nil {
		return nil, errors.Errorf("subrange for offset %d not found", offset)
	}
	return b, nil
}
