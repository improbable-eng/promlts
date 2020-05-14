// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"regexp"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	cache "github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// BucketCacheProvider is a type used to evaluate all bucket cache providers.
type BucketCacheProvider string

const (
	MemcachedBucketCacheProvider BucketCacheProvider = "memcached" // Memcached cache-provider for caching bucket.

	metaFilenameSuffix         = "/" + metadata.MetaFilename
	deletionMarkFilenameSuffix = "/" + metadata.DeletionMarkFilename
)

// CachingWithBackendConfig is a configuration of caching bucket used by Store component.
type CachingWithBackendConfig struct {
	Type          BucketCacheProvider `yaml:"backend"`
	BackendConfig interface{}         `yaml:"backend_config"`

	// Basic unit used to cache chunks.
	ChunkSubrangeSize int64 `yaml:"chunk_subrange_size"`

	// Maximum number of GetRange requests issued by this bucket for single GetRange call. Zero or negative value = unlimited.
	MaxChunksGetRangeRequests int `yaml:"max_chunks_get_range_requests"`

	// TTLs for various cache items.
	ChunkObjectSizeTTL time.Duration `yaml:"chunk_object_size_ttl"`
	ChunkSubrangeTTL   time.Duration `yaml:"chunk_subrange_ttl"`

	// How long to cache result of Iter call in root directory.
	RootIterTTL time.Duration `yaml:"root_iter_ttl"`

	// Config for Exists and Get operations for metadata files.
	MetafileExistsTTL      time.Duration `yaml:"metafile_exists_ttl"`
	MetafileDoesntExistTTL time.Duration `yaml:"metafile_doesnt_exist_ttl"`
	MetafileContentTTL     time.Duration `yaml:"metafile_content_ttl"`
}

func (cfg *CachingWithBackendConfig) Defaults() {
	cfg.ChunkSubrangeSize = 16000 // Equal to max chunk size.
	cfg.ChunkObjectSizeTTL = 24 * time.Hour
	cfg.ChunkSubrangeTTL = 24 * time.Hour
	cfg.MaxChunksGetRangeRequests = 3
	cfg.RootIterTTL = 5 * time.Minute
	cfg.MetafileExistsTTL = 10 * time.Minute
	cfg.MetafileDoesntExistTTL = 3 * time.Minute
	cfg.MetafileContentTTL = 1 * time.Hour
}

// NewCachingBucketFromYaml uses YAML configuration to create new caching bucket.
func NewCachingBucketFromYaml(yamlContent []byte, bucket objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (objstore.InstrumentedBucket, error) {
	level.Info(logger).Log("msg", "loading caching bucket configuration")

	config := &CachingWithBackendConfig{}
	config.Defaults()

	if err := yaml.UnmarshalStrict(yamlContent, config); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	backendConfig, err := yaml.Marshal(config.BackendConfig)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of cache backend configuration")
	}

	var c cache.Cache

	switch config.Type {
	case MemcachedBucketCacheProvider:
		var memcached cacheutil.MemcachedClient
		memcached, err := cacheutil.NewMemcachedClient(logger, "caching-bucket", backendConfig, reg)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create memcached client")
		}
		c = cache.NewMemcachedCache("caching-bucket", logger, memcached, reg)
	default:
		return nil, errors.Errorf("unsupported cache type: %s", config.Type)
	}

	cfg := NewCachingBucketConfig()

	// Configure cache.
	cfg.CacheGetRange("chunks", c, isTSDBChunkFile, config.ChunkSubrangeSize, config.ChunkObjectSizeTTL, config.ChunkSubrangeTTL, config.MaxChunksGetRangeRequests)
	cfg.CacheExists("metafile", c, isMetaFile, config.MetafileExistsTTL, config.MetafileDoesntExistTTL)
	cfg.CacheGet("metafile", c, isMetaFile, config.MetafileContentTTL, config.MetafileExistsTTL, config.MetafileDoesntExistTTL)

	// Cache Iter requests for root.
	cfg.CacheIter("dir", c, func(dir string) bool { return dir == "" }, config.RootIterTTL)

	// Enabling index caching (example).
	cfg.CacheObjectSize("index", c, isIndexFile, time.Hour)
	cfg.CacheGetRange("index", c, isIndexFile, 32000, time.Hour, 24*time.Hour, 3)

	cb, err := NewCachingBucket(bucket, cfg, logger, reg)
	if err != nil {
		return nil, err
	}

	return cb, nil
}

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool { return chunksMatcher.MatchString(name) }

func isMetaFile(name string) bool {
	return strings.HasSuffix(name, metaFilenameSuffix) || strings.HasSuffix(name, deletionMarkFilenameSuffix)
}

func isIndexFile(name string) bool {
	return strings.HasSuffix(name, "/index")
}
