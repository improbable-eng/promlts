package cache

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/thanos-io/thanos/pkg/tracing"
)

type TracingCache struct {
	c Cache
}

func (t TracingCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	t.c.Store(ctx, data, ttl)
}

func (t TracingCache) Fetch(ctx context.Context, keys []string) (result map[string][]byte) {
	tracing.DoWithSpan(ctx, "cache_fetch", func(spanCtx context.Context, span opentracing.Span) {
		span.LogKV("requested keys", len(keys))

		result = t.c.Fetch(spanCtx, keys)

		bytes := 0
		for _, v := range result {
			bytes += len(v)
		}
		span.LogKV("returned keys", len(result), "returned bytes", bytes)
	})
	return
}
