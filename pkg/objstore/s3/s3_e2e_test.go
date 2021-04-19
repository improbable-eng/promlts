// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package s3_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"

	"github.com/thanos-io/thanos/pkg/testutil"
)

// Regression benchmark for https://github.com/thanos-io/thanos/issues/3917.
func BenchmarkUpload(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	s, err := e2e.NewScenario("e2e_bench_mino_client")
	testutil.Ok(b, err)
	b.Cleanup(e2ethanos.CleanScenario(b, s))

	const bucket = "test"
	m := e2edb.NewMinio(8080, bucket)
	testutil.Ok(b, s.StartAndWaitReady(m))
	var path []string
	bkt, err := s3.NewBucketWithConfig(log.NewNopLogger(), s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.HTTPEndpoint(),
		Insecure:  true,
	}, "test-feed", path...)
	testutil.Ok(b, err)

	buf := bytes.Buffer{}
	buf.Grow(1028 * 1028 * 100) // 100MB.
	word := "abcdefghij"
	for i := 0; i < buf.Cap()/len(word); i++ {
		_, _ = buf.WriteString(word)
	}
	str := buf.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testutil.Ok(b, bkt.Upload(ctx, "test", strings.NewReader(str)))
	}
}
