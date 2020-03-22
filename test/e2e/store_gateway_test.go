// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/store"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// TODO(bwplotka): Extend this test to have multiple stores and memcached.
// TODO(bwplotka): Extend this test for downsampling.
func TestStoreGateway(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_store_gateway")
	testutil.Ok(t, err)
	defer s.Close()

	m := e2edb.NewMinio(80, "thanos")
	testutil.Ok(t, s.StartAndWaitReady(m))

	s1, err := e2ethanos.NewStoreGW(s.SharedDir(), "1", client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    "thanos",
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	}, relabel.Config{
		Action:       relabel.Drop,
		Regex:        relabel.MustNewRegexp("value2"),
		SourceLabels: model.LabelNames{"ext1"},
	})
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(s1))

	storeCfg := []store.Config{
		{
			EndpointsConfig: store.EndpointsConfig{
				StaticAddresses: []string{s1.GRPCNetworkEndpoint()},
			},
		},
	}
	q, err := e2ethanos.NewQuerier("1", storeCfg)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	dir := filepath.Join(s.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), dir), os.ModePerm))

	series := []labels.Labels{labels.FromStrings("a", "1", "b", "2")}
	extLset := labels.FromStrings("ext1", "value1", "replica", "1")
	extLset2 := labels.FromStrings("ext1", "value1", "replica", "2")
	extLset3 := labels.FromStrings("ext1", "value2", "replica", "3")
	extLset4 := labels.FromStrings("ext1", "value1", "replica", "3")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	now := time.Now()
	id1, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset, 0)
	testutil.Ok(t, err)
	id2, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset2, 0)
	testutil.Ok(t, err)
	id3, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset3, 0)
	testutil.Ok(t, err)
	id4, err := e2eutil.CreateBlock(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), extLset, 0)
	testutil.Ok(t, err)
	l := log.NewLogfmtLogger(os.Stdout)
	bkt, err := s3.NewBucketWithConfig(l, s3.Config{
		Bucket:    "thanos",
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.HTTPEndpoint(), // We need separate client config, when connecting to minio from outside.
		Insecure:  true,
	}, "test-feed")
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id2.String()), id2.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id3.String()), id3.String()))
	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id4.String()), id4.String()))

	// Wait for store to sync blocks.
	// thanos_blocks_meta_synced: 2x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta.
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(4), "thanos_blocks_meta_synced"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(2), "thanos_bucket_store_blocks_loaded"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_drops_total"))
	testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

	t.Run("query works", func(t *testing.T) {
		queryAndAssert(t, ctx, q.HTTPEndpoint(), "{a=\"1\"}",
			promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "1",
				},
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "2",
				},
			},
		)

		// 2 x postings, 2 x series, 2x chunks.
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(6), "thanos_bucket_store_series_data_touched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(6), "thanos_bucket_store_series_data_fetched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(2), "thanos_bucket_store_series_blocks_queried"))

		queryAndAssert(t, ctx, q.HTTPEndpoint(), "{a=\"1\"}",
			promclient.QueryOptions{
				Deduplicate: true,
			},
			[]model.Metric{
				{
					"a":    "1",
					"b":    "2",
					"ext1": "value1",
				},
			},
		)

		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(12), "thanos_bucket_store_series_data_touched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(8), "thanos_bucket_store_series_data_fetched"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(2+2), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("remove meta.json from id1 block", func(t *testing.T) {
		testutil.Ok(t, bkt.Delete(ctx, filepath.Join(id1.String(), block.MetaFilename)))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 1x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(4), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		// TODO(bwplotka): Entries are still in LRU cache.
		queryAndAssert(t, ctx, q.HTTPEndpoint(), "{a=\"1\"}",
			promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "2",
				},
			},
		)
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(4+1), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("upload block id5, similar to id1", func(t *testing.T) {
		id5, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset4, 0)
		testutil.Ok(t, err)
		testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id5.String()), id5.String()))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 2x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(5), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(2), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		queryAndAssert(t, ctx, q.HTTPEndpoint(), "{a=\"1\"}",
			promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "2",
				},
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "3", // New block.
				},
			},
		)
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(5+2), "thanos_bucket_store_series_blocks_queried"))
	})
	t.Run("delete whole id2 block #yolo", func(t *testing.T) {
		testutil.Ok(t, block.Delete(ctx, l, bkt, id2))

		// Wait for store to sync blocks.
		// thanos_blocks_meta_synced: 1x loadedMeta 1x labelExcludedMeta 1x TooFreshMeta 1x noMeta.
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(4), "thanos_blocks_meta_synced"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1), "thanos_bucket_store_blocks_loaded"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(1+1), "thanos_bucket_store_block_drops_total"))
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(0), "thanos_bucket_store_block_load_failures_total"))

		queryAndAssert(t, ctx, q.HTTPEndpoint(), "{a=\"1\"}",
			promclient.QueryOptions{
				Deduplicate: false,
			},
			[]model.Metric{
				{
					"a":       "1",
					"b":       "2",
					"ext1":    "value1",
					"replica": "3",
				},
			},
		)
		testutil.Ok(t, s1.WaitSumMetrics(e2e.Equals(7+1), "thanos_bucket_store_series_blocks_queried"))
	})

	// TODO(khyati) Let's add some case for compaction-meta.json once the PR will be merged: https://github.com/thanos-io/thanos/pull/2136.
}
