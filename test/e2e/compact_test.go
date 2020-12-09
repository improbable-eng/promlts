// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func isEmptyDir(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

type blockDesc struct {
	series  []labels.Labels
	extLset labels.Labels
	mint    int64
	maxt    int64

	markedForNoCompact bool
}

func (b *blockDesc) Create(ctx context.Context, dir string, delay time.Duration) (ulid.ULID, error) {
	if delay == 0*time.Second {
		return e2eutil.CreateBlock(ctx, dir, b.series, 120, b.mint, b.maxt, b.extLset, 0)
	}
	return e2eutil.CreateBlockWithBlockDelay(ctx, dir, b.series, 120, b.mint, b.maxt, delay, b.extLset, 0)
}

func TestCompactWithStoreGateway(t *testing.T) {
	t.Parallel()

	logger := log.NewLogfmtLogger(os.Stdout)

	justAfterConsistencyDelay := 30 * time.Minute
	// Make sure to take realistic timestamp for start. This is to align blocks as if they would be aligned on Prometheus.
	// To have deterministic compaction, let's have fixed date:
	now, err := time.Parse(time.RFC3339, "2020-03-24T08:00:00Z")
	testutil.Ok(t, err)

	// Simulate real scenario, including more complex cases like overlaps if needed.
	// TODO(bwplotka): Add blocks to downsample and test delayed delete.
	blocks := []blockDesc{
		// Non overlapping blocks, not ready for compaction.
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "no-compaction", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "no-compaction", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "no-compaction", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
	}
	blocks = append(blocks,
		// Non overlapping blocks, ready for compaction.
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "3")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "4")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "5")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(6 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(8 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "6")},
			extLset: labels.FromStrings("case", "compaction-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(8 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(10 * time.Hour)),
		},
		// Non overlapping blocks, ready for compaction, with one blocked marked for no-compact (no-compact-mark.json)
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "3")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "4")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),

			markedForNoCompact: true,
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "5")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(6 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(8 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "6")},
			extLset: labels.FromStrings("case", "compaction-ready-one-block-marked-for-no-compact", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(8 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(10 * time.Hour)),
		},

		// Non overlapping blocks, ready for compaction, only after deduplication.
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "3")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "4")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "5")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "2"),
			mint:    timestamp.FromTime(now.Add(6 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(8 * time.Hour)),
		},
		blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "6")},
			extLset: labels.FromStrings("case", "compaction-ready-after-dedup", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(8 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(10 * time.Hour)),
		},

		// Replica partial overlapping blocks, not ready for compaction, among no-overlapping blocks.
		// NOTE: We put a- in front to make sure this will be compacted as first one (:
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("case", "a-partial-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("case", "a-partial-overlap-dedup-ready", "replica", "2"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			extLset: labels.FromStrings("case", "a-partial-overlap-dedup-ready", "replica", "3"),
			mint:    timestamp.FromTime(now.Add(3 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("case", "a-partial-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},

		// Multi-Replica partial overlapping blocks, not ready for compaction, among no-overlapping blocks.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("case", "partial-multi-replica-overlap-dedup-ready", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("case", "partial-multi-replica-overlap-dedup-ready", "rule_replica", "2", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			// TODO(bwplotka): This is wrong, but let's fix in next PR. We should error out in this case as we should
			// never support overlaps before we modify dedup labels. This probably means another check in fetcher.
			extLset: labels.FromStrings("case", "partial-multi-replica-overlap-dedup-ready", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("case", "partial-multi-replica-overlap-dedup-ready", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},

		// Replica full overlapping blocks, not ready for compaction, among no-overlapping blocks.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("case", "full-replica-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("case", "full-replica-overlap-dedup-ready", "replica", "2"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			extLset: labels.FromStrings("case", "full-replica-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("case", "full-replica-overlap-dedup-ready", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
	)

	s, err := e2e.NewScenario("e2e_test_compact")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	dir := filepath.Join(s.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))

	const bucket = "compact_test"
	m := e2edb.NewMinio(8080, bucket)
	testutil.Ok(t, s.StartAndWaitReady(m))

	bkt, err := s3.NewBucketWithConfig(logger, s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.HTTPEndpoint(), // We need separate client config, when connecting to minio from outside.
		Insecure:  true,
	}, "test-feed")
	testutil.Ok(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	rawBlockIDs := map[ulid.ULID]struct{}{}
	for _, b := range blocks {
		id, err := b.Create(ctx, dir, justAfterConsistencyDelay)
		testutil.Ok(t, err)
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))
		rawBlockIDs[id] = struct{}{}
		if b.markedForNoCompact {
			testutil.Ok(t, block.MarkForNoCompact(ctx, logger, bkt, id, metadata.ManualNoCompactReason, "why not", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		}
	}
	{
		// On top of that, add couple of other tricky cases with different meta.
		malformedBase := blockDesc{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("case", "malformed-things", "replica", "101"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		}

		// New Partial block.
		id, err := malformedBase.Create(ctx, dir, 0*time.Second)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// New Partial block + deletion mark.
		id, err = malformedBase.Create(ctx, dir, 0*time.Second)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, block.MarkForDeletion(ctx, logger, bkt, id, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after consistency delay.
		id, err = malformedBase.Create(ctx, dir, justAfterConsistencyDelay)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after consistency delay + deletion mark.
		id, err = malformedBase.Create(ctx, dir, justAfterConsistencyDelay)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, block.MarkForDeletion(ctx, logger, bkt, id, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after consistency delay + old deletion mark ready to be deleted.
		id, err = malformedBase.Create(ctx, dir, justAfterConsistencyDelay)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		deletionMark, err := json.Marshal(metadata.DeletionMark{
			ID: id,
			// Deletion threshold is usually 2 days.
			DeletionTime: time.Now().Add(-50 * time.Hour).Unix(),
			Version:      metadata.DeletionMarkVersion1,
		})
		testutil.Ok(t, err)
		testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.DeletionMarkFilename), bytes.NewBuffer(deletionMark)))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after delete threshold.
		id, err = malformedBase.Create(ctx, dir, 50*time.Hour)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))

		// Partial block after delete threshold + deletion mark.
		id, err = malformedBase.Create(ctx, dir, 50*time.Hour)
		testutil.Ok(t, err)
		testutil.Ok(t, os.Remove(path.Join(dir, id.String(), metadata.MetaFilename)))
		testutil.Ok(t, block.MarkForDeletion(ctx, logger, bkt, id, "", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))
		testutil.Ok(t, objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String()))
	}

	svcConfig := client.TestBucketConfig{
		Type: client.S3,
		Config: struct {
			Bucket    string `yaml:"bucket"`
			AccessKey string `yaml:"access_key"`
			SecretKey string `yaml:"secret_key"`
			Endpoint  string `yaml:"endpoint"`
			Insecure  bool   `yaml:"insecure"`
		}{
			Bucket:    bucket,
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	}
	str, err := e2ethanos.NewStoreGW(s.SharedDir(), "1", svcConfig)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(str))
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs)+7)), "thanos_blocks_meta_synced"))
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_modified"))

	q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{str.GRPCNetworkEndpoint()}, nil, nil, "", "")
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	// Check if query detects current series, even if overlapped.
	queryAndAssert(t, ctx, q.HTTPEndpoint(),
		fmt.Sprintf(`count_over_time({a="1"}[13h] offset %ds)`, int64(time.Since(now.Add(12*time.Hour)).Seconds())),
		promclient.QueryOptions{
			Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
		},
		model.Vector{
			{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "no-compaction", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-after-dedup", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-after-dedup", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
			{Value: 240, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "a-partial-overlap-dedup-ready", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "3"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "a-partial-overlap-dedup-ready", "replica": "3"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 320, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
			{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "full-replica-overlap-dedup-ready", "replica": "2"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
			{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
		},
	)
	// Store view:
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs)+7)), "thanos_blocks_meta_synced"))
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_modified"))

	expectedEndVector := model.Vector{
		// NOTE(bwplotka): Even after deduplication some series has still replica labels. This is because those blocks did not overlap yet with anything.
		// This is fine as querier deduplication will remove it if needed.
		{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "no-compaction", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-one-block-marked-for-no-compact"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-one-block-marked-for-no-compact"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-one-block-marked-for-no-compact", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "compaction-ready-after-dedup"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "compaction-ready-after-dedup"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "compaction-ready-after-dedup"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "compaction-ready-after-dedup"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "6", "case": "compaction-ready-after-dedup", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "a-partial-overlap-dedup-ready"}},
		{Value: 360, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "a-partial-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "a-partial-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "a-partial-overlap-dedup-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "partial-multi-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
		{Value: 240, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "partial-multi-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "partial-multi-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "partial-multi-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "partial-multi-replica-overlap-dedup-ready", "replica": "1", "rule_replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "1", "case": "full-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready"}},
		{Value: 240, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "2", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "3", "case": "full-replica-overlap-dedup-ready"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "4", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
		{Value: 120, Metric: map[model.LabelName]model.LabelValue{"a": "1", "b": "5", "case": "full-replica-overlap-dedup-ready", "replica": "1"}},
	}

	// No replica label with overlaps should halt compactor. This test is sequential
	// because we do not want two Thanos Compact instances deleting the same partially
	// uploaded blocks and blocks with deletion marks.
	{
		c, err := e2ethanos.NewCompactor(s.SharedDir(), "expect-to-halt", svcConfig, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(c))

		// Expect compactor halted and for one cleanup iteration to happen.
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(1), "thanos_compact_halted"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(1), "thanos_compact_block_cleanup_loops_total"))

		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs)+5)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_modified"))

		// The compact directory is still there.
		dataDir := filepath.Join(s.SharedDir(), "data", "compact", "expect-to-halt")
		empty, err := isEmptyDir(dataDir)
		testutil.Ok(t, err)
		testutil.Equals(t, false, empty, "directory %s should not be empty", dataDir)

		// We expect no ops.
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_iterations_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_block_cleanup_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_blocks_marked_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_group_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_group_vertical_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(1), "thanos_compact_group_compactions_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(3), "thanos_compact_group_compaction_runs_started_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(2), "thanos_compact_group_compaction_runs_completed_total"))

		// However, the blocks have been cleaned because that happens concurrently.
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(2), "thanos_compact_aborted_partial_uploads_deletion_attempts_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(2), "thanos_compact_blocks_cleaned_total"))

		// Ensure bucket UI.
		ensureGETStatusCode(t, http.StatusOK, "http://"+path.Join(c.HTTPEndpoint(), "global"))
		ensureGETStatusCode(t, http.StatusOK, "http://"+path.Join(c.HTTPEndpoint(), "loaded"))

		testutil.Ok(t, s.Stop(c))
	}

	t.Run("dedup enabled; compactor should work as expected", func(t *testing.T) {
		// We expect 2x 4-block compaction, 2-block vertical compaction, 2x 3-block compaction.
		c, err := e2ethanos.NewCompactor(s.SharedDir(), "working", svcConfig, nil, "--deduplication.replica-label=replica", "--deduplication.replica-label=rule_replica")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(c))

		// NOTE: We cannot assert on intermediate `thanos_blocks_meta_` metrics as those are gauge and change dynamically due to many
		// compaction groups. Wait for at least first compaction iteration (next is in 5m).
		testutil.Ok(t, c.WaitSumMetrics(e2e.Greater(0), "thanos_compact_iterations_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_blocks_cleaned_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_block_cleanup_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(2*4+2+2*3+2), "thanos_compact_blocks_marked_total")) // 18.
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_aborted_partial_uploads_deletion_attempts_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(6), "thanos_compact_group_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(3), "thanos_compact_group_vertical_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_group_compactions_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(14), "thanos_compact_group_compaction_runs_started_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(14), "thanos_compact_group_compaction_runs_completed_total"))

		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_downsample_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_downsample_failures_total"))

		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(
			len(rawBlockIDs)+7+
				6+ // 6 compactions, 6 newly added blocks.
				-2, // Partial block removed.
		)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_halted"))
		// Make sure compactor does not modify anything else over time.
		testutil.Ok(t, s.Stop(c))

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		// Check if query detects new blocks.
		queryAndAssert(t, ctx, q.HTTPEndpoint(),
			fmt.Sprintf(`count_over_time({a="1"}[13h] offset %ds)`, int64(time.Since(now.Add(12*time.Hour)).Seconds())),
			promclient.QueryOptions{
				Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
			},
			expectedEndVector,
		)
		// Store view:
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs)+7+6-2)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_modified"))
	})

	t.Run("dedup enabled; no delete delay; compactor should work and remove things as expected", func(t *testing.T) {
		c, err := e2ethanos.NewCompactor(s.SharedDir(), "working", svcConfig, nil, "--deduplication.replica-label=replica", "--deduplication.replica-label=rule_replica", "--delete-delay=0s")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(c))

		// NOTE: We cannot assert on intermediate `thanos_blocks_meta_` metrics as those are gauge and change dynamically due to many
		// compaction groups. Wait for at least first compaction iteration (next is in 5m).
		testutil.Ok(t, c.WaitSumMetrics(e2e.Greater(0), "thanos_compact_iterations_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(18), "thanos_compact_blocks_cleaned_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_block_cleanup_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_blocks_marked_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_aborted_partial_uploads_deletion_attempts_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_group_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_group_vertical_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_group_compactions_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(7), "thanos_compact_group_compaction_runs_started_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(7), "thanos_compact_group_compaction_runs_completed_total"))

		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_downsample_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_downsample_failures_total"))

		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs)+7+6-18-2)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_halted"))
		// Make sure compactor does not modify anything else over time.
		testutil.Ok(t, s.Stop(c))

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		// Check if query detects new blocks.
		queryAndAssert(t, ctx, q.HTTPEndpoint(),
			fmt.Sprintf(`count_over_time({a="1"}[13h] offset %ds)`, int64(time.Since(now.Add(12*time.Hour)).Seconds())),
			promclient.QueryOptions{
				Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
			},
			expectedEndVector,
		)

		// Store view:
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs)+7-18+6-2)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_modified"))
	})
}

func ensureGETStatusCode(t testing.TB, code int, url string) {
	t.Helper()

	r, err := http.Get(url)
	testutil.Ok(t, err)
	testutil.Equals(t, code, r.StatusCode)
}
