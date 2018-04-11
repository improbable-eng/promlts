package verifier

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/pkg/errors"
)

const IndexIssueID = "index_issue"

// IndexIssue verifies any known index issue.
// It rewrites the problematic blocks while fixing repairable inconsistencies.
// If the replacement was created successfully it is uploaded to the bucket and the input
// block is deleted.
// NOTE: This also verifies all indexes against chunks mismatches and duplicates.
func IndexIssue(ctx context.Context, logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, repair bool) error {
	level.Info(logger).Log("msg", "started verifying issue", "with-repair", repair, "issue", IndexIssueID)

	err := bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}

		tmpdir, err := ioutil.TempDir("", fmt.Sprintf("index-issue-block-%s-", id))
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpdir)

		err = objstore.DownloadFile(ctx, bkt, path.Join(id.String(), "index"), filepath.Join(tmpdir, "index"))
		if err != nil {
			return errors.Wrapf(err, "download index file %s", path.Join(id.String(), "index"))
		}

		meta, err := block.DownloadMeta(ctx, bkt, id)
		if err != nil {
			return errors.Wrapf(err, "download meta file %s", id)
		}

		stats, err := block.GatherIndexIssueStats(filepath.Join(tmpdir, "index"), meta.MinTime, meta.MaxTime)
		if err != nil {
			return errors.Wrapf(err, "gather index issues %s", id)
		}

		err = stats.ErrSummary()
		if err == nil {
			return nil
		}

		level.Warn(logger).Log("msg", "detected issue", "id", id, "err", err, "issue", IndexIssueID)

		if !repair {
			// Only verify.
			return nil
		}

		if stats.OutOfOrderSum > stats.ExactSum {
			level.Warn(logger).Log("msg", "detected overlaps are not entirely by duplicated chunks. We are able to repair only duplicates", "id", id, "issue", IndexIssueID)
		}

		if stats.Outsiders > stats.CompleteOutsiders {
			level.Warn(logger).Log("msg", "detected outsiders are not all 'complete' outsiders. We can safely delete only complete outsiders", "id", id, "issue", IndexIssueID)
		}

		level.Info(logger).Log("msg", "repairing block", "id", id, "issue", IndexIssueID)

		if meta.Thanos.Downsample.Resolution > 0 {
			return errors.New("cannot repair downsampled blocks")
		}

		err = block.Download(ctx, bkt, id, path.Join(tmpdir, id.String()))
		if err != nil {
			return errors.Wrapf(err, "download block %s", id)
		}
		level.Info(logger).Log("msg", "downloaded block to be repaired", "id", id, "issue", IndexIssueID)

		resid, err := block.Repair(tmpdir, id)
		if err != nil {
			return errors.Wrapf(err, "repair failed for block %s", id)
		}

		// Verify repaired block before uploading it.
		err = block.VerifyIndex(filepath.Join(tmpdir, resid.String(), "index"), meta.MinTime, meta.MaxTime)
		if err != nil {
			return errors.Wrapf(err, "repaired block is invalid %s", resid)
		}

		level.Info(logger).Log("msg", "uploading repaired block", "newID", resid, "issue", IndexIssueID)
		err = objstore.UploadDir(ctx, bkt, filepath.Join(tmpdir, resid.String()), resid.String())
		if err != nil {
			return errors.Wrapf(err, "upload of %s failed", resid)
		}

		level.Info(logger).Log("msg", "safe deleting broken block", "id", id, "issue", IndexIssueID)
		if err := SafeDelete(ctx, bkt, backupBkt, id); err != nil {
			return errors.Wrapf(err, "safe deleting old block %s failed", id)
		}
		level.Info(logger).Log("msg", "all good, continuing", "id", id, "issue", IndexIssueID)
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "verify iter, issue %s", IndexIssueID)
	}

	level.Info(logger).Log("msg", "verified issue", "with-repair", repair, "issue", IndexIssueID)
	return nil
}
