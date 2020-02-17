// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/diskusage"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestHaltError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsHaltError(err), "halt error")

	err = halt(errors.New("test"))
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(halt(errors.New("test")), "something")
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(errors.Wrap(halt(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error")
}

func TestHaltMultiError(t *testing.T) {
	haltErr := halt(errors.New("halt error"))
	nonHaltErr := errors.New("not a halt error")

	errs := terrors.MultiError{nonHaltErr}
	testutil.Assert(t, !IsHaltError(errs), "should not be a halt error")

	errs.Add(haltErr)
	testutil.Assert(t, IsHaltError(errs), "if any halt errors are present this should return true")
}

func TestRetryMultiError(t *testing.T) {
	retryErr := retry(errors.New("retry error"))
	nonRetryErr := errors.New("not a retry error")

	errs := terrors.MultiError{nonRetryErr}
	testutil.Assert(t, !IsRetryError(errs), "should not be a retry error")

	errs = terrors.MultiError{retryErr}
	testutil.Assert(t, IsRetryError(errs), "if all errors are retriable this should return true")

	errs = terrors.MultiError{nonRetryErr, retryErr}
	testutil.Assert(t, !IsRetryError(errs), "mixed errors should return false")
}

func TestRetryError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsRetryError(err), "retry error")

	err = retry(errors.New("test"))
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.New("test")), "something")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(errors.Wrap(retry(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.Wrap(halt(errors.New("test")), "something")), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error. Retry should not hide halt error")
}

func TestGroupKey(t *testing.T) {
	for _, tcase := range []struct {
		input    metadata.Thanos
		expected string
	}{
		{
			input:    metadata.Thanos{},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{"foo": "bar", "foo1": "bar2"},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@2124638872457683483",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{`foo/some..thing/some.thing/../`: `a_b_c/bar-something-a\metric/a\x`},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@16590761456214576373",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			testutil.Equals(t, tcase.expected, GroupKey(tcase.input))
		}); !ok {
			return
		}
	}
}

func Test_DiskUsage_Corner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tmpDir, err := ioutil.TempDir("", "test-block-delete")
	testutil.Ok(t, err)
	bkt := inmem.NewBucket()

	lbls := labels.Labels{{Name: "ext1", Value: "val1"}}
	b1, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, lbls, 124)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b1.String())))
	testutil.Equals(t, 4, len(bkt.Objects()))

	defer func() {
		// Full delete.
		testutil.Ok(t, block.Delete(ctx, log.NewNopLogger(), bkt, b1))
	}()

	// Error case.
	duErr := func(p string) (diskusage.Usage, error) {
		return diskusage.Usage{}, errors.New("sentinel")
	}

	cg, err := newGroup(log.NewNopLogger(), bkt, lbls, 0, false, duErr, false, nil, nil, nil, nil, nil, nil)
	testutil.Ok(t, err)

	err = cg.isEnoughDiskSpace(ctx, b1.String())
	testutil.Equals(t, err.Error(), "get disk usage: sentinel")

	// Not enough disk space case.
	duNotEnough := func(p string) (diskusage.Usage, error) {
		return diskusage.Usage{AvailBytes: 1500}, nil
	}
	cg, err = newGroup(log.NewNopLogger(), bkt, lbls, 0, false, duNotEnough, false, nil, nil, nil, nil, nil, nil)
	testutil.Ok(t, err)
	err = cg.Add(&metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: b1,
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{"ext1": "val1"},
			Downsample: metadata.ThanosDownsample{
				Resolution: 0,
			},
		},
	})
	testutil.Ok(t, err)

	err = cg.isEnoughDiskSpace(ctx, b1.String())
	testutil.NotOk(t, err)
	testutil.Equals(t, err.Error(), "needed 1.5 kB available space, got 1.5 kB")

	// Just enough disk space case.
	duEnough := func(p string) (diskusage.Usage, error) {
		return diskusage.Usage{AvailBytes: 1600}, nil
	}
	cg, err = newGroup(log.NewNopLogger(), bkt, lbls, 0, false, duEnough, false, nil, nil, nil, nil, nil, nil)
	testutil.Ok(t, err)
	err = cg.Add(&metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: b1,
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{"ext1": "val1"},
			Downsample: metadata.ThanosDownsample{
				Resolution: 0,
			},
		},
	})
	testutil.Ok(t, err)

	err = cg.isEnoughDiskSpace(ctx, b1.String())
	testutil.Ok(t, err)
}
