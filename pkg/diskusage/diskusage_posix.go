// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// +build linux darwin freebsd

package diskusage

import (
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
)

// Get returns the Usage of a given path, or an error if usage data is
// unavailable.
func Get(path string) (Usage, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(filepath.Clean(path), &stat)
	if err != nil {
		return Usage{}, errors.Wrapf(err, "statfs %s", path)
	}
	return Usage{
		FreeBytes:  uint64(stat.Bfree) * uint64(stat.Bsize),
		TotalBytes: uint64(stat.Blocks) * uint64(stat.Bsize),
		AvailBytes: uint64(stat.Bavail) * uint64(stat.Bsize),
	}, nil
}
