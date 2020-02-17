// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// +build windows

package diskusage

import (
	"syscall"
	"unsafe"

	"github.com/pkg/errors"
)

// Get returns the Usage of a given path, or an error if usage data is
// unavailable.
func Get(path string) (Usage, error) {
	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")

	var u Usage

	ret, _, err := c.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&u.FreeBytes)),
		uintptr(unsafe.Pointer(&u.TotalBytes)),
		uintptr(unsafe.Pointer(&u.AvailBytes)))

	if ret == 0 {
		return u, errors.Wrap(err, "call GetDiskFreeSpaceExW")
	}

	return u, nil
}
