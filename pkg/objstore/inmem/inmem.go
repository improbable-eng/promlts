// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package inmem

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
)

var errNotFound = errors.New("inmem: object not found")

// Bucket implements the objstore.Bucket interfaces against local memory.
// Methods from Bucket interface are thread-safe. Objects are assumed to be immutable.
type Bucket struct {
	mtx     sync.RWMutex
	objects map[string][]byte
}

// NewBucket returns a new in memory Bucket.
// NOTE: Returned bucket is just a naive in memory bucket implementation. For test use cases only.
func NewBucket() *Bucket {
	return &Bucket{objects: map[string][]byte{}}
}

// Objects returns internally stored objects.
// NOTE: For assert purposes.
func (b *Bucket) Objects() map[string][]byte {
	return b.objects
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(_ context.Context, dir string, f func(string) error) error {
	unique := map[string]struct{}{}

	var dirPartsCount int
	dirParts := strings.SplitAfter(dir, objstore.DirDelim)
	for _, p := range dirParts {
		if p == "" {
			continue
		}
		dirPartsCount++
	}

	b.mtx.RLock()
	for filename := range b.objects {
		if !strings.HasPrefix(filename, dir) || dir == filename {
			continue
		}

		parts := strings.SplitAfter(filename, objstore.DirDelim)
		unique[strings.Join(parts[:dirPartsCount+1], "")] = struct{}{}
	}
	b.mtx.RUnlock()

	var keys []string
	for n := range unique {
		keys = append(keys, n)
	}
	sort.Slice(keys, func(i, j int) bool {
		if strings.HasSuffix(keys[i], objstore.DirDelim) && strings.HasSuffix(keys[j], objstore.DirDelim) {
			return strings.Compare(keys[i], keys[j]) < 0
		}
		if strings.HasSuffix(keys[i], objstore.DirDelim) {
			return false
		}
		if strings.HasSuffix(keys[j], objstore.DirDelim) {
			return true
		}

		return strings.Compare(keys[i], keys[j]) < 0
	})

	for _, k := range keys {
		if err := f(k); err != nil {
			return err
		}
	}
	return nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(_ context.Context, name string) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("inmem: object name is empty")
	}

	b.mtx.RLock()
	file, ok := b.objects[name]
	b.mtx.RUnlock()
	if !ok {
		return nil, errNotFound
	}

	return ioutil.NopCloser(bytes.NewReader(file)), nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(_ context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("inmem: object name is empty")
	}

	b.mtx.RLock()
	file, ok := b.objects[name]
	b.mtx.RUnlock()
	if !ok {
		return nil, errNotFound
	}

	if int64(len(file)) < off {
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}

	if length == -1 {
		return ioutil.NopCloser(bytes.NewReader(file[off:])), nil
	}

	if length <= 0 {
		return ioutil.NopCloser(bytes.NewReader(nil)), errors.New("length cannot be smaller or equal 0")
	}

	if int64(len(file)) <= off+length {
		// Just return maximum of what we have.
		length = int64(len(file)) - off
	}

	return ioutil.NopCloser(bytes.NewReader(file[off : off+length])), nil
}

// Exists checks if the given directory exists in memory.
func (b *Bucket) Exists(_ context.Context, name string) (bool, error) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	_, ok := b.objects[name]
	return ok, nil
}

// ObjectSize returns the size of the specified object.
func (b *Bucket) ObjectSize(_ context.Context, name string) (uint64, error) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	file, ok := b.objects[name]
	if !ok {
		return 0, errNotFound
	}
	return uint64(len(file)), nil
}

// Upload writes the file specified in src to into the memory.
func (b *Bucket) Upload(_ context.Context, name string, r io.Reader) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	b.objects[name] = body
	return nil
}

// Delete removes all data prefixed with the dir.
func (b *Bucket) Delete(_ context.Context, name string) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	if _, ok := b.objects[name]; !ok {
		return errNotFound
	}
	delete(b.objects, name)
	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return err == errNotFound
}

func (b *Bucket) Close() error { return nil }

// Name returns the bucket name.
func (b *Bucket) Name() string {
	return "inmem"
}
