// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package testutil

import (
	"os"
	"path/filepath"
	"runtime/pprof"
	"testing"

	"github.com/thanos-io/thanos/pkg/runutil"
)

// TB represents union of test and benchmark.
// This allows the same test suite to be run by both benchmark and test, helping to reuse more code.
// The reason is that usually benchmarks are not being run on CI, especially for short tests, so you need to recreate
// usually similar tests for `Test<Name>(t *testing.T)` methods. Example of usage is presented here:
//
//	 func TestTestOrBench(t *testing.T) {
//		tb := NewTB(t)
//		tb.Run("1", func(tb TB) { testorbenchComplexTest(tb) })
//		tb.Run("2", func(tb TB) { testorbenchComplexTest(tb) })
//	}
//
//	func BenchmarkTestOrBench(b *testing.B) {
//		tb := NewTB(t)
//		tb.Run("1", func(tb TB) { testorbenchComplexTest(tb) })
//		tb.Run("2", func(tb TB) { testorbenchComplexTest(tb) })
//	}
type TB interface {
	testing.TB
	IsBenchmark() bool
	Run(name string, f func(t TB)) bool

	SetBytes(n int64)
	N() int
	ResetTimer()
}

// tb implements TB as well as testing.TB interfaces.
type tb struct {
	testing.TB
}

// NewTB creates tb from testing.TB.
func NewTB(t testing.TB) TB { return &tb{TB: t} }

// Run benchmarks/tests f as a subbenchmark/subtest with the given name. It reports
// whether there were any failures.
//
// A subbenchmark/subtest is like any other benchmark/test.
func (t *tb) Run(name string, f func(t TB)) bool {
	if b, ok := t.TB.(*testing.B); ok {
		return b.Run(name, func(nested *testing.B) { f(&tb{TB: nested}) })
	}
	if t, ok := t.TB.(*testing.T); ok {
		return t.Run(name, func(nested *testing.T) { f(&tb{TB: nested}) })
	}
	panic("not a benchmark and not a test")
}

// N returns number of iterations to do for benchmark, 1 in case of test.
func (t *tb) N() int {
	if b, ok := t.TB.(*testing.B); ok {
		return b.N
	}
	return 1
}

// SetBytes records the number of bytes processed in a single operation for benchmark, noop otherwise.
// If this is called, the benchmark will report ns/op and MB/s.
func (t *tb) SetBytes(n int64) {
	if b, ok := t.TB.(*testing.B); ok {
		b.SetBytes(n)
	}
}

// ResetTimer resets a timer, if it's a benchmark, noop otherwise.
func (t *tb) ResetTimer() {
	if b, ok := t.TB.(*testing.B); ok {
		b.ResetTimer()
	}
}

// IsBenchmark returns true if it's a benchmark.
func (t *tb) IsBenchmark() bool {
	_, ok := t.TB.(*testing.B)
	return ok
}

// WriteHeapProfile creates heap profile that snapshots all the allocations from the beginning of program run (with rate
// configured by runtime.MemProfileRate). It will also record currently inuse objects.
// NOTE: Manually instrumented heap profile can be superior to mem profile configured using `go test` because you can decide in
// what moment to do it. This allows to check if resources are cleaned after the test case etc.
// NOTE: It's adviced to run `runtime.GC()` before this to ensure you have clean view what would be cleaned on next GC run.
func WriteHeapProfile(path string) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, f, "close")
	return pprof.WriteHeapProfile(f)
}
