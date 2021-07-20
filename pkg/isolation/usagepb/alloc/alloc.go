// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package alloc

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/pkg/errors"
)

type Tracker interface {
	MemoryBytesAllocated(b int)
}

type codeRef struct {
	file string
	line int
}

type tracker struct {
	mtx            sync.Mutex
	allocatedBytes map[codeRef]uint64
}

func (t *tracker) MemoryBytesAllocated(b int) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	var (
		c  = codeRef{}
		ok bool
	)
	_, c.file, c.line, ok = runtime.Caller(1)
	if !ok {
		panic("could not get caller")
	}
	t.allocatedBytes[c] += uint64(b)
}

type ValidationFailedError struct {
	Problems []Problem
}

func (e ValidationFailedError) Error() string {
	ret := "alloc: Validation did not passed, found:\n"
	for _, p := range e.Problems {
		ret += p.Error()
	}
	return ret
}

type Problem struct {
	TrackedBytes uint64
	Allocated    MemProfileFramedRecords
	File         string
	Line         int
}

func (p Problem) Error() string {
	prefix := ""
	if p.File != "" {
		prefix = fmt.Sprintf("%v:%v ", p.File, p.Line)
	}
	if p.Allocated.AllocatedBytes == 0 {
		return fmt.Sprintf("%sTracked %v bytes, but nothing was actually allocated in near location\n", prefix, p.TrackedBytes)
	}

	return fmt.Sprintf("%sTracked %v bytes, but allocated nearby %v\n", prefix, p.TrackedBytes, p.Allocated.String())
}

// ValidateTracking...
// Not go-routine safe due to global memory profiling.
func ValidateTracking(skip int, toleranceBytes uint64, f func(tr Tracker)) (err error) {
	var (
		tracked         = &tracker{allocatedBytes: map[codeRef]uint64{}}
		foundAllocs     = make(map[codeRef]MemProfileFramedRecords, len(tracked.allocatedBytes))
		problems        []Problem
		fr              runtime.Frame
		untrackedAllocs []memProfileFramedRecord
		toleratedBytes  uint64
	)

	records, err := recordMemoryProfiles(func() { f(tracked) })
	if err != nil {
		return err
	}
RecordsLoop:
	for _, r := range records {
		frames := runtime.CallersFrames(r.relevantPCs)

		ok := true
		framed := memProfileFramedRecord{
			MemProfileRecord: r.MemProfileRecord,
		}
		for ok {
			fr, ok = frames.Next()
			if _, isTrackerInvocation := tracked.allocatedBytes[codeRef{file: fr.File, line: fr.Line}]; isTrackerInvocation {
				// Ignore tracker allocated bytes.
				continue RecordsLoop
			}
			framed.frames = append(framed.frames, fr)
		}

		// Skip unrelated frames further + 2 for 2 anonymous functions we create.
		framed.frames = framed.frames[:len(framed.frames)-skip-2]

		for _, fr := range framed.frames {
			// Hacky: Assume for now tracker is right after allocation.
			fcodeRef := codeRef{file: fr.File, line: fr.Line + 1}
			_, ok = tracked.allocatedBytes[fcodeRef]
			if ok {
				found := foundAllocs[fcodeRef]
				found.AllocatedBytes += uint64(r.AllocBytes)
				found.records = append(found.records, framed)
				foundAllocs[fcodeRef] = found
				break
			}
		}
		if !ok {
			untrackedAllocs = append(untrackedAllocs, framed)
		}
	}

	for cr, b := range tracked.allocatedBytes {
		if found, ok := foundAllocs[cr]; ok {
			if b >= found.AllocatedBytes && b-found.AllocatedBytes <= toleranceBytes {
				continue
			} else if found.AllocatedBytes-b <= toleranceBytes {
				continue
			}
			problems = append(problems, Problem{TrackedBytes: b, File: cr.file, Line: cr.line, Allocated: found})
			continue
		}
		problems = append(problems, Problem{TrackedBytes: b, File: cr.file, Line: cr.line})
	}

	var toleratedAllocs []memProfileFramedRecord
	for _, u := range untrackedAllocs {
		if uint64(u.AllocBytes) <= toleranceBytes {
			toleratedBytes += uint64(u.AllocBytes)
			toleratedAllocs = append(toleratedAllocs, u)
			continue
		}
		problems = append(problems, Problem{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: uint64(u.AllocBytes), records: []memProfileFramedRecord{u}}})
	}

	// Only care about small things if they accumulate above 1MB.
	if toleratedBytes > 1024*1024 {
		// Untracked tiny allocations accumulated above tolerance.
		problems = append(problems, Problem{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: toleratedBytes, records: toleratedAllocs}})
	}
	if len(problems) > 0 {
		return ValidationFailedError{Problems: problems}
	}
	return nil
}

type MemProfileFramedRecords struct {
	AllocatedBytes uint64
	records        []memProfileFramedRecord
}

func (r MemProfileFramedRecords) String() string {
	msg := fmt.Sprintf("%v Details:\n", r.AllocatedBytes)
	for _, rec := range r.records {
		msg += "  " + rec.String()
	}
	return msg
}

type memProfileFramedRecord struct {
	runtime.MemProfileRecord
	frames []runtime.Frame
}

func (r memProfileFramedRecord) String() string {
	msg := fmt.Sprintf("%v Bytes for: \n", r.AllocBytes)
	for _, f := range r.frames {
		msg += fmt.Sprintf("    %v:%v \n", f.File, f.Line)
	}
	return msg
}

type memProfileRecord struct {
	runtime.MemProfileRecord
	relevantPCs []uintptr
}

func recordMemoryProfiles(fn func()) (ret []memProfileRecord, err error) {
	// Setting 1 to include every allocated block in the profile. (512 * 1024 by default).
	oldProfileRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	defer func() { runtime.MemProfileRate = oldProfileRate }()

	var (
		// NOTE: PC means program [stack] counter.
		funcPC, beforePC, sincePC uintptr
		before, since             []runtime.MemProfileRecord
		ok                        bool
	)

	// Anonymous function to get scoped PC.
	func() {
		funcPC, _, _, ok = runtime.Caller(1)
		if !ok {
			err = errors.New("failed to get caller info")
			return
		}

		beforePC, before, err = getMemProfile()
		if err != nil {
			return
		}
		// Run requested workload.
		fn()
		sincePC, since, err = memProfileSince(before)
	}()
	if err != nil {
		return nil, err
	}

	for _, r := range since {
		if r.AllocBytes == 0 && r.AllocObjects == 0 {
			break
		}
		s := r.Stack()

		// Get records only downstream of this PC.
		for i, pc := range s {
			// PCs to ignore should be at the higher of stack vs function PC (they are deeper).
			if pc == beforePC+1 || pc == sincePC+1 {
				break
			}

			if pc == funcPC+1 {
				// Copy as runtime reuse all arrays.
				relevantPCs := make([]uintptr, i)
				copy(relevantPCs, s[:i])
				ret = append(ret, memProfileRecord{
					MemProfileRecord: r,
					relevantPCs:      relevantPCs,
				})
				break
			}
		}
	}
	return ret, nil

}

func getMemProfile() (uintptr, []runtime.MemProfileRecord, error) {
	myPC, _, _, ok := runtime.Caller(1)
	if !ok {
		return 0, nil, errors.New("failed to get caller info")
	}
	// The returned profile may be up to two garbage collection cycles old, that's why make sure to get latest.
	runtime.GC()
	runtime.GC()
	runtime.GC()

	// Find out how many records there are (MemProfile(nil, true)),
	// allocate that many records, and get the data.
	// There's a race—more records might be added between
	// the two calls—so allocate a few extra records for safety
	// and also try again if we're very unlucky.
	// The loop should only execute one iteration in the common case.
	var p []runtime.MemProfileRecord
	n, _ := runtime.MemProfile(nil, true)
	for {
		// Allocate room for a slightly bigger profile,
		// in case a few more entries have been added
		// since the call to MemProfile.
		p = make([]runtime.MemProfileRecord, n+50)
		n, ok = runtime.MemProfile(p, true)
		if ok {
			p = p[0:n]
			break
		}
		// Profile grew; try again.
	}
	return myPC, p, nil
}

func memProfileSince(since []runtime.MemProfileRecord) (uintptr, []runtime.MemProfileRecord, error) {
	myPC, _, _, ok := runtime.Caller(1)
	if !ok {
		return 0, nil, errors.New("failed to get caller info")
	}

	_, now, err := getMemProfile()
	if err != nil {
		return 0, nil, err
	}
	var ret []runtime.MemProfileRecord
	for i := range now {
		for _, sinceRecord := range since {
			if now[i].Stack0 == sinceRecord.Stack0 {
				now[i].AllocObjects -= sinceRecord.AllocObjects
				now[i].AllocBytes -= sinceRecord.AllocBytes
				break
			}
		}
		if now[i].AllocBytes > 0 {
			ret = append(ret, now[i])
		}
	}
	return myPC, ret, nil
}
