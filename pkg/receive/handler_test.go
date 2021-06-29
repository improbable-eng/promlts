// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestDetermineWriteErrorCause(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		threshold int
		exp       error
	}{
		{
			name: "nil",
		},
		{
			name: "nil multierror",
			err:  errutil.NonNilMultiError([]error{}),
		},
		{
			name:      "matching simple",
			err:       errConflict,
			threshold: 1,
			exp:       errConflict,
		},
		{
			name: "non-matching multierror",
			err: errutil.NonNilMultiError([]error{
				errors.New("foo"),
				errors.New("bar"),
			}),
			exp: errors.New("2 errors: foo; bar"),
		},
		{
			name: "nested non-matching multierror",
			err: errors.Wrap(errutil.NonNilMultiError([]error{
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			threshold: 1,
			exp:       errors.New("baz: 2 errors: foo; bar"),
		},
		{
			name: "deep nested non-matching multierror",
			err: errors.Wrap(errutil.NonNilMultiError([]error{
				errors.New("foo"),
				errutil.NonNilMultiError([]error{
					errors.New("bar"),
					errors.New("qux"),
				}),
			}), "baz"),
			threshold: 1,
			exp:       errors.New("baz: 2 errors: foo; 2 errors: bar; qux"),
		},
		{
			name: "matching multierror",
			err: errutil.NonNilMultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}),
			threshold: 1,
			exp:       errConflict,
		},
		{
			name: "matching but below threshold multierror",
			err: errutil.NonNilMultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}),
			threshold: 2,
			exp:       errors.New("3 errors: out of order sample; foo; bar"),
		},
		{
			name: "matching multierror many",
			err: errutil.NonNilMultiError([]error{
				storage.ErrOutOfOrderSample,
				errConflict,
				status.Error(codes.AlreadyExists, "conflict"),
				errors.New("foo"),
				errors.New("bar"),
			}),
			threshold: 1,
			exp:       errConflict,
		},
		{
			name: "matching multierror many, one above threshold",
			err: errutil.NonNilMultiError([]error{
				storage.ErrOutOfOrderSample,
				errConflict,
				tsdb.ErrNotReady,
				tsdb.ErrNotReady,
				tsdb.ErrNotReady,
				errors.New("foo"),
			}),
			threshold: 2,
			exp:       errNotReady,
		},
		{
			name: "matching multierror many, both above threshold, conflict have precedence",
			err: errutil.NonNilMultiError([]error{
				storage.ErrOutOfOrderSample,
				errConflict,
				tsdb.ErrNotReady,
				tsdb.ErrNotReady,
				tsdb.ErrNotReady,
				status.Error(codes.AlreadyExists, "conflict"),
				errors.New("foo"),
			}),
			threshold: 2,
			exp:       errConflict,
		},
		{
			name: "nested matching multierror",
			err: errors.Wrap(errors.Wrap(errutil.NonNilMultiError([]error{
				storage.ErrOutOfOrderSample,
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"), "qux"),
			threshold: 1,
			exp:       errConflict,
		},
		{
			name: "deep nested matching multierror",
			err: errors.Wrap(errutil.NonNilMultiError([]error{
				errutil.NonNilMultiError([]error{
					errors.New("qux"),
					status.Error(codes.AlreadyExists, "conflict"),
					status.Error(codes.AlreadyExists, "conflict"),
				}),
				errors.New("foo"),
				errors.New("bar"),
			}), "baz"),
			threshold: 1,
			exp:       errors.New("baz: 3 errors: 3 errors: qux; rpc error: code = AlreadyExists desc = conflict; rpc error: code = AlreadyExists desc = conflict; foo; bar"),
		},
	} {
		err := determineWriteErrorCause(tc.err, tc.threshold)
		if tc.exp != nil {
			testutil.NotOk(t, err)
			testutil.Equals(t, tc.exp.Error(), err.Error())
			continue
		}
		testutil.Ok(t, err)
	}
}

type fakeTenantAppendable struct {
	f *fakeAppendable
}

func newFakeTenantAppendable(f *fakeAppendable) *fakeTenantAppendable {
	return &fakeTenantAppendable{f: f}
}

func (t *fakeTenantAppendable) TenantAppendable(_ string) (Appendable, error) {
	return t.f, nil
}

type fakeAppendable struct {
	appender    storage.Appender
	appenderErr func() error
}

var _ Appendable = &fakeAppendable{}

func nilErrFn() error {
	return nil
}

func (f *fakeAppendable) Appender(_ context.Context) (storage.Appender, error) {
	errf := f.appenderErr
	if errf == nil {
		errf = nilErrFn
	}
	return f.appender, errf()
}

type fakeAppender struct {
	sync.Mutex
	samples     map[uint64][]prompb.Sample
	exemplars   map[uint64][]exemplar.Exemplar
	appendErr   func() error
	commitErr   func() error
	rollbackErr func() error
}

var _ storage.Appender = &fakeAppender{}
var _ storage.GetRef = &fakeAppender{}

func newFakeAppender(appendErr, commitErr, rollbackErr func() error) *fakeAppender { //nolint:unparam
	if appendErr == nil {
		appendErr = nilErrFn
	}
	if commitErr == nil {
		commitErr = nilErrFn
	}
	if rollbackErr == nil {
		rollbackErr = nilErrFn
	}
	return &fakeAppender{
		samples:     make(map[uint64][]prompb.Sample),
		appendErr:   appendErr,
		commitErr:   commitErr,
		rollbackErr: rollbackErr,
	}
}

func (f *fakeAppender) Get(l labels.Labels) []prompb.Sample {
	f.Lock()
	defer f.Unlock()
	s := f.samples[l.Hash()]
	res := make([]prompb.Sample, len(s))
	copy(res, s)
	return res
}

func (f *fakeAppender) Append(ref uint64, l labels.Labels, t int64, v float64) (uint64, error) {
	f.Lock()
	defer f.Unlock()
	if ref == 0 {
		ref = l.Hash()
	}
	f.samples[ref] = append(f.samples[ref], prompb.Sample{Timestamp: t, Value: v})
	return ref, f.appendErr()
}

func (f *fakeAppender) AppendExemplar(ref uint64, l labels.Labels, e exemplar.Exemplar) (uint64, error) {
	f.Lock()
	defer f.Unlock()
	if ref == 0 {
		ref = l.Hash()
	}
	f.exemplars[ref] = append(f.exemplars[ref], e)
	return ref, f.appendErr()
}

func (f *fakeAppender) GetRef(l labels.Labels) (uint64, labels.Labels) {
	return l.Hash(), l
}

func (f *fakeAppender) Commit() error {
	return f.commitErr()
}

func (f *fakeAppender) Rollback() error {
	return f.rollbackErr()
}

func newTestHandlerHashring(appendables []*fakeAppendable, replicationFactor uint64, samplesLimitPerEachWrite uint64) ([]*Handler, Hashring) {
	var (
		cfg      = []HashringConfig{{Hashring: "test"}}
		handlers []*Handler
	)
	// create a fake peer group where we manually fill the cache with fake addresses pointed to our handlers
	// This removes the network from the tests and creates a more consistent testing harness.
	peers := &peerGroup{
		dialOpts: nil,
		m:        sync.RWMutex{},
		cache:    map[string]storepb.WriteableStoreClient{},
		dialer: func(context.Context, string, ...grpc.DialOption) (*grpc.ClientConn, error) {
			// dialer should never be called since we are creating fake clients with fake addresses
			// this protects against some leaking test that may attempt to dial random IP addresses
			// which may pose a security risk.
			return nil, errors.New("unexpected dial called in testing")
		},
	}

	for i := range appendables {
		h := NewHandler(nil, &Options{
			TenantHeader:             DefaultTenantHeader,
			ReplicaHeader:            DefaultReplicaHeader,
			ReplicationFactor:        replicationFactor,
			ForwardTimeout:           5 * time.Second,
			SamplesLimitPerEachWrite: samplesLimitPerEachWrite,
			Writer:                   NewWriter(log.NewNopLogger(), newFakeTenantAppendable(appendables[i])),
		})
		handlers = append(handlers, h)
		h.peers = peers
		addr := randomAddr()
		h.options.Endpoint = addr
		cfg[0].Endpoints = append(cfg[0].Endpoints, h.options.Endpoint)
		peers.cache[addr] = &fakeRemoteWriteGRPCServer{h: h}
	}
	hashring := newMultiHashring(cfg)
	for _, h := range handlers {
		h.Hashring(hashring)
	}
	return handlers, hashring
}

func TestReceiveSamplesLimit(t *testing.T) {
	wreq1 := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{
						Name:  "foo",
						Value: "bar",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     1,
						Timestamp: 1,
					},
				},
			},
		},
	}
	for _, tc := range []struct {
		name                     string
		status                   int
		replicationFactor        uint64
		wreq                     *prompb.WriteRequest
		samplesLimitPerEachWrite uint64
		appendables              []*fakeAppendable
	}{
		{
			name:                     "success: recevied samples are less than samplesLimitPerEachWrite",
			status:                   http.StatusOK,
			replicationFactor:        1,
			wreq:                     wreq1,
			samplesLimitPerEachWrite: 2,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "failure: recevied samples are higher than samplesLimitPerEachWrite",
			status:                   http.StatusRequestEntityTooLarge,
			replicationFactor:        1,
			wreq:                     wreq1,
			samplesLimitPerEachWrite: 0,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handlers, _ := newTestHandlerHashring(tc.appendables, tc.replicationFactor, tc.samplesLimitPerEachWrite)
			tenant := "test"
			// Test from the point of view of every node
			// so that we know status code does not depend
			// on which node is erroring and which node is receiving.
			for i, handler := range handlers {
				// Test that the correct status is returned.
				rec, err := makeRequest(handler, tenant, tc.wreq)
				if err != nil {
					t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", tc.status, err)
				}
				if rec.Code != tc.status {
					t.Errorf("handler %d: got unexpected HTTP status code: expected %d, got %d; body: %s", i, tc.status, rec.Code, rec.Body.String())
				}
			}
		})
	}
}

func TestReceiveQuorum(t *testing.T) {
	appenderErrFn := func() error { return errors.New("failed to get appender") }
	conflictErrFn := func() error { return storage.ErrOutOfBounds }
	commitErrFn := func() error { return errors.New("failed to commit") }
	wreq1 := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{
						Name:  "foo",
						Value: "bar",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     1,
						Timestamp: 1,
					},
					{
						Value:     2,
						Timestamp: 2,
					},
					{
						Value:     3,
						Timestamp: 3,
					},
				},
			},
		},
	}
	for _, tc := range []struct {
		name                     string
		status                   int
		replicationFactor        uint64
		samplesLimitPerEachWrite uint64
		wreq                     *prompb.WriteRequest
		appendables              []*fakeAppendable
	}{
		{
			name:                     "size 1 success",
			status:                   http.StatusOK,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 1 commit error",
			status:                   http.StatusInternalServerError,
			replicationFactor:        1,
			wreq:                     wreq1,
			samplesLimitPerEachWrite: 5000,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:                     "size 1 conflict",
			status:                   http.StatusConflict,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
			},
		},
		{
			name:                     "size 2 success",
			status:                   http.StatusOK,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 success",
			status:                   http.StatusOK,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 success with replication",
			status:                   http.StatusOK,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 commit error",
			status:                   http.StatusInternalServerError,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:                     "size 3 commit error with replication",
			status:                   http.StatusInternalServerError,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:                     "size 3 appender error with replication",
			status:                   http.StatusInternalServerError,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
			},
		},
		{
			name:                     "size 3 conflict with replication",
			status:                   http.StatusConflict,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 conflict and commit error with replication",
			status:                   http.StatusConflict,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication and one faulty",
			status:                   http.StatusOK,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication and one commit error",
			status:                   http.StatusOK,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication and two conflicts",
			status:                   http.StatusConflict,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication one conflict and one commit error",
			status:                   http.StatusInternalServerError,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication two commit errors",
			status:                   http.StatusInternalServerError,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handlers, hashring := newTestHandlerHashring(tc.appendables, tc.replicationFactor, tc.samplesLimitPerEachWrite)
			tenant := "test"
			// Test from the point of view of every node
			// so that we know status code does not depend
			// on which node is erroring and which node is receiving.
			for i, handler := range handlers {
				// Test that the correct status is returned.
				rec, err := makeRequest(handler, tenant, tc.wreq)
				if err != nil {
					t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", tc.status, err)
				}
				if rec.Code != tc.status {
					t.Errorf("handler %d: got unexpected HTTP status code: expected %d, got %d; body: %s", i, tc.status, rec.Code, rec.Body.String())
				}
			}
			// Test that each time series is stored
			// the correct amount of times in each fake DB.
			for _, ts := range tc.wreq.Timeseries {
				lset := make(labels.Labels, len(ts.Labels))
				for j := range ts.Labels {
					lset[j] = labels.Label{
						Name:  ts.Labels[j].Name,
						Value: ts.Labels[j].Value,
					}
				}
				for j, a := range tc.appendables {
					var expectedMin int
					n := a.appender.(*fakeAppender).Get(lset)
					got := uint64(len(n))
					if a.appenderErr == nil && endpointHit(t, hashring, tc.replicationFactor, handlers[j].options.Endpoint, tenant, &ts) {
						// We have len(handlers) copies of each sample because the test case
						// is run once for each handler and they all use the same appender.
						expectedMin = int((tc.replicationFactor/2)+1) * len(ts.Samples)
					}
					if uint64(expectedMin) > got {
						t.Errorf("handler: %d, labels %q: expected minimum of %d samples, got %d", j, lset.String(), expectedMin, got)
					}
				}
			}
		})
	}
}

func TestReceiveWithConsistencyDelay(t *testing.T) {
	appenderErrFn := func() error { return errors.New("failed to get appender") }
	conflictErrFn := func() error { return storage.ErrOutOfBounds }
	commitErrFn := func() error { return errors.New("failed to commit") }
	wreq1 := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{
						Name:  "foo",
						Value: "bar",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     1,
						Timestamp: 1,
					},
					{
						Value:     2,
						Timestamp: 2,
					},
					{
						Value:     3,
						Timestamp: 3,
					},
				},
			},
		},
	}
	for _, tc := range []struct {
		name                     string
		status                   int
		replicationFactor        uint64
		samplesLimitPerEachWrite uint64
		wreq                     *prompb.WriteRequest
		appendables              []*fakeAppendable
	}{
		{
			name:                     "size 1 success",
			status:                   http.StatusOK,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 1 commit error",
			status:                   http.StatusInternalServerError,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:                     "size 1 conflict",
			status:                   http.StatusConflict,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
			},
		},
		{
			name:                     "size 2 success",
			status:                   http.StatusOK,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 success",
			status:                   http.StatusOK,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 success with replication",
			status:                   http.StatusOK,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 commit error",
			status:                   http.StatusInternalServerError,
			replicationFactor:        1,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:                     "size 3 commit error with replication",
			status:                   http.StatusInternalServerError,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
			},
		},
		{
			name:                     "size 3 appender error with replication",
			status:                   http.StatusInternalServerError,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
				{
					appender:    newFakeAppender(nil, nil, nil),
					appenderErr: appenderErrFn,
				},
			},
		},
		{
			name:                     "size 3 conflict with replication",
			status:                   http.StatusConflict,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 conflict and commit error with replication",
			status:                   http.StatusConflict,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, commitErrFn, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication and one faulty",
			status:                   http.StatusOK,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication and one commit error",
			status:                   http.StatusOK,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication and two conflicts",
			status:                   http.StatusConflict,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
				{
					appender: newFakeAppender(conflictErrFn, nil, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication one conflict and one commit error",
			status:                   http.StatusInternalServerError,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(cycleErrors([]error{storage.ErrOutOfBounds, storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp}), nil, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
		{
			name:                     "size 3 with replication two commit errors",
			status:                   http.StatusInternalServerError,
			replicationFactor:        3,
			samplesLimitPerEachWrite: 5000,
			wreq:                     wreq1,
			appendables: []*fakeAppendable{
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, commitErrFn, nil),
				},
				{
					appender: newFakeAppender(nil, nil, nil),
				},
			},
		},
	} {
		// Run the quorum tests with consistency delay, which should allow us
		// to see all requests completing all the time, since we're using local
		// network we are not expecting anything to go wrong with these.
		t.Run(tc.name, func(t *testing.T) {
			handlers, hashring := newTestHandlerHashring(tc.appendables, tc.replicationFactor, tc.samplesLimitPerEachWrite)
			tenant := "test"
			// Test from the point of view of every node
			// so that we know status code does not depend
			// on which node is erroring and which node is receiving.
			for i, handler := range handlers {
				// Test that the correct status is returned.
				rec, err := makeRequest(handler, tenant, tc.wreq)
				if err != nil {
					t.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", tc.status, err)
				}
				if rec.Code != tc.status {
					t.Errorf("handler %d: got unexpected HTTP status code: expected %d, got %d; body: %s", i, tc.status, rec.Code, rec.Body.String())
				}
			}

			time.Sleep(50 * time.Millisecond)

			// Test that each time series is stored
			// the correct amount of times in each fake DB.
			for _, ts := range tc.wreq.Timeseries {
				lset := make(labels.Labels, len(ts.Labels))
				for j := range ts.Labels {
					lset[j] = labels.Label{
						Name:  ts.Labels[j].Name,
						Value: ts.Labels[j].Value,
					}
				}
				for j, a := range tc.appendables {
					var expected int
					n := a.appender.(*fakeAppender).Get(lset)
					got := uint64(len(n))
					if a.appenderErr == nil && endpointHit(t, hashring, tc.replicationFactor, handlers[j].options.Endpoint, tenant, &ts) {
						// We have len(handlers) copies of each sample because the test case
						// is run once for each handler and they all use the same appender.
						expected = len(handlers) * len(ts.Samples)
					}
					if uint64(expected) != got {
						t.Errorf("handler: %d, labels %q: expected %d samples, got %d", j, lset.String(), expected, got)
					}
				}
			}
		})
	}
}

// endpointHit is a helper to determine if a given endpoint in a hashring would be selected
// for a given time series, tenant, and replication factor.
func endpointHit(t *testing.T, h Hashring, rf uint64, endpoint, tenant string, timeSeries *prompb.TimeSeries) bool {
	for i := uint64(0); i < rf; i++ {
		e, err := h.GetN(tenant, timeSeries, i)
		if err != nil {
			t.Fatalf("got unexpected error querying hashring: %v", err)
		}
		if e == endpoint {
			return true
		}
	}
	return false
}

// cycleErrors returns an error generator that cycles through every given error.
func cycleErrors(errs []error) func() error {
	var mu sync.Mutex
	var i int
	return func() error {
		mu.Lock()
		defer mu.Unlock()
		err := errs[i]
		i++
		if i >= len(errs) {
			i = 0
		}
		return err
	}
}

// makeRequest is a helper to make a correct request against a remote write endpoint given a request.
func makeRequest(h *Handler, tenant string, wreq *prompb.WriteRequest) (*httptest.ResponseRecorder, error) {
	buf, err := proto.Marshal(wreq)
	if err != nil {
		return nil, errors.Wrap(err, "marshal request")
	}
	req, err := http.NewRequest("POST", h.options.Endpoint, bytes.NewBuffer(snappy.Encode(nil, buf)))
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	req.Header.Add(h.options.TenantHeader, tenant)

	rec := httptest.NewRecorder()
	h.receiveHTTP(rec, req)
	rec.Flush()

	return rec, nil
}

func randomAddr() string {
	return fmt.Sprintf("http://%d.%d.%d.%d:%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(35000)+30000)
}

type fakeRemoteWriteGRPCServer struct {
	h storepb.WriteableStoreServer
}

func (f *fakeRemoteWriteGRPCServer) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, opts ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return f.h.RemoteWrite(ctx, in)
}

func BenchmarkHandlerReceiveHTTP(b *testing.B) {
	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTB(b))
}

func TestHandlerReceiveHTTP(t *testing.T) {
	benchmarkHandlerMultiTSDBReceiveRemoteWrite(testutil.NewTB(t))
}

// tsOverrideTenantStorage is storage that overrides timestamp to make it have consistent interval.
type tsOverrideTenantStorage struct {
	TenantStorage

	interval int64
}

func (s *tsOverrideTenantStorage) TenantAppendable(tenant string) (Appendable, error) {
	a, err := s.TenantStorage.TenantAppendable(tenant)
	return &tsOverrideAppendable{Appendable: a, interval: s.interval}, err
}

type tsOverrideAppendable struct {
	Appendable

	interval int64
}

func (a *tsOverrideAppendable) Appender(ctx context.Context) (storage.Appender, error) {
	ret, err := a.Appendable.Appender(ctx)
	return &tsOverrideAppender{Appender: ret, interval: a.interval}, err
}

type tsOverrideAppender struct {
	storage.Appender

	interval int64
}

var cnt int64

func (a *tsOverrideAppender) Append(ref uint64, l labels.Labels, _ int64, v float64) (uint64, error) {
	cnt += a.interval
	return a.Appender.Append(ref, l, cnt, v)
}

func (a *tsOverrideAppender) GetRef(lset labels.Labels) (uint64, labels.Labels) {
	return a.Appender.(storage.GetRef).GetRef(lset)
}

// serializeSeriesWithOneSample returns marshaled and compressed remote write requests like it would
// be send to Thanos receive.
// It has one sample and allow passing multiple series, in same manner as typical Prometheus would batch it.
func serializeSeriesWithOneSample(t testing.TB, series [][]labelpb.ZLabel) []byte {
	r := &prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, 0, len(series))}

	for _, s := range series {
		r.Timeseries = append(r.Timeseries, prompb.TimeSeries{
			Labels: s,
			// Timestamp does not matter, it will be overridden.
			Samples: []prompb.Sample{{Value: math.MaxFloat64, Timestamp: math.MinInt64}},
		})
	}
	body, err := proto.Marshal(r)
	testutil.Ok(t, err)
	return snappy.Encode(nil, body)
}

func benchmarkHandlerMultiTSDBReceiveRemoteWrite(b testutil.TB) {
	dir, err := ioutil.TempDir("", "test_receive")
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, os.RemoveAll(dir)) }()

	handlers, _ := newTestHandlerHashring([]*fakeAppendable{nil}, 1, 5000)
	handler := handlers[0]

	reg := prometheus.NewRegistry()

	logger := log.NewNopLogger()
	m := NewMultiTSDB(
		dir, logger, reg, &tsdb.Options{
			MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			RetentionDuration: int64(6 * time.Hour / time.Millisecond),
			NoLockfile:        true,
			StripeSize:        1, // Disable stripe pre allocation so we can have clear profiles.
		},
		labels.FromStrings("replica", "01"),
		"tenant_id",
		nil,
		false,
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(b, m.Close()) }()
	handler.writer = NewWriter(logger, m)

	testutil.Ok(b, m.Flush())
	testutil.Ok(b, m.Open())

	for _, tcase := range []struct {
		name         string
		writeRequest []byte
	}{
		{
			name: "typical labels under 1KB, 500 of them",
			writeRequest: serializeSeriesWithOneSample(b, func() [][]labelpb.ZLabel {
				series := make([][]labelpb.ZLabel, 500)
				for s := 0; s < len(series); s++ {
					lbls := make([]labelpb.ZLabel, 10)
					for i := 0; i < len(lbls); i++ {
						// Label ~20B name, 50B value.
						lbls[i] = labelpb.ZLabel{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
					}
					series[s] = lbls
				}
				return series
			}()),
		},
		{
			name: "typical labels under 1KB, 5000 of them",
			writeRequest: serializeSeriesWithOneSample(b, func() [][]labelpb.ZLabel {
				series := make([][]labelpb.ZLabel, 5000)
				for s := 0; s < len(series); s++ {
					lbls := make([]labelpb.ZLabel, 10)
					for i := 0; i < len(lbls); i++ {
						// Label ~20B name, 50B value.
						lbls[i] = labelpb.ZLabel{Name: fmt.Sprintf("abcdefghijabcdefghijabcdefghij%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij%d", i)}
					}
					series[s] = lbls
				}
				return series
			}()),
		},
		{
			name: "extremely large label value 10MB, 10 of them",
			writeRequest: serializeSeriesWithOneSample(b, func() [][]labelpb.ZLabel {
				series := make([][]labelpb.ZLabel, 10)
				for s := 0; s < len(series); s++ {
					lbl := &strings.Builder{}
					lbl.Grow(1024 * 1024 * 10) // 10MB.
					word := "abcdefghij"
					for i := 0; i < lbl.Cap()/len(word); i++ {
						_, _ = lbl.WriteString(word)
					}
					series[s] = []labelpb.ZLabel{{Name: "__name__", Value: lbl.String()}}
				}
				return series
			}()),
		},
	} {
		b.Run(tcase.name, func(b testutil.TB) {
			handler.options.DefaultTenantID = fmt.Sprintf("%v-ok", tcase.name)
			handler.writer.multiTSDB = &tsOverrideTenantStorage{TenantStorage: m, interval: 1}

			// It takes time to create new tenant, wait for it.
			{
				app, err := m.TenantAppendable(handler.options.DefaultTenantID)
				testutil.Ok(b, err)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				testutil.Ok(b, runutil.Retry(1*time.Second, ctx.Done(), func() error {
					_, err = app.Appender(ctx)
					return err
				}))
			}

			b.Run("OK", func(b testutil.TB) {
				n := b.N()
				b.ResetTimer()
				for i := 0; i < n; i++ {
					r := httptest.NewRecorder()
					handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: ioutil.NopCloser(bytes.NewReader(tcase.writeRequest))})
					testutil.Equals(b, http.StatusOK, r.Code, "got non 200 error: %v", r.Body.String())
				}
			})

			handler.options.DefaultTenantID = fmt.Sprintf("%v-conflicting", tcase.name)
			handler.writer.multiTSDB = &tsOverrideTenantStorage{TenantStorage: m, interval: -1} // Timestamp can't go down, which will cause conflict error.

			// It takes time to create new tenant, wait for it.
			{
				app, err := m.TenantAppendable(handler.options.DefaultTenantID)
				testutil.Ok(b, err)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				testutil.Ok(b, runutil.Retry(1*time.Second, ctx.Done(), func() error {
					_, err = app.Appender(ctx)
					return err
				}))
			}

			// First request should be fine, since we don't change timestamp, rest is wrong.
			r := httptest.NewRecorder()
			handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: ioutil.NopCloser(bytes.NewReader(tcase.writeRequest))})
			testutil.Equals(b, http.StatusOK, r.Code, "got non 200 error: %v", r.Body.String())

			b.Run("conflict errors", func(b testutil.TB) {
				n := b.N()
				b.ResetTimer()
				for i := 0; i < n; i++ {
					r := httptest.NewRecorder()
					handler.receiveHTTP(r, &http.Request{ContentLength: int64(len(tcase.writeRequest)), Body: ioutil.NopCloser(bytes.NewReader(tcase.writeRequest))})
					testutil.Equals(b, http.StatusConflict, r.Code, "%v-%s", i, func() string {
						b, _ := ioutil.ReadAll(r.Body)
						return string(b)
					}())
				}
			})
		})
	}

	runtime.GC()
	// Take snapshot at the end to reveal how much memory we keep in TSDB.
	testutil.Ok(b, Heap("../../../_dev/thanos/2021/receive2"))

}

func Heap(dir string) (err error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(dir, "errimpr1-go1.16.3.pprof"))
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, f, "close")
	return pprof.WriteHeapProfile(f)
}
