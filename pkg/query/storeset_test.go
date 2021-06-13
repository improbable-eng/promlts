// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

var testGRPCOpts = []grpc.DialOption{
	grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	grpc.WithInsecure(),
}

type mockedStore struct {
	infoDelay time.Duration
	info      storepb.InfoResponse
}

func (s *mockedStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	if s.infoDelay > 0 {
		time.Sleep(s.infoDelay)
	}
	return &s.info, nil
}

func (s *mockedStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}

func (s *mockedStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *mockedStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

type testStoreMeta struct {
	extlsetFn        func(addr string) []labelpb.ZLabelSet
	storeType        component.StoreAPI
	minTime, maxTime int64
	infoDelay        time.Duration
}

type testStores struct {
	srvs       map[string]*grpc.Server
	orderAddrs []string
}

func startTestStores(storeMetas []testStoreMeta) (*testStores, error) {
	st := &testStores{
		srvs: map[string]*grpc.Server{},
	}

	for _, meta := range storeMetas {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			// Close so far started servers.
			st.Close()
			return nil, err
		}

		srv := grpc.NewServer()

		storeSrv := &mockedStore{
			info: storepb.InfoResponse{
				LabelSets: meta.extlsetFn(listener.Addr().String()),
				MaxTime:   meta.maxTime,
				MinTime:   meta.minTime,
			},
			infoDelay: meta.infoDelay,
		}
		if meta.storeType != nil {
			storeSrv.info.StoreType = meta.storeType.ToProto()
		}
		storepb.RegisterStoreServer(srv, storeSrv)
		go func() {
			_ = srv.Serve(listener)
		}()

		st.srvs[listener.Addr().String()] = srv
		st.orderAddrs = append(st.orderAddrs, listener.Addr().String())
	}

	return st, nil
}

func (s *testStores) StoreAddresses() []string {
	var stores []string
	stores = append(stores, s.orderAddrs...)
	return stores
}

func (s *testStores) Close() {
	for _, srv := range s.srvs {
		srv.Stop()
	}
	s.srvs = nil
}

func (s *testStores) CloseOne(addr string) {
	srv, ok := s.srvs[addr]
	if !ok {
		return
	}

	srv.Stop()
	delete(s.srvs, addr)
}

type mockedInfo struct {
	info infopb.InfoResp
}

func (s *mockedInfo) Info(ctx context.Context, r *infopb.InfoReq) (*infopb.InfoResp, error) {
	return &s.info, nil
}

type testInfoMeta struct {
	extlsetFn func(add string) []labelpb.ZLabelSet
	storeType component.StoreAPI
	store     infopb.StoreInfo
	rule      infopb.RulesInfo
	metadata  infopb.MetricMetadataInfo
	target    infopb.TargetsInfo
	exemplar  infopb.ExemplarsInfo
}

type testInfoSrvs struct {
	srvs       map[string]*grpc.Server
	orderAddrs []string
}

func startInfoSrvs(infoMetas []testInfoMeta) (*testInfoSrvs, error) {
	info := &testInfoSrvs{
		srvs: map[string]*grpc.Server{},
	}

	for _, meta := range infoMetas {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			// Close the servers
			info.Close()
			return nil, err
		}

		srv := grpc.NewServer()

		infoSrv := &mockedInfo{
			info: infopb.InfoResp{
				LabelSets:      meta.extlsetFn(listener.Addr().String()),
				Store:          &meta.store,
				MetricMetadata: &meta.metadata,
				Rules:          &meta.rule,
				Targets:        &meta.target,
				Exemplars:      &meta.exemplar,
			},
		}

		if meta.storeType != nil {
			infoSrv.info.ComponentType = meta.storeType.String()
		}
		infopb.RegisterInfoServer(srv, infoSrv)
		go func() {
			_ = srv.Serve(listener)
		}()

		info.srvs[listener.Addr().String()] = srv
		info.orderAddrs = append(info.orderAddrs, listener.Addr().String())
	}

	return info, nil
}

func (s *testInfoSrvs) Close() {
	for _, srv := range s.srvs {
		srv.Stop()
	}
	s.srvs = nil
}

func (s *testInfoSrvs) CloseOne(addr string) {
	srv, ok := s.srvs[addr]
	if !ok {
		return
	}

	srv.Stop()
	delete(s.srvs, addr)
}

func (s *testInfoSrvs) InfoAddresses() []string {
	var stores []string
	stores = append(stores, s.orderAddrs...)
	return stores
}

func TestStoreSet_Update(t *testing.T) {
	stores, err := startTestStores([]testStoreMeta{
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "addr", Value: addr},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "a", Value: "b"},
						},
					},
				}
			},
		},
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "addr", Value: addr},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "a", Value: "b"},
						},
					},
				}
			},
		},
		{
			storeType: component.Query,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "a", Value: "broken"},
						},
					},
				}
			},
		},
	})
	testutil.Ok(t, err)
	defer stores.Close()

	discoveredStoreAddr := stores.StoreAddresses()

	infoSrvs, err := startInfoSrvs([]testInfoMeta{
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "addr", Value: addr},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "a", Value: "b"},
						},
					},
				}
			},
			store: infopb.StoreInfo{
				MinTime: math.MaxInt64,
				MaxTime: math.MinInt64,
			},
			exemplar: infopb.ExemplarsInfo{
				MinTime: math.MaxInt64,
				MaxTime: math.MinInt64,
			},
			rule:     infopb.RulesInfo{},
			metadata: infopb.MetricMetadataInfo{},
			target:   infopb.TargetsInfo{},
		},
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "addr", Value: addr},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "a", Value: "b"},
						},
					},
				}
			},
			store: infopb.StoreInfo{
				MinTime: math.MaxInt64,
				MaxTime: math.MinInt64,
			},
			exemplar: infopb.ExemplarsInfo{
				MinTime: math.MaxInt64,
				MaxTime: math.MinInt64,
			},
			rule:     infopb.RulesInfo{},
			metadata: infopb.MetricMetadataInfo{},
			target:   infopb.TargetsInfo{},
		},
		{
			storeType: component.Query,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "addr", Value: "broken"},
						},
					},
				}
			},
			store: infopb.StoreInfo{
				MinTime: math.MaxInt64,
				MaxTime: math.MinInt64,
			},
			exemplar: infopb.ExemplarsInfo{
				MinTime: math.MaxInt64,
				MaxTime: math.MinInt64,
			},
			rule:     infopb.RulesInfo{},
			metadata: infopb.MetricMetadataInfo{},
			target:   infopb.TargetsInfo{},
		},
	})
	testutil.Ok(t, err)
	defer infoSrvs.Close()

	discoveredInfoAddr := infoSrvs.InfoAddresses()

	// Testing if duplicates can cause weird results.
	discoveredStoreAddr = append(discoveredStoreAddr, discoveredStoreAddr[0])
	discoveredInfoAddr = append(discoveredInfoAddr, discoveredInfoAddr[0])
	storeSet := NewStoreSet(nil, nil,
		func() (specs []StoreSpec) {
			for _, addr := range discoveredStoreAddr {
				specs = append(specs, NewGRPCStoreSpec(addr, false))
			}
			return specs
		},
		func() (specs []RuleSpec) {
			return nil
		},
		func() (specs []TargetSpec) {
			return nil
		},
		func() (specs []MetadataSpec) {
			return nil
		},
		func() (specs []ExemplarSpec) {
			return nil
		},
		func() (specs []InfoSpec) {
			for _, addr := range discoveredInfoAddr {
				specs = append(specs, NewGRPCStoreSpec(addr, false))
			}
			return specs
		},
		testGRPCOpts, time.Minute)
	storeSet.gRPCInfoCallTimeout = 2 * time.Second
	defer storeSet.Close()

	// Initial update.
	storeSet.Update(context.Background())

	// Start with one not available.
	stores.CloseOne(discoveredStoreAddr[2])

	// Make one address discovered by Info Servers unavailable.
	infoSrvs.CloseOne(discoveredInfoAddr[2])

	// Should not matter how many of these we run.
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())
	testutil.Equals(t, 4, len(storeSet.stores))
	testutil.Equals(t, 6, len(storeSet.storeStatuses))

	for addr, st := range storeSet.stores {
		testutil.Equals(t, addr, st.addr)

		lset := st.LabelSets()
		testutil.Equals(t, 2, len(lset))
		testutil.Equals(t, "addr", lset[0][0].Name)
		testutil.Equals(t, addr, lset[0][0].Value)
		testutil.Equals(t, "a", lset[1][0].Name)
		testutil.Equals(t, "b", lset[1][0].Value)
	}

	// Check stats.
	expected := newStoreAPIStats()
	expected[component.Sidecar] = map[string]int{
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredStoreAddr[0]): 1,
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredStoreAddr[1]): 1,
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredInfoAddr[0]):  1,
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredInfoAddr[1]):  1,
	}
	testutil.Equals(t, expected, storeSet.storesMetric.storeNodes)

	// Remove address from discovered and reset last check, which should ensure cleanup of status on next update.
	storeSet.storeStatuses[discoveredStoreAddr[2]].LastCheck = time.Now().Add(-4 * time.Minute)
	discoveredStoreAddr = discoveredStoreAddr[:len(discoveredStoreAddr)-2]
	storeSet.storeStatuses[discoveredInfoAddr[2]].LastCheck = time.Now().Add(-4 * time.Minute)
	discoveredInfoAddr = discoveredInfoAddr[:len(discoveredInfoAddr)-2]
	storeSet.Update(context.Background())
	testutil.Equals(t, 4, len(storeSet.storeStatuses))

	stores.CloseOne(discoveredStoreAddr[0])
	delete(expected[component.Sidecar], fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredStoreAddr[0]))
	infoSrvs.CloseOne(discoveredInfoAddr[0])
	delete(expected[component.Sidecar], fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredInfoAddr[0]))

	// We expect Update to tear down store client for closed store server.
	storeSet.Update(context.Background())
	testutil.Equals(t, 2, len(storeSet.stores), "only two service should respond just fine, so we expect one client to be ready.")
	testutil.Equals(t, 4, len(storeSet.storeStatuses))

	addr := discoveredStoreAddr[1]
	st, ok := storeSet.stores[addr]
	testutil.Assert(t, ok, "addr exist")
	testutil.Equals(t, addr, st.addr)

	lset := st.LabelSets()
	testutil.Equals(t, 2, len(lset))
	testutil.Equals(t, "addr", lset[0][0].Name)
	testutil.Equals(t, addr, lset[0][0].Value)
	testutil.Equals(t, "a", lset[1][0].Name)
	testutil.Equals(t, "b", lset[1][0].Value)
	testutil.Equals(t, expected, storeSet.storesMetric.storeNodes)

	// New big batch of storeAPIs.
	stores2, err := startTestStores([]testStoreMeta{
		{
			storeType: component.Query,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "l3", Value: "v4"},
						},
					},
				}
			},
		},
		{
			// Duplicated Querier, in previous versions it would be deduplicated. Now it should be not.
			storeType: component.Query,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "l3", Value: "v4"},
						},
					},
				}
			},
		},
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			// Duplicated Sidecar, in previous versions it would be deduplicated. Now it should be not.
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			// Querier that duplicates with sidecar, in previous versions it would be deduplicated. Now it should be not.
			storeType: component.Query,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			// Ruler that duplicates with sidecar, in previous versions it would be deduplicated. Now it should be not.
			// Warning should be produced.
			storeType: component.Rule,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			// Duplicated Rule, in previous versions it would be deduplicated. Now it should be not. Warning should be produced.
			storeType: component.Rule,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		{
			// No storeType.
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "no-store-type"},
							{Name: "l2", Value: "v3"},
						},
					},
				}
			},
		},
		// Two pre v0.8.0 store gateway nodes, they don't have ext labels set.
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
		// Regression tests against https://github.com/thanos-io/thanos/issues/1632: From v0.8.0 stores advertise labels.
		// If the object storage handled by store gateway has only one sidecar we used to hitting issue.
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "l3", Value: "v4"},
						},
					},
				}
			},
		},
		// Stores v0.8.1 has compatibility labels. Check if they are correctly removed.
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "l3", Value: "v4"},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: store.CompatibilityTypeLabelName, Value: "store"},
						},
					},
				}
			},
		},
		// Duplicated store, in previous versions it would be deduplicated. Now it should be not.
		{
			storeType: component.Store,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "l1", Value: "v2"},
							{Name: "l2", Value: "v3"},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: "l3", Value: "v4"},
						},
					},
					{
						Labels: []labelpb.ZLabel{
							{Name: store.CompatibilityTypeLabelName, Value: "store"},
						},
					},
				}
			},
		},
	})
	testutil.Ok(t, err)
	defer stores2.Close()

	discoveredStoreAddr = append(discoveredStoreAddr, stores2.StoreAddresses()...)

	// New stores should be loaded.
	storeSet.Update(context.Background())
	testutil.Equals(t, 2+len(stores2.srvs), len(storeSet.stores))

	// Check stats.
	expected = newStoreAPIStats()
	expected[component.UnknownStoreAPI] = map[string]int{
		"{l1=\"no-store-type\", l2=\"v3\"}": 1,
	}
	expected[component.Query] = map[string]int{
		"{l1=\"v2\", l2=\"v3\"}":             1,
		"{l1=\"v2\", l2=\"v3\"},{l3=\"v4\"}": 2,
	}
	expected[component.Rule] = map[string]int{
		"{l1=\"v2\", l2=\"v3\"}": 2,
	}
	expected[component.Sidecar] = map[string]int{
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredStoreAddr[1]): 1,
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredInfoAddr[1]):  1,
		"{l1=\"v2\", l2=\"v3\"}": 2,
	}
	expected[component.Store] = map[string]int{
		"":                                   2,
		"{l1=\"v2\", l2=\"v3\"},{l3=\"v4\"}": 3,
	}
	testutil.Equals(t, expected, storeSet.storesMetric.storeNodes)

	// Check statuses.
	testutil.Equals(t, 4+len(stores2.srvs), len(storeSet.storeStatuses))
}

func TestStoreSet_Update_NoneAvailable(t *testing.T) {
	st, err := startTestStores([]testStoreMeta{
		{
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
		{
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
	})
	testutil.Ok(t, err)
	defer st.Close()

	infoSrvs, err := startInfoSrvs([]testInfoMeta{
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "addr", Value: addr},
						},
					},
				}
			},
		},
		{
			storeType: component.Sidecar,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{Name: "addr", Value: addr},
						},
					},
				}
			},
		},
	})
	testutil.Ok(t, err)
	defer infoSrvs.Close()

	initialStoreAddr := st.StoreAddresses()
	st.CloseOne(initialStoreAddr[0])
	st.CloseOne(initialStoreAddr[1])

	initialInfoAddr := infoSrvs.InfoAddresses()
	infoSrvs.CloseOne(initialInfoAddr[0])
	infoSrvs.CloseOne(initialInfoAddr[1])

	storeSet := NewStoreSet(nil, nil,
		func() (specs []StoreSpec) {
			for _, addr := range initialStoreAddr {
				specs = append(specs, NewGRPCStoreSpec(addr, false))
			}
			return specs
		},
		func() (specs []RuleSpec) { return nil },
		func() (specs []TargetSpec) { return nil },
		func() (specs []MetadataSpec) { return nil },
		func() (specs []ExemplarSpec) { return nil },
		func() (specs []InfoSpec) {
			for _, addr := range initialInfoAddr {
				specs = append(specs, NewGRPCStoreSpec(addr, false))
			}
			return specs
		},
		testGRPCOpts, time.Minute)
	storeSet.gRPCInfoCallTimeout = 2 * time.Second

	// Should not matter how many of these we run.
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())
	testutil.Equals(t, 0, len(storeSet.stores), "none of services should respond just fine, so we expect no client to be ready.")

	// Leak test will ensure that we don't keep client connection around.

	expected := newStoreAPIStats()
	testutil.Equals(t, expected, storeSet.storesMetric.storeNodes)
}

// TestQuerierStrict tests what happens when the strict mode is enabled/disabled.
func TestQuerierStrict(t *testing.T) {
	st, err := startTestStores([]testStoreMeta{
		{
			minTime: 12345,
			maxTime: 54321,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
		{
			minTime: 66666,
			maxTime: 77777,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
		},
		// Slow store.
		{
			minTime: 65644,
			maxTime: 77777,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{
					{
						Labels: []labelpb.ZLabel{
							{
								Name:  "addr",
								Value: addr,
							},
						},
					},
				}
			},
			storeType: component.Sidecar,
			infoDelay: 2 * time.Second,
		},
	})

	testutil.Ok(t, err)
	defer st.Close()

	staticStoreAddr := st.StoreAddresses()[0]
	slowStaticStoreAddr := st.StoreAddresses()[2]
	storeSet := NewStoreSet(nil, nil, func() (specs []StoreSpec) {
		return []StoreSpec{
			NewGRPCStoreSpec(st.StoreAddresses()[0], true),
			NewGRPCStoreSpec(st.StoreAddresses()[1], false),
			NewGRPCStoreSpec(st.StoreAddresses()[2], true),
		}
	}, func() []RuleSpec {
		return nil
	}, func() []TargetSpec {
		return nil
	}, func() (specs []MetadataSpec) {
		return nil
	}, func() []ExemplarSpec {
		return nil
	}, func() []InfoSpec {
		return nil
	}, testGRPCOpts, time.Minute)
	defer storeSet.Close()
	storeSet.gRPCInfoCallTimeout = 1 * time.Second

	// Initial update.
	storeSet.Update(context.Background())
	testutil.Equals(t, 3, len(storeSet.stores), "three clients must be available for running store nodes")

	// The store has not responded to the info call and is assumed to cover everything.
	curMin, curMax := storeSet.stores[slowStaticStoreAddr].minTime, storeSet.stores[slowStaticStoreAddr].maxTime
	testutil.Assert(t, storeSet.stores[slowStaticStoreAddr].cc.GetState().String() != "SHUTDOWN", "slow store's connection should not be closed")
	testutil.Equals(t, int64(0), curMin)
	testutil.Equals(t, int64(math.MaxInt64), curMax)

	// The store is statically defined + strict mode is enabled
	// so its client + information must be retained.
	curMin, curMax = storeSet.stores[staticStoreAddr].minTime, storeSet.stores[staticStoreAddr].maxTime
	testutil.Equals(t, int64(12345), curMin, "got incorrect minimum time")
	testutil.Equals(t, int64(54321), curMax, "got incorrect minimum time")

	// Successfully retrieve the information and observe minTime/maxTime updating.
	storeSet.gRPCInfoCallTimeout = 3 * time.Second
	storeSet.Update(context.Background())
	updatedCurMin, updatedCurMax := storeSet.stores[slowStaticStoreAddr].minTime, storeSet.stores[slowStaticStoreAddr].maxTime
	testutil.Equals(t, int64(65644), updatedCurMin)
	testutil.Equals(t, int64(77777), updatedCurMax)
	storeSet.gRPCInfoCallTimeout = 1 * time.Second

	// Turn off the stores.
	st.Close()

	// Update again many times. Should not matter WRT the static one.
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())
	storeSet.Update(context.Background())

	// Check that the information is the same.
	testutil.Equals(t, 2, len(storeSet.stores), "two static clients must remain available")
	testutil.Equals(t, curMin, storeSet.stores[staticStoreAddr].minTime, "minimum time reported by the store node is different")
	testutil.Equals(t, curMax, storeSet.stores[staticStoreAddr].maxTime, "minimum time reported by the store node is different")
	testutil.NotOk(t, storeSet.storeStatuses[staticStoreAddr].LastError.originalErr)

	testutil.Equals(t, updatedCurMin, storeSet.stores[slowStaticStoreAddr].minTime, "minimum time reported by the store node is different")
	testutil.Equals(t, updatedCurMax, storeSet.stores[slowStaticStoreAddr].maxTime, "minimum time reported by the store node is different")
}

func TestStoreSet_Update_Rules(t *testing.T) {
	stores, err := startTestStores([]testStoreMeta{
		{
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
			storeType: component.Sidecar,
		},
		{
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
			storeType: component.Rule,
		},
	})
	testutil.Ok(t, err)
	defer stores.Close()

	for _, tc := range []struct {
		name           string
		storeSpecs     func() []StoreSpec
		ruleSpecs      func() []RuleSpec
		exemplarSpecs  func() []ExemplarSpec
		expectedStores int
		expectedRules  int
	}{
		{
			name: "stores, no rules",
			storeSpecs: func() []StoreSpec {
				return []StoreSpec{
					NewGRPCStoreSpec(stores.orderAddrs[0], false),
					NewGRPCStoreSpec(stores.orderAddrs[1], false),
				}
			},
			expectedStores: 2,
			expectedRules:  0,
		},
		{
			name: "rules, no stores",
			ruleSpecs: func() []RuleSpec {
				return []RuleSpec{
					NewGRPCStoreSpec(stores.orderAddrs[0], false),
				}
			},
			expectedStores: 0,
			expectedRules:  0,
		},
		{
			name: "one store, different rule",
			storeSpecs: func() []StoreSpec {
				return []StoreSpec{
					NewGRPCStoreSpec(stores.orderAddrs[0], false),
				}
			},
			ruleSpecs: func() []RuleSpec {
				return []RuleSpec{
					NewGRPCStoreSpec(stores.orderAddrs[1], false),
				}
			},
			expectedStores: 1,
			expectedRules:  0,
		},
		{
			name: "two stores, one rule",
			storeSpecs: func() []StoreSpec {
				return []StoreSpec{
					NewGRPCStoreSpec(stores.orderAddrs[0], false),
					NewGRPCStoreSpec(stores.orderAddrs[1], false),
				}
			},
			ruleSpecs: func() []RuleSpec {
				return []RuleSpec{
					NewGRPCStoreSpec(stores.orderAddrs[0], false),
				}
			},
			expectedStores: 2,
			expectedRules:  1,
		},
		{
			name: "two stores, two rules",
			storeSpecs: func() []StoreSpec {
				return []StoreSpec{
					NewGRPCStoreSpec(stores.orderAddrs[0], false),
					NewGRPCStoreSpec(stores.orderAddrs[1], false),
				}
			},
			ruleSpecs: func() []RuleSpec {
				return []RuleSpec{
					NewGRPCStoreSpec(stores.orderAddrs[0], false),
					NewGRPCStoreSpec(stores.orderAddrs[1], false),
				}
			},
			exemplarSpecs: func() []ExemplarSpec {
				return []ExemplarSpec{
					NewGRPCStoreSpec(stores.orderAddrs[0], false),
					NewGRPCStoreSpec(stores.orderAddrs[1], false),
				}
			},
			expectedStores: 2,
			expectedRules:  2,
		},
	} {
		storeSet := NewStoreSet(nil, nil,
			tc.storeSpecs,
			tc.ruleSpecs,
			func() []TargetSpec { return nil },
			func() []MetadataSpec { return nil },
			tc.exemplarSpecs,
			func() []InfoSpec { return nil },
			testGRPCOpts, time.Minute)

		t.Run(tc.name, func(t *testing.T) {
			defer storeSet.Close()
			storeSet.Update(context.Background())
			testutil.Equals(t, tc.expectedStores, len(storeSet.stores))

			gotRules := 0
			for _, ref := range storeSet.stores {
				if ref.HasRulesAPI() {
					gotRules += 1
				}
			}

			testutil.Equals(t, tc.expectedRules, gotRules)
		})
	}
}

func TestStoreSet_Rules_Discovery(t *testing.T) {
	stores, err := startTestStores([]testStoreMeta{
		{
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
			storeType: component.Sidecar,
		},
		{
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
			storeType: component.Rule,
		},
	})
	testutil.Ok(t, err)
	defer stores.Close()

	type discoveryState struct {
		name           string
		storeSpecs     func() []StoreSpec
		ruleSpecs      func() []RuleSpec
		expectedStores int
		expectedRules  int
	}

	for _, tc := range []struct {
		states []discoveryState
		name   string
	}{
		{
			name: "StoreAPI and RulesAPI concurrent discovery",
			states: []discoveryState{
				{
					name:           "no stores",
					storeSpecs:     nil,
					ruleSpecs:      nil,
					expectedRules:  0,
					expectedStores: 0,
				},
				{
					name: "RulesAPI discovered",
					storeSpecs: func() []StoreSpec {
						return []StoreSpec{
							NewGRPCStoreSpec(stores.orderAddrs[0], false),
						}
					},
					ruleSpecs: func() []RuleSpec {
						return []RuleSpec{
							NewGRPCStoreSpec(stores.orderAddrs[0], false),
						}
					},
					expectedRules:  1,
					expectedStores: 1,
				},
			},
		},

		{
			name: "StoreAPI discovery first, eventually discovered RulesAPI",
			states: []discoveryState{
				{
					name:           "no stores",
					storeSpecs:     nil,
					ruleSpecs:      nil,
					expectedRules:  0,
					expectedStores: 0,
				},
				{
					name: "StoreAPI discovered, no RulesAPI discovered",
					storeSpecs: func() []StoreSpec {
						return []StoreSpec{
							NewGRPCStoreSpec(stores.orderAddrs[0], false),
						}
					},
					expectedStores: 1,
					expectedRules:  0,
				},
				{
					name: "RulesAPI discovered",
					storeSpecs: func() []StoreSpec {
						return []StoreSpec{
							NewGRPCStoreSpec(stores.orderAddrs[0], false),
						}
					},
					ruleSpecs: func() []RuleSpec {
						return []RuleSpec{
							NewGRPCStoreSpec(stores.orderAddrs[0], false),
						}
					},
					expectedStores: 1,
					expectedRules:  1,
				},
			},
		},

		{
			name: "RulesAPI discovery first, eventually discovered StoreAPI",
			states: []discoveryState{
				{
					name:           "no stores",
					storeSpecs:     nil,
					ruleSpecs:      nil,
					expectedRules:  0,
					expectedStores: 0,
				},
				{
					name:       "RulesAPI discovered, no StoreAPI discovered",
					storeSpecs: nil,
					ruleSpecs: func() []RuleSpec {
						return []RuleSpec{
							NewGRPCStoreSpec(stores.orderAddrs[0], false),
						}
					},
					expectedStores: 0,
					expectedRules:  0,
				},
				{
					name: "StoreAPI discovered",
					storeSpecs: func() []StoreSpec {
						return []StoreSpec{
							NewGRPCStoreSpec(stores.orderAddrs[0], false),
						}
					},
					ruleSpecs: func() []RuleSpec {
						return []RuleSpec{
							NewGRPCStoreSpec(stores.orderAddrs[0], false),
						}
					},
					expectedStores: 1,
					expectedRules:  1,
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			currentState := 0

			storeSet := NewStoreSet(nil, nil,
				func() []StoreSpec {
					if tc.states[currentState].storeSpecs == nil {
						return nil
					}

					return tc.states[currentState].storeSpecs()
				},
				func() []RuleSpec {
					if tc.states[currentState].ruleSpecs == nil {
						return nil
					}

					return tc.states[currentState].ruleSpecs()
				},
				func() []TargetSpec { return nil },
				func() []MetadataSpec {
					return nil
				},
				func() []ExemplarSpec { return nil },
				func() []InfoSpec {
					return nil
				},
				testGRPCOpts, time.Minute)

			defer storeSet.Close()

			for {
				storeSet.Update(context.Background())
				testutil.Equals(
					t,
					tc.states[currentState].expectedStores,
					len(storeSet.stores),
					"unexepected discovered stores in state %q",
					tc.states[currentState].name,
				)

				gotRules := 0
				for _, ref := range storeSet.stores {
					if ref.HasRulesAPI() {
						gotRules += 1
					}
				}
				testutil.Equals(
					t,
					tc.states[currentState].expectedRules,
					gotRules,
					"unexpected discovered rules in state %q",
					tc.states[currentState].name,
				)

				currentState = currentState + 1
				if len(tc.states) == currentState {
					break
				}
			}
		})
	}
}

type errThatMarshalsToEmptyDict struct {
	msg string
}

// MarshalJSON marshals the error and returns and empty dict, not the error string.
func (e *errThatMarshalsToEmptyDict) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{})
}

// Error returns the original, underlying string.
func (e *errThatMarshalsToEmptyDict) Error() string {
	return e.msg
}

// Test highlights that without wrapping the error, it is marshaled to empty dict {}, not its message.
func TestStringError(t *testing.T) {
	dictErr := &errThatMarshalsToEmptyDict{msg: "Error message"}
	stringErr := &stringError{originalErr: dictErr}

	storestatusMock := map[string]error{}
	storestatusMock["dictErr"] = dictErr
	storestatusMock["stringErr"] = stringErr

	b, err := json.Marshal(storestatusMock)

	testutil.Ok(t, err)
	testutil.Equals(t, []byte(`{"dictErr":{},"stringErr":"Error message"}`), b, "expected to get proper results")
}

// Errors that usually marshal to empty dict should return the original error string.
func TestUpdateStoreStateLastError(t *testing.T) {
	tcs := []struct {
		InputError      error
		ExpectedLastErr string
	}{
		{errors.New("normal_err"), `"normal_err"`},
		{nil, `null`},
		{&errThatMarshalsToEmptyDict{"the error message"}, `"the error message"`},
	}

	for _, tc := range tcs {
		mockStoreSet := &StoreSet{
			storeStatuses: map[string]*StoreStatus{},
		}
		mockStoreRef := &storeRef{
			addr: "mockedStore",
		}

		mockStoreSet.updateStoreStatus(mockStoreRef, tc.InputError)

		b, err := json.Marshal(mockStoreSet.storeStatuses["mockedStore"].LastError)
		testutil.Ok(t, err)
		testutil.Equals(t, tc.ExpectedLastErr, string(b))
	}
}

func TestUpdateStoreStateForgetsPreviousErrors(t *testing.T) {
	mockStoreSet := &StoreSet{
		storeStatuses: map[string]*StoreStatus{},
	}
	mockStoreRef := &storeRef{
		addr: "mockedStore",
	}

	mockStoreSet.updateStoreStatus(mockStoreRef, errors.New("test err"))

	b, err := json.Marshal(mockStoreSet.storeStatuses["mockedStore"].LastError)
	testutil.Ok(t, err)
	testutil.Equals(t, `"test err"`, string(b))

	// updating status without and error should clear the previous one.
	mockStoreSet.updateStoreStatus(mockStoreRef, nil)

	b, err = json.Marshal(mockStoreSet.storeStatuses["mockedStore"].LastError)
	testutil.Ok(t, err)
	testutil.Equals(t, `null`, string(b))
}
