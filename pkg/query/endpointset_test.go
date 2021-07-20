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

	"google.golang.org/grpc"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

var testGRPCOpts = []grpc.DialOption{
	grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	grpc.WithInsecure(),
}

var (
	sidecarInfo = &infopb.InfoResponse{
		ComponentType: component.Sidecar.String(),
		Store: &infopb.StoreInfo{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
		Exemplars: &infopb.ExemplarsInfo{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
		Rules:          &infopb.RulesInfo{},
		MetricMetadata: &infopb.MetricMetadataInfo{},
		Targets:        &infopb.TargetsInfo{},
	}
	queryInfo = &infopb.InfoResponse{
		ComponentType: component.Query.String(),
		Store: &infopb.StoreInfo{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
		Exemplars: &infopb.ExemplarsInfo{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
		Rules:          &infopb.RulesInfo{},
		MetricMetadata: &infopb.MetricMetadataInfo{},
		Targets:        &infopb.TargetsInfo{},
	}
	ruleInfo = &infopb.InfoResponse{
		ComponentType: component.Rule.String(),
		Rules:         &infopb.RulesInfo{},
	}
	storeGWInfo = &infopb.InfoResponse{
		ComponentType: component.Store.String(),
		Store: &infopb.StoreInfo{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
	}
	receiveInfo = &infopb.InfoResponse{
		ComponentType: component.Receive.String(),
		Store: &infopb.StoreInfo{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
		Exemplars: &infopb.ExemplarsInfo{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		},
	}
)

type mockedEndpoint struct {
	infoDelay time.Duration
	info      infopb.InfoResponse
}

func (c *mockedEndpoint) Info(ctx context.Context, r *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	if c.infoDelay > 0 {
		time.Sleep(c.infoDelay)
	}

	return &c.info, nil
}

type APIs struct {
	store          bool
	metricMetadata bool
	rules          bool
	target         bool
	exemplars      bool
}

type testEndpointMeta struct {
	*infopb.InfoResponse
	extlsetFn func(addr string) []labelpb.ZLabelSet
	infoDelay time.Duration
}

type testEndpoints struct {
	srvs        map[string]*grpc.Server
	orderAddrs  []string
	exposedAPIs map[string]*APIs
}

func startTestEndpoints(testEndpointMeta []testEndpointMeta) (*testEndpoints, error) {
	e := &testEndpoints{
		srvs:        map[string]*grpc.Server{},
		exposedAPIs: map[string]*APIs{},
	}

	for _, meta := range testEndpointMeta {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			// Close so far started servers.
			e.Close()
			return nil, err
		}

		srv := grpc.NewServer()
		addr := listener.Addr().String()

		endpointSrv := &mockedEndpoint{
			info: infopb.InfoResponse{
				LabelSets:      meta.extlsetFn(listener.Addr().String()),
				Store:          meta.Store,
				MetricMetadata: meta.MetricMetadata,
				Rules:          meta.Rules,
				Targets:        meta.Targets,
				Exemplars:      meta.Exemplars,
				ComponentType:  meta.ComponentType,
			},
			infoDelay: meta.infoDelay,
		}
		infopb.RegisterInfoServer(srv, endpointSrv)
		go func() {
			_ = srv.Serve(listener)
		}()

		e.exposedAPIs[addr] = exposedAPIs(meta.ComponentType)
		e.srvs[addr] = srv
		e.orderAddrs = append(e.orderAddrs, listener.Addr().String())
	}

	return e, nil
}

func (e *testEndpoints) EndpointAddresses() []string {
	var endpoints []string
	endpoints = append(endpoints, e.orderAddrs...)
	return endpoints
}

func (e *testEndpoints) Close() {
	for _, srv := range e.srvs {
		srv.Stop()
	}
	e.srvs = nil
}

func (e *testEndpoints) CloseOne(addr string) {
	srv, ok := e.srvs[addr]
	if !ok {
		return
	}

	srv.Stop()
	delete(e.srvs, addr)
}

func TestEndpointSet_Update(t *testing.T) {

	endpoints, err := startTestEndpoints([]testEndpointMeta{
		{
			InfoResponse: sidecarInfo,
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
			InfoResponse: sidecarInfo,
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
			InfoResponse: queryInfo,
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
	})
	testutil.Ok(t, err)
	defer endpoints.Close()

	discoveredEndpointAddr := endpoints.EndpointAddresses()

	// Testing if duplicates can cause weird results.
	discoveredEndpointAddr = append(discoveredEndpointAddr, discoveredEndpointAddr[0])
	endpointSet := NewEndpointSet(nil, nil,
		func() (specs []EndpointSpec) {
			for _, addr := range discoveredEndpointAddr {
				specs = append(specs, NewGRPCEndpointSpec(addr, false))
			}
			return specs
		},
		testGRPCOpts, time.Minute)
	endpointSet.gRPCInfoCallTimeout = 2 * time.Second
	defer endpointSet.Close()

	// Initial update.
	endpointSet.Update(context.Background())

	// Start with one not available.
	endpoints.CloseOne(discoveredEndpointAddr[2])

	// Should not matter how many of these we run.
	endpointSet.Update(context.Background())
	endpointSet.Update(context.Background())
	testutil.Equals(t, 2, len(endpointSet.endpoints))
	testutil.Equals(t, 3, len(endpointSet.endpointStatuses))

	for addr, e := range endpointSet.endpoints {
		testutil.Equals(t, addr, e.addr)

		lset := e.LabelSets()
		testutil.Equals(t, 2, len(lset))
		testutil.Equals(t, "addr", lset[0][0].Name)
		testutil.Equals(t, addr, lset[0][0].Value)
		testutil.Equals(t, "a", lset[1][0].Name)
		testutil.Equals(t, "b", lset[1][0].Value)
		assertRegisteredAPIs(t, endpoints.exposedAPIs[addr], e)
	}

	// Check stats.
	expected := newEndpointAPIStats()
	expected[component.Sidecar] = map[string]int{
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredEndpointAddr[0]): 1,
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredEndpointAddr[1]): 1,
	}
	testutil.Equals(t, expected, endpointSet.endpointsMetric.storeNodes)

	// Remove address from discovered and reset last check, which should ensure cleanup of status on next update.
	endpointSet.endpointStatuses[discoveredEndpointAddr[2]].LastCheck = time.Now().Add(-4 * time.Minute)
	discoveredEndpointAddr = discoveredEndpointAddr[:len(discoveredEndpointAddr)-2]
	endpointSet.Update(context.Background())
	testutil.Equals(t, 2, len(endpointSet.endpointStatuses))

	endpoints.CloseOne(discoveredEndpointAddr[0])
	delete(expected[component.Sidecar], fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredEndpointAddr[0]))

	// We expect Update to tear down store client for closed store server.
	endpointSet.Update(context.Background())
	testutil.Equals(t, 1, len(endpointSet.endpoints), "only one service should respond just fine, so we expect one client to be ready.")
	testutil.Equals(t, 2, len(endpointSet.endpointStatuses))

	addr := discoveredEndpointAddr[1]
	st, ok := endpointSet.endpoints[addr]
	testutil.Assert(t, ok, "addr exist")
	testutil.Equals(t, addr, st.addr)

	lset := st.LabelSets()
	testutil.Equals(t, 2, len(lset))
	testutil.Equals(t, "addr", lset[0][0].Name)
	testutil.Equals(t, addr, lset[0][0].Value)
	testutil.Equals(t, "a", lset[1][0].Name)
	testutil.Equals(t, "b", lset[1][0].Value)
	testutil.Equals(t, expected, endpointSet.endpointsMetric.storeNodes)

	// New big batch of endpoints.
	endpoint2, err := startTestEndpoints([]testEndpointMeta{
		{
			InfoResponse: queryInfo,
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
			InfoResponse: queryInfo,
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
			InfoResponse: sidecarInfo,
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
			InfoResponse: sidecarInfo,
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
			InfoResponse: queryInfo,
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
			InfoResponse: ruleInfo,
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
			InfoResponse: ruleInfo,
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
		// Two pre v0.8.0 store gateway nodes, they don't have ext labels set.
		{
			InfoResponse: storeGWInfo,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
		{
			InfoResponse: storeGWInfo,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
		// Regression tests against https://github.com/thanos-io/thanos/issues/1632: From v0.8.0 stores advertise labels.
		// If the object storage handled by store gateway has only one sidecar we used to hitting issue.
		{
			InfoResponse: storeGWInfo,
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
			InfoResponse: storeGWInfo,
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
			InfoResponse: storeGWInfo,
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
		{
			InfoResponse: receiveInfo,
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
		// Duplicate receiver
		{
			InfoResponse: receiveInfo,
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
	})
	testutil.Ok(t, err)
	defer endpoint2.Close()

	discoveredEndpointAddr = append(discoveredEndpointAddr, endpoint2.EndpointAddresses()...)

	// New stores should be loaded.
	endpointSet.Update(context.Background())
	testutil.Equals(t, 1+len(endpoint2.srvs), len(endpointSet.endpoints))

	// Check stats.
	expected = newEndpointAPIStats()
	expected[component.Query] = map[string]int{
		"{l1=\"v2\", l2=\"v3\"}":             1,
		"{l1=\"v2\", l2=\"v3\"},{l3=\"v4\"}": 2,
	}
	expected[component.Rule] = map[string]int{
		"{l1=\"v2\", l2=\"v3\"}": 2,
	}
	expected[component.Sidecar] = map[string]int{
		fmt.Sprintf("{a=\"b\"},{addr=\"%s\"}", discoveredEndpointAddr[1]): 1,
		"{l1=\"v2\", l2=\"v3\"}": 2,
	}
	expected[component.Store] = map[string]int{
		"":                                   2,
		"{l1=\"v2\", l2=\"v3\"},{l3=\"v4\"}": 3,
	}
	expected[component.Receive] = map[string]int{
		"{l1=\"v2\", l2=\"v3\"},{l3=\"v4\"}": 2,
	}
	testutil.Equals(t, expected, endpointSet.endpointsMetric.storeNodes)

	// Close remaining endpoint from previous batch
	endpoints.CloseOne(discoveredEndpointAddr[1])
	endpointSet.Update(context.Background())

	for addr, e := range endpointSet.endpoints {
		testutil.Equals(t, addr, e.addr)
		assertRegisteredAPIs(t, endpoint2.exposedAPIs[addr], e)
	}

	// Check statuses.
	testutil.Equals(t, 2+len(endpoint2.srvs), len(endpointSet.endpointStatuses))
}

func TestEndpointSet_Update_NoneAvailable(t *testing.T) {
	endpoints, err := startTestEndpoints([]testEndpointMeta{
		{
			InfoResponse: sidecarInfo,
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
		},
		{
			InfoResponse: sidecarInfo,
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
		},
	})
	testutil.Ok(t, err)
	defer endpoints.Close()

	initialEndpointAddr := endpoints.EndpointAddresses()
	endpoints.CloseOne(initialEndpointAddr[0])
	endpoints.CloseOne(initialEndpointAddr[1])

	endpointSet := NewEndpointSet(nil, nil,
		func() (specs []EndpointSpec) {
			for _, addr := range initialEndpointAddr {
				specs = append(specs, NewGRPCEndpointSpec(addr, false))
			}
			return specs
		},
		testGRPCOpts, time.Minute)
	endpointSet.gRPCInfoCallTimeout = 2 * time.Second

	// Should not matter how many of these we run.
	endpointSet.Update(context.Background())
	endpointSet.Update(context.Background())
	testutil.Equals(t, 0, len(endpointSet.endpoints), "none of services should respond just fine, so we expect no client to be ready.")

	// Leak test will ensure that we don't keep client connection around.
	expected := newEndpointAPIStats()
	testutil.Equals(t, expected, endpointSet.endpointsMetric.storeNodes)
}

// TestEndpoint_Update_QuerierStrict tests what happens when the strict mode is enabled/disabled.
func TestEndpoint_Update_QuerierStrict(t *testing.T) {
	endpoints, err := startTestEndpoints([]testEndpointMeta{
		{
			InfoResponse: &infopb.InfoResponse{
				ComponentType: component.Sidecar.String(),
				Store: &infopb.StoreInfo{
					MinTime: 12345,
					MaxTime: 54321,
				},
				Exemplars: &infopb.ExemplarsInfo{
					MinTime: math.MinInt64,
					MaxTime: math.MaxInt64,
				},
				Rules:          &infopb.RulesInfo{},
				MetricMetadata: &infopb.MetricMetadataInfo{},
				Targets:        &infopb.TargetsInfo{},
			},
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
		},
		{
			InfoResponse: &infopb.InfoResponse{
				ComponentType: component.Sidecar.String(),
				Store: &infopb.StoreInfo{
					MinTime: 66666,
					MaxTime: 77777,
				},
				Exemplars: &infopb.ExemplarsInfo{
					MinTime: math.MinInt64,
					MaxTime: math.MaxInt64,
				},
				Rules:          &infopb.RulesInfo{},
				MetricMetadata: &infopb.MetricMetadataInfo{},
				Targets:        &infopb.TargetsInfo{},
			},
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
		},
		// Slow store.
		{
			InfoResponse: &infopb.InfoResponse{
				ComponentType: component.Sidecar.String(),
				Store: &infopb.StoreInfo{
					MinTime: 65644,
					MaxTime: 77777,
				},
				Exemplars: &infopb.ExemplarsInfo{
					MinTime: math.MinInt64,
					MaxTime: math.MaxInt64,
				},
				Rules:          &infopb.RulesInfo{},
				MetricMetadata: &infopb.MetricMetadataInfo{},
				Targets:        &infopb.TargetsInfo{},
			},
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
			infoDelay: 2 * time.Second,
		},
	})

	testutil.Ok(t, err)
	defer endpoints.Close()

	discoveredEndpointAddr := endpoints.EndpointAddresses()

	staticEndpointAddr := discoveredEndpointAddr[0]
	slowStaticEndpointAddr := discoveredEndpointAddr[2]
	endpointSet := NewEndpointSet(nil, nil, func() (specs []EndpointSpec) {
		return []EndpointSpec{
			NewGRPCEndpointSpec(discoveredEndpointAddr[0], true),
			NewGRPCEndpointSpec(discoveredEndpointAddr[1], false),
			NewGRPCEndpointSpec(discoveredEndpointAddr[2], true),
		}
	}, testGRPCOpts, time.Minute)
	defer endpointSet.Close()
	endpointSet.gRPCInfoCallTimeout = 1 * time.Second

	// Initial update.
	endpointSet.Update(context.Background())
	testutil.Equals(t, 3, len(endpointSet.endpoints), "three clients must be available for running nodes")

	// The endpoint has not responded to the info call and is assumed to cover everything.
	curMin, curMax := endpointSet.endpoints[slowStaticEndpointAddr].metadata.Store.MinTime, endpointSet.endpoints[slowStaticEndpointAddr].metadata.Store.MaxTime
	testutil.Assert(t, endpointSet.endpoints[slowStaticEndpointAddr].cc.GetState().String() != "SHUTDOWN", "slow store's connection should not be closed")
	testutil.Equals(t, int64(MinTime), curMin)
	testutil.Equals(t, int64(MaxTime), curMax)

	// The endpoint is statically defined + strict mode is enabled
	// so its client + information must be retained.
	curMin, curMax = endpointSet.endpoints[staticEndpointAddr].metadata.Store.MinTime, endpointSet.endpoints[staticEndpointAddr].metadata.Store.MaxTime
	testutil.Equals(t, int64(12345), curMin, "got incorrect minimum time")
	testutil.Equals(t, int64(54321), curMax, "got incorrect minimum time")

	// Successfully retrieve the information and observe minTime/maxTime updating.
	endpointSet.gRPCInfoCallTimeout = 3 * time.Second
	endpointSet.Update(context.Background())
	updatedCurMin, updatedCurMax := endpointSet.endpoints[slowStaticEndpointAddr].metadata.Store.MinTime, endpointSet.endpoints[slowStaticEndpointAddr].metadata.Store.MaxTime
	testutil.Equals(t, int64(65644), updatedCurMin)
	testutil.Equals(t, int64(77777), updatedCurMax)
	endpointSet.gRPCInfoCallTimeout = 1 * time.Second

	// Turn off the endpoints.
	endpoints.Close()

	// Update again many times. Should not matter WRT the static one.
	endpointSet.Update(context.Background())
	endpointSet.Update(context.Background())
	endpointSet.Update(context.Background())

	// Check that the information is the same.
	testutil.Equals(t, 2, len(endpointSet.endpoints), "two static clients must remain available")
	testutil.Equals(t, curMin, endpointSet.endpoints[staticEndpointAddr].metadata.Store.MinTime, "minimum time reported by the store node is different")
	testutil.Equals(t, curMax, endpointSet.endpoints[staticEndpointAddr].metadata.Store.MaxTime, "minimum time reported by the store node is different")
	testutil.NotOk(t, endpointSet.endpointStatuses[staticEndpointAddr].LastError.originalErr)

	testutil.Equals(t, updatedCurMin, endpointSet.endpoints[slowStaticEndpointAddr].metadata.Store.MinTime, "minimum time reported by the store node is different")
	testutil.Equals(t, updatedCurMax, endpointSet.endpoints[slowStaticEndpointAddr].metadata.Store.MaxTime, "minimum time reported by the store node is different")
}

func TestEndpointSet_APIs_Discovery(t *testing.T) {
	endpoints, err := startTestEndpoints([]testEndpointMeta{
		{
			InfoResponse: sidecarInfo,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
		{
			InfoResponse: ruleInfo,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
		{
			InfoResponse: receiveInfo,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
		{
			InfoResponse: storeGWInfo,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
		{
			InfoResponse: queryInfo,
			extlsetFn: func(addr string) []labelpb.ZLabelSet {
				return []labelpb.ZLabelSet{}
			},
		},
	})
	testutil.Ok(t, err)
	defer endpoints.Close()

	type discoveryState struct {
		name                   string
		endpointSpec           func() []EndpointSpec
		expectedStores         int
		expectedRules          int
		expectedTarget         int
		expectedMetricMetadata int
		expectedExemplars      int
	}

	for _, tc := range []struct {
		states []discoveryState
		name   string
	}{
		{
			name: "All endpoints discovered concurrently",
			states: []discoveryState{
				{
					name:         "no endpoints",
					endpointSpec: nil,
				},
				{
					name: "Sidecar, Ruler, Querier, Receiver and StoreGW discovered",
					endpointSpec: func() []EndpointSpec {
						endpointSpec := make([]EndpointSpec, 0, len(endpoints.orderAddrs))
						for _, addr := range endpoints.orderAddrs {
							endpointSpec = append(endpointSpec, NewGRPCEndpointSpec(addr, false))
						}
						return endpointSpec
					},
					expectedStores:         4, // sidecar + querier + receiver + storeGW
					expectedRules:          3, // sidecar + querier + ruler
					expectedTarget:         2, // sidecar + querier
					expectedMetricMetadata: 2, // sidecar + querier
					expectedExemplars:      3, // sidecar + querier + receiver
				},
			},
		},
		{
			name: "Sidecar discovery first, eventually Ruler discovered and then Sidecar removed",
			states: []discoveryState{
				{
					name:         "no stores",
					endpointSpec: nil,
				},
				{
					name: "Sidecar discovered, no Ruler discovered",
					endpointSpec: func() []EndpointSpec {
						return []EndpointSpec{
							NewGRPCEndpointSpec(endpoints.orderAddrs[0], false),
						}
					},
					expectedStores:         1, // sidecar
					expectedRules:          1, // sidecar
					expectedTarget:         1, // sidecar
					expectedMetricMetadata: 1, // sidecar
					expectedExemplars:      1, // sidecar
				},
				{
					name: "Ruler discovered",
					endpointSpec: func() []EndpointSpec {
						return []EndpointSpec{
							NewGRPCEndpointSpec(endpoints.orderAddrs[0], false),
							NewGRPCEndpointSpec(endpoints.orderAddrs[1], false),
						}
					},
					expectedStores:         1, // sidecar
					expectedRules:          2, // sidecar + ruler
					expectedTarget:         1, // sidecar
					expectedMetricMetadata: 1, // sidecar
					expectedExemplars:      1, // sidecar
				},
				{
					name: "Sidecar removed",
					endpointSpec: func() []EndpointSpec {
						return []EndpointSpec{
							NewGRPCEndpointSpec(endpoints.orderAddrs[1], false),
						}
					},
					expectedRules: 1, // ruler
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			currentState := 0

			endpointSet := NewEndpointSet(nil, nil,
				func() []EndpointSpec {
					if tc.states[currentState].endpointSpec == nil {
						return nil
					}

					return tc.states[currentState].endpointSpec()
				},
				testGRPCOpts, time.Minute)

			defer endpointSet.Close()

			for {
				endpointSet.Update(context.Background())

				gotStores := 0
				gotRules := 0
				gotTarget := 0
				gotExemplars := 0
				gotMetricMetadata := 0

				for _, er := range endpointSet.endpoints {
					if er.HasStoreAPI() {
						gotStores += 1
					}
					if er.HasRulesAPI() {
						gotRules += 1
					}
					if er.HasTargetsAPI() {
						gotTarget += 1
					}
					if er.HasExemplarsAPI() {
						gotExemplars += 1
					}
					if er.HasMetricMetadataAPI() {
						gotMetricMetadata += 1
					}
				}
				testutil.Equals(
					t,
					tc.states[currentState].expectedStores,
					gotStores,
					"unexepected discovered storeAPIs in state %q",
					tc.states[currentState].name)
				testutil.Equals(
					t,
					tc.states[currentState].expectedRules,
					gotRules,
					"unexepected discovered rulesAPIs in state %q",
					tc.states[currentState].name)
				testutil.Equals(
					t,
					tc.states[currentState].expectedTarget,
					gotTarget,
					"unexepected discovered targetAPIs in state %q",
					tc.states[currentState].name,
				)
				testutil.Equals(
					t,
					tc.states[currentState].expectedMetricMetadata,
					gotMetricMetadata,
					"unexepected discovered metricMetadataAPIs in state %q",
					tc.states[currentState].name,
				)
				testutil.Equals(
					t,
					tc.states[currentState].expectedExemplars,
					gotExemplars,
					"unexepected discovered ExemplarsAPIs in state %q",
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
func TestEndpointStringError(t *testing.T) {
	dictErr := &errThatMarshalsToEmptyDict{msg: "Error message"}
	stringErr := &stringError{originalErr: dictErr}

	endpointstatusMock := map[string]error{}
	endpointstatusMock["dictErr"] = dictErr
	endpointstatusMock["stringErr"] = stringErr

	b, err := json.Marshal(endpointstatusMock)

	testutil.Ok(t, err)
	testutil.Equals(t, []byte(`{"dictErr":{},"stringErr":"Error message"}`), b, "expected to get proper results")
}

// Errors that usually marshal to empty dict should return the original error string.
func TestUpdateEndpointStateLastError(t *testing.T) {
	tcs := []struct {
		InputError      error
		ExpectedLastErr string
	}{
		{errors.New("normal_err"), `"normal_err"`},
		{nil, `null`},
		{&errThatMarshalsToEmptyDict{"the error message"}, `"the error message"`},
	}

	for _, tc := range tcs {
		mockedEndpointSet := &EndpointSet{
			endpointStatuses: map[string]*EndpointStatus{},
		}
		mockEndpointRef := &endpointRef{
			addr: "mockedStore",
			metadata: &endpointMetadata{
				&infopb.InfoResponse{},
			},
		}

		mockedEndpointSet.updateEndpointStatus(mockEndpointRef, tc.InputError)

		b, err := json.Marshal(mockedEndpointSet.endpointStatuses["mockedStore"].LastError)
		testutil.Ok(t, err)
		testutil.Equals(t, tc.ExpectedLastErr, string(b))
	}
}

func TestUpdateEndpointStateForgetsPreviousErrors(t *testing.T) {
	mockEndpointSet := &EndpointSet{
		endpointStatuses: map[string]*EndpointStatus{},
	}
	mockEndpointRef := &endpointRef{
		addr: "mockedStore",
		metadata: &endpointMetadata{
			&infopb.InfoResponse{},
		},
	}

	mockEndpointSet.updateEndpointStatus(mockEndpointRef, errors.New("test err"))

	b, err := json.Marshal(mockEndpointSet.endpointStatuses["mockedStore"].LastError)
	testutil.Ok(t, err)
	testutil.Equals(t, `"test err"`, string(b))

	// updating status without and error should clear the previous one.
	mockEndpointSet.updateEndpointStatus(mockEndpointRef, nil)

	b, err = json.Marshal(mockEndpointSet.endpointStatuses["mockedStore"].LastError)
	testutil.Ok(t, err)
	testutil.Equals(t, `null`, string(b))
}

func exposedAPIs(c string) *APIs {
	switch c {
	case component.Sidecar.String():
		return &APIs{
			store:          true,
			target:         true,
			rules:          true,
			metricMetadata: true,
			exemplars:      true,
		}
	case component.Query.String():
		return &APIs{
			store:          true,
			target:         true,
			rules:          true,
			metricMetadata: true,
			exemplars:      true,
		}
	case component.Receive.String():
		return &APIs{
			store:     true,
			exemplars: true,
		}
	case component.Rule.String():
		return &APIs{
			rules: true,
		}
	case component.Store.String():
		return &APIs{
			store: true,
		}
	}
	return &APIs{}
}

func assertRegisteredAPIs(t *testing.T, expectedAPIs *APIs, er *endpointRef) {
	testutil.Equals(t, expectedAPIs.store, er.HasStoreAPI())
	testutil.Equals(t, expectedAPIs.rules, er.HasRulesAPI())
	testutil.Equals(t, expectedAPIs.target, er.HasTargetsAPI())
	testutil.Equals(t, expectedAPIs.metricMetadata, er.HasMetricMetadataAPI())
	testutil.Equals(t, expectedAPIs.exemplars, er.HasExemplarsAPI())
}
