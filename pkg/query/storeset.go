// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

const (
	unhealthyStoreMessage = "removing store because it's unhealthy or does not exist"
)

type StoreSpec interface {
	// Addr returns StoreAPI Address for the store spec. It is used as ID for store.
	Addr() string
	// Metadata returns current labels, store type and min, max ranges for store.
	// It can change for every call for this method.
	// If metadata call fails we assume that store is no longer accessible and we should not use it.
	// NOTE: It is implementation responsibility to retry until context timeout, but a caller responsibility to manage
	// given store connection.
	Metadata(ctx context.Context, client storepb.StoreClient) (labelSets []storepb.LabelSet, mint int64, maxt int64, storeType component.StoreAPI, err error)
}

type StoreStatus struct {
	Name      string
	LastCheck time.Time
	LastError error
	LabelSets []storepb.LabelSet
	StoreType component.StoreAPI
	MinTime   int64
	MaxTime   int64
}

type grpcStoreSpec struct {
	addr string
}

// NewGRPCStoreSpec creates store pure gRPC spec.
// It uses Info gRPC call to get Metadata.
func NewGRPCStoreSpec(addr string) StoreSpec {
	return &grpcStoreSpec{addr: addr}
}

func (s *grpcStoreSpec) Addr() string {
	// API addr should not change between state changes.
	return s.addr
}

// Metadata method for gRPC store API tries to reach host Info method until context timeout. If we are unable to get metadata after
// that time, we assume that the host is unhealthy and return error.
func (s *grpcStoreSpec) Metadata(ctx context.Context, client storepb.StoreClient) (labelSets []storepb.LabelSet, mint int64, maxt int64, storeType component.StoreAPI, err error) {
	resp, err := client.Info(ctx, &storepb.InfoRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return nil, 0, 0, nil, errors.Wrapf(err, "fetching store info from %s", s.addr)
	}
	if len(resp.LabelSets) == 0 && len(resp.Labels) > 0 {
		resp.LabelSets = []storepb.LabelSet{{Labels: resp.Labels}}
	}

	return resp.LabelSets, resp.MinTime, resp.MaxTime, component.FromProto(resp.StoreType), nil
}

// storeSetNodeCollector is metric collector for Guge indicated number of available storeAPIs for Querier.
// Collector is requires as we want atomic updates for all 'thanos_store_nodes_grpc_connections' series.
type storeSetNodeCollector struct {
	mtx             sync.Mutex
	storeNodes      map[component.StoreAPI]map[string]int
	storePerExtLset map[string]int

	connectionsDesc *prometheus.Desc
	nodeInfoDesc    *prometheus.Desc
}

func newStoreSetNodeCollector(configInstance string) *storeSetNodeCollector {
	if configInstance == "" {
		configInstance = "default"
	}

	return &storeSetNodeCollector{
		storeNodes: map[component.StoreAPI]map[string]int{},
		connectionsDesc: prometheus.NewDesc(
			"thanos_store_nodes_grpc_connections",
			"Number of gRPC connection to Store APIs. Opened connection means healthy store APIs available for Querier.",
			[]string{"external_labels", "store_type"}, map[string]string{"config_name": configInstance},
		),
		// TODO(bwplotka): Obsolete; Replaced by thanos_store_nodes_grpc_connections.
		// Remove in next minor release.
		nodeInfoDesc: prometheus.NewDesc(
			"thanos_store_node_info",
			"Deprecated, use thanos_store_nodes_grpc_connections instead.",
			[]string{"external_labels"}, map[string]string{"config_name": configInstance},
		),
	}
}

func (c *storeSetNodeCollector) Update(nodes map[component.StoreAPI]map[string]int) {
	storeNodes := make(map[component.StoreAPI]map[string]int, len(nodes))
	storePerExtLset := map[string]int{}

	for k, v := range nodes {
		storeNodes[k] = make(map[string]int, len(v))
		for kk, vv := range v {
			storePerExtLset[kk] += vv
			storeNodes[k][kk] = vv
		}
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.storeNodes = storeNodes
	c.storePerExtLset = storePerExtLset
}

func (c *storeSetNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.connectionsDesc
	ch <- c.nodeInfoDesc
}

func (c *storeSetNodeCollector) Collect(ch chan<- prometheus.Metric) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for storeType, occurrencesPerExtLset := range c.storeNodes {
		for externalLabels, occurrences := range occurrencesPerExtLset {
			var storeTypeStr string
			if storeType != nil {
				storeTypeStr = storeType.String()
			}
			ch <- prometheus.MustNewConstMetric(c.connectionsDesc, prometheus.GaugeValue, float64(occurrences), externalLabels, storeTypeStr)
		}
	}
	for externalLabels, occur := range c.storePerExtLset {
		ch <- prometheus.MustNewConstMetric(c.nodeInfoDesc, prometheus.GaugeValue, float64(occur), externalLabels)
	}
}

// StoreSet maintains a set of active stores. It is backed up by Store Specifications that are dynamically fetched on
// every Update() call.
type StoreSet struct {
	logger log.Logger

	// Store specifications can change dynamically. If some store is missing from the list, we assuming it is no longer
	// accessible and we close gRPC client for it.
	storeSpecs          func() []StoreSpec
	dialOpts            []grpc.DialOption
	gRPCInfoCallTimeout time.Duration

	updateMtx         sync.Mutex
	storesMtx         sync.RWMutex
	storesStatusesMtx sync.RWMutex

	// Main map of stores currently used for fanout.
	stores       map[string]*storeRef
	storesMetric *storeSetNodeCollector

	// Map of statuses used only by UI.
	storeStatuses         map[string]*StoreStatus
	unhealthyStoreTimeout time.Duration
}

// NewStoreSet returns a new set of stores from cluster peers and statically configured ones.
func NewStoreSet(
	logger log.Logger,
	configInstance string,
	reg *prometheus.Registry,
	storeSpecs func() []StoreSpec,
	dialOpts []grpc.DialOption,
	unhealthyStoreTimeout time.Duration,
) *StoreSet {
	storesMetric := newStoreSetNodeCollector(configInstance)
	if reg != nil {
		reg.MustRegister(storesMetric)
	}

	if logger == nil {
		logger = log.NewNopLogger()
	}
	if storeSpecs == nil {
		storeSpecs = func() []StoreSpec { return nil }
	}

	ss := &StoreSet{
		logger:                log.With(logger, "component", "storeset"),
		storeSpecs:            storeSpecs,
		dialOpts:              dialOpts,
		storesMetric:          storesMetric,
		gRPCInfoCallTimeout:   5 * time.Second,
		stores:                make(map[string]*storeRef),
		storeStatuses:         make(map[string]*StoreStatus),
		unhealthyStoreTimeout: unhealthyStoreTimeout,
	}
	return ss
}

type storeRef struct {
	storepb.StoreClient

	mtx  sync.RWMutex
	cc   *grpc.ClientConn
	addr string

	// Meta (can change during runtime).
	labelSets []storepb.LabelSet
	storeType component.StoreAPI
	minTime   int64
	maxTime   int64

	logger log.Logger
}

func (s *storeRef) Update(labelSets []storepb.LabelSet, minTime int64, maxTime int64, storeType component.StoreAPI) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.storeType = storeType
	s.labelSets = labelSets
	s.minTime = minTime
	s.maxTime = maxTime
}

func (s *storeRef) StoreType() component.StoreAPI {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.storeType
}

func (s *storeRef) LabelSets() []storepb.LabelSet {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	labelSet := make([]storepb.LabelSet, 0, len(s.labelSets))
	for _, ls := range s.labelSets {
		if len(ls.Labels) == 0 {
			continue
		}
		// Compatibility label for Queriers pre 0.8.1. Filter it out now.
		if ls.Labels[0].Name == store.CompatibilityTypeLabelName {
			continue
		}

		lbls := make([]storepb.Label, 0, len(ls.Labels))
		for _, l := range ls.Labels {
			lbls = append(lbls, storepb.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
		labelSet = append(labelSet, storepb.LabelSet{Labels: lbls})
	}
	return labelSet
}

func (s *storeRef) LabelSetsString() string {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	labelSet := make([]string, 0, len(s.labelSets))
	for _, ls := range s.labelSets {
		if len(ls.Labels) == 0 {
			continue
		}
		// Compatibility label for Queriers pre 0.8.1. Filter it out now.
		if ls.Labels[0].Name == store.CompatibilityTypeLabelName {
			continue
		}

		lbls := labels.Labels(make([]labels.Label, 0, len(ls.Labels)))
		for _, l := range ls.Labels {
			lbls = append(lbls, labels.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
		sort.Sort(lbls)
		labelSet = append(labelSet, lbls.String())
	}
	sort.Strings(labelSet)
	return strings.Join(labelSet, ",")
}

func (s *storeRef) TimeRange() (mint int64, maxt int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.minTime, s.maxTime
}

func (s *storeRef) String() string {
	mint, maxt := s.TimeRange()
	return fmt.Sprintf("Addr: %s LabelSets: %v Mint: %d Maxt: %d", s.addr, storepb.LabelSetsToString(s.LabelSets()), mint, maxt)
}

func (s *storeRef) Addr() string {
	return s.addr
}

func (s *storeRef) Close() {
	runutil.CloseWithLogOnErr(s.logger, s.cc, fmt.Sprintf("store %v connection close", s.addr))
}

func newStoreAPIStats() map[component.StoreAPI]map[string]int {
	nodes := make(map[component.StoreAPI]map[string]int, len(storepb.StoreType_name))
	for i := range storepb.StoreType_name {
		nodes[component.FromProto(storepb.StoreType(i))] = map[string]int{}
	}
	return nodes
}

// Update updates the store set. It fetches current list of store specs from function and updates the fresh metadata
// from all stores.
func (s *StoreSet) Update(ctx context.Context) {
	s.updateMtx.Lock()
	defer s.updateMtx.Unlock()

	s.storesMtx.RLock()
	stores := make(map[string]*storeRef, len(s.stores))
	for addr, st := range s.stores {
		stores[addr] = st
	}
	s.storesMtx.RUnlock()

	level.Debug(s.logger).Log("msg", "starting updating storeAPIs", "cachedStores", len(stores))

	healthyStores := s.getHealthyStores(ctx, stores)
	level.Debug(s.logger).Log("msg", "checked requested storeAPIs", "healthyStores", len(healthyStores), "cachedStores", len(stores))

	stats := newStoreAPIStats()

	// Close stores that where not healthy this time (are not in healthy stores map).
	for addr, st := range stores {
		if _, ok := healthyStores[addr]; ok {
			stats[st.StoreType()][st.LabelSetsString()]++
			continue
		}

		st.Close()
		delete(stores, addr)
		s.updateStoreStatus(st, errors.New(unhealthyStoreMessage))
		level.Info(s.logger).Log("msg", unhealthyStoreMessage, "address", addr, "extLset", st.LabelSetsString())
	}

	// Add stores that are not yet in stores.
	for addr, st := range healthyStores {
		if _, ok := stores[addr]; ok {
			continue
		}

		extLset := st.LabelSetsString()

		// All producers should have unique external labels. While this does not check only StoreAPIs connected to
		// this querier this allows to notify early user about misconfiguration. Warn only. This is also detectable from metric.
		if st.StoreType() != nil &&
			(st.StoreType() == component.Sidecar || st.StoreType() == component.Rule) &&
			stats[component.Sidecar][extLset]+stats[component.Rule][extLset] > 0 {

			level.Warn(s.logger).Log("msg", "found duplicate storeAPI producer (sidecar or ruler). This is not advices as it will malform data in in the same bucket",
				"address", addr, "extLset", extLset, "duplicates", fmt.Sprintf("%v", stats[component.Sidecar][extLset]+stats[component.Rule][extLset]+1))
		}
		stats[st.StoreType()][st.LabelSetsString()]++

		stores[addr] = st
		s.updateStoreStatus(st, nil)
		level.Info(s.logger).Log("msg", "adding new storeAPI to query storeset", "address", addr, "extLset", extLset)
	}

	s.storesMetric.Update(stats)
	s.storesMtx.Lock()
	s.stores = stores
	s.storesMtx.Unlock()

	s.cleanUpStoreStatuses(stores)
}

func (s *StoreSet) getHealthyStores(ctx context.Context, stores map[string]*storeRef) map[string]*storeRef {
	var (
		unique        = make(map[string]struct{})
		healthyStores = make(map[string]*storeRef, len(stores))
		mtx           sync.Mutex
		wg            sync.WaitGroup
	)

	// Gather healthy stores map concurrently. Build new store if does not exist already.
	for _, storeSpec := range s.storeSpecs() {
		if _, ok := unique[storeSpec.Addr()]; ok {
			level.Warn(s.logger).Log("msg", "duplicated address in store nodes", "address", storeSpec.Addr())
			continue
		}
		unique[storeSpec.Addr()] = struct{}{}

		wg.Add(1)
		go func(spec StoreSpec) {
			defer wg.Done()

			addr := spec.Addr()

			ctx, cancel := context.WithTimeout(ctx, s.gRPCInfoCallTimeout)
			defer cancel()

			st, seenAlready := stores[addr]
			if !seenAlready {
				// New store or was unhealthy and was removed in the past - create new one.
				conn, err := grpc.DialContext(ctx, addr, s.dialOpts...)
				if err != nil {
					s.updateStoreStatus(&storeRef{addr: addr}, err)
					level.Warn(s.logger).Log("msg", "update of store node failed", "err", errors.Wrap(err, "dialing connection"), "address", addr)
					return
				}
				st = &storeRef{StoreClient: storepb.NewStoreClient(conn), cc: conn, addr: addr, logger: s.logger}
			}

			// Check existing or new store. Is it healthy? What are current metadata?
			labelSets, minTime, maxTime, storeType, err := spec.Metadata(ctx, st.StoreClient)
			if err != nil {
				if !seenAlready {
					// Close only if new. Unhealthy `s.stores` will be closed later on.
					st.Close()
				}
				s.updateStoreStatus(st, err)
				level.Warn(s.logger).Log("msg", "update of store node failed", "err", errors.Wrap(err, "getting metadata"), "address", addr)
				return
			}
			s.updateStoreStatus(st, nil)
			st.Update(labelSets, minTime, maxTime, storeType)

			mtx.Lock()
			defer mtx.Unlock()

			healthyStores[addr] = st
		}(storeSpec)
	}
	wg.Wait()

	return healthyStores
}

func (s *StoreSet) updateStoreStatus(store *storeRef, err error) {
	s.storesStatusesMtx.Lock()
	defer s.storesStatusesMtx.Unlock()

	status := StoreStatus{Name: store.addr}
	prev, ok := s.storeStatuses[store.addr]
	if ok {
		status = *prev
	}

	status.LastError = err
	status.LastCheck = time.Now()

	if err == nil {

		mint, maxt := store.TimeRange()
		status.LabelSets = store.LabelSets()
		status.StoreType = store.StoreType()
		status.MinTime = mint
		status.MaxTime = maxt
	}

	s.storeStatuses[store.addr] = &status
}

func (s *StoreSet) GetStoreStatus() []StoreStatus {
	s.storesStatusesMtx.RLock()
	defer s.storesStatusesMtx.RUnlock()

	statuses := make([]StoreStatus, 0, len(s.storeStatuses))
	for _, v := range s.storeStatuses {
		statuses = append(statuses, *v)
	}

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Name < statuses[j].Name
	})
	return statuses
}

// Get returns a list of all active stores.
func (s *StoreSet) Get() []store.Client {
	s.storesMtx.RLock()
	defer s.storesMtx.RUnlock()

	stores := make([]store.Client, 0, len(s.stores))
	for _, st := range s.stores {
		stores = append(stores, st)
	}
	return stores
}

func (s *StoreSet) Close() {
	s.storesMtx.Lock()
	defer s.storesMtx.Unlock()

	for _, st := range s.stores {
		st.Close()
	}
	s.stores = map[string]*storeRef{}
}

func (s *StoreSet) cleanUpStoreStatuses(stores map[string]*storeRef) {
	s.storesStatusesMtx.Lock()
	defer s.storesStatusesMtx.Unlock()

	now := time.Now()
	for addr, status := range s.storeStatuses {
		if _, ok := stores[addr]; ok {
			continue
		}

		if now.Sub(status.LastCheck) >= s.unhealthyStoreTimeout {
			delete(s.storeStatuses, addr)
		}
	}
}
