// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
)

const (
	unhealthyEndpointMessage = "removing endpoint because it's unhealthy or does not exist"

	// Default minimum and maximum time values used by Prometheus when they are not passed as query parameter.
	MinTime = -9223309901257974
	MaxTime = 9223309901257974
)

type EndpointSpec interface {
	// Addr returns Thanos API Address for the endpoint spec. It is used as ID for endpoint.
	Addr() string
	// Metadata returns current labels, component type and min, max ranges for store.
	// It can change for every call for this method.
	// If metadata call fails we assume that store is no longer accessible and we should not use it.
	// NOTE: It is implementation responsibility to retry until context timeout, but a caller responsibility to manage
	// given store connection.
	Metadata(ctx context.Context, client infopb.InfoClient) (*endpointMetadata, error)

	// StrictStatic returns true if the endpoint has been statically defined and it is under a strict mode.
	StrictStatic() bool
}

type grpcEndpointSpec struct {
	addr         string
	strictstatic bool
}

// NewGRPCEndpointSpec creates gRPC endpoint spec.
// It uses InfoAPI to get Metadata.
func NewGRPCEndpointSpec(addr string, strictstatic bool) EndpointSpec {
	return &grpcEndpointSpec{addr: addr, strictstatic: strictstatic}
}

// StrictStatic returns true if the endpoint has been statically defined and it is under a strict mode.
func (es *grpcEndpointSpec) StrictStatic() bool {
	return es.strictstatic
}

func (es *grpcEndpointSpec) Addr() string {
	// API address should not change between state changes.
	return es.addr
}

// Metadata method for gRPC endpoint tries to call InfoAPI exposed by Thanos components until context timeout. If we are unable to get metadata after
// that time, we assume that the host is unhealthy and return error.
func (es *grpcEndpointSpec) Metadata(ctx context.Context, client infopb.InfoClient) (metadata *endpointMetadata, err error) {
	resp, err := client.Info(ctx, &infopb.InfoRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return &endpointMetadata{&infopb.InfoResponse{
			ComponentType: component.UnknownStoreAPI.String(),
		}}, errors.Wrapf(err, "fetching info from %s", es.addr)
	}

	return &endpointMetadata{resp}, nil
}

// stringError forces the error to be a string
// when marshaled into a JSON.
type stringError struct {
	originalErr error
}

// MarshalJSON marshals the error into a string form.
func (e *stringError) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.originalErr.Error())
}

// Error returns the original underlying error.
func (e *stringError) Error() string {
	return e.originalErr.Error()
}

type EndpointStatus struct {
	Name          string              `json:"name"`
	LastCheck     time.Time           `json:"lastCheck"`
	LastError     *stringError        `json:"lastError"`
	LabelSets     []labels.Labels     `json:"labelSets"`
	ComponentType component.Component `json:"-"`
	MinTime       int64               `json:"minTime"`
	MaxTime       int64               `json:"maxTime"`
}

// storeSetNodeCollector is a metric collector reporting the number of available storeAPIs for Querier.
// A Collector is required as we want atomic updates for all 'thanos_store_nodes_grpc_connections' series.
// TODO(hitanshu-mehta) Currently,only collecting metrices of storeAPI. Make this struct generic.
type endpointSetNodeCollector struct {
	mtx             sync.Mutex
	storeNodes      map[component.Component]map[string]int
	storePerExtLset map[string]int

	connectionsDesc *prometheus.Desc
}

func newEndpointSetNodeCollector() *endpointSetNodeCollector {
	return &endpointSetNodeCollector{
		storeNodes: map[component.Component]map[string]int{},
		connectionsDesc: prometheus.NewDesc(
			"thanos_store_nodes_grpc_connections",
			"Number of gRPC connection to Store APIs. Opened connection means healthy store APIs available for Querier.",
			[]string{"external_labels", "store_type"}, nil,
		),
	}
}

func (c *endpointSetNodeCollector) Update(nodes map[component.Component]map[string]int) {
	storeNodes := make(map[component.Component]map[string]int, len(nodes))
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

func (c *endpointSetNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.connectionsDesc
}

func (c *endpointSetNodeCollector) Collect(ch chan<- prometheus.Metric) {
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
}

// EndpointSet maintains a set of active Thanos endpoints. It is backed up by Endpoint Specifications that are dynamically fetched on
// every Update() call.
type EndpointSet struct {
	logger log.Logger

	// Endpoint specifications can change dynamically. If some store is missing from the list, we assuming it is no longer
	// accessible and we close gRPC client for it.
	endpointSpec        func() []EndpointSpec
	dialOpts            []grpc.DialOption
	gRPCInfoCallTimeout time.Duration

	updateMtx            sync.Mutex
	endpointsMtx         sync.RWMutex
	endpointsStatusesMtx sync.RWMutex

	// Main map of stores currently used for fanout.
	endpoints       map[string]*endpointRef
	endpointsMetric *endpointSetNodeCollector

	// Map of statuses used only by UI.
	endpointStatuses         map[string]*EndpointStatus
	unhealthyEndpointTimeout time.Duration
}

// NewEndpointSet returns a new set of Thanos APIs.
func NewEndpointSet(
	logger log.Logger,
	reg *prometheus.Registry,
	endpointSpecs func() []EndpointSpec,
	dialOpts []grpc.DialOption,
	unhealthyStoreTimeout time.Duration,
) *EndpointSet {
	endpointsMetric := newEndpointSetNodeCollector()
	if reg != nil {
		reg.MustRegister(endpointsMetric)
	}

	if logger == nil {
		logger = log.NewNopLogger()
	}

	if endpointSpecs == nil {
		endpointSpecs = func() []EndpointSpec { return nil }
	}

	es := &EndpointSet{
		logger:                   log.With(logger, "component", "endpointset"),
		dialOpts:                 dialOpts,
		endpointsMetric:          endpointsMetric,
		gRPCInfoCallTimeout:      5 * time.Second,
		endpoints:                make(map[string]*endpointRef),
		endpointStatuses:         make(map[string]*EndpointStatus),
		unhealthyEndpointTimeout: unhealthyStoreTimeout,
		endpointSpec:             endpointSpecs,
	}
	return es
}

// Update updates the endpoint set. It fetches current list of endpoint specs from function and updates the fresh metadata
// from all endpoints. Keeps around statically defined nodes that were defined with the strict mode.
func (e *EndpointSet) Update(ctx context.Context) {
	e.updateMtx.Lock()
	defer e.updateMtx.Unlock()

	e.endpointsMtx.RLock()
	endpoints := make(map[string]*endpointRef, len(e.endpoints))
	for addr, er := range e.endpoints {
		endpoints[addr] = er
	}
	e.endpointsMtx.RUnlock()

	level.Debug(e.logger).Log("msg", "starting to update API endpoints", "cachedEndpoints", len(endpoints))

	activeEndpoints := e.getActiveEndpoints(ctx, endpoints)
	level.Debug(e.logger).Log("msg", "checked requested endpoints", "activeEndpoints", len(activeEndpoints), "cachedEndpoints", len(endpoints))

	stats := newEndpointAPIStats()

	// Close endpoints which are not active this time (are not in active endpoints map).
	for addr, er := range endpoints {
		if _, ok := activeEndpoints[addr]; ok {
			stats[er.ComponentType()][labelpb.PromLabelSetsToString(er.LabelSets())]++
			continue
		}

		er.Close()
		delete(endpoints, addr)
		e.updateEndpointStatus(er, errors.New(unhealthyEndpointMessage))
		level.Info(er.logger).Log("msg", unhealthyEndpointMessage, "address", addr, "extLset", labelpb.PromLabelSetsToString(er.LabelSets()))
	}

	// Add stores that are not yet in stores.
	for addr, er := range activeEndpoints {
		if _, ok := endpoints[addr]; ok {
			continue
		}

		extLset := labelpb.PromLabelSetsToString(er.LabelSets())

		// All producers should have unique external labels. While this does not check only StoreAPIs connected to
		// this querier this allows to notify early user about misconfiguration. Warn only. This is also detectable from metric.
		if (er.ComponentType() != nil &&
			(er.ComponentType() == component.Sidecar || er.ComponentType() == component.Rule)) &&
			stats[component.Sidecar][extLset]+stats[component.Rule][extLset] > 0 {

			level.Warn(e.logger).Log("msg", "found duplicate storeAPI producer (sidecar or ruler). This is not advices as it will malform data in in the same bucket",
				"address", addr, "extLset", extLset, "duplicates", fmt.Sprintf("%v", stats[component.Sidecar][extLset]+stats[component.Rule][extLset]+1))
		}
		stats[er.ComponentType()][extLset]++

		endpoints[addr] = er
		e.updateEndpointStatus(er, nil)

		if er.HasStoreAPI() {
			level.Info(e.logger).Log("msg", "adding new storeAPI to query endpointset", "address", addr, "extLset", extLset)
		}

		if er.HasRulesAPI() {
			level.Info(e.logger).Log("msg", "adding new rulesAPI to query endpointset", "address", addr)
		}

		if er.HasExemplarsAPI() {
			level.Info(e.logger).Log("msg", "adding new exemplarsAPI to query endpointset", "address", addr)
		}

		if er.HasTargetsAPI() {
			level.Info(e.logger).Log("msg", "adding new targetsAPI to query endpointset", "address", addr)
		}

		if er.HasMetricMetadataAPI() {
			level.Info(e.logger).Log("msg", "adding new MetricMetadataAPI to query endpointset", "address", addr)
		}
	}

	e.endpointsMetric.Update(stats)
	e.endpointsMtx.Lock()
	e.endpoints = endpoints
	e.endpointsMtx.Unlock()

	e.cleanUpStoreStatuses(endpoints)
}

// Get returns a list of all active stores.
func (e *EndpointSet) GetStoreClients() []storepb.StoreClient {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	stores := make([]storepb.StoreClient, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasStoreAPI() {
			stores = append(stores, er.clients.store)
		}
	}
	return stores
}

// GetRulesClients returns a list of all active rules clients.
func (e *EndpointSet) GetRulesClients() []rulespb.RulesClient {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	rules := make([]rulespb.RulesClient, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasRulesAPI() {
			rules = append(rules, er.clients.rule)
		}
	}
	return rules
}

// GetTargetsClients returns a list of all active targets clients.
func (e *EndpointSet) GetTargetsClients() []targetspb.TargetsClient {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	targets := make([]targetspb.TargetsClient, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasTargetsAPI() {
			targets = append(targets, er.clients.target)
		}
	}
	return targets
}

// GetMetricMetadataClients returns a list of all active metadata clients.
func (e *EndpointSet) GetMetricMetadataClients() []metadatapb.MetadataClient {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	metadataClients := make([]metadatapb.MetadataClient, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasMetricMetadataAPI() {
			metadataClients = append(metadataClients, er.clients.metricMetadata)
		}
	}
	return metadataClients
}

// GetExemplarsStores returns a list of all active exemplars stores.
func (e *EndpointSet) GetExemplarsStores() []*exemplarspb.ExemplarStore {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	exemplarStores := make([]*exemplarspb.ExemplarStore, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasExemplarsAPI() {
			exemplarStores = append(exemplarStores, &exemplarspb.ExemplarStore{
				ExemplarsClient: er.clients.exemplar,
				LabelSets:       labelpb.ZLabelSetsToPromLabelSets(er.metadata.LabelSets...),
			})
		}
	}
	return exemplarStores
}

func (e *EndpointSet) Close() {
	e.endpointsMtx.Lock()
	defer e.endpointsMtx.Unlock()

	for _, ef := range e.endpoints {
		ef.Close()
	}
	e.endpoints = map[string]*endpointRef{}
}

func (e *EndpointSet) getActiveEndpoints(ctx context.Context, endpoints map[string]*endpointRef) map[string]*endpointRef {
	var (
		activeEndpoints = make(map[string]*endpointRef, len(endpoints))
		mtx             sync.Mutex
		wg              sync.WaitGroup

		endpointAddrSet = make(map[string]struct{})
	)

	// Gather healthy endpoints map concurrently using info API. Build new clients if does not exist already.
	for _, es := range e.endpointSpec() {
		if _, ok := endpointAddrSet[es.Addr()]; ok {
			level.Warn(e.logger).Log("msg", "duplicated address in nodes", "address", es.Addr())
			continue
		}
		endpointAddrSet[es.Addr()] = struct{}{}

		wg.Add(1)
		go func(spec EndpointSpec) {
			defer wg.Done()

			addr := spec.Addr()

			ctx, cancel := context.WithTimeout(ctx, e.gRPCInfoCallTimeout)
			defer cancel()

			er, seenAlready := endpoints[addr]
			if !seenAlready {
				// New endpoint or was unactive and was removed in the past - create the new one.
				conn, err := grpc.DialContext(ctx, addr, e.dialOpts...)
				if err != nil {
					e.updateEndpointStatus(&endpointRef{addr: addr}, err)
					level.Warn(e.logger).Log("msg", "update of node failed", "err", errors.Wrap(err, "dialing connection"), "address", addr)
					return
				}

				er = &endpointRef{
					cc:     conn,
					addr:   addr,
					logger: e.logger,
					clients: &endpointClients{
						info: infopb.NewInfoClient(conn),
					},
				}
			}

			metadata, err := spec.Metadata(ctx, er.clients.info)
			if err != nil {
				if !seenAlready && !spec.StrictStatic() {
					// Close only if new and not a strict static node.
					// Unactive `e.endpoints` will be closed later on.
					er.Close()
				}

				e.updateEndpointStatus(er, err)
				level.Warn(e.logger).Log("msg", "update of node failed", "err", errors.Wrap(err, "getting metadata"), "address", addr)

				if !spec.StrictStatic() {
					return
				}

				// Still keep it around if static & strict mode enabled.
				// Assume that it expose storeAPI and cover all complete possible time range.
				if !seenAlready {
					metadata.Store = &infopb.StoreInfo{
						MinTime: MinTime,
						MaxTime: MaxTime,
					}
					er.Update(metadata)
				}

				mtx.Lock()
				defer mtx.Unlock()

				activeEndpoints[addr] = er
				return
			}

			er.Update(metadata)
			e.updateEndpointStatus(er, nil)

			mtx.Lock()
			defer mtx.Unlock()

			activeEndpoints[addr] = er
		}(es)
	}
	wg.Wait()

	return activeEndpoints
}

func (e *EndpointSet) updateEndpointStatus(er *endpointRef, err error) {
	e.endpointsStatusesMtx.Lock()
	defer e.endpointsStatusesMtx.Unlock()

	status := EndpointStatus{Name: er.addr}
	prev, ok := e.endpointStatuses[er.addr]
	if ok {
		status = *prev
	} else {
		mint, maxt := er.TimeRange()
		status.MinTime = mint
		status.MaxTime = maxt
	}

	if err == nil {
		status.LastCheck = time.Now()
		mint, maxt := er.TimeRange()
		status.LabelSets = er.LabelSets()
		status.ComponentType = er.ComponentType()
		status.MinTime = mint
		status.MaxTime = maxt
		status.LastError = nil
	} else {
		status.LastError = &stringError{originalErr: err}
	}

	e.endpointStatuses[er.addr] = &status
}

func (e *EndpointSet) GetStoreStatus() []EndpointStatus {
	e.endpointsStatusesMtx.RLock()
	defer e.endpointsStatusesMtx.RUnlock()

	statuses := make([]EndpointStatus, 0, len(e.endpointStatuses))
	for _, v := range e.endpointStatuses {
		statuses = append(statuses, *v)
	}

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Name < statuses[j].Name
	})
	return statuses
}

func (e *EndpointSet) cleanUpStoreStatuses(endpoints map[string]*endpointRef) {
	e.endpointsStatusesMtx.Lock()
	defer e.endpointsStatusesMtx.Unlock()

	now := time.Now()
	for addr, status := range e.endpointStatuses {
		if _, ok := endpoints[addr]; ok {
			continue
		}

		if now.Sub(status.LastCheck) >= e.unhealthyEndpointTimeout {
			delete(e.endpointStatuses, addr)
		}
	}
}

// TODO(bwplotka): Consider moving storeRef out of this package and renaming it, as it also supports rules API.
type endpointRef struct {
	mtx  sync.RWMutex
	cc   *grpc.ClientConn
	addr string

	clients *endpointClients

	// Metadata can change during runtime.
	metadata *endpointMetadata

	logger log.Logger
}

func (er *endpointRef) Update(metadata *endpointMetadata) {
	er.mtx.Lock()
	defer er.mtx.Unlock()

	clients := er.clients

	if metadata.Store != nil {
		clients.store = storepb.NewStoreClient(er.cc)
	}

	if metadata.Rules != nil {
		clients.rule = rulespb.NewRulesClient(er.cc)
	}

	if metadata.Targets != nil {
		clients.target = targetspb.NewTargetsClient(er.cc)
	}

	if metadata.MetricMetadata != nil {
		clients.metricMetadata = metadatapb.NewMetadataClient(er.cc)
	}

	if metadata.Exemplars != nil {
		// min/max range is also provided by in the response of Info rpc call
		// but we are not using this metadata anywhere right now so ignoring.
		clients.exemplar = exemplarspb.NewExemplarsClient(er.cc)
	}

	er.clients = clients
	er.metadata = metadata
}

func (er *endpointRef) ComponentType() component.Component {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	if er.metadata == nil {
		return component.UnknownStoreAPI
	}

	return component.FromString(er.metadata.ComponentType)
}

func (er *endpointRef) HasStoreAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.clients.store != nil
}

func (er *endpointRef) HasRulesAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.clients.rule != nil
}

func (er *endpointRef) HasTargetsAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.clients.target != nil
}

func (er *endpointRef) HasMetricMetadataAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.clients.metricMetadata != nil
}

func (er *endpointRef) HasExemplarsAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.clients.exemplar != nil
}

func (er *endpointRef) LabelSets() []labels.Labels {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	if er.metadata == nil {
		return make([]labels.Labels, 0)
	}

	labelSet := make([]labels.Labels, 0, len(er.metadata.LabelSets))
	for _, ls := range labelpb.ZLabelSetsToPromLabelSets(er.metadata.LabelSets...) {
		if len(ls) == 0 {
			continue
		}
		// Compatibility label for Queriers pre 0.8.1. Filter it out now.
		if ls[0].Name == store.CompatibilityTypeLabelName {
			continue
		}
		labelSet = append(labelSet, ls.Copy())
	}
	return labelSet
}

func (er *endpointRef) TimeRange() (mint int64, maxt int64) {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	if er.metadata == nil || er.metadata.Store == nil {
		return MinTime, MaxTime
	}

	// Currently, min/max time of only StoreAPI is being updated by all components.
	return er.metadata.Store.MinTime, er.metadata.Store.MaxTime
}

func (er *endpointRef) String() string {
	mint, maxt := er.TimeRange()
	return fmt.Sprintf("Addr: %s LabelSets: %v Mint: %d Maxt: %d", er.addr, labelpb.PromLabelSetsToString(er.LabelSets()), mint, maxt)
}

func (er *endpointRef) Addr() string {
	return er.addr
}

func (er *endpointRef) Close() {
	runutil.CloseWithLogOnErr(er.logger, er.cc, fmt.Sprintf("endpoint %v connection closed", er.addr))
}

type endpointClients struct {
	store          storepb.StoreClient
	rule           rulespb.RulesClient
	metricMetadata metadatapb.MetadataClient
	exemplar       exemplarspb.ExemplarsClient
	target         targetspb.TargetsClient
	info           infopb.InfoClient
}

type endpointMetadata struct {
	*infopb.InfoResponse
}

func newEndpointAPIStats() map[component.Component]map[string]int {
	nodes := make(map[component.Component]map[string]int, len(storepb.StoreType_name))
	for i := range storepb.StoreType_name {
		nodes[component.FromProto(storepb.StoreType(i))] = map[string]int{}
	}
	return nodes
}
