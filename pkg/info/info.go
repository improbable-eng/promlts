// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package info

import (
	"context"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"google.golang.org/grpc"
)

type InfoServer struct {
	infopb.UnimplementedInfoServer

	component string

	getLabelSet           func() []labelpb.ZLabelSet
	getStoreInfo          func() *infopb.StoreInfo
	getExemplarsInfo      func() *infopb.ExemplarsInfo
	getRulesInfo          func() *infopb.RulesInfo
	getTargetsInfo        func() *infopb.TargetsInfo
	getMetricMetadataInfo func() *infopb.MetricMetadataInfo
}

func NewInfoServer(
	component string,
	getLabelSet func() []labelpb.ZLabelSet,
	getStoreInfo func() *infopb.StoreInfo,
	getExemplarsInfo func() *infopb.ExemplarsInfo,
	getRulesInfo func() *infopb.RulesInfo,
	getTargetsInfo func() *infopb.TargetsInfo,
	getMetricMetadataInfo func() *infopb.MetricMetadataInfo,
) *InfoServer {
	return &InfoServer{
		component:             component,
		getLabelSet:           getLabelSet,
		getStoreInfo:          getStoreInfo,
		getExemplarsInfo:      getExemplarsInfo,
		getRulesInfo:          getRulesInfo,
		getTargetsInfo:        getTargetsInfo,
		getMetricMetadataInfo: getMetricMetadataInfo,
	}
}

// RegisterInfoServer register info server.
func RegisterInfoServer(infoSrv infopb.InfoServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		infopb.RegisterInfoServer(s, infoSrv)
	}
}

func (srv *InfoServer) Info(ctx context.Context, req *infopb.InfoRequest) (*infopb.InfoResponse, error) {

	if srv.getStoreInfo == nil {
		srv.getStoreInfo = func() *infopb.StoreInfo { return nil }
	}

	if srv.getExemplarsInfo == nil {
		srv.getExemplarsInfo = func() *infopb.ExemplarsInfo { return nil }
	}

	if srv.getRulesInfo == nil {
		srv.getRulesInfo = func() *infopb.RulesInfo { return nil }
	}

	if srv.getTargetsInfo == nil {
		srv.getTargetsInfo = func() *infopb.TargetsInfo { return nil }
	}

	if srv.getMetricMetadataInfo == nil {
		srv.getMetricMetadataInfo = func() *infopb.MetricMetadataInfo { return nil }
	}

	resp := &infopb.InfoResponse{
		LabelSets:      srv.getLabelSet(),
		ComponentType:  srv.component,
		Store:          srv.getStoreInfo(),
		Exemplars:      srv.getExemplarsInfo(),
		Rules:          srv.getRulesInfo(),
		Targets:        srv.getTargetsInfo(),
		MetricMetadata: srv.getMetricMetadataInfo(),
	}

	return resp, nil
}
