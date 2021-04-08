// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package info

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"google.golang.org/grpc"
)

type InfoServer struct {
	infopb.UnimplementedInfoServer

	logger    log.Logger
	component infopb.ComponentType

	getLabelSet      func() []labelpb.ZLabelSet
	getStoreInfo     func() *infopb.StoreInfo
	getExemplarsInfo func() *infopb.ExemplarsInfo
}

func NewInfoServer(
	component infopb.ComponentType,
	getLabelSet func() []labelpb.ZLabelSet,
	getStoreInfo func() *infopb.StoreInfo,
	getExemplarsInfo func() *infopb.ExemplarsInfo,
) *InfoServer {
	return &InfoServer{
		component:        component,
		getLabelSet:      getLabelSet,
		getStoreInfo:     getStoreInfo,
		getExemplarsInfo: getExemplarsInfo,
	}
}

// RegisterInfoServer register info server.
func RegisterInfoServer(infoSrv infopb.InfoServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		infopb.RegisterInfoServer(s, infoSrv)
	}
}

func (srv *InfoServer) Info(ctx context.Context, req *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	return &infopb.InfoResponse{
		LabelSets:     srv.getLabelSet(),
		ComponentType: srv.component,
		StoreInfo:     srv.getStoreInfo(),
		ExemplarsInfo: srv.getExemplarsInfo(),
	}, nil
}
