// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

var _ UnaryClient = &GRPCClient{}

// UnaryClient is a gRPC metadatapb.Metadata client which expands streaming metadata API. Useful for consumers that does not
// support streaming.
type UnaryClient interface {
	MetricMetadata(ctx context.Context, req *metadatapb.MetricMetadataRequest) (map[string][]metadatapb.Meta, storage.Warnings, error)
	TargetMetadata(ctx context.Context, req *metadatapb.TargetMetadataRequest) ([]*metadatapb.TargetMetadata, storage.Warnings, error)
}

// GRPCClient allows to retrieve metadata from local gRPC streaming server implementation.
// TODO(bwplotka): Switch to native gRPC transparent client->server adapter once available.
type GRPCClient struct {
	proxy metadatapb.MetadataServer
}

func NewGRPCClient(ts metadatapb.MetadataServer) *GRPCClient {
	return &GRPCClient{
		proxy: ts,
	}
}

func (rr *GRPCClient) MetricMetadata(ctx context.Context, req *metadatapb.MetricMetadataRequest) (map[string][]metadatapb.Meta, storage.Warnings, error) {
	span, ctx := tracing.StartSpan(ctx, "metric_metadata_grpc_request")
	defer span.Finish()

	srv := &metricMetadataServer{ctx: ctx, metric: req.Metric, limit: int(req.Limit)}

	if req.Limit >= 0 {
		if req.Metric != "" {
			srv.metadataMap = make(map[string][]metadatapb.Meta, 1)
		} else if req.Limit <= 100 {
			srv.metadataMap = make(map[string][]metadatapb.Meta, req.Limit)
		} else {
			srv.metadataMap = make(map[string][]metadatapb.Meta)
		}
	} else {
		srv.metadataMap = make(map[string][]metadatapb.Meta)
	}

	if err := rr.proxy.MetricMetadata(req, srv); err != nil {
		return nil, nil, errors.Wrap(err, "proxy MetricMetadata")
	}

	return srv.metadataMap, srv.warnings, nil
}

func (rr *GRPCClient) TargetMetadata(ctx context.Context, req *metadatapb.TargetMetadataRequest) ([]*metadatapb.TargetMetadata, storage.Warnings, error) {
	span, ctx := tracing.StartSpan(ctx, "target_metadata_grpc_request")
	defer span.Finish()

	srv := &targetMetadataServer{ctx: ctx, metric: req.Metric, limit: int(req.Limit), target: req.MatchTarget}

	if err := rr.proxy.TargetMetadata(req, srv); err != nil {
		return nil, nil, errors.Wrap(err, "proxy TargetMetadata")
	}

	return srv.metadata, srv.warnings, nil
}

type metricMetadataServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	metadatapb.Metadata_MetricMetadataServer
	ctx context.Context

	metric string
	limit  int

	warnings    []error
	metadataMap map[string][]metadatapb.Meta
}

func (srv *metricMetadataServer) Send(res *metadatapb.MetricMetadataResponse) error {
	if res.GetWarning() != "" {
		srv.warnings = append(srv.warnings, errors.New(res.GetWarning()))
		return nil
	}

	if res.GetMetadata() == nil {
		return errors.New("no metadata")
	}

	// If limit is set to 0, we don't need to add anything.
	if srv.limit == 0 {
		return nil
	}

	for k, v := range res.GetMetadata().Metadata {
		if metadata, ok := srv.metadataMap[k]; !ok {
			// If limit is set and it is positive, we limit the size of the map.
			if srv.limit < 0 || srv.limit > 0 && len(srv.metadataMap) < srv.limit {
				srv.metadataMap[k] = v.Metas
			}
		} else {
			// There shouldn't be many metadata for one single metric.
		Outer:
			for _, meta := range v.Metas {
				for _, m := range metadata {
					if meta == m {
						continue Outer
					}
				}
				srv.metadataMap[k] = append(srv.metadataMap[k], meta)
			}
		}
	}

	return nil
}

func (srv *metricMetadataServer) Context() context.Context {
	return srv.ctx
}

type targetMetadataServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	metadatapb.Metadata_TargetMetadataServer
	ctx context.Context

	metric string
	limit  int
	target string

	warnings []error
	metadata []*metadatapb.TargetMetadata
}

func (srv *targetMetadataServer) Send(res *metadatapb.TargetMetadataResponse) error {
	if res.GetWarning() != "" {
		srv.warnings = append(srv.warnings, errors.New(res.GetWarning()))
		return nil
	}

	if res.GetData() == nil {
		return errors.New("no target metadata")
	}

	// If limit is set to 0, we don't need to add anything.
	if srv.limit == 0 {
		return nil
	}

	srv.metadata = append(srv.metadata, res.GetData())
	return nil
}

func (srv *targetMetadataServer) Context() context.Context {
	return srv.ctx
}
