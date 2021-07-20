// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"context"
	"net/url"

	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// Prometheus implements metadatapb.Metadata gRPC service that allows to fetch metric metadata from Prometheus HTTP /api/v1/metadata endpoint.
type Prometheus struct {
	base   *url.URL
	client *promclient.Client
}

// NewPrometheus creates a new metadata.Prometheus.
func NewPrometheus(base *url.URL, client *promclient.Client) *Prometheus {
	return &Prometheus{
		base:   base,
		client: client,
	}
}

// MetricMetadata returns all specified metric metadata from Prometheus.
func (p *Prometheus) MetricMetadata(r *metadatapb.MetricMetadataRequest, s metadatapb.Metadata_MetricMetadataServer) error {
	md, err := p.client.MetricMetadataInGRPC(s.Context(), p.base, r.Metric, int(r.Limit))
	if err != nil {
		return err
	}

	tracing.DoInSpan(s.Context(), "send_metric_metadata_response", func(_ context.Context) {
		err = s.Send(&metadatapb.MetricMetadataResponse{Result: &metadatapb.MetricMetadataResponse_Metadata{
			Metadata: metadatapb.FromMetadataMap(md)}})
	})
	return err
}

// TargetMetadata returns all specified target metadata from Prometheus.
func (p *Prometheus) TargetMetadata(r *metadatapb.TargetMetadataRequest, s metadatapb.Metadata_TargetMetadataServer) error {
	targetsMetadata, err := p.client.TargetMetadataInGRPC(s.Context(), p.base, r.MatchTarget, r.Metric, int(r.Limit))
	if err != nil {
		return err
	}

	for _, targetMetadata := range targetsMetadata {
		tracing.DoInSpan(s.Context(), "send_target_metadata_response", func(_ context.Context) {
			err = s.Send(&metadatapb.TargetMetadataResponse{
				Result: &metadatapb.TargetMetadataResponse_Data{Data: targetMetadata},
			})
		})
	}

	return err
}
