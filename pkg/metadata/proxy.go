// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// Proxy implements metadatapb.Metadata gRPC that fanouts requests to given metadatapb.Metadata and deduplication on the way.
type Proxy struct {
	logger   log.Logger
	metadata func() []metadatapb.MetadataClient
}

func RegisterMetadataServer(metadataSrv metadatapb.MetadataServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		metadatapb.RegisterMetadataServer(s, metadataSrv)
	}
}

// NewProxy returns a new metadata.Proxy.
func NewProxy(logger log.Logger, metadata func() []metadatapb.MetadataClient) *Proxy {
	return &Proxy{
		logger:   logger,
		metadata: metadata,
	}
}

func (s *Proxy) MetricMetadata(req *metadatapb.MetricMetadataRequest, srv metadatapb.Metadata_MetricMetadataServer) error {
	span, ctx := tracing.StartSpan(srv.Context(), "proxy_metadata")
	defer span.Finish()

	var (
		g, gctx  = errgroup.WithContext(ctx)
		respChan = make(chan *metadatapb.MetricMetadata, 10)
		metas    []*metadatapb.MetricMetadata
		err      error
	)

	for _, metadataClient := range s.metadata() {
		rs := &metricMetadataStream{
			client:  metadataClient,
			request: req,
			channel: respChan,
			server:  srv,
		}
		g.Go(func() error { return rs.receive(gctx) })
	}

	go func() {
		_ = g.Wait()
		close(respChan)
	}()

	for resp := range respChan {
		metas = append(metas, resp)
	}

	if err := g.Wait(); err != nil {
		level.Error(s.logger).Log("err", err)
		return err
	}

	for _, t := range metas {
		tracing.DoInSpan(srv.Context(), "send_metadata_response", func(_ context.Context) {
			err = srv.Send(metadatapb.NewMetricMetadataResponse(t))
		})
		if err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send metric metadata response").Error())
		}
	}

	return nil
}

type metricMetadataStream struct {
	client  metadatapb.MetadataClient
	request *metadatapb.MetricMetadataRequest
	channel chan<- *metadatapb.MetricMetadata
	server  metadatapb.Metadata_MetricMetadataServer
}

func (stream *metricMetadataStream) receive(ctx context.Context) error {
	var (
		err         error
		metadataCli metadatapb.Metadata_MetricMetadataClient
	)

	tracing.DoInSpan(ctx, "receive_metadata_stream_request", func(ctx context.Context) {
		metadataCli, err = stream.client.MetricMetadata(ctx, stream.request)
	})

	if err != nil {
		err = errors.Wrapf(err, "fetching metric metadata from metadata client %v", stream.client)

		if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
			return err
		}

		if serr := stream.server.Send(metadatapb.NewWarningMetadataResponse(err)); serr != nil {
			return serr
		}
		// Not an error if response strategy is warning.
		return nil
	}

	for {
		resp, err := metadataCli.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			err = errors.Wrapf(err, "receiving metric metadata from metadata client %v", stream.client)

			if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
				return err
			}

			if err := stream.server.Send(metadatapb.NewWarningMetadataResponse(err)); err != nil {
				return errors.Wrapf(err, "sending metadata error to server %v", stream.server)
			}

			continue
		}

		if w := resp.GetWarning(); w != "" {
			if err := stream.server.Send(metadatapb.NewWarningMetadataResponse(errors.New(w))); err != nil {
				return errors.Wrapf(err, "sending metadata warning to server %v", stream.server)
			}
			continue
		}

		select {
		case stream.channel <- resp.GetMetadata():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Proxy) TargetMetadata(request *metadatapb.TargetMetadataRequest, server metadatapb.Metadata_TargetMetadataServer) error {
	panic("implement me")
}
