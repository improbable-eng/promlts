// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package targets

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

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// Proxy implements targetspb.Targets gRPC that fans out requests to given targetspb.Targets.
type Proxy struct {
	logger  log.Logger
	targets func() []targetspb.TargetsClient
}

func RegisterTargetsServer(targetsSrv targetspb.TargetsServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		targetspb.RegisterTargetsServer(s, targetsSrv)
	}
}

// NewProxy returns new targets.Proxy.
func NewProxy(logger log.Logger, targets func() []targetspb.TargetsClient) *Proxy {
	return &Proxy{
		logger:  logger,
		targets: targets,
	}
}

func (s *Proxy) Targets(req *targetspb.TargetsRequest, srv targetspb.Targets_TargetsServer) error {
	span, ctx := tracing.StartSpan(srv.Context(), "proxy_targets")
	defer span.Finish()

	var (
		g, gctx  = errgroup.WithContext(ctx)
		respChan = make(chan *targetspb.TargetDiscovery, 10)
		targets  []*targetspb.TargetDiscovery
		err      error
	)

	for _, targetsClient := range s.targets() {
		rs := &targetsStream{
			client:  targetsClient,
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
		// TODO: Stream it
		targets = append(targets, resp)
	}

	if err := g.Wait(); err != nil {
		level.Error(s.logger).Log("err", err)
		return err
	}

	for _, t := range targets {
		tracing.DoInSpan(srv.Context(), "send_targets_response", func(_ context.Context) {
			err = srv.Send(targetspb.NewTargetsResponse(t))
		})
		if err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send targets response").Error())
		}
	}

	return nil
}

type targetsStream struct {
	client  targetspb.TargetsClient
	request *targetspb.TargetsRequest
	channel chan<- *targetspb.TargetDiscovery
	server  targetspb.Targets_TargetsServer
}

func (stream *targetsStream) receive(ctx context.Context) error {
	var (
		err     error
		targets targetspb.Targets_TargetsClient
	)

	tracing.DoInSpan(ctx, "receive_stream_request", func(ctx context.Context) {
		targets, err = stream.client.Targets(ctx, stream.request)
	})

	if err != nil {
		err = errors.Wrapf(err, "fetching targets from targets client %v", stream.client)

		if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
			return err
		}

		if serr := stream.server.Send(targetspb.NewWarningTargetsResponse(err)); serr != nil {
			return serr
		}
		// Not an error if response strategy is warning.
		return nil
	}

	for {
		target, err := targets.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			err = errors.Wrapf(err, "receiving targets from targets client %v", stream.client)

			if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
				return err
			}

			if err := stream.server.Send(targetspb.NewWarningTargetsResponse(err)); err != nil {
				return errors.Wrapf(err, "sending targets error to server %v", stream.server)
			}

			continue
		}

		if w := target.GetWarning(); w != "" {
			if err := stream.server.Send(targetspb.NewWarningTargetsResponse(errors.New(w))); err != nil {
				return errors.Wrapf(err, "sending targets warning to server %v", stream.server)
			}
			continue
		}

		select {
		case stream.channel <- target.GetTargets():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
