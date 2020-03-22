// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"math"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func StoreClientGRPCOptsFromTlsConfig(logger log.Logger, clientInstance string, reg *prometheus.Registry, tracer opentracing.Tracer, tlsConfig *store.TlsConfig) ([]grpc.DialOption, error) {
	if tlsConfig != nil {
		return StoreClientGRPCOpts(logger, clientInstance, reg, tracer, true, tlsConfig.Cert, tlsConfig.Key, tlsConfig.CaCert, tlsConfig.ServerName)
	}
	return StoreClientGRPCOpts(logger, clientInstance, reg, tracer, false, "", "", "", "")
}

// StoreClientGRPCOpts creates gRPC dial options for connecting to a store client.
func StoreClientGRPCOpts(logger log.Logger, clientInstance string, reg *prometheus.Registry, tracer opentracing.Tracer, secure bool, cert, key, caCert, serverName string) ([]grpc.DialOption, error) {
	var grpcMets *grpc_prometheus.ClientMetrics
	histogramBuckets := grpc_prometheus.WithHistogramBuckets(
		[]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	)
	if clientInstance != "" {
		constLabels := map[string]string{"config_name": clientInstance}
		grpcMets = grpc_prometheus.NewClientMetrics(grpc_prometheus.WithConstLabels(constLabels))
		grpcMets.EnableClientHandlingTimeHistogram(
			grpc_prometheus.WithHistogramConstLabels(constLabels),
			histogramBuckets,
		)
	} else {
		grpcMets = grpc_prometheus.NewClientMetrics()
		grpcMets.EnableClientHandlingTimeHistogram(histogramBuckets)
	}
	dialOpts := []grpc.DialOption{
		// We want to make sure that we can receive huge gRPC messages from storeAPI.
		// On TCP level we can be fine, but the gRPC overhead for huge messages could be significant.
		// Current limit is ~2GB.
		// TODO(bplotka): Split sent chunks on store node per max 4MB chunks if needed.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				grpcMets.UnaryClientInterceptor(),
				tracing.UnaryClientInterceptor(tracer),
			),
		),
		grpc.WithStreamInterceptor(
			grpc_middleware.ChainStreamClient(
				grpcMets.StreamClientInterceptor(),
				tracing.StreamClientInterceptor(tracer),
			),
		),
	}
	if reg != nil {
		reg.MustRegister(grpcMets)
	}

	if !secure {
		return append(dialOpts, grpc.WithInsecure()), nil
	}

	level.Info(logger).Log("msg", "enabling client to server TLS")

	tlsCfg, err := tls.NewClientConfig(logger, cert, key, caCert, serverName)
	if err != nil {
		return nil, err
	}
	return append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))), nil
}
