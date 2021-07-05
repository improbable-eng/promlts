// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"fmt"
	"strings"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/prometheus/common/model"
	"gopkg.in/alecthomas/kingpin.v2"
)

func ModelDuration(flags *kingpin.FlagClause) *model.Duration {
	value := new(model.Duration)
	flags.SetValue(value)

	return value
}

// RegisterGRPCFlags registers flags commonly used to configure gRPC servers with.
func RegisterGRPCFlags(cmd FlagClause) (
	grpcBindAddr *string,
	grpcGracePeriod *model.Duration,
	grpcTLSSrvCert *string,
	grpcTLSSrvKey *string,
	grpcTLSSrvClientCA *string,
) {
	grpcBindAddr = cmd.Flag("grpc-address", "Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components.").
		Default("0.0.0.0:10901").String()
	grpcGracePeriod = ModelDuration(cmd.Flag("grpc-grace-period", "Time to wait after an interrupt received for GRPC Server.").Default("2m")) // by default it's the same as query.timeout.

	grpcTLSSrvCert = cmd.Flag("grpc-server-tls-cert", "TLS Certificate for gRPC server, leave blank to disable TLS").Default("").String()
	grpcTLSSrvKey = cmd.Flag("grpc-server-tls-key", "TLS Key for the gRPC server, leave blank to disable TLS").Default("").String()
	grpcTLSSrvClientCA = cmd.Flag("grpc-server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").String()

	return grpcBindAddr,
		grpcGracePeriod,
		grpcTLSSrvCert,
		grpcTLSSrvKey,
		grpcTLSSrvClientCA
}

// RegisterCommonObjStoreFlags register flags commonly used to configure http servers with.
func RegisterHTTPFlags(cmd FlagClause) (httpBindAddr *string, httpGracePeriod *model.Duration, httpTLSConfig *string) {
	httpBindAddr = cmd.Flag("http-address", "Listen host:port for HTTP endpoints.").Default("0.0.0.0:10902").String()
	httpGracePeriod = ModelDuration(cmd.Flag("http-grace-period", "Time to wait after an interrupt received for HTTP Server.").Default("2m")) // by default it's the same as query.timeout.
	httpTLSConfig = cmd.Flag(
		"http.config",
		"[EXPERIMENTAL] Path to the configuration file that can enable TLS or authentication for all HTTP endpoints.",
	).Default("").String()
	return httpBindAddr, httpGracePeriod, httpTLSConfig
}

// RegisterCommonObjStoreFlags register flags to specify object storage configuration.
func RegisterCommonObjStoreFlags(cmd FlagClause, suffix string, required bool, extraDesc ...string) *extflag.PathOrContent {
	help := fmt.Sprintf("YAML file that contains object store%s configuration. See format details: https://thanos.io/tip/thanos/storage.md/#configuration ", suffix)
	help = strings.Join(append([]string{help}, extraDesc...), " ")
	opts := []extflag.Option{extflag.WithEnvSubstitution()}
	if required {
		opts = append(opts, extflag.WithRequired())
	}
	return extflag.RegisterPathOrContent(cmd, fmt.Sprintf("objstore%s.config", suffix), help, opts...)
}

// RegisterCommonTracingFlags registers flags to pass a tracing configuration to be used with OpenTracing.
func RegisterCommonTracingFlags(app FlagClause) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		app,
		"tracing.config",
		"YAML file with tracing configuration. See format details: https://thanos.io/tip/thanos/tracing.md/#configuration ",
		extflag.WithEnvSubstitution(),
	)
}

// RegisterRequestLoggingFlags registers flags to pass a request logging configuration to be used.
func RegisterRequestLoggingFlags(app FlagClause) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		app,
		"request.logging-config",
		// TODO @yashrsharma44: Change the link with the documented link for yaml configuration.
		"YAML file with request logging configuration. See format details: https://gist.github.com/yashrsharma44/02f5765c5710dd09ce5d14e854f22825",
		extflag.WithEnvSubstitution(),
	)
}

// RegisterSelectorRelabelFlags register flags to specify relabeling configuration selecting blocks to process.
func RegisterSelectorRelabelFlags(cmd FlagClause) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		cmd,
		"selector.relabel-config",
		"YAML file that contains relabeling configuration that allows selecting blocks. It follows native Prometheus relabel-config syntax. See format details: https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config ",
		extflag.WithEnvSubstitution(),
	)
}

func RegisterHTTPConfigFlags(cmd FlagClause) *extflag.PathOrContent {
	return extflag.RegisterPathOrContent(
		cmd,
		"prometheus.http-client",
		"YAML file or string with http client configs. see Format details : ...",
		false,
	)
}
