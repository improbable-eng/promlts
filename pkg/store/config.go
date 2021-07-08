// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"gopkg.in/yaml.v2"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/discovery/file"
)

// Config represents the configuration of a set of Store API endpoints.
type Config struct {
	TLSConfig   TLSConfiguration `yaml:"tls_config"`
	Endpoints   []string         `yaml:"endpoints"`
	EndpointsSD []file.SDConfig  `yaml:"endpoints_sd_files"`
	Mode        EndpointMode     `yaml:"mode"`
}

// TlsConfiguration represents the TLS configuration for a set of Store API endpoints.
type TLSConfiguration struct {
	// TLS Certificates file to use to identify this client to the server.
	CertFile string `yaml:"cert_file"`
	// TLS Key file for the client's certificate.
	KeyFile string `yaml:"key_file"`
	// TLS CA Certificates file to use to verify gRPC servers.
	CaCertFile string `yaml:"ca_file"`
	// Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1
	ServerName string `yaml:"server_name"`
}

type EndpointMode string

const (
	DefaultEndpointMode EndpointMode = ""
	StrictEndpointMode  EndpointMode = "strict"
)

func NewConfig(endpointAddrs []string, strictEndpointAddrs []string, fileSDConfig *file.SDConfig, TLSConfig TLSConfiguration) ([]Config, error) {
	var endpointConfig []Config

	// Adding --endpoint, --endpoint.sd-files info to []endpointConfig, if provided.
	if len(endpointAddrs) > 0 || fileSDConfig != nil {
		cfg1 := Config{}
		cfg1.TLSConfig = TLSConfig
		cfg1.Endpoints = strictEndpointAddrs
		cfg1.EndpointsSD = []file.SDConfig{*fileSDConfig}
		endpointConfig = append(endpointConfig, cfg1)
	}

	// Adding --endpoint-strict endpoints if provided.
	if len(strictEndpointAddrs) > 0 {
		cfg2 := Config{}
		cfg2.TLSConfig = TLSConfig
		cfg2.Endpoints = strictEndpointAddrs
		cfg2.Mode = StrictEndpointMode
		endpointConfig = append(endpointConfig, cfg2)
	}
	return endpointConfig, nil
}

// LoadConfig loads and returns list of per-endpoint TLS config.
func LoadConfig(confYAML []byte) ([]Config, error) {
	var endpointConfig []Config

	if err := yaml.UnmarshalStrict(confYAML, &endpointConfig); err != nil {
		return nil, err
	}

	// Checking if wrong mode is provided.
	for _, config := range endpointConfig {
		if config.Mode != StrictEndpointMode && config.Mode != DefaultEndpointMode {
			return nil, errors.Errorf("%s is wrong mode", config.Mode)
		}
	}

	// No dynamic endpoints in strict mode.
	for _, config := range endpointConfig {
		if config.Mode == StrictEndpointMode && len(config.EndpointsSD) != 0 {
			return nil, errors.Errorf("no sd-files allowed in strict mode")
		}
	}

	// Checking if some endpoints are inputted more than once.
	allEndpoints := make(map[string]struct{})
	for _, config := range endpointConfig {
		for _, addr := range config.Endpoints {
			if _, exists := allEndpoints[addr]; exists {
				return nil, errors.Errorf("%s endpoint provided more than once", addr)
			}
			allEndpoints[addr] = struct{}{}
		}
	}

	return endpointConfig, nil
}
