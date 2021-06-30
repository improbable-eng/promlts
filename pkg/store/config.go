// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v2"

	"github.com/prometheus/prometheus/discovery/file"
)

// Config represents the configuration of a set of Store API endpoints.
type Config struct {
	TlsConfig    TlsConfiguration `yaml:"tls_config"`
	EndPoints    []string         `yaml:"endpoints"`
	EndPoints_sd []file.SDConfig  `yaml:"endpoints_sd_files"`
	Mode         string           `yaml:"mode"`
}

// TlsConfiguration represents the TLS configuration for a set of Store API endpoints.
type TlsConfiguration struct {
	// TLS Certificates to use to identify this client to the server.
	Cert string `yaml:"cert_file"`
	// TLS Key for the client's certificate.
	Key string `yaml:"key_file"`
	// TLS CA Certificates to use to verify gRPC servers.
	CaCert string `yaml:"ca_file"`
	// Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1
	ServerName string `yaml:"server_name"`
}

func LoadConfig(yamlPath string, endpointAddrs []string, strictEndpointAddrs []string, fileSDConfig *file.SDConfig) ([]Config, error) {
	var endpointConfig []Config

	if len(yamlPath) > 0 {
		filename, _ := filepath.Abs(yamlPath)
		yamlFile, err := ioutil.ReadFile(filename)
		if err != nil {
			return []Config{}, fmt.Errorf("cannot read file at path %s", yamlPath)
		}

		if err := yaml.UnmarshalStrict(yamlFile, &endpointConfig); err != nil {
			return []Config{}, fmt.Errorf("yaml file not in proper format")
		}
	}

	// No dynamic endpoints in strict mode
	for _, config := range endpointConfig {
		if config.Mode == "strict" && len(config.EndPoints_sd) != 0 {
			return []Config{}, fmt.Errorf("no sd-files allowed in strict mode")
		}
	}

	// Checking if some endpoints are inputted more than once
	mp := map[string]bool{}
	for _, config := range endpointConfig {
		for _, ep := range config.EndPoints {
			if mp[ep] {
				return []Config{}, fmt.Errorf("%s endpoint provided more than once", ep)
			}
			mp[ep] = true
		}
	}

	// Adding --endpoint, --endpoint_sd_files info to []endpointConfig
	cfg1 := Config{}
	for _, addr := range endpointAddrs {
		if mp[addr] {
			return []Config{}, fmt.Errorf("%s endpoint provided more than once", addr)
		}
		mp[addr] = true
		cfg1.EndPoints = append(cfg1.EndPoints, addr)
	}
	cfg1.EndPoints_sd = []file.SDConfig{*fileSDConfig}
	endpointConfig = append(endpointConfig, cfg1)

	// Adding --store-strict endpoints
	cfg2 := Config{}
	for _, addr := range strictEndpointAddrs {
		if mp[addr] {
			return []Config{}, fmt.Errorf("%s endpoint provided more than once", addr)
		}
		mp[addr] = true
		cfg2.EndPoints = append(cfg2.EndPoints, addr)
	}
	cfg2.Mode = "strict"
	endpointConfig = append(endpointConfig, cfg2)

	return endpointConfig, nil
}
