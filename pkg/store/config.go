// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"

	"gopkg.in/yaml.v2"

	"github.com/prometheus/prometheus/discovery/file"
)

// Config represents the configuration of a set of Store API endpoints.
type Config struct {
	Name            string          `yaml:"name"`
	TlsConfig       *TlsConfig      `yaml:"tls_config"`
	EndpointsConfig EndpointsConfig `yaml:",inline"`
}

// TlsConfig represents the TLS configuration for a set of Store API endpoints.
type TlsConfig struct {
	// TLS Certificates to use to identify this client to the server.
	Cert string `yaml:"cert_file"`
	// TLS Key for the client's certificate.
	Key string `yaml:"key_file"`
	// TLS CA Certificates to use to verify gRPC servers.
	CaCert string `yaml:"ca_file"`
	// Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1
	ServerName string `yaml:"server_name"`
}

// EndpointsConfig represents the address discovery configuration for a set of Store API endpoints.
type EndpointsConfig struct {
	// List of addresses with DNS prefixes.
	StaticAddresses []string `yaml:"static_configs"`
	// List of file  configurations (our FileSD supports different DNS lookups).
	FileSDConfigs []file.SDConfig `yaml:"file_sd_configs"`
}

func DefaultConfig() Config {
	return Config{
		Name: "default",
		EndpointsConfig: EndpointsConfig{
			StaticAddresses: []string{},
			FileSDConfigs:   []file.SDConfig{},
		},
	}
}

func NewConfig(storeAddrs []string, fileSDConfig *file.SDConfig, secure bool, cert string, key string, caCert string, serverName string) (Config, error) {
	for _, addr := range storeAddrs {
		if addr == "" {
			return Config{}, fmt.Errorf("static store address cannot be empty")
		}
	}

	endpointsConfig := EndpointsConfig{
		StaticAddresses: storeAddrs,
	}
	if fileSDConfig != nil {
		endpointsConfig.FileSDConfigs = []file.SDConfig{*fileSDConfig}
	}
	storeConfig := Config{
		Name:            "default",
		EndpointsConfig: endpointsConfig,
	}
	if secure {
		storeConfig.TlsConfig = &TlsConfig{
			Cert:       cert,
			Key:        key,
			CaCert:     caCert,
			ServerName: serverName,
		}
	}
	return storeConfig, nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig()
	type plain Config
	return unmarshal((*plain)(c))
}

// LoadConfigs loads a list of Config from YAML data.
func LoadConfig(confYAML []byte) ([]Config, error) {
	var queryCfg []Config
	if err := yaml.UnmarshalStrict(confYAML, &queryCfg); err != nil {
		return nil, err
	}
	seenNames := map[string]string{}
	for _, cfg := range queryCfg {
		if _, exists := seenNames[cfg.Name]; exists {
			return nil, fmt.Errorf("config must have a non-empty, unique name")
		} else {
			seenNames[cfg.Name] = cfg.Name
		}

	}
	return queryCfg, nil
}
