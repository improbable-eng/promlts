// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/store"
	"gopkg.in/yaml.v2"
)

const logLevel = "info"

// TODO(bwplotka): Run against multiple?
func DefaultPrometheusImage() string {
	return "quay.io/prometheus/prometheus:v2.16.0"
}

func DefaultAlertmanagerImage() string {
	return "quay.io/prometheus/alertmanager:v0.20.0"
}

// DefaultImage returns the local docker image to use to run Thanos.
func DefaultImage() string {
	// Get the Thanos image from the THANOS_IMAGE env variable.
	if os.Getenv("THANOS_IMAGE") != "" {
		return os.Getenv("THANOS_IMAGE")
	}

	return "thanos"
}

func NewPrometheus(sharedDir string, name string, config, promImage string) (*e2e.HTTPService, string, error) {
	dir := filepath.Join(sharedDir, "data", "prometheus", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "prometheus", name)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, "", errors.Wrap(err, "create prometheus dir")
	}

	if err := ioutil.WriteFile(filepath.Join(dir, "prometheus.yml"), []byte(config), 0666); err != nil {
		return nil, "", errors.Wrap(err, "creating prom config failed")
	}

	prom := e2e.NewHTTPService(
		fmt.Sprintf("prometheus-%s", name),
		promImage,
		e2e.NewCommandWithoutEntrypoint("prometheus", e2e.BuildArgs(map[string]string{
			"--config.file":                     filepath.Join(container, "prometheus.yml"),
			"--storage.tsdb.path":               container,
			"--storage.tsdb.max-block-duration": "2h",
			"--log.level":                       logLevel,
			"--web.listen-address":              ":9090",
		})...),
		e2e.NewReadinessProbe(9090, "/-/ready", 200),
		9090,
	)
	prom.SetUser("root")

	return prom, container, nil
}

func NewPrometheusWithSidecar(sharedDir string, netName string, name string, config, promImage string) (*e2e.HTTPService, *Service, error) {
	prom, dataDir, err := NewPrometheus(sharedDir, name, config, promImage)
	if err != nil {
		return nil, nil, err
	}

	sidecar := NewService(
		fmt.Sprintf("sidecar-%s", name),
		DefaultImage(),
		e2e.NewCommand("sidecar", e2e.BuildArgs(map[string]string{
			"--debug.name":        fmt.Sprintf("sidecar-%v", name),
			"--grpc-address":      ":9091",
			"--grpc-grace-period": "0s",
			"--http-address":      ":80",
			"--prometheus.url":    "http://" + prom.NetworkEndpointFor(netName, 9090),
			"--tsdb.path":         dataDir,
			"--log.level":         logLevel,
		})...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
		9091,
	)
	return prom, sidecar, nil
}

func NewQuerier(name string, storeCfg []store.Config) (*Service, error) {
	const replicaLabel = "replica"

	storeCfgBytes, err := yaml.Marshal(storeCfg)
	if err != nil {
		return nil, errors.Wrapf(err, "generate store file: %v", storeCfg)
	}

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":            fmt.Sprintf("querier-%v", name),
		"--grpc-address":          ":9091",
		"--grpc-grace-period":     "0s",
		"--http-address":          ":80",
		"--query.replica-label":   replicaLabel,
		"--store.config":          string(storeCfgBytes),
		"--store.sd-dns-interval": "5s",
		"--log.level":             logLevel,
		"--store.sd-interval":     "5s",
	})

	return NewService(
		fmt.Sprintf("querier-%v", name),
		DefaultImage(),
		e2e.NewCommand("query", args...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
		9091,
	), nil
}

func RemoteWriteEndpoint(addr string) string { return fmt.Sprintf("http://%s/api/v1/receive", addr) }

func NewReceiver(sharedDir string, networkName string, name string, replicationFactor int, hashring ...receive.HashringConfig) (*Service, error) {
	localEndpoint := NewService(fmt.Sprintf("receive-%v", name), "", e2e.NewCommand("", ""), nil, 80, 9091, 81).GRPCNetworkEndpointFor(networkName)
	if len(hashring) == 0 {
		hashring = []receive.HashringConfig{{Endpoints: []string{localEndpoint}}}
	}

	dir := filepath.Join(sharedDir, "data", "receive", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "receive", name)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	b, err := json.Marshal(hashring)
	if err != nil {
		return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
	}

	if err := ioutil.WriteFile(filepath.Join(dir, "hashrings.json"), b, 0666); err != nil {
		return nil, errors.Wrap(err, "creating receive config")
	}

	return NewService(
		fmt.Sprintf("receive-%v", name),
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":                              fmt.Sprintf("receive-%v", name),
			"--grpc-address":                            ":9091",
			"--grpc-grace-period":                       "0s",
			"--http-address":                            ":80",
			"--remote-write.address":                    ":81",
			"--label":                                   fmt.Sprintf(`receive="%s"`, name),
			"--tsdb.path":                               container,
			"--log.level":                               logLevel,
			"--receive.replication-factor":              strconv.Itoa(replicationFactor),
			"--receive.local-endpoint":                  localEndpoint,
			"--receive.hashrings-file":                  filepath.Join(container, "hashrings.json"),
			"--receive.hashrings-file-refresh-interval": "5s",
		})...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
		9091,
		81,
	), nil
}

func NewRuler(sharedDir string, name string, ruleSubDir string, amCfg []alert.AlertmanagerConfig, queryCfg []query.Config) (*Service, error) {
	dir := filepath.Join(sharedDir, "data", "rule", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "rule", name)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create rule dir")
	}

	amCfgBytes, err := yaml.Marshal(alert.AlertingConfig{
		Alertmanagers: amCfg,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "generate am file: %v", amCfg)
	}

	queryCfgBytes, err := yaml.Marshal(queryCfg)
	if err != nil {
		return nil, errors.Wrapf(err, "generate query file: %v", queryCfg)
	}

	return NewService(
		fmt.Sprintf("rule-%v", name),
		DefaultImage(),
		e2e.NewCommand("rule", e2e.BuildArgs(map[string]string{
			"--debug.name":                    fmt.Sprintf("rule-%v", name),
			"--grpc-address":                  ":9091",
			"--grpc-grace-period":             "0s",
			"--http-address":                  ":80",
			"--label":                         fmt.Sprintf(`replica="%s"`, name),
			"--data-dir":                      container,
			"--rule-file":                     filepath.Join(e2e.ContainerSharedDir, ruleSubDir, "*.yaml"),
			"--eval-interval":                 "1s",
			"--alertmanagers.config":          string(amCfgBytes),
			"--alertmanagers.sd-dns-interval": "1s",
			"--log.level":                     logLevel,
			"--query.config":                  string(queryCfgBytes),
			"--query.sd-dns-interval":         "1s",
			"--resend-delay":                  "5s",
		})...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
		9091,
	), nil
}

func NewAlertmanager(sharedDir string, name string) (*e2e.HTTPService, error) {
	dir := filepath.Join(sharedDir, "data", "am", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "am", name)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create am dir")
	}
	const config = `
route:
  group_by: ['alertname']
  group_wait: 1s
  group_interval: 1s
  receiver: 'null'
receivers:
- name: 'null'
`
	if err := ioutil.WriteFile(filepath.Join(dir, "config.yaml"), []byte(config), 0666); err != nil {
		return nil, errors.Wrap(err, "creating alertmanager config file failed")
	}

	s := e2e.NewHTTPService(
		fmt.Sprintf("alertmanager-%v", name),
		DefaultAlertmanagerImage(),
		e2e.NewCommandWithoutEntrypoint("/bin/alertmanager", e2e.BuildArgs(map[string]string{
			"--config.file":        filepath.Join(container, "config.yaml"),
			"--web.listen-address": "0.0.0.0:80",
			"--log.level":          logLevel,
			"--storage.path":       container,
		})...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
	)
	s.SetUser("root")
	return s, nil
}

func NewStoreGW(sharedDir string, name string, bucketConfig client.BucketConfig, relabelConfig ...relabel.Config) (*Service, error) {
	dir := filepath.Join(sharedDir, "data", "store", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "store", name)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create store dir")
	}

	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate store config file: %v", bucketConfig)
	}

	relabelConfigBytes, err := yaml.Marshal(relabelConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate store relabel file: %v", relabelConfig)
	}

	return NewService(
		fmt.Sprintf("store-gw-%v", name),
		DefaultImage(),
		e2e.NewCommand("store", append(e2e.BuildArgs(map[string]string{
			"--debug.name":        fmt.Sprintf("store-gw-%v", name),
			"--grpc-address":      ":9091",
			"--grpc-grace-period": "0s",
			"--http-address":      ":80",
			"--log.level":         logLevel,
			"--data-dir":          container,
			"--objstore.config":   string(bktConfigBytes),
			// Accelerated sync time for quicker test (3m by default).
			"--sync-block-duration":     "1s",
			"--selector.relabel-config": string(relabelConfigBytes),
			"--consistency-delay":       "30m",
		}), "--experimental.enable-index-header")...),
		e2e.NewReadinessProbe(80, "/-/ready", 200),
		80,
		9091,
	), nil
}
