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
	"strings"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/relabel"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/rules/remotewrite"
)

const infoLogLevel = "info"

// Same as default for now.
var defaultBackoffConfig = util.BackoffConfig{
	MinBackoff: 300 * time.Millisecond,
	MaxBackoff: 600 * time.Millisecond,
	MaxRetries: 50,
}

const (
	FeatureExemplarStorage = "exemplar-storage"
)

// TODO(bwplotka): Run against multiple?
func DefaultPrometheusImage() string {
	return "quay.io/prometheus/prometheus:v2.26.0"
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

func NewPrometheus(sharedDir string, name string, config string, promImage string, enableFeatures ...string) (*e2e.HTTPService, string, error) {
	dir := filepath.Join(sharedDir, "data", "prometheus", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "prometheus", name)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, "", errors.Wrap(err, "create prometheus dir")
	}

	if err := ioutil.WriteFile(filepath.Join(dir, "prometheus.yml"), []byte(config), 0600); err != nil {
		return nil, "", errors.Wrap(err, "creating prom config failed")
	}

	args := e2e.BuildArgs(map[string]string{
		"--config.file":                     filepath.Join(container, "prometheus.yml"),
		"--storage.tsdb.path":               container,
		"--storage.tsdb.max-block-duration": "2h",
		"--log.level":                       infoLogLevel,
		"--web.listen-address":              ":9090",
	})

	if len(enableFeatures) > 0 {
		args = append(args, fmt.Sprintf("--enable-feature=%s", strings.Join(enableFeatures, ",")))
	}
	prom := e2e.NewHTTPService(
		fmt.Sprintf("prometheus-%s", name),
		promImage,
		e2e.NewCommandWithoutEntrypoint("prometheus", args...),
		e2e.NewHTTPReadinessProbe(9090, "/-/ready", 200, 200),
		9090,
	)
	prom.SetUser(strconv.Itoa(os.Getuid()))
	prom.SetBackoff(defaultBackoffConfig)

	return prom, container, nil
}

func NewPrometheusWithSidecar(sharedDir string, netName string, name string, config, promImage string, enableFeatures ...string) (*e2e.HTTPService, *Service, error) {
	prom, dataDir, err := NewPrometheus(sharedDir, name, config, promImage, enableFeatures...)
	if err != nil {
		return nil, nil, err
	}
	prom.SetBackoff(defaultBackoffConfig)

	sidecar := NewService(
		fmt.Sprintf("sidecar-%s", name),
		DefaultImage(),
		e2e.NewCommand("sidecar", e2e.BuildArgs(map[string]string{
			"--debug.name":        fmt.Sprintf("sidecar-%v", name),
			"--grpc-address":      ":9091",
			"--grpc-grace-period": "0s",
			"--http-address":      ":8080",
			"--prometheus.url":    "http://" + prom.NetworkEndpointFor(netName, 9090),
			"--tsdb.path":         dataDir,
			"--log.level":         infoLogLevel,
		})...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
	)
	sidecar.SetUser(strconv.Itoa(os.Getuid()))
	sidecar.SetBackoff(defaultBackoffConfig)

	return prom, sidecar, nil
}

type QuerierBuilder struct {
	sharedDir      string
	name           string
	routePrefix    string
	externalPrefix string

	storeAddresses       []string
	fileSDStoreAddresses []string
	ruleAddresses        []string
	metadataAddresses    []string
	targetAddresses      []string
	exemplarAddresses    []string

	tracingConfig string
}

func NewQuerierBuilder(sharedDir, name string, storeAddresses []string) *QuerierBuilder {
	return &QuerierBuilder{
		sharedDir:      sharedDir,
		name:           name,
		storeAddresses: storeAddresses,
	}
}

func (q *QuerierBuilder) WithFileSDStoreAddresses(fileSDStoreAddresses []string) *QuerierBuilder {
	q.fileSDStoreAddresses = fileSDStoreAddresses
	return q
}

func (q *QuerierBuilder) WithRuleAddresses(ruleAddresses []string) *QuerierBuilder {
	q.ruleAddresses = ruleAddresses
	return q
}

func (q *QuerierBuilder) WithTargetAddresses(targetAddresses []string) *QuerierBuilder {
	q.targetAddresses = targetAddresses
	return q
}

func (q *QuerierBuilder) WithExemplarAddresses(exemplarAddresses []string) *QuerierBuilder {
	q.exemplarAddresses = exemplarAddresses
	return q
}

func (q *QuerierBuilder) WithMetadataAddresses(metadataAddresses []string) *QuerierBuilder {
	q.metadataAddresses = metadataAddresses
	return q
}

func (q *QuerierBuilder) WithRoutePrefix(routePrefix string) *QuerierBuilder {
	q.routePrefix = routePrefix
	return q
}

func (q *QuerierBuilder) WithExternalPrefix(externalPrefix string) *QuerierBuilder {
	q.externalPrefix = externalPrefix
	return q
}

func (q *QuerierBuilder) WithTracingConfig(tracingConfig string) *QuerierBuilder {
	q.tracingConfig = tracingConfig
	return q
}

func (q *QuerierBuilder) Build() (*Service, error) {
	const replicaLabel = "replica"

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":            fmt.Sprintf("querier-%v", q.name),
		"--grpc-address":          ":9091",
		"--grpc-grace-period":     "0s",
		"--http-address":          ":8080",
		"--query.replica-label":   replicaLabel,
		"--store.sd-dns-interval": "5s",
		"--log.level":             infoLogLevel,
		"--query.max-concurrent":  "1",
		"--store.sd-interval":     "5s",
	})
	for _, addr := range q.storeAddresses {
		args = append(args, "--store="+addr)
	}

	for _, addr := range q.ruleAddresses {
		args = append(args, "--rule="+addr)
	}

	for _, addr := range q.targetAddresses {
		args = append(args, "--target="+addr)
	}

	for _, addr := range q.metadataAddresses {
		args = append(args, "--metadata="+addr)
	}

	for _, addr := range q.exemplarAddresses {
		args = append(args, "--exemplar="+addr)
	}

	if len(q.fileSDStoreAddresses) > 0 {
		queryFileSDDir := filepath.Join(q.sharedDir, "data", "querier", q.name)
		container := filepath.Join(e2e.ContainerSharedDir, "data", "querier", q.name)
		if err := os.MkdirAll(queryFileSDDir, 0750); err != nil {
			return nil, errors.Wrap(err, "create query dir failed")
		}

		fileSD := []*targetgroup.Group{{}}
		for _, a := range q.fileSDStoreAddresses {
			fileSD[0].Targets = append(fileSD[0].Targets, model.LabelSet{model.AddressLabel: model.LabelValue(a)})
		}

		b, err := yaml.Marshal(fileSD)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(queryFileSDDir+"/filesd.yaml", b, 0600); err != nil {
			return nil, errors.Wrap(err, "creating query SD config failed")
		}

		args = append(args, "--store.sd-files="+filepath.Join(container, "filesd.yaml"))
	}

	if q.routePrefix != "" {
		args = append(args, "--web.route-prefix="+q.routePrefix)
	}

	if q.externalPrefix != "" {
		args = append(args, "--web.external-prefix="+q.externalPrefix)
	}

	if q.tracingConfig != "" {
		args = append(args, "--tracing.config="+q.tracingConfig)
	}

	querier := NewService(
		fmt.Sprintf("querier-%v", q.name),
		DefaultImage(),
		e2e.NewCommand("query", args...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
	)
	querier.SetUser(strconv.Itoa(os.Getuid()))
	querier.SetBackoff(defaultBackoffConfig)

	return querier, nil
}

func RemoteWriteEndpoint(addr string) string { return fmt.Sprintf("http://%s/api/v1/receive", addr) }

// NewRoutingAndIngestingReceiver creates a Thanos Receive instances that is configured both for ingesting samples and routing samples to other receivers.
func NewRoutingAndIngestingReceiver(sharedDir string, networkName string, name string, replicationFactor int, hashring ...receive.HashringConfig) (*Service, error) {

	localEndpoint := NewService(fmt.Sprintf("receive-%v", name), "", e2e.NewCommand("", ""), nil, 8080, 9091, 8081).GRPCNetworkEndpointFor(networkName)
	if len(hashring) == 0 {
		hashring = []receive.HashringConfig{{Endpoints: []string{localEndpoint}}}
	}

	dir := filepath.Join(sharedDir, "data", "receive", name)
	dataDir := filepath.Join(dir, "data")
	container := filepath.Join(e2e.ContainerSharedDir, "data", "receive", name)
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	b, err := json.Marshal(hashring)
	if err != nil {
		return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
	}

	receiver := NewService(
		fmt.Sprintf("receive-%v", name),
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":                 fmt.Sprintf("receive-%v", name),
			"--grpc-address":               ":9091",
			"--grpc-grace-period":          "0s",
			"--http-address":               ":8080",
			"--remote-write.address":       ":8081",
			"--label":                      fmt.Sprintf(`receive="%s"`, name),
			"--tsdb.path":                  filepath.Join(container, "data"),
			"--log.level":                  infoLogLevel,
			"--receive.replication-factor": strconv.Itoa(replicationFactor),
			"--receive.local-endpoint":     localEndpoint,
			"--receive.hashrings":          string(b),
		})...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
		8081,
	)
	receiver.SetUser(strconv.Itoa(os.Getuid()))
	receiver.SetBackoff(defaultBackoffConfig)

	return receiver, nil
}

// NewRoutingReceiver creates a Thanos Receive instance that is only configured to route to other receive instances. It has no local storage.
func NewRoutingReceiver(sharedDir string, name string, replicationFactor int, hashring ...receive.HashringConfig) (*Service, error) {

	if len(hashring) == 0 {
		return nil, errors.New("hashring should not be empty for receive-distributor mode")
	}

	dir := filepath.Join(sharedDir, "data", "receive", name)
	dataDir := filepath.Join(dir, "data")
	container := filepath.Join(e2e.ContainerSharedDir, "data", "receive", name)
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	b, err := json.Marshal(hashring)
	if err != nil {
		return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
	}

	receiver := NewService(
		fmt.Sprintf("receive-%v", name),
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":                 fmt.Sprintf("receive-%v", name),
			"--grpc-address":               ":9091",
			"--grpc-grace-period":          "0s",
			"--http-address":               ":8080",
			"--remote-write.address":       ":8081",
			"--label":                      fmt.Sprintf(`receive="%s"`, name),
			"--tsdb.path":                  filepath.Join(container, "data"),
			"--log.level":                  infoLogLevel,
			"--receive.replication-factor": strconv.Itoa(replicationFactor),
			"--receive.hashrings":          string(b),
		})...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
		8081,
	)
	receiver.SetUser(strconv.Itoa(os.Getuid()))
	receiver.SetBackoff(defaultBackoffConfig)

	return receiver, nil
}

// NewIngestingReceiver creates a Thanos Receive instance that is only configured to ingest, not route to other receivers.
func NewIngestingReceiver(sharedDir string, name string) (*Service, error) {
	dir := filepath.Join(sharedDir, "data", "receive", name)
	dataDir := filepath.Join(dir, "data")
	container := filepath.Join(e2e.ContainerSharedDir, "data", "receive", name)
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	receiver := NewService(
		fmt.Sprintf("receive-%v", name),
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":           fmt.Sprintf("receive-%v", name),
			"--grpc-address":         ":9091",
			"--grpc-grace-period":    "0s",
			"--http-address":         ":8080",
			"--remote-write.address": ":8081",
			"--label":                fmt.Sprintf(`receive="%s"`, name),
			"--tsdb.path":            filepath.Join(container, "data"),
			"--log.level":            infoLogLevel,
		})...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
		8081,
	)
	receiver.SetUser(strconv.Itoa(os.Getuid()))
	receiver.SetBackoff(defaultBackoffConfig)

	return receiver, nil
}

func NewRoutingAndIngestingReceiverWithConfigWatcher(sharedDir string, networkName string, name string, replicationFactor int, hashring ...receive.HashringConfig) (*Service, error) {
	localEndpoint := NewService(fmt.Sprintf("receive-%v", name), "", e2e.NewCommand("", ""), nil, 8080, 9091, 8081).GRPCNetworkEndpointFor(networkName)
	if len(hashring) == 0 {
		hashring = []receive.HashringConfig{{Endpoints: []string{localEndpoint}}}
	}

	dir := filepath.Join(sharedDir, "data", "receive", name)
	dataDir := filepath.Join(dir, "data")
	container := filepath.Join(e2e.ContainerSharedDir, "data", "receive", name)
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	b, err := json.Marshal(hashring)
	if err != nil {
		return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
	}

	if err := ioutil.WriteFile(filepath.Join(dir, "hashrings.json"), b, 0600); err != nil {
		return nil, errors.Wrap(err, "creating receive config")
	}

	receiver := NewService(
		fmt.Sprintf("receive-%v", name),
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":                              fmt.Sprintf("receive-%v", name),
			"--grpc-address":                            ":9091",
			"--grpc-grace-period":                       "0s",
			"--http-address":                            ":8080",
			"--remote-write.address":                    ":8081",
			"--label":                                   fmt.Sprintf(`receive="%s"`, name),
			"--tsdb.path":                               filepath.Join(container, "data"),
			"--log.level":                               infoLogLevel,
			"--receive.replication-factor":              strconv.Itoa(replicationFactor),
			"--receive.local-endpoint":                  localEndpoint,
			"--receive.hashrings-file":                  filepath.Join(container, "hashrings.json"),
			"--receive.hashrings-file-refresh-interval": "5s",
		})...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
		8081,
	)
	receiver.SetUser(strconv.Itoa(os.Getuid()))
	receiver.SetBackoff(defaultBackoffConfig)

	return receiver, nil
}

func NewTSDBRuler(sharedDir string, name string, ruleSubDir string, amCfg []alert.AlertmanagerConfig, queryCfg []query.Config) (*Service, error) {
	return newRuler(sharedDir, name, ruleSubDir, amCfg, queryCfg, nil)
}

func NewStatelessRuler(sharedDir string, name string, ruleSubDir string, amCfg []alert.AlertmanagerConfig, queryCfg []query.Config, remoteWriteCfg *remotewrite.Config) (*Service, error) {
	return newRuler(sharedDir, name, ruleSubDir, amCfg, queryCfg, remoteWriteCfg)
}

func newRuler(sharedDir string, name string, ruleSubDir string, amCfg []alert.AlertmanagerConfig, queryCfg []query.Config, remoteWriteCfg *remotewrite.Config) (*Service, error) {
	dir := filepath.Join(sharedDir, "data", "rule", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "rule", name)
	if err := os.MkdirAll(dir, 0750); err != nil {
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

	ruleArgs := map[string]string{
		"--debug.name":                    fmt.Sprintf("rule-%v", name),
		"--grpc-address":                  ":9091",
		"--grpc-grace-period":             "0s",
		"--http-address":                  ":8080",
		"--label":                         fmt.Sprintf(`replica="%s"`, name),
		"--data-dir":                      container,
		"--rule-file":                     filepath.Join(e2e.ContainerSharedDir, ruleSubDir, "*.yaml"),
		"--eval-interval":                 "3s",
		"--alertmanagers.config":          string(amCfgBytes),
		"--alertmanagers.sd-dns-interval": "1s",
		"--log.level":                     infoLogLevel,
		"--query.config":                  string(queryCfgBytes),
		"--query.sd-dns-interval":         "1s",
		"--resend-delay":                  "5s",
	}
	if remoteWriteCfg != nil {
		rwCfgBytes, err := yaml.Marshal(remoteWriteCfg)
		if err != nil {
			return nil, errors.Wrapf(err, "generate remote write config: %v", remoteWriteCfg)
		}
		ruleArgs["--remote-write.config"] = string(rwCfgBytes)
	}

	ruler := NewService(
		fmt.Sprintf("rule-%v", name),
		DefaultImage(),
		e2e.NewCommand("rule", e2e.BuildArgs(ruleArgs)...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
	)
	ruler.SetUser(strconv.Itoa(os.Getuid()))
	ruler.SetBackoff(defaultBackoffConfig)

	return ruler, nil
}

func NewAlertmanager(sharedDir string, name string) (*e2e.HTTPService, error) {
	dir := filepath.Join(sharedDir, "data", "am", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "am", name)
	if err := os.MkdirAll(dir, 0750); err != nil {
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
	if err := ioutil.WriteFile(filepath.Join(dir, "config.yaml"), []byte(config), 0600); err != nil {
		return nil, errors.Wrap(err, "creating alertmanager config file failed")
	}

	s := e2e.NewHTTPService(
		fmt.Sprintf("alertmanager-%v", name),
		DefaultAlertmanagerImage(),
		e2e.NewCommandWithoutEntrypoint("/bin/alertmanager", e2e.BuildArgs(map[string]string{
			"--config.file":         filepath.Join(container, "config.yaml"),
			"--web.listen-address":  "0.0.0.0:8080",
			"--log.level":           infoLogLevel,
			"--storage.path":        container,
			"--web.get-concurrency": "1",
			"--web.timeout":         "2m",
		})...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
	)
	s.SetUser(strconv.Itoa(os.Getuid()))
	s.SetBackoff(defaultBackoffConfig)

	return s, nil
}

func NewStoreGW(sharedDir string, name string, bucketConfig client.BucketConfig, relabelConfig ...relabel.Config) (*Service, error) {
	dir := filepath.Join(sharedDir, "data", "store", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "store", name)
	if err := os.MkdirAll(dir, 0750); err != nil {
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

	store := NewService(
		fmt.Sprintf("store-gw-%v", name),
		DefaultImage(),
		e2e.NewCommand("store", e2e.BuildArgs(map[string]string{
			"--debug.name":        fmt.Sprintf("store-gw-%v", name),
			"--grpc-address":      ":9091",
			"--grpc-grace-period": "0s",
			"--http-address":      ":8080",
			"--log.level":         infoLogLevel,
			"--data-dir":          container,
			"--objstore.config":   string(bktConfigBytes),
			// Accelerated sync time for quicker test (3m by default).
			"--sync-block-duration":               "3s",
			"--block-sync-concurrency":            "1",
			"--store.grpc.series-max-concurrency": "1",
			"--selector.relabel-config":           string(relabelConfigBytes),
			"--consistency-delay":                 "30m",
		})...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
	)
	store.SetUser(strconv.Itoa(os.Getuid()))
	store.SetBackoff(defaultBackoffConfig)

	return store, nil
}

func NewCompactor(sharedDir string, name string, bucketConfig client.BucketConfig, relabelConfig []relabel.Config, extArgs ...string) (*e2e.HTTPService, error) {
	dir := filepath.Join(sharedDir, "data", "compact", name)
	container := filepath.Join(e2e.ContainerSharedDir, "data", "compact", name)

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create compact dir")
	}

	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate compact config file: %v", bucketConfig)
	}

	relabelConfigBytes, err := yaml.Marshal(relabelConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate compact relabel file: %v", relabelConfig)
	}

	compactor := e2e.NewHTTPService(
		fmt.Sprintf("compact-%s", name),
		DefaultImage(),
		e2e.NewCommand("compact", append(e2e.BuildArgs(map[string]string{
			"--debug.name":              fmt.Sprintf("compact-%s", name),
			"--log.level":               infoLogLevel,
			"--data-dir":                container,
			"--objstore.config":         string(bktConfigBytes),
			"--http-address":            ":8080",
			"--block-sync-concurrency":  "20",
			"--selector.relabel-config": string(relabelConfigBytes),
			"--wait":                    "",
		}), extArgs...)...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
	)
	compactor.SetUser(strconv.Itoa(os.Getuid()))
	compactor.SetBackoff(defaultBackoffConfig)

	return compactor, nil
}

func NewQueryFrontend(name string, downstreamURL string, cacheConfig queryfrontend.CacheProviderConfig) (*e2e.HTTPService, error) {
	cacheConfigBytes, err := yaml.Marshal(cacheConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal response cache config file: %v", cacheConfig)
	}

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":                        fmt.Sprintf("query-frontend-%s", name),
		"--http-address":                      ":8080",
		"--query-frontend.downstream-url":     downstreamURL,
		"--log.level":                         infoLogLevel,
		"--query-range.response-cache-config": string(cacheConfigBytes),
	})

	queryFrontend := e2e.NewHTTPService(
		fmt.Sprintf("query-frontend-%s", name),
		DefaultImage(),
		e2e.NewCommand("query-frontend", args...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
	)
	queryFrontend.SetUser(strconv.Itoa(os.Getuid()))
	queryFrontend.SetBackoff(defaultBackoffConfig)

	return queryFrontend, nil
}

func NewMemcached(name string) *e2e.ConcreteService {
	memcached := e2e.NewConcreteService(
		fmt.Sprintf("memcached-%s", name),
		"docker.io/memcached:1.6.3-alpine",
		e2e.NewCommand("memcached", []string{"-m 1024", "-I 1m", "-c 1024", "-v"}...),
		nil,
		11211,
	)
	memcached.SetUser(strconv.Itoa(os.Getuid()))
	memcached.SetBackoff(defaultBackoffConfig)

	return memcached
}

func NewToolsBucketWeb(name string, bucketConfig client.BucketConfig, routePrefix, externalPrefix string) (*Service, error) {
	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate tools bucket web config file: %v", bucketConfig)
	}

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":      fmt.Sprintf("toolsBucketWeb-%s", name),
		"--http-address":    ":8080",
		"--log.level":       infoLogLevel,
		"--objstore.config": string(bktConfigBytes),
	})
	if routePrefix != "" {
		args = append(args, "--web.route-prefix="+routePrefix)
	}

	if externalPrefix != "" {
		args = append(args, "--web.external-prefix="+externalPrefix)
	}

	args = append([]string{"bucket", "web"}, args...)

	toolsBucketWeb := NewService(
		fmt.Sprintf("toolsBucketWeb-%s", name),
		DefaultImage(),
		e2e.NewCommand("tools", args...),
		e2e.NewHTTPReadinessProbe(8080, "/-/ready", 200, 200),
		8080,
		9091,
	)
	toolsBucketWeb.SetUser(strconv.Itoa(os.Getuid()))
	toolsBucketWeb.SetBackoff(defaultBackoffConfig)

	return toolsBucketWeb, nil
}
