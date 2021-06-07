package remotewrite

import (
	"time"

	"github.com/pkg/errors"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"gopkg.in/yaml.v2"
)

// Config represents a remote write configuration for Thanos stateless ruler.
type Config struct {
	Name        string                    `yaml:"name"`
	RemoteStore *config.RemoteWriteConfig `yaml:"remote_write,omitempty"`
}

// LoadRemoteWriteConfig prepares a Config instance from a given YAML config.
func LoadRemoteWriteConfig(configYAML []byte) (Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(configYAML, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// NewFanoutStorage creates a storage that fans-out to both the WAL and a configured remote storage.
// The remote storage tails the WAL and sends the metrics it reads using Prometheus' remote_write.
func NewFanoutStorage(logger log.Logger, reg prometheus.Registerer, walDir string, rwConfig Config) (storage.Storage, error) {
	walStore, err := NewStorage(logger, reg, walDir)
	if err != nil {
		return nil, err
	}
	remoteStore := remote.NewStorage(logger, reg, walStore.StartTime, walStore.Directory(), 1*time.Minute, nil)
	if err := remoteStore.ApplyConfig(&config.Config{
		GlobalConfig:       config.DefaultGlobalConfig,
		RemoteWriteConfigs: []*config.RemoteWriteConfig{rwConfig.RemoteStore},
	}); err != nil {
		return nil, errors.Wrap(err, "applying config to remote storage")
	}
	return storage.NewFanout(logger, walStore, remoteStore), nil
}
