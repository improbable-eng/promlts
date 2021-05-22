package remotewrite

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Name         string                    `yaml:"name"`
	RemoteStore  *config.RemoteWriteConfig `yaml:"remote_write,omitempty"`
}

func LoadRemoteWriteConfig(configYAML []byte) (Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(configYAML, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

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
		return nil, fmt.Errorf("failed applying config to remote storage: %w", err)
	}
	return storage.NewFanout(logger, walStore, remoteStore), nil
}