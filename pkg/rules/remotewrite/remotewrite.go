package remotewrite

import (
	"errors"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"gopkg.in/yaml.v2"
	"sync"
	"time"
)

var (
	managerMtx            sync.Mutex
)

type Config struct {
	Name string `yaml:"name"`
	RemoteStore *config.RemoteWriteConfig `yaml:"remote_write,omitempty"`
	ScrapeConfig *config.ScrapeConfig `yaml:"scrape_config,omitempty"`
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
	scrapeMgr := &readyScrapeManager{}
	remoteStore := remote.NewStorage(logger, reg, walStore.StartTime, walStore.Directory(), 1*time.Minute, scrapeMgr)
	err = remoteStore.ApplyConfig(&config.Config{
		GlobalConfig:       config.DefaultGlobalConfig,
		RemoteWriteConfigs: []*config.RemoteWriteConfig{rwConfig.RemoteStore},
	})
	if err != nil {
		return nil, fmt.Errorf("failed applying config to remote storage: %w", err)
	}
	fanoutStorage := storage.NewFanout(logger, walStore, remoteStore)

	scrapeManager := newScrapeManager(log.With(logger, "component", "scrape manager"), fanoutStorage)
	err = scrapeManager.ApplyConfig(&config.Config{
		GlobalConfig:  config.DefaultGlobalConfig,
		ScrapeConfigs: []*config.ScrapeConfig{rwConfig.ScrapeConfig},
	})
	if err != nil {
		return nil, fmt.Errorf("failed applying config to scrape manager: %w", err)
	}
	return fanoutStorage, nil
}

func newScrapeManager(logger log.Logger, app storage.Appendable) *scrape.Manager {
	// scrape.NewManager modifies a global variable in Prometheus. To avoid a
	// data race of modifying that global, we lock a mutex here briefly.
	managerMtx.Lock()
	defer managerMtx.Unlock()
	return scrape.NewManager(logger, app)
}

// ErrNotReady is returned when the scrape manager is used but has not been
// initialized yet.
var ErrNotReady = errors.New("scrape manager not ready")

// readyScrapeManager allows a scrape manager to be retrieved. Even if it's set at a later point in time.
type readyScrapeManager struct {
	mtx sync.RWMutex
	m   *scrape.Manager
}

// Set the scrape manager.
func (rm *readyScrapeManager) Set(m *scrape.Manager) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	rm.m = m
}

// Get the scrape manager. If is not ready, return an error.
func (rm *readyScrapeManager) Get() (*scrape.Manager, error) {
	rm.mtx.RLock()
	defer rm.mtx.RUnlock()

	if rm.m != nil {
		return rm.m, nil
	}

	return nil, ErrNotReady
}
