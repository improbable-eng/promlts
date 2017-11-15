package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"os"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/okgroup"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/shipper"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

func registerSidecar(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "sidecar for Prometheus server")

	apiAddr := cmd.Flag("api-address", "listen host:port address for the store API").
		Default("0.0.0.0:19090").String()

	metricsAddr := cmd.Flag("metrics-address", "metrics host:port address for the sidecar").
		Default("0.0.0.0:19091").String()

	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API").
		Default("http://localhost:9090").URL()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").String()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for clutser").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	m[name] = func(logger log.Logger, reg *prometheus.Registry) (okgroup.Group, error) {
		joinConfig, err := createJoinConfig(
			logger,
			*clusterBindAddr,
			os.ExpandEnv(*clusterAdvertiseAddr),
			*peers,
			false,
		)
		if err != nil {
			return okgroup.Group{}, errors.Wrap(err, "create join cluster config")
		}
		return runSidecar(logger, reg, *apiAddr, *metricsAddr, *promURL, *dataDir, joinConfig, *gcsBucket)
	}
}

func runSidecar(
	logger log.Logger,
	reg *prometheus.Registry,
	apiAddr string,
	metricsAddr string,
	promURL *url.URL,
	dataDir string,
	joinConfig cluster.JoinConfig,
	gcsBucket string,
) (okgroup.Group, error) {

	var g okgroup.Group
	externalLabels := &extLabelSet{promURL: promURL}

	// Blocking query of external labels before anything else.
	// We retry infinitely until we reach and fetch labels from our Prometheus.
	{
		ctx := context.Background()
		err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
			err := externalLabels.Update(ctx)
			if err != nil {
				level.Warn(logger).Log(
					"msg", "failed to fetch initial external labels. Retrying",
					"err", err,
				)
			}
			return err
		})
		if err != nil {
			return g, errors.Wrap(err, "initial external labels query")
		}
	}

	_, err := cluster.Join(logger, joinConfig, cluster.PeerState{
		Type:    cluster.PeerTypeStore,
		APIAddr: apiAddr,
		Labels:  externalLabels.GetPB(),
	}, reg)
	if err != nil {
		return g, errors.Wrap(err, "join cluster")
	}

	// Setup all the concurrent groups.
	{
		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)

		l, err := net.Listen("tcp", metricsAddr)
		if err != nil {
			return g, errors.Wrap(err, "listen metrics address")
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve metrics")
		}, func(error) {
			l.Close()
		})
	}
	{
		l, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return g, errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", "proxy")

		var client http.Client

		proxy, err := store.NewPrometheusProxy(
			logger, prometheus.DefaultRegisterer, &client, promURL, externalLabels.Get)
		if err != nil {
			return g, errors.Wrap(err, "create Prometheus proxy")
		}

		s := grpc.NewServer()
		storepb.RegisterStoreServer(s, proxy)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			l.Close()
		})
	}
	// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
	// the external labels we apply.
	{
		promUp := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_prometheus_up",
			Help: "Boolean indicator whether the sidecar can reach its Prometheus peer.",
		})
		lastHeartbeat := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_last_heartbeat_success_time_seconds",
			Help: "Second timestamp of the last successful heartbeat.",
		})
		reg.MustRegister(promUp, lastHeartbeat)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer iterCancel()

				err := externalLabels.Update(iterCtx)
				if err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					promUp.Set(1)
					lastHeartbeat.Set(float64(time.Now().Unix()))
				}

				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	if gcsBucket != "" {
		// The background shipper continuously scans the data directory and uploads
		// new found blocks to Google Cloud Storage.
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return g, errors.Wrap(err, "create GCS client")
		}

		remote := shipper.NewGCSRemote(logger, nil, gcsClient.Bucket(gcsBucket))
		s := shipper.New(logger, nil, dataDir, remote, externalLabels.Get)

		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer gcsClient.Close()

			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				s.Sync(ctx)
				return nil
			})
		}, func(error) {
			cancel()
		})
	} else {
		level.Info(logger).Log("msg", "No GCS bucket were configured, GCS uploads will be disabled")
	}

	level.Info(logger).Log("msg", "starting sidecar")
	return g, nil
}

type extLabelSet struct {
	promURL *url.URL

	mtx    sync.Mutex
	labels labels.Labels
}

func (s *extLabelSet) Update(ctx context.Context) error {
	elset, err := queryExternalLabels(ctx, s.promURL)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	s.labels = elset
	s.mtx.Unlock()

	return nil
}

func (s *extLabelSet) Get() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.labels
}

func (s *extLabelSet) GetPB() []storepb.Label {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := make([]storepb.Label, 0, len(s.labels))
	for _, l := range s.labels {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return lset
}

func queryExternalLabels(ctx context.Context, base *url.URL) (labels.Labels, error) {
	u := *base
	u.Path = path.Join(u.Path, "/api/v1/status/config")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrapf(err, "request config against %s", u.String())
	}
	defer resp.Body.Close()

	var d struct {
		Data struct {
			YAML string `json:"yaml"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, errors.Wrap(err, "decode response")
	}
	var cfg struct {
		Global struct {
			ExternalLabels map[string]string `yaml:"external_labels"`
		} `yaml:"global"`
	}
	if err := yaml.Unmarshal([]byte(d.Data.YAML), &cfg); err != nil {
		return nil, errors.Wrap(err, "parse Prometheus config")
	}
	return labels.FromMap(cfg.Global.ExternalLabels), nil
}
