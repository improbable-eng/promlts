// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2eutil

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"golang.org/x/sync/errgroup"
)

const (
	defaultPrometheusVersion   = "v2.13.0"
	defaultAlertmanagerVersion = "v0.20.0"
	defaultMinioVersion        = "RELEASE.2018-10-06T00-15-16Z"

	// Space delimited list of versions.
	promVersionsEnvVar    = "THANOS_TEST_PROMETHEUS_VERSIONS"
	alertmanagerBinEnvVar = "THANOS_TEST_ALERTMANAGER_PATH"
	minioBinEnvVar        = "THANOS_TEST_MINIO_PATH"
)

func PrometheusBinary() string {
	return prometheusBin(defaultPrometheusVersion)
}

func prometheusBin(version string) string {
	return fmt.Sprintf("prometheus-%s", version)
}

func AlertmanagerBinary() string {
	b := os.Getenv(alertmanagerBinEnvVar)
	if b == "" {
		return fmt.Sprintf("alertmanager-%s", defaultAlertmanagerVersion)
	}
	return b
}

func MinioBinary() string {
	b := os.Getenv(minioBinEnvVar)
	if b == "" {
		return fmt.Sprintf("minio-%s", defaultMinioVersion)
	}
	return b
}

// Prometheus represents a test instance for integration testing.
// It can be populated with data before being started.
type Prometheus struct {
	dir     string
	db      *tsdb.DB
	prefix  string
	version string

	running            bool
	cmd                *exec.Cmd
	disabledCompaction bool
	addr               string
}

func NewTSDB() (*tsdb.DB, error) {
	dir, err := ioutil.TempDir("", "prometheus-test")
	if err != nil {
		return nil, err
	}
	return tsdb.Open(dir, nil, nil, &tsdb.Options{
		BlockRanges:       []int64{2 * 3600 * 1000},
		RetentionDuration: math.MaxInt64,
	})
}

func ForeachPrometheus(t *testing.T, testFn func(t testing.TB, p *Prometheus)) {
	vers := os.Getenv(promVersionsEnvVar)
	if vers == "" {
		vers = defaultPrometheusVersion
	}

	for _, ver := range strings.Split(vers, " ") {
		if ok := t.Run(ver, func(t *testing.T) {
			p, err := newPrometheus(ver, "")
			testutil.Ok(t, err)

			testFn(t, p)
			testutil.Ok(t, p.Stop())
		}); !ok {
			return
		}
	}
}

// NewPrometheus creates a new test Prometheus instance that will listen on local address.
// DEPRECATED: Use ForeachPrometheus instead.
func NewPrometheus() (*Prometheus, error) {
	return newPrometheus("", "")
}

// NewPrometheusOnPath creates a new test Prometheus instance that will listen on local address and given prefix path.
func NewPrometheusOnPath(prefix string) (*Prometheus, error) {
	return newPrometheus("", prefix)
}

func newPrometheus(version string, prefix string) (*Prometheus, error) {
	if version == "" {
		version = defaultPrometheusVersion
	}

	db, err := NewTSDB()
	if err != nil {
		return nil, err
	}

	// Just touch an empty config file. We don't need to actually scrape anything.
	_, err = os.Create(filepath.Join(db.Dir(), "prometheus.yml"))
	if err != nil {
		return nil, err
	}

	return &Prometheus{
		dir:     db.Dir(),
		db:      db,
		prefix:  prefix,
		version: version,
		addr:    "<prometheus-not-started>",
	}, nil
}

// Start running the Prometheus instance and return.
func (p *Prometheus) Start() error {
	if p.running {
		return errors.New("Already started")
	}

	if err := p.db.Close(); err != nil {
		return err
	}
	return p.start()
}

func (p *Prometheus) start() error {
	p.running = true

	port, err := FreePort()
	if err != nil {
		return err
	}

	var extra []string
	if p.disabledCompaction {
		extra = append(extra,
			"--storage.tsdb.min-block-duration=2h",
			"--storage.tsdb.max-block-duration=2h",
		)
	}
	p.addr = fmt.Sprintf("localhost:%d", port)
	args := append([]string{
		"--storage.tsdb.retention=2d", // Pass retention cause prometheus since 2.8.0 don't show default value for that flags in web/api: https://github.com/prometheus/prometheus/pull/5433.
		"--storage.tsdb.path=" + p.db.Dir(),
		"--web.listen-address=" + p.addr,
		"--web.route-prefix=" + p.prefix,
		"--web.enable-admin-api",
		"--config.file=" + filepath.Join(p.db.Dir(), "prometheus.yml"),
	}, extra...)

	p.cmd = exec.Command(prometheusBin(p.version), args...)
	p.cmd.SysProcAttr = SysProcAttr()

	go func() {
		if b, err := p.cmd.CombinedOutput(); err != nil {
			fmt.Fprintln(os.Stderr, "running Prometheus failed", err)
			fmt.Fprintln(os.Stderr, string(b))
		}
	}()
	time.Sleep(2 * time.Second)

	return nil
}

func (p *Prometheus) WaitPrometheusUp(ctx context.Context) error {
	if !p.running {
		return errors.New("method Start was not invoked.")
	}
	return runutil.Retry(time.Second, ctx.Done(), func() error {
		r, err := http.Get(fmt.Sprintf("http://%s/-/ready", p.addr))
		if err != nil {
			return err
		}

		if r.StatusCode != 200 {
			return errors.Errorf("Got non 200 response: %v", r.StatusCode)
		}
		return nil
	})
}

func (p *Prometheus) Restart() error {
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return errors.Wrap(err, "failed to kill Prometheus. Kill it manually")
	}
	_ = p.cmd.Wait()
	return p.start()
}

// Dir returns TSDB dir.
func (p *Prometheus) Dir() string {
	return p.dir
}

// Addr returns correct address after Start method.
func (p *Prometheus) Addr() string {
	return p.addr + p.prefix
}

func (p *Prometheus) DisableCompaction() {
	p.disabledCompaction = true
}

// SetConfig updates the contents of the config file. By default it is empty.
func (p *Prometheus) SetConfig(s string) (err error) {
	f, err := os.Create(filepath.Join(p.dir, "prometheus.yml"))
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, f, "prometheus config")

	_, err = f.Write([]byte(s))
	return err
}

// Stop terminates Prometheus and clean up its data directory.
func (p *Prometheus) Stop() error {
	if !p.running {
		return nil
	}

	if p.cmd.Process != nil {
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return errors.Wrapf(err, "failed to Prometheus. Kill it manually and clean %s dir", p.db.Dir())
		}
	}
	time.Sleep(time.Second / 2)
	return p.cleanup()
}

func (p *Prometheus) cleanup() error {
	p.running = false
	return os.RemoveAll(p.db.Dir())
}

// Appender returns a new appender to populate the Prometheus instance with data.
// All appenders must be closed before Start is called and no new ones must be opened
// afterwards.
func (p *Prometheus) Appender() tsdb.Appender {
	if p.running {
		panic("Appender must not be called after start")
	}
	return p.db.Appender()
}

// CreateBlock writes a block with the given series and numSamples samples each.
// Samples will be in the time range [mint, maxt).
func CreateBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
) (id ulid.ULID, err error) {
	return createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, false)
}

// CreateBlockWithTombstone is same as CreateBlock but leaves tombstones which mimics the Prometheus local block.
func CreateBlockWithTombstone(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
) (id ulid.ULID, err error) {
	return createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, true)
}

func createBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
	tombstones bool,
) (id ulid.ULID, err error) {
	h, err := tsdb.NewHead(nil, nil, nil, 10000000000)
	if err != nil {
		return id, errors.Wrap(err, "create head block")
	}
	defer runutil.CloseWithErrCapture(&err, h, "TSDB Head")

	var g errgroup.Group
	var timeStepSize = (maxt - mint) / int64(numSamples+1)
	var batchSize = len(series) / runtime.GOMAXPROCS(0)

	for len(series) > 0 {
		l := batchSize
		if len(series) < 1000 {
			l = len(series)
		}
		batch := series[:l]
		series = series[l:]

		g.Go(func() error {
			t := mint

			for i := 0; i < numSamples; i++ {
				app := h.Appender()

				for _, lset := range batch {
					_, err := app.Add(lset, t, rand.Float64())
					if err != nil {
						if rerr := app.Rollback(); rerr != nil {
							err = errors.Wrapf(err, "rollback failed: %v", rerr)
						}

						return errors.Wrap(err, "add sample")
					}
				}
				if err := app.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
				t += timeStepSize
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return id, err
	}
	c, err := tsdb.NewLeveledCompactor(ctx, nil, log.NewNopLogger(), []int64{maxt - mint}, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	id, err = c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}

	bdir := filepath.Join(dir, id.String())
	meta, err := metadata.Read(bdir)
	if err != nil {
		return id, errors.Wrap(err, "read meta")
	}
	meta.Thanos = metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     metadata.TestSource,
	}

	if err = metadata.Write(log.NewNopLogger(), bdir, meta); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	if !tombstones {
		if err = os.Remove(filepath.Join(bdir, "tombstones")); err != nil {
			return id, errors.Wrap(err, "remove tombstones")
		}
	}

	return id, nil
}
