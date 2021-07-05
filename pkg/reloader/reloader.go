// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package reloader contains helpers to trigger reloads of Prometheus instances
// on configuration changes and to substitute environment variables in config files.
//
// Reloader type is useful when you want to:
//
// 	* Watch on changes against certain file e.g (`cfgFile`).
// 	* Optionally, specify different different output file for watched `cfgFile` (`cfgOutputFile`).
// 	This will also try decompress the `cfgFile` if needed and substitute ALL the envvars using Kubernetes substitution format: (`$(var)`)
// 	* Watch on changes against certain directories (`watchedDirs`).
//
// Once any of those two changes, Prometheus on given `reloadURL` will be notified, causing Prometheus to reload configuration and rules.
//
// This and below for reloader:
//
// 	u, _ := url.Parse("http://localhost:9090")
// 	rl := reloader.New(nil, nil, &reloader.Options{
// 		ReloadURL:     reloader.ReloadURLFromBase(u),
// 		CfgFile:       "/path/to/cfg",
// 		CfgOutputFile: "/path/to/cfg.out",
// 		WatchedDirs:      []string{"/path/to/dirs"},
// 		WatchInterval: 3 * time.Minute,
// 		RetryInterval: 5 * time.Second,
//  })
//
// The url of reloads can be generated with function ReloadURLFromBase().
// It will append the default path of reload into the given url:
//
// 	u, _ := url.Parse("http://localhost:9090")
// 	reloader.ReloadURLFromBase(u) // It will return "http://localhost:9090/-/reload"
//
// Start watching changes and stopped until the context gets canceled:
//
// 	ctx, cancel := context.WithCancel(context.Background())
// 	go func() {
// 		if err := rl.Watch(ctx); err != nil {
// 			log.Fatal(err)
// 		}
// 	}()
// 	// ...
// 	cancel()
//
// Reloader will make a schedule to check the given config files and dirs of sum of hash with the last result,
// even if it is no changes.
//
// A basic example of configuration template with environment variables:
//
//   global:
//     external_labels:
//       replica: '$(HOSTNAME)'
package reloader

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// Reloader can watch config files and trigger reloads of a Prometheus server.
// It optionally substitutes environment variables in the configuration.
// Referenced environment variables must be of the form `$(var)` (not `$var` or `${var}`).
type Reloader struct {
	logger        log.Logger
	reloadURL     *url.URL
	HTTPClient    http.Client
	cfgFile       string
	cfgOutputFile string
	watchInterval time.Duration
	retryInterval time.Duration
	watchedDirs   []string
	watcher       *watcher

	lastCfgHash         []byte
	lastWatchedDirsHash []byte

	reloads           prometheus.Counter
	reloadErrors      prometheus.Counter
	configApplyErrors prometheus.Counter
	configApply       prometheus.Counter
}

// Options bundles options for the Reloader.
type Options struct {
	// ReloadURL is a prometheus URL to trigger reloads.
	ReloadURL *url.URL
	// CfgFile is a path to the prometheus config file to watch.
	CfgFile string
	// CfgOutputFile is a path for the output config file.
	// If cfgOutputFile is not empty the config file will be decompressed if needed, environment variables
	// will be substituted and the output written into the given path. Prometheus should then use
	// cfgOutputFile as its config file path.
	CfgOutputFile string
	// WatchedDirs is a collection of paths for the reloader to watch over.
	WatchedDirs []string
	// DelayInterval controls how long the reloader will wait without receiving
	// new file-system events before it applies the reload.
	DelayInterval time.Duration
	// WatchInterval controls how often reloader re-reads config and directories.
	WatchInterval time.Duration
	// RetryInterval controls how often the reloader retries a reloading of the
	// configuration in case the endpoint returned an error.
	RetryInterval time.Duration
}

var firstGzipBytes = []byte{0x1f, 0x8b, 0x08}

// New creates a new reloader that watches the given config file and directories
// and triggers a Prometheus reload upon changes.
func New(logger log.Logger, reg prometheus.Registerer, o *Options) *Reloader {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	r := &Reloader{
		logger:        logger,
		reloadURL:     o.ReloadURL,
		cfgFile:       o.CfgFile,
		cfgOutputFile: o.CfgOutputFile,
		watcher:       newWatcher(logger, reg, o.DelayInterval),
		watchedDirs:   o.WatchedDirs,
		watchInterval: o.WatchInterval,
		retryInterval: o.RetryInterval,

		reloads: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_reloads_total",
				Help: "Total number of reload requests.",
			},
		),
		reloadErrors: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_reloads_failed_total",
				Help: "Total number of reload requests that failed.",
			},
		),
		configApply: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_config_apply_operations_total",
				Help: "Total number of config apply operations.",
			},
		),
		configApplyErrors: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_config_apply_operations_failed_total",
				Help: "Total number of config apply operations that failed.",
			},
		),
	}
	return r
}

// Watch detects any change made to the watched config file and directories. It
// returns when the context is canceled.
// Whenever a filesystem change is detected or the watch interval has elapsed,
// the reloader expands the config file (if cfgOutputFile is specified) and
// triggers a reload if the configuration file or files in the watched
// directories have changed.
// Because some edge cases might be missing, the reloader also relies on the
// watch interval.
func (r *Reloader) Watch(ctx context.Context) error {
	if r.cfgFile == "" && len(r.watchedDirs) == 0 {
		level.Info(r.logger).Log("msg", "nothing to be watched")
		<-ctx.Done()
		return nil
	}

	defer runutil.CloseWithLogOnErr(r.logger, r.watcher, "config watcher close")

	if r.cfgFile != "" {
		if err := r.watcher.addFile(r.cfgFile); err != nil {
			return errors.Wrapf(err, "add config file %s to watcher", r.cfgFile)
		}

		if err := r.apply(ctx); err != nil {
			return err
		}
	}

	if r.watchInterval == 0 {
		// Skip watching the file-system.
		return nil
	}

	for _, dir := range r.watchedDirs {
		if err := r.watcher.addDirectory(dir); err != nil {
			return errors.Wrapf(err, "add directory %s to watcher", dir)
		}
	}

	// Start watching the file-system.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		r.watcher.run(ctx)
		wg.Done()
	}()

	level.Info(r.logger).Log(
		"msg", "started watching config file and directories for changes",
		"cfg", r.cfgFile,
		"out", r.cfgOutputFile,
		"dirs", strings.Join(r.watchedDirs, ","))

	applyCtx, applyCancel := context.WithTimeout(ctx, r.watchInterval)

	for {
		select {
		case <-applyCtx.Done():
			if ctx.Err() != nil {
				applyCancel()
				wg.Wait()
				return nil
			}
		case <-r.watcher.notify:
		}

		// Reset the watch timeout.
		applyCancel()
		applyCtx, applyCancel = context.WithTimeout(ctx, r.watchInterval)

		r.configApply.Inc()
		if err := r.apply(applyCtx); err != nil {
			r.configApplyErrors.Inc()
			level.Error(r.logger).Log("msg", "apply error", "err", err)
		}
	}
}

// apply triggers Prometheus reload if rules or config changed. If cfgOutputFile is set, we also
// expand env vars into config file before reloading.
// Reload is retried in retryInterval until watchInterval.
func (r *Reloader) apply(ctx context.Context) error {
	var (
		cfgHash         []byte
		watchedDirsHash []byte
	)
	if r.cfgFile != "" {
		h := sha256.New()
		if err := hashFile(h, r.cfgFile); err != nil {
			return errors.Wrap(err, "hash file")
		}
		cfgHash = h.Sum(nil)
		if r.cfgOutputFile != "" {
			b, err := ioutil.ReadFile(r.cfgFile)
			if err != nil {
				return errors.Wrap(err, "read file")
			}

			// Detect and extract gzipped file.
			if bytes.Equal(b[0:3], firstGzipBytes) {
				zr, err := gzip.NewReader(bytes.NewReader(b))
				if err != nil {
					return errors.Wrap(err, "create gzip reader")
				}
				defer runutil.CloseWithLogOnErr(r.logger, zr, "gzip reader close")

				b, err = ioutil.ReadAll(zr)
				if err != nil {
					return errors.Wrap(err, "read compressed config file")
				}
			}

			b, err = expandEnv(b)
			if err != nil {
				return errors.Wrap(err, "expand environment variables")
			}

			tmpFile := r.cfgOutputFile + ".tmp"
			defer func() {
				_ = os.Remove(tmpFile)
			}()
			if err := ioutil.WriteFile(tmpFile, b, 0644); err != nil {
				return errors.Wrap(err, "write file")
			}
			if err := os.Rename(tmpFile, r.cfgOutputFile); err != nil {
				return errors.Wrap(err, "rename file")
			}
		}
	}

	h := sha256.New()
	for _, dir := range r.watchedDirs {
		walkDir, err := filepath.EvalSymlinks(dir)
		if err != nil {
			return errors.Wrap(err, "dir symlink eval")
		}
		err = filepath.Walk(walkDir, func(path string, f os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// filepath.Walk uses Lstat to retrieve os.FileInfo. Lstat does not
			// follow symlinks. Make sure to follow a symlink before checking
			// if it is a directory.
			targetFile, err := os.Stat(path)
			if err != nil {
				return err
			}

			if targetFile.IsDir() {
				return nil
			}

			if err := hashFile(h, path); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "build hash")
		}
	}
	if len(r.watchedDirs) > 0 {
		watchedDirsHash = h.Sum(nil)
	}

	if bytes.Equal(r.lastCfgHash, cfgHash) && bytes.Equal(r.lastWatchedDirsHash, watchedDirsHash) {
		// Nothing to do.
		return nil
	}

	if err := runutil.RetryWithLog(r.logger, r.retryInterval, ctx.Done(), func() error {
		if r.watchInterval == 0 {
			return nil
		}
		r.reloads.Inc()
		if err := r.triggerReload(ctx); err != nil {
			r.reloadErrors.Inc()
			return errors.Wrap(err, "trigger reload")
		}

		r.lastCfgHash = cfgHash
		r.lastWatchedDirsHash = watchedDirsHash
		level.Info(r.logger).Log(
			"msg", "Reload triggered",
			"cfg_in", r.cfgFile,
			"cfg_out", r.cfgOutputFile,
			"watched_dirs", strings.Join(r.watchedDirs, ", "))
		return nil
	}); err != nil {
		level.Error(r.logger).Log("msg", "Failed to trigger reload. Retrying.", "err", err)
	}

	return nil
}

func hashFile(h hash.Hash, fn string) error {
	f, err := os.Open(filepath.Clean(fn))
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, f, "close file")

	if _, err := h.Write([]byte{'\xff'}); err != nil {
		return err
	}
	if _, err := h.Write([]byte(fn)); err != nil {
		return err
	}
	if _, err := h.Write([]byte{'\xff'}); err != nil {
		return err
	}

	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	return nil
}

func (r *Reloader) triggerReload(ctx context.Context) error {
	req, err := http.NewRequest("POST", r.reloadURL.String(), nil)
	if err != nil {
		return errors.Wrap(err, "create request")
	}
	req = req.WithContext(ctx)

	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "reload request failed")
	}
	defer runutil.ExhaustCloseWithLogOnErr(r.logger, resp.Body, "trigger reload resp body")

	if resp.StatusCode != 200 {
		return errors.Errorf("received non-200 response: %s; have you set `--web.enable-lifecycle` Prometheus flag?", resp.Status)
	}
	return nil
}

// ReloadURLFromBase returns the standard Prometheus reload URL from its base URL.
func ReloadURLFromBase(u *url.URL) *url.URL {
	r := *u
	r.Path = path.Join(r.Path, "/-/reload")
	return &r
}

var envRe = regexp.MustCompile(`\$\(([a-zA-Z_0-9]+)\)`)

func expandEnv(b []byte) (r []byte, err error) {
	r = envRe.ReplaceAllFunc(b, func(n []byte) []byte {
		if err != nil {
			return nil
		}
		n = n[2 : len(n)-1]

		v, ok := os.LookupEnv(string(n))
		if !ok {
			err = errors.Errorf("found reference to unset environment variable %q", n)
			return nil
		}
		return []byte(v)
	})
	return r, err
}

type watcher struct {
	notify chan struct{}

	w             *fsnotify.Watcher
	watchedDirs   map[string]struct{}
	delayInterval time.Duration

	logger       log.Logger
	watchedItems prometheus.Gauge
	watchEvents  prometheus.Counter
	watchErrors  prometheus.Counter
}

func newWatcher(logger log.Logger, reg prometheus.Registerer, delayInterval time.Duration) *watcher {
	return &watcher{
		logger:        logger,
		delayInterval: delayInterval,
		notify:        make(chan struct{}),
		watchedDirs:   make(map[string]struct{}),

		watchedItems: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "reloader_watches",
				Help: "Number of resources watched by the reloader.",
			},
		),
		watchEvents: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_watch_events_total",
				Help: "Total number of events received by the reloader from the watcher.",
			},
		),
		watchErrors: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "reloader_watch_errors_total",
				Help: "Total number of errors received by the reloader from the watcher.",
			},
		),
	}
}

// Close implements the io.Closer interface.
func (w *watcher) Close() error {
	if w.w == nil {
		return nil
	}
	watcher := w.w
	w.w = nil
	return watcher.Close()
}

func (w *watcher) addPath(name string) error {
	if w.w == nil {
		fsWatcher, err := fsnotify.NewWatcher()
		if err != nil {
			return errors.Wrap(err, "create watcher")
		}
		w.w = fsWatcher
	}

	if err := w.w.Add(name); err != nil {
		return err
	}

	w.watchedDirs[name] = struct{}{}
	w.watchedItems.Set(float64(len(w.watchedDirs)))

	return nil
}

func (w *watcher) addDirectory(name string) error {
	w.watchedDirs[name] = struct{}{}
	return w.addPath(name)
}

func (w *watcher) addFile(name string) error {
	w.watchedDirs[filepath.Dir(name)] = struct{}{}
	return w.addPath(name)
}

func (w *watcher) run(ctx context.Context) {
	defer runutil.CloseWithLogOnErr(w.logger, w.w, "config watcher close")

	var (
		wg     sync.WaitGroup
		notify = make(chan struct{})
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var (
			delayCtx context.Context
			cancel   context.CancelFunc
		)

		for {
			select {
			case <-ctx.Done():
				if cancel != nil {
					cancel()
				}
				return

			case <-notify:
				if cancel != nil {
					cancel()
				}

				delayCtx, cancel = context.WithCancel(ctx)

				wg.Add(1)
				go func(ctx context.Context) {
					defer wg.Done()

					if w.delayInterval > 0 {
						t := time.NewTicker(w.delayInterval)
						defer t.Stop()

						select {
						case <-ctx.Done():
							return
						case <-t.C:
						}
					}

					select {
					case w.notify <- struct{}{}:
					case <-ctx.Done():
					}
				}(delayCtx)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return

		case event := <-w.w.Events:
			w.watchEvents.Inc()
			if _, ok := w.watchedDirs[filepath.Dir(event.Name)]; ok {
				select {
				case notify <- struct{}{}:
				default:
				}
			}

		case err := <-w.w.Errors:
			w.watchErrors.Inc()
			level.Error(w.logger).Log("msg", "watch error", "err", err)
		}
	}
}
