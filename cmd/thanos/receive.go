package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/thanos-io/thanos/pkg/extflag"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/tsdb"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"google.golang.org/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func registerReceive(m map[string]setupFunc, app *kingpin.Application) {
	comp := component.Receive
	cmd := app.Command(comp.String(), "Accept Prometheus remote write API requests and write to local tsdb (EXPERIMENTAL, this may change drastically without notice)")

	httpBindAddr := regHTTPAddrFlag(cmd)
	grpcBindAddr, grpcCert, grpcKey, grpcClientCA := regGRPCFlags(cmd)

	rwAddress := cmd.Flag("remote-write.address", "Address to listen on for remote write requests.").
		Default("0.0.0.0:19291").String()
	rwServerCert := cmd.Flag("remote-write.server-tls-cert", "TLS Certificate for HTTP server, leave blank to disable TLS").Default("").String()
	rwServerKey := cmd.Flag("remote-write.server-tls-key", "TLS Key for the HTTP server, leave blank to disable TLS").Default("").String()
	rwServerClientCA := cmd.Flag("remote-write.server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").String()
	rwClientCert := cmd.Flag("remote-write.client-tls-cert", "TLS Certificates to use to identify this client to the server").Default("").String()
	rwClientKey := cmd.Flag("remote-write.client-tls-key", "TLS Key for the client's certificate").Default("").String()
	rwClientServerCA := cmd.Flag("remote-write.client-tls-ca", "TLS CA Certificates to use to verify servers").Default("").String()
	rwClientServerName := cmd.Flag("remote-write.client-server-name", "Server name to verify the hostname on the returned gRPC certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").String()

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	labelStrs := cmd.Flag("label", "External labels to announce. This flag will be removed in the future when handling multiple tsdb instances is added.").PlaceHolder("key=\"value\"").Strings()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", false)

	retention := modelDuration(cmd.Flag("tsdb.retention", "How long to retain raw samples on local storage. 0d - disables this retention").Default("15d"))

	hashringsFile := cmd.Flag("receive.hashrings-file", "Path to file that contains the hashring configuration.").
		PlaceHolder("<path>").String()

	refreshInterval := modelDuration(cmd.Flag("receive.hashrings-file-refresh-interval", "Refresh interval to re-read the hashring configuration file. (used as a fallback)").
		Default("5m"))

	local := cmd.Flag("receive.local-endpoint", "Endpoint of local receive node. Used to identify the local node in the hashring configuration.").String()

	tenantHeader := cmd.Flag("receive.tenant-header", "HTTP header to determine tenant for write requests.").Default(receive.DefaultTenantHeader).String()

	replicaHeader := cmd.Flag("receive.replica-header", "HTTP header specifying the replica number of a write request.").Default(receive.DefaultReplicaHeader).String()

	replicationFactor := cmd.Flag("receive.replication-factor", "How many times to replicate incoming write requests.").Default("1").Uint64()

	tsdbBlockDuration := modelDuration(cmd.Flag("tsdb.block-duration", "Duration for local TSDB blocks").Default("2h").Hidden())

	m[comp.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		lset, err := parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		var cw *receive.ConfigWatcher
		if *hashringsFile != "" {
			cw, err = receive.NewConfigWatcher(log.With(logger, "component", "config-watcher"), reg, *hashringsFile, *refreshInterval)
			if err != nil {
				return err
			}
		}

		// Local is empty, so try to generate a local endpoint
		// based on the hostname and the listening port.
		if *local == "" {
			hostname, err := os.Hostname()
			if hostname == "" || err != nil {
				return errors.New("--receive.local-endpoint is empty and host could not be determined.")
			}
			parts := strings.Split(*rwAddress, ":")
			port := parts[len(parts)-1]
			*local = fmt.Sprintf("http://%s:%s/api/v1/receive", hostname, port)
		}

		return runReceive(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			*rwAddress,
			*rwServerCert,
			*rwServerKey,
			*rwServerClientCA,
			*rwClientCert,
			*rwClientKey,
			*rwClientServerCA,
			*rwClientServerName,
			*dataDir,
			objStoreConfig,
			lset,
			*retention,
			cw,
			*local,
			*tenantHeader,
			*replicaHeader,
			*replicationFactor,
			*tsdbBlockDuration,
			comp,
		)
	}
}

func runReceive(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string,
	rwAddress string,
	rwServerCert string,
	rwServerKey string,
	rwServerClientCA string,
	rwClientCert string,
	rwClientKey string,
	rwClientServerCA string,
	rwClientServerName string,
	dataDir string,
	objStoreConfig *extflag.PathOrContent,
	lset labels.Labels,
	retention model.Duration,
	cw *receive.ConfigWatcher,
	endpoint string,
	tenantHeader string,
	replicaHeader string,
	replicationFactor uint64,
	tsdbBlockDuration model.Duration,
	comp component.Component,
) error {
	logger = log.With(logger, "component", "receive")
	level.Warn(logger).Log("msg", "setting up receive; the Thanos receive component is EXPERIMENTAL, it may break significantly without notice")

	tsdbCfg := &tsdb.Options{
		RetentionDuration: retention,
		NoLockfile:        true,
		MinBlockDuration:  tsdbBlockDuration,
		MaxBlockDuration:  tsdbBlockDuration,
		WALCompression:    true,
	}

	localStorage := &tsdb.ReadyStorage{}
	rwTLSConfig, err := defaultTLSServerOpts(log.With(logger, "protocol", "HTTP"), rwServerCert, rwServerKey, rwServerClientCA)
	if err != nil {
		return err
	}
	rwTLSClientConfig, err := defaultTLSClientOpts(logger, rwClientCert, rwClientKey, rwClientServerCA, rwClientServerName)
	if err != nil {
		return err
	}
	webHandler := receive.NewHandler(log.With(logger, "component", "receive-handler"), &receive.Options{
		ListenAddress:     rwAddress,
		Registry:          reg,
		Endpoint:          endpoint,
		TenantHeader:      tenantHeader,
		ReplicaHeader:     replicaHeader,
		ReplicationFactor: replicationFactor,
		Tracer:            tracer,
		TLSConfig:         rwTLSConfig,
		TLSClientConfig:   rwTLSClientConfig,
	})

	statusProber := prober.NewProber(comp, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}
	upload := true
	if len(confContentYaml) == 0 {
		level.Info(logger).Log("msg", "No supported bucket was configured, uploads will be disabled")
		upload = false
	}

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.

	// dbReady signals when TSDB is ready and the Store gRPC server can start.
	dbReady := make(chan struct{}, 1)
	// updateDB signals when TSDB needs to be flushed and updated.
	updateDB := make(chan struct{}, 1)
	// uploadC signals when new blocks should be uploaded.
	uploadC := make(chan struct{}, 1)
	// uploadDone signals when uploading has finished.
	uploadDone := make(chan struct{}, 1)

	level.Debug(logger).Log("msg", "setting up tsdb")
	{
		// TSDB.
		cancel := make(chan struct{})
		startTimeMargin := int64(2 * time.Duration(tsdbCfg.MinBlockDuration).Seconds() * 1000)
		g.Add(func() error {
			defer close(dbReady)
			defer close(uploadC)

			db := receive.NewFlushableStorage(
				dataDir,
				log.With(logger, "component", "tsdb"),
				reg,
				tsdbCfg,
			)

			// Before actually starting, we need to make sure the
			// WAL is flushed. The WAL is flushed after the
			// hashring ring is loaded.
			if err := db.Open(); err != nil {
				return errors.Wrap(err, "opening storage")
			}

			// Before quitting, ensure the WAL is flushed and the DB is closed.
			defer func() {
				if err := db.Flush(); err != nil {
					level.Warn(logger).Log("err", err, "msg", "failed to flush storage")
					return
				}
				if err := db.Close(); err != nil {
					level.Warn(logger).Log("err", err, "msg", "failed to close storage")
					return
				}
			}()

			for {
				select {
				case <-cancel:
					return nil
				case _, ok := <-updateDB:
					if !ok {
						return nil
					}
					if err := db.Flush(); err != nil {
						return errors.Wrap(err, "flushing storage")
					}
					if upload {
						uploadC <- struct{}{}
						<-uploadDone
					}
					level.Info(logger).Log("msg", "tsdb started")
					localStorage.Set(db.Get(), startTimeMargin)
					webHandler.SetWriter(receive.NewWriter(log.With(logger, "component", "receive-writer"), localStorage))
					statusProber.SetReady()
					level.Info(logger).Log("msg", "server is ready to receive web requests.")
					dbReady <- struct{}{}
				}
			}
		}, func(err error) {
			close(cancel)
		},
		)
	}

	level.Debug(logger).Log("msg", "setting up hashring")
	{
		// Note: the hashring configuration watcher
		// is the sender and thus closes the chan.
		// In the single-node case, which has no configuration
		// watcher, we close the chan ourselves.
		updates := make(chan receive.Hashring, 1)
		if cw != nil {
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				receive.HashringFromConfig(ctx, updates, cw)
				return nil
			}, func(error) {
				cancel()
			})
		} else {
			cancel := make(chan struct{})
			g.Add(func() error {
				defer close(updates)
				updates <- receive.SingleNodeHashring(endpoint)
				<-cancel
				return nil
			}, func(error) {
				close(cancel)
			})
		}

		cancel := make(chan struct{})
		g.Add(func() error {
			defer close(updateDB)
			for {
				select {
				case h, ok := <-updates:
					if !ok {
						return nil
					}
					webHandler.SetWriter(nil)
					webHandler.Hashring(h)
					msg := "hashring has changed; server is not ready to receive web requests."
					statusProber.SetNotReady(errors.New(msg))
					level.Info(logger).Log("msg", msg)
					updateDB <- struct{}{}
				case <-cancel:
					return nil
				}
			}
		}, func(err error) {
			close(cancel)
		},
		)
	}

	level.Debug(logger).Log("msg", "setting up http server")
	// Initiate HTTP listener providing metrics endpoint and readiness/liveness probes.
	if err := scheduleHTTPServer(g, logger, reg, statusProber, httpBindAddr, nil, comp); err != nil {
		return errors.Wrap(err, "schedule HTTP server with probes")
	}

	level.Debug(logger).Log("msg", "setting up grpc server")
	{
		var (
			s *grpc.Server
			l net.Listener
		)
		startGRPC := make(chan struct{})
		g.Add(func() error {
			defer close(startGRPC)
			opts, err := defaultGRPCTLSServerOpts(logger, grpcCert, grpcKey, grpcClientCA)
			if err != nil {
				return errors.Wrap(err, "setup gRPC server")
			}

			for range dbReady {
				if s != nil {
					s.Stop()
				}
				l, err = net.Listen("tcp", grpcBindAddr)
				if err != nil {
					return errors.Wrap(err, "listen API address")
				}
				tsdbStore := store.NewTSDBStore(log.With(logger, "component", "thanos-tsdb-store"), nil, localStorage.Get(), component.Receive, lset)
				s = newStoreGRPCServer(logger, &receive.UnRegisterer{Registerer: reg}, tracer, tsdbStore, opts)
				startGRPC <- struct{}{}
			}
			return nil
		}, func(error) {
			if s != nil {
				s.Stop()
			}
		})
		// We need to be able to start and stop the gRPC server
		// whenever the DB changes, thus it needs its own run group.
		g.Add(func() error {
			for range startGRPC {
				level.Info(logger).Log("msg", "listening for StoreAPI gRPC", "address", grpcBindAddr)
				if err := s.Serve(l); err != nil {
					return errors.Wrap(err, "serve gRPC")
				}
			}
			return nil
		}, func(error) {})
	}

	level.Debug(logger).Log("msg", "setting up receive http handler")
	{
		g.Add(
			func() error {
				return errors.Wrap(webHandler.Run(), "error starting web server")
			},
			func(err error) {
				webHandler.Close()
			},
		)
	}

	if upload {
		// The background shipper continuously scans the data directory and uploads
		// new blocks to Google Cloud Storage or an S3-compatible storage service.
		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Sidecar.String())
		if err != nil {
			return err
		}

		s := shipper.New(logger, reg, dataDir, bkt, func() labels.Labels { return lset }, metadata.ReceiveSource)

		// Before starting, ensure any old blocks are uploaded.
		if uploaded, err := s.Sync(context.Background()); err != nil {
			level.Warn(logger).Log("err", err, "failed to upload", uploaded)
		}

		{
			// Run the uploader in a loop.
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
					if uploaded, err := s.Sync(ctx); err != nil {
						level.Warn(logger).Log("err", err, "uploaded", uploaded)
					}

					return nil
				})
			}, func(error) {
				cancel()
			})
		}

		{
			// Upload on demand.
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				// Ensure we clean up everything properly.
				defer func() {
					runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
				}()
				// Before quitting, ensure all blocks are uploaded.
				defer func() {
					<-uploadC
					if uploaded, err := s.Sync(context.Background()); err != nil {
						level.Warn(logger).Log("err", err, "failed to upload", uploaded)
					}
				}()
				defer close(uploadDone)
				for {
					select {
					case <-ctx.Done():
						return nil
					case <-uploadC:
						if uploaded, err := s.Sync(ctx); err != nil {
							level.Warn(logger).Log("err", err, "failed to upload", uploaded)
						}
						uploadDone <- struct{}{}
					}
				}
			}, func(error) {
				cancel()
			})
		}
	}

	level.Info(logger).Log("msg", "starting receiver")
	return nil
}
