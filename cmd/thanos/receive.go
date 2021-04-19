// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/logging"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/tls"
)

func registerReceive(app *extkingpin.App) {
	cmd := app.Command(component.Receive.String(), "Accept Prometheus remote write API requests and write to local tsdb.")

	httpBindAddr, httpGracePeriod := extkingpin.RegisterHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := extkingpin.RegisterGRPCFlags(cmd)

	rwAddress := cmd.Flag("remote-write.address", "Address to listen on for remote write requests.").
		Default("0.0.0.0:19291").String()
	rwServerCert := cmd.Flag("remote-write.server-tls-cert", "TLS Certificate for HTTP server, leave blank to disable TLS.").Default("").String()
	rwServerKey := cmd.Flag("remote-write.server-tls-key", "TLS Key for the HTTP server, leave blank to disable TLS.").Default("").String()
	rwServerClientCA := cmd.Flag("remote-write.server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").String()
	rwClientCert := cmd.Flag("remote-write.client-tls-cert", "TLS Certificates to use to identify this client to the server.").Default("").String()
	rwClientKey := cmd.Flag("remote-write.client-tls-key", "TLS Key for the client's certificate.").Default("").String()
	rwClientServerCA := cmd.Flag("remote-write.client-tls-ca", "TLS CA Certificates to use to verify servers.").Default("").String()
	rwClientServerName := cmd.Flag("remote-write.client-server-name", "Server name to verify the hostname on the returned TLS certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").String()

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	labelStrs := cmd.Flag("label", "External labels to announce. This flag will be removed in the future when handling multiple tsdb instances is added.").PlaceHolder("key=\"value\"").Strings()

	objStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	retention := extkingpin.ModelDuration(cmd.Flag("tsdb.retention", "How long to retain raw samples on local storage. 0d - disables this retention.").Default("15d"))

	hashringsFilePath := cmd.Flag("receive.hashrings-file", "Path to file that contains the hashring configuration. A watcher is initialized to watch changes and update the hashring dynamically.").PlaceHolder("<path>").String()
	hashringsFileContent := cmd.Flag("receive.hashrings", "Alternative to 'receive.hashrings-file' flag (lower priority). Content of file that contains the hashring configuration.").PlaceHolder("<content>").String()

	refreshInterval := extkingpin.ModelDuration(cmd.Flag("receive.hashrings-file-refresh-interval", "Refresh interval to re-read the hashring configuration file. (used as a fallback)").
		Default("5m"))

	localEndpoint := cmd.Flag("receive.local-endpoint", "Endpoint of local receive node. Used to identify the local node in the hashring configuration.").String()

	tenantHeader := cmd.Flag("receive.tenant-header", "HTTP header to determine tenant for write requests.").Default(receive.DefaultTenantHeader).String()

	defaultTenantID := cmd.Flag("receive.default-tenant-id", "Default tenant ID to use when none is provided via a header.").Default(receive.DefaultTenant).String()

	tenantLabelName := cmd.Flag("receive.tenant-label-name", "Label name through which the tenant will be announced.").Default(receive.DefaultTenantLabel).String()

	replicaHeader := cmd.Flag("receive.replica-header", "HTTP header specifying the replica number of a write request.").Default(receive.DefaultReplicaHeader).String()

	replicationFactor := cmd.Flag("receive.replication-factor", "How many times to replicate incoming write requests.").Default("1").Uint64()

	forwardTimeout := extkingpin.ModelDuration(cmd.Flag("receive-forward-timeout", "Timeout for each forward request.").Default("5s").Hidden())

	tsdbMinBlockDuration := extkingpin.ModelDuration(cmd.Flag("tsdb.min-block-duration", "Min duration for local TSDB blocks").Default("2h").Hidden())
	tsdbMaxBlockDuration := extkingpin.ModelDuration(cmd.Flag("tsdb.max-block-duration", "Max duration for local TSDB blocks").Default("2h").Hidden())
	tsdbAllowOverlappingBlocks := cmd.Flag("tsdb.allow-overlapping-blocks", "Allow overlapping blocks, which in turn enables vertical compaction and vertical query merge.").Default("false").Bool()
	walCompression := cmd.Flag("tsdb.wal-compression", "Compress the tsdb WAL.").Default("true").Bool()
	noLockFile := cmd.Flag("tsdb.no-lockfile", "Do not create lockfile in TSDB data directory. In any case, the lockfiles will be deleted on next startup.").Default("false").Bool()

	hashFunc := cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").Enum("SHA256", "")

	ignoreBlockSize := cmd.Flag("shipper.ignore-unequal-block-size", "If true receive will not require min and max block size flags to be set to the same value. Only use this if you want to keep long retention and compaction enabled, as in the worst case it can result in ~2h data loss for your Thanos bucket storage.").Default("false").Hidden().Bool()
	allowOutOfOrderUpload := cmd.Flag("shipper.allow-out-of-order-uploads",
		"If true, shipper will skip failed block uploads in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().Bool()

	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		lset, err := parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		if len(lset) == 0 {
			return errors.New("no external labels configured for receive, uniquely identifying external labels must be configured (ideally with `receive_` prefix); see https://thanos.io/tip/thanos/storage.md#external-labels for details.")
		}

		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions("", reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		tsdbOpts := &tsdb.Options{
			MinBlockDuration:       int64(time.Duration(*tsdbMinBlockDuration) / time.Millisecond),
			MaxBlockDuration:       int64(time.Duration(*tsdbMaxBlockDuration) / time.Millisecond),
			RetentionDuration:      int64(time.Duration(*retention) / time.Millisecond),
			NoLockfile:             *noLockFile,
			WALCompression:         *walCompression,
			AllowOverlappingBlocks: *tsdbAllowOverlappingBlocks,
		}

		// Local is empty, so try to generate a local endpoint
		// based on the hostname and the listening port.
		if *localEndpoint == "" {
			hostname, err := os.Hostname()
			if hostname == "" || err != nil {
				return errors.New("--receive.local-endpoint is empty and host could not be determined.")
			}
			parts := strings.Split(*grpcBindAddr, ":")
			port := parts[len(parts)-1]
			*localEndpoint = fmt.Sprintf("%s:%s", hostname, port)
		}

		return runReceive(
			g,
			logger,
			reg,
			tracer,
			grpcLogOpts, tagOpts,
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
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
			tsdbOpts,
			*ignoreBlockSize,
			lset,
			*hashringsFilePath,
			*hashringsFileContent,
			refreshInterval,
			*localEndpoint,
			*tenantHeader,
			*defaultTenantID,
			*tenantLabelName,
			*replicaHeader,
			*replicationFactor,
			time.Duration(*forwardTimeout),
			*allowOutOfOrderUpload,
			component.Receive,
			metadata.HashFunc(*hashFunc),
		)
	})
}

func runReceive(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string,
	httpGracePeriod time.Duration,
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
	tsdbOpts *tsdb.Options,
	ignoreBlockSize bool,
	lset labels.Labels,
	hashringsFilePath string,
	hashringsFileContent string,
	refreshInterval *model.Duration,
	endpoint string,
	tenantHeader string,
	defaultTenantID string,
	tenantLabelName string,
	replicaHeader string,
	replicationFactor uint64,
	forwardTimeout time.Duration,
	allowOutOfOrderUpload bool,
	comp component.SourceStoreAPI,
	hashFunc metadata.HashFunc,
) error {
	logger = log.With(logger, "component", "receive")
	level.Warn(logger).Log("msg", "setting up receive")
	rwTLSConfig, err := tls.NewServerConfig(log.With(logger, "protocol", "HTTP"), rwServerCert, rwServerKey, rwServerClientCA)
	if err != nil {
		return err
	}
	dialOpts, err := extgrpc.StoreClientGRPCOpts(
		logger,
		reg,
		tracer,
		rwServerCert != "",
		rwServerClientCA == "",
		rwClientCert,
		rwClientKey,
		rwClientServerCA,
		rwClientServerName,
	)
	if err != nil {
		return err
	}

	var bkt objstore.Bucket
	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}
	confPathYaml, err := objStoreConfig.GetPath()
	upload := len(confContentYaml) > 0
	if upload {
		if tsdbOpts.MinBlockDuration != tsdbOpts.MaxBlockDuration {
			if !ignoreBlockSize {
				return errors.Errorf("found that TSDB Max time is %d and Min time is %d. "+
					"Compaction needs to be disabled (tsdb.min-block-duration = tsdb.max-block-duration)", tsdbOpts.MaxBlockDuration, tsdbOpts.MinBlockDuration)
			}
			level.Warn(logger).Log("msg", "flag to ignore min/max block duration flags differing is being used. If the upload of a 2h block fails and a tsdb compaction happens that block may be missing from your Thanos bucket storage.")
		}
		// The background shipper continuously scans the data directory and uploads
		// new blocks to object storage service.
		bkt, err = client.NewBucket(logger, confContentYaml, reg, comp.String(), confPathYaml...)
		if err != nil {
			return err
		}
	} else {
		level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
	}

	// TODO(brancz): remove after a couple of versions
	// Migrate non-multi-tsdb capable storage to multi-tsdb disk layout.
	if err := migrateLegacyStorage(logger, dataDir, defaultTenantID); err != nil {
		return errors.Wrapf(err, "migrate legacy storage in %v to default tenant %v", dataDir, defaultTenantID)
	}

	dbs := receive.NewMultiTSDB(
		dataDir,
		logger,
		reg,
		tsdbOpts,
		lset,
		tenantLabelName,
		bkt,
		allowOutOfOrderUpload,
		hashFunc,
	)
	writer := receive.NewWriter(log.With(logger, "component", "receive-writer"), dbs)
	webHandler := receive.NewHandler(log.With(logger, "component", "receive-handler"), &receive.Options{
		Writer:            writer,
		ListenAddress:     rwAddress,
		Registry:          reg,
		Endpoint:          endpoint,
		TenantHeader:      tenantHeader,
		DefaultTenantID:   defaultTenantID,
		ReplicaHeader:     replicaHeader,
		ReplicationFactor: replicationFactor,
		Tracer:            tracer,
		TLSConfig:         rwTLSConfig,
		DialOpts:          dialOpts,
		ForwardTimeout:    forwardTimeout,
	})

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.

	// dbReady signals when TSDB is ready and the Store gRPC server can start.
	dbReady := make(chan struct{}, 1)
	// hashringChangedChan signals when TSDB needs to be flushed and updated due to hashring config change.
	hashringChangedChan := make(chan struct{}, 1)
	// uploadC signals when new blocks should be uploaded.
	uploadC := make(chan struct{}, 1)
	// uploadDone signals when uploading has finished.
	uploadDone := make(chan struct{}, 1)

	level.Debug(logger).Log("msg", "setting up tsdb")
	{
		log.With(logger, "component", "storage")
		dbUpdatesStarted := promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_multi_db_updates_attempted_total",
			Help: "Number of Multi DB attempted reloads with flush and potential upload due to hashring changes",
		})
		dbUpdatesCompleted := promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_receive_multi_db_updates_completed_total",
			Help: "Number of Multi DB completed reloads with flush and potential upload due to hashring changes",
		})

		level.Debug(logger).Log("msg", "removing storage lock files if any")
		if err := dbs.RemoveLockFilesIfAny(); err != nil {
			return errors.Wrap(err, "remove storage lock files")
		}

		// TSDBs reload logic, listening on hashring changes.
		cancel := make(chan struct{})
		g.Add(func() error {
			defer close(dbReady)
			defer close(uploadC)

			// Before quitting, ensure the WAL is flushed and the DBs are closed.
			defer func() {
				level.Info(logger).Log("msg", "shutting down storage")
				if err := dbs.Flush(); err != nil {
					level.Error(logger).Log("err", err, "msg", "failed to flush storage")
				} else {
					level.Info(logger).Log("msg", "storage is flushed successfully")
				}
				if err := dbs.Close(); err != nil {
					level.Error(logger).Log("err", err, "msg", "failed to close storage")
					return
				}
				level.Info(logger).Log("msg", "storage is closed")
			}()

			for {
				select {
				case <-cancel:
					return nil
				case _, ok := <-hashringChangedChan:
					if !ok {
						return nil
					}
					dbUpdatesStarted.Inc()
					level.Info(logger).Log("msg", "updating storage")

					if err := dbs.Flush(); err != nil {
						return errors.Wrap(err, "flushing storage")
					}
					if err := dbs.Open(); err != nil {
						return errors.Wrap(err, "opening storage")
					}
					if upload {
						uploadC <- struct{}{}
						<-uploadDone
					}
					statusProber.Ready()
					level.Info(logger).Log("msg", "storage started, and server is ready to receive web requests")
					dbUpdatesCompleted.Inc()
					dbReady <- struct{}{}
				}
			}
		}, func(err error) {
			close(cancel)
		})
	}

	level.Debug(logger).Log("msg", "setting up hashring")
	{
		// Note: the hashring configuration watcher
		// is the sender and thus closes the chan.
		// In the single-node case, which has no configuration
		// watcher, we close the chan ourselves.
		updates := make(chan receive.Hashring, 1)

		// The Hashrings config file path is given initializing config watcher.
		if hashringsFilePath != "" {
			cw, err := receive.NewConfigWatcher(log.With(logger, "component", "config-watcher"), reg, hashringsFilePath, *refreshInterval)
			if err != nil {
				return errors.Wrap(err, "failed to initialize config watcher")
			}

			// Check the hashring configuration on before running the watcher.
			if err := cw.ValidateConfig(); err != nil {
				cw.Stop()
				close(updates)
				return errors.Wrap(err, "failed to validate hashring configuration file")
			}

			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				level.Info(logger).Log("msg", "the hashring initialized with config watcher.")
				return receive.HashringFromConfigWatcher(ctx, updates, cw)
			}, func(error) {
				cancel()
			})
		} else {
			var ring receive.Hashring
			// The Hashrings config file content given initialize configuration from content.
			if len(hashringsFileContent) > 0 {
				ring, err = receive.HashringFromConfig(hashringsFileContent)
				if err != nil {
					close(updates)
					return errors.Wrap(err, "failed to validate hashring configuration file")
				}
				level.Info(logger).Log("msg", "the hashring initialized directly with the given content through the flag.")
			} else {
				level.Info(logger).Log("msg", "the hashring file is not specified use single node hashring.")
				ring = receive.SingleNodeHashring(endpoint)
			}

			cancel := make(chan struct{})
			g.Add(func() error {
				defer close(updates)
				updates <- ring
				<-cancel
				return nil
			}, func(error) {
				close(cancel)
			})
		}

		cancel := make(chan struct{})
		g.Add(func() error {
			defer close(hashringChangedChan)
			for {
				select {
				case h, ok := <-updates:
					if !ok {
						return nil
					}
					webHandler.Hashring(h)
					msg := "hashring has changed; server is not ready to receive web requests"
					statusProber.NotReady(errors.New(msg))
					level.Info(logger).Log("msg", msg)
					hashringChangedChan <- struct{}{}
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
	srv := httpserver.New(logger, reg, comp, httpProbe,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
	)
	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	level.Debug(logger).Log("msg", "setting up grpc server")
	{
		var s *grpcserver.Server
		startGRPC := make(chan struct{})
		g.Add(func() error {
			defer close(startGRPC)

			tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
			if err != nil {
				return errors.Wrap(err, "setup gRPC server")
			}

			for range dbReady {
				if s != nil {
					s.Shutdown(errors.New("reload hashrings"))
				}

				rw := store.ReadWriteTSDBStore{
					StoreServer: store.NewMultiTSDBStore(
						logger,
						reg,
						comp,
						dbs.TSDBStores,
					),
					WriteableStoreServer: webHandler,
				}

				s = grpcserver.New(logger, &receive.UnRegisterer{Registerer: reg}, tracer, grpcLogOpts, tagOpts, comp, grpcProbe,
					grpcserver.WithServer(store.RegisterStoreServer(rw)),
					grpcserver.WithServer(store.RegisterWritableStoreServer(rw)),
					grpcserver.WithListen(grpcBindAddr),
					grpcserver.WithGracePeriod(grpcGracePeriod),
					grpcserver.WithTLSConfig(tlsCfg),
				)
				startGRPC <- struct{}{}
			}
			if s != nil {
				s.Shutdown(err)
			}
			return nil
		}, func(error) {})
		// We need to be able to start and stop the gRPC server
		// whenever the DB changes, thus it needs its own run group.
		g.Add(func() error {
			for range startGRPC {
				level.Info(logger).Log("msg", "listening for StoreAPI and WritableStoreAPI gRPC", "address", grpcBindAddr)
				if err := s.ListenAndServe(); err != nil {
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
		logger := log.With(logger, "component", "uploader")
		upload := func(ctx context.Context) error {
			level.Debug(logger).Log("msg", "upload phase starting")
			start := time.Now()

			uploaded, err := dbs.Sync(ctx)
			if err != nil {
				level.Warn(logger).Log("msg", "upload failed", "elapsed", time.Since(start), "err", err)
				return err
			}
			level.Debug(logger).Log("msg", "upload phase done", "uploaded", uploaded, "elapsed", time.Since(start))
			return nil
		}
		{
			level.Info(logger).Log("msg", "upload enabled, starting initial sync")
			if err := upload(context.Background()); err != nil {
				return errors.Wrap(err, "initial upload failed")
			}
			level.Info(logger).Log("msg", "initial sync done")
		}
		{
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				// Ensure we clean up everything properly.
				defer func() {
					runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
				}()

				// Before quitting, ensure all blocks are uploaded.
				defer func() {
					<-uploadC // Closed by storage routine when it's done.
					level.Info(logger).Log("msg", "uploading the final cut block before exiting")
					ctx, cancel := context.WithCancel(context.Background())
					uploaded, err := dbs.Sync(ctx)
					if err != nil {
						cancel()
						level.Error(logger).Log("msg", "the final upload failed", "err", err)
						return
					}
					cancel()
					level.Info(logger).Log("msg", "the final cut block was uploaded", "uploaded", uploaded)
				}()

				defer close(uploadDone)

				// Run the uploader in a loop.
				tick := time.NewTicker(30 * time.Second)
				defer tick.Stop()

				for {
					select {
					case <-ctx.Done():
						return nil
					case <-uploadC:
						// Upload on demand.
						if err := upload(ctx); err != nil {
							level.Warn(logger).Log("msg", "on demand upload failed", "err", err)
						}
						uploadDone <- struct{}{}
					case <-tick.C:
						if err := upload(ctx); err != nil {
							level.Warn(logger).Log("msg", "recurring upload failed", "err", err)
						}
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

func migrateLegacyStorage(logger log.Logger, dataDir, defaultTenantID string) error {
	defaultTenantDataDir := path.Join(dataDir, defaultTenantID)

	if _, err := os.Stat(defaultTenantDataDir); !os.IsNotExist(err) {
		level.Info(logger).Log("msg", "default tenant data dir already present, not attempting to migrate storage")
		return nil
	}

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		level.Info(logger).Log("msg", "no existing storage found, no data migration attempted")
		return nil
	}

	level.Info(logger).Log("msg", "found legacy storage, migrating to multi-tsdb layout with default tenant", "defaultTenantID", defaultTenantID)

	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return errors.Wrapf(err, "read legacy data dir: %v", dataDir)
	}

	if err := os.MkdirAll(defaultTenantDataDir, 0777); err != nil {
		return errors.Wrapf(err, "create default tenant data dir: %v", defaultTenantDataDir)
	}

	for _, f := range files {
		from := path.Join(dataDir, f.Name())
		to := path.Join(defaultTenantDataDir, f.Name())
		if err := os.Rename(from, to); err != nil {
			return errors.Wrapf(err, "migrate file from %v to %v", from, to)
		}
	}

	return nil
}
