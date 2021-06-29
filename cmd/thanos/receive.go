// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
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
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/logging"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/thanos-io/thanos/pkg/component"
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

	conf := &receiveConfig{}
	conf.registerFlag(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		lset, err := parseFlagLabels(conf.labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		if len(lset) == 0 {
			return errors.New("no external labels configured for receive, uniquely identifying external labels must be configured (ideally with `receive_` prefix); see https://thanos.io/tip/thanos/storage.md#external-labels for details.")
		}

		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions("", conf.reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		tsdbOpts := &tsdb.Options{
			MinBlockDuration:       int64(time.Duration(*conf.tsdbMinBlockDuration) / time.Millisecond),
			MaxBlockDuration:       int64(time.Duration(*conf.tsdbMaxBlockDuration) / time.Millisecond),
			RetentionDuration:      int64(time.Duration(*conf.retention) / time.Millisecond),
			NoLockfile:             conf.noLockFile,
			WALCompression:         conf.walCompression,
			AllowOverlappingBlocks: conf.tsdbAllowOverlappingBlocks,
			MaxExemplars:           conf.tsdbMaxExemplars,
		}

		// Enable ingestion if endpoint is specified or if both the hashrings configs are empty.
		// Otherwise, run the receiver exclusively as a distributor.
		enableIngestion := conf.endpoint != "" || (conf.hashringsFileContent == "" && conf.hashringsFilePath == "")

		return runReceive(
			g,
			logger,
			reg,
			tracer,
			grpcLogOpts, tagOpts,
			tsdbOpts,
			lset,
			component.Receive,
			metadata.HashFunc(conf.hashFunc),
			enableIngestion,
			conf,
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
	tsdbOpts *tsdb.Options,
	lset labels.Labels,
	comp component.SourceStoreAPI,
	hashFunc metadata.HashFunc,
	enableIngestion bool,
	conf *receiveConfig,
) error {
	logger = log.With(logger, "component", "receive")
	level.Warn(logger).Log("msg", "setting up receive")

	if !enableIngestion {
		level.Info(logger).Log("msg", "ingestion is disabled for receiver")
	}

	rwTLSConfig, err := tls.NewServerConfig(log.With(logger, "protocol", "HTTP"), conf.rwServerCert, conf.rwServerKey, conf.rwServerClientCA)
	if err != nil {
		return err
	}

	dialOpts, err := extgrpc.StoreClientGRPCOpts(
		logger,
		reg,
		tracer,
		conf.rwServerCert != "",
		conf.rwServerClientCA == "",
		conf.rwClientCert,
		conf.rwClientKey,
		conf.rwClientServerCA,
		conf.rwClientServerName,
	)
	if err != nil {
		return err
	}

	var bkt objstore.Bucket
	confContentYaml, err := conf.objStoreConfig.Content()
	if err != nil {
		return err
	}

	upload := len(confContentYaml) > 0
	if enableIngestion {
		if upload {
			if tsdbOpts.MinBlockDuration != tsdbOpts.MaxBlockDuration {
				if !conf.ignoreBlockSize {
					return errors.Errorf("found that TSDB Max time is %d and Min time is %d. "+
						"Compaction needs to be disabled (tsdb.min-block-duration = tsdb.max-block-duration)", tsdbOpts.MaxBlockDuration, tsdbOpts.MinBlockDuration)
				}
				level.Warn(logger).Log("msg", "flag to ignore min/max block duration flags differing is being used. If the upload of a 2h block fails and a tsdb compaction happens that block may be missing from your Thanos bucket storage.")
			}
			// The background shipper continuously scans the data directory and uploads
			// new blocks to object storage service.
			bkt, err = client.NewBucket(logger, confContentYaml, reg, comp.String())
			if err != nil {
				return err
			}
		} else {
			level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
		}
	}

	// TODO(brancz): remove after a couple of versions
	// Migrate non-multi-tsdb capable storage to multi-tsdb disk layout.
	if err := migrateLegacyStorage(logger, conf.dataDir, conf.defaultTenantID); err != nil {
		return errors.Wrapf(err, "migrate legacy storage in %v to default tenant %v", conf.dataDir, conf.defaultTenantID)
	}

	dbs := receive.NewMultiTSDB(
		conf.dataDir,
		logger,
		reg,
		tsdbOpts,
		lset,
		conf.tenantLabelName,
		bkt,
		conf.allowOutOfOrderUpload,
		hashFunc,
	)
	writer := receive.NewWriter(log.With(logger, "component", "receive-writer"), dbs)
	webHandler := receive.NewHandler(log.With(logger, "component", "receive-handler"), &receive.Options{
		Writer:                   writer,
		ListenAddress:            conf.rwAddress,
		Registry:                 reg,
		Endpoint:                 conf.endpoint,
		TenantHeader:             conf.tenantHeader,
		DefaultTenantID:          conf.defaultTenantID,
		ReplicaHeader:            conf.replicaHeader,
		ReplicationFactor:        conf.replicationFactor,
		Tracer:                   tracer,
		TLSConfig:                rwTLSConfig,
		DialOpts:                 dialOpts,
		ForwardTimeout:           time.Duration(*conf.forwardTimeout),
		SamplesLimitPerEachWrite: conf.samplesLimitPerEachWrite,
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

	// reloadGRPCServer signals when - (1)TSDB is ready and the Store gRPC server can start.
	// (2) The Hashring files have changed if tsdb ingestion is disabled.
	reloadGRPCServer := make(chan struct{}, 1)
	// hashringChangedChan signals when TSDB needs to be flushed and updated due to hashring config change.
	hashringChangedChan := make(chan struct{}, 1)
	// uploadC signals when new blocks should be uploaded.
	uploadC := make(chan struct{}, 1)
	// uploadDone signals when uploading has finished.
	uploadDone := make(chan struct{}, 1)

	if enableIngestion {
		level.Debug(logger).Log("msg", "setting up tsdb")
		{
			if err := startTSDBAndUpload(g, logger, reg, dbs, reloadGRPCServer, uploadC, hashringChangedChan, upload, uploadDone, statusProber, bkt); err != nil {
				return err
			}
		}
	}

	level.Debug(logger).Log("msg", "setting up hashring")
	{
		if err := setupHashring(g, logger, reg, conf, hashringChangedChan, webHandler, statusProber, reloadGRPCServer, enableIngestion); err != nil {
			return err
		}
	}

	level.Debug(logger).Log("msg", "setting up http server")
	{
		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(*conf.httpBindAddr),
			httpserver.WithGracePeriod(time.Duration(*conf.httpGracePeriod)),
			httpserver.WithTLSConfig(*conf.httpTLSConfig),
		)
		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})
	}

	level.Debug(logger).Log("msg", "setting up grpc server")
	{
		if err := setupAndRunGRPCServer(g, logger, reg, tracer, conf, reloadGRPCServer, comp, dbs, webHandler, grpcLogOpts, tagOpts, grpcProbe); err != nil {
			return err
		}
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

	level.Info(logger).Log("msg", "starting receiver")
	return nil
}

// setupAndRunGRPCServer sets up the configuration for the gRPC server.
// It also sets up a handler for reloading the server if tsdb reloads.
func setupAndRunGRPCServer(g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	conf *receiveConfig,
	reloadGRPCServer chan struct{},
	comp component.SourceStoreAPI,
	dbs *receive.MultiTSDB,
	webHandler *receive.Handler,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
	grpcProbe *prober.GRPCProbe,

) error {

	var s *grpcserver.Server
	// startGRPCListening re-starts the gRPC server once it receives a signal.
	startGRPCListening := make(chan struct{})

	g.Add(func() error {
		defer close(startGRPCListening)

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), *conf.grpcCert, *conf.grpcKey, *conf.grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		for range reloadGRPCServer {
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
				grpcserver.WithServer(exemplars.RegisterExemplarsServer(exemplars.NewMultiTSDB(dbs.TSDBExemplars))),
				grpcserver.WithListen(*conf.grpcBindAddr),
				grpcserver.WithGracePeriod(time.Duration(*conf.grpcGracePeriod)),
				grpcserver.WithTLSConfig(tlsCfg),
			)
			startGRPCListening <- struct{}{}
		}
		if s != nil {
			s.Shutdown(err)
		}
		return nil
	}, func(error) {})

	// We need to be able to start and stop the gRPC server
	// whenever the DB changes, thus it needs its own run group.
	g.Add(func() error {
		for range startGRPCListening {
			level.Info(logger).Log("msg", "listening for StoreAPI and WritableStoreAPI gRPC", "address", *conf.grpcBindAddr)
			if err := s.ListenAndServe(); err != nil {
				return errors.Wrap(err, "serve gRPC")
			}
		}
		return nil
	}, func(error) {})

	return nil

}

// setupHashring sets up the hashring configuration provided.
// If no hashring is provided, we setup a single node hashring with local endpoint.
func setupHashring(g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	conf *receiveConfig,
	hashringChangedChan chan struct{},
	webHandler *receive.Handler,
	statusProber prober.Probe,
	reloadGRPCServer chan struct{},
	enableIngestion bool,
) error {
	// Note: the hashring configuration watcher
	// is the sender and thus closes the chan.
	// In the single-node case, which has no configuration
	// watcher, we close the chan ourselves.
	updates := make(chan receive.Hashring, 1)

	// The Hashrings config file path is given initializing config watcher.
	if conf.hashringsFilePath != "" {
		cw, err := receive.NewConfigWatcher(log.With(logger, "component", "config-watcher"), reg, conf.hashringsFilePath, *conf.refreshInterval)
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
		var (
			ring receive.Hashring
			err  error
		)
		// The Hashrings config file content given initialize configuration from content.
		if len(conf.hashringsFileContent) > 0 {
			ring, err = receive.HashringFromConfig(conf.hashringsFileContent)
			if err != nil {
				close(updates)
				return errors.Wrap(err, "failed to validate hashring configuration file")
			}
			level.Info(logger).Log("msg", "the hashring initialized directly with the given content through the flag.")
		} else {
			level.Info(logger).Log("msg", "the hashring file is not specified use single node hashring.")
			ring = receive.SingleNodeHashring(conf.endpoint)
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

		if enableIngestion {
			defer close(hashringChangedChan)
		}

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

				if enableIngestion {
					// send a signal to tsdb to reload, and then restart the gRPC server.
					hashringChangedChan <- struct{}{}
				} else {
					// we dont need tsdb to reload, so restart the gRPC server.
					level.Info(logger).Log("msg", "server has reloaded, ready to start accepting requests")
					statusProber.Ready()
					reloadGRPCServer <- struct{}{}
				}
			case <-cancel:
				return nil
			}
		}
	}, func(err error) {
		close(cancel)
	},
	)
	return nil
}

// startTSDBAndUpload starts up the multi-tsdb and sets up the rungroup to flush the tsdb and reload on hashring change.
// It also uploads the tsdb to object store if upload is enabled.
func startTSDBAndUpload(g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	dbs *receive.MultiTSDB,
	reloadGRPCServer chan struct{},
	uploadC chan struct{},
	hashringChangedChan chan struct{},
	upload bool,
	uploadDone chan struct{},
	statusProber prober.Probe,
	bkt objstore.Bucket,

) error {

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
		defer close(reloadGRPCServer)
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
				reloadGRPCServer <- struct{}{}
			}
		}
	}, func(err error) {
		close(cancel)
	})

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

	if err := os.MkdirAll(defaultTenantDataDir, 0750); err != nil {
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

type receiveConfig struct {
	httpBindAddr    *string
	httpGracePeriod *model.Duration
	httpTLSConfig   *string

	grpcBindAddr    *string
	grpcGracePeriod *model.Duration
	grpcCert        *string
	grpcKey         *string
	grpcClientCA    *string

	rwAddress          string
	rwServerCert       string
	rwServerKey        string
	rwServerClientCA   string
	rwClientCert       string
	rwClientKey        string
	rwClientServerCA   string
	rwClientServerName string

	dataDir   string
	labelStrs []string

	objStoreConfig *extflag.PathOrContent
	retention      *model.Duration

	hashringsFilePath    string
	hashringsFileContent string

	refreshInterval          *model.Duration
	endpoint                 string
	tenantHeader             string
	defaultTenantID          string
	tenantLabelName          string
	replicaHeader            string
	replicationFactor        uint64
	forwardTimeout           *model.Duration
	samplesLimitPerEachWrite uint64

	tsdbMinBlockDuration       *model.Duration
	tsdbMaxBlockDuration       *model.Duration
	tsdbAllowOverlappingBlocks bool
	tsdbMaxExemplars           int

	walCompression bool
	noLockFile     bool

	hashFunc string

	ignoreBlockSize       bool
	allowOutOfOrderUpload bool

	reqLogConfig *extflag.PathOrContent
}

func (rc *receiveConfig) registerFlag(cmd extkingpin.FlagClause) {
	rc.httpBindAddr, rc.httpGracePeriod, rc.httpTLSConfig = extkingpin.RegisterHTTPFlags(cmd)
	rc.grpcBindAddr, rc.grpcGracePeriod, rc.grpcCert, rc.grpcKey, rc.grpcClientCA = extkingpin.RegisterGRPCFlags(cmd)

	cmd.Flag("remote-write.address", "Address to listen on for remote write requests.").
		Default("0.0.0.0:19291").StringVar(&rc.rwAddress)

	cmd.Flag("remote-write.server-tls-cert", "TLS Certificate for HTTP server, leave blank to disable TLS.").Default("").StringVar(&rc.rwServerCert)

	cmd.Flag("remote-write.server-tls-key", "TLS Key for the HTTP server, leave blank to disable TLS.").Default("").StringVar(&rc.rwServerKey)

	cmd.Flag("remote-write.server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").StringVar(&rc.rwServerClientCA)

	cmd.Flag("remote-write.client-tls-cert", "TLS Certificates to use to identify this client to the server.").Default("").StringVar(&rc.rwClientCert)

	cmd.Flag("remote-write.client-tls-key", "TLS Key for the client's certificate.").Default("").StringVar(&rc.rwClientKey)

	cmd.Flag("remote-write.client-tls-ca", "TLS CA Certificates to use to verify servers.").Default("").StringVar(&rc.rwClientServerCA)

	cmd.Flag("remote-write.client-server-name", "Server name to verify the hostname on the returned TLS certificates. See https://tools.ietf.org/html/rfc4366#section-3.1").Default("").StringVar(&rc.rwClientServerName)

	cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").StringVar(&rc.dataDir)

	cmd.Flag("label", "External labels to announce. This flag will be removed in the future when handling multiple tsdb instances is added.").PlaceHolder("key=\"value\"").StringsVar(&rc.labelStrs)

	rc.objStoreConfig = extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	rc.retention = extkingpin.ModelDuration(cmd.Flag("tsdb.retention", "How long to retain raw samples on local storage. 0d - disables this retention.").Default("15d"))

	cmd.Flag("receive.hashrings-file", "Path to file that contains the hashring configuration. A watcher is initialized to watch changes and update the hashring dynamically.").PlaceHolder("<path>").StringVar(&rc.hashringsFilePath)

	cmd.Flag("receive.hashrings", "Alternative to 'receive.hashrings-file' flag (lower priority). Content of file that contains the hashring configuration.").PlaceHolder("<content>").StringVar(&rc.hashringsFileContent)

	rc.refreshInterval = extkingpin.ModelDuration(cmd.Flag("receive.hashrings-file-refresh-interval", "Refresh interval to re-read the hashring configuration file. (used as a fallback)").
		Default("5m"))

	cmd.Flag("receive.local-endpoint", "Endpoint of local receive node. Used to identify the local node in the hashring configuration.").StringVar(&rc.endpoint)

	cmd.Flag("receive.tenant-header", "HTTP header to determine tenant for write requests.").Default(receive.DefaultTenantHeader).StringVar(&rc.tenantHeader)

	cmd.Flag("receive.default-tenant-id", "Default tenant ID to use when none is provided via a header.").Default(receive.DefaultTenant).StringVar(&rc.defaultTenantID)

	cmd.Flag("receive.tenant-label-name", "Label name through which the tenant will be announced.").Default(receive.DefaultTenantLabel).StringVar(&rc.tenantLabelName)

	cmd.Flag("receive.replica-header", "HTTP header specifying the replica number of a write request.").Default(receive.DefaultReplicaHeader).StringVar(&rc.replicaHeader)

	cmd.Flag("receive.replication-factor", "How many times to replicate incoming write requests.").Default("1").Uint64Var(&rc.replicationFactor)

	cmd.Flag("receive.samples-limit-per-each-write", "Number of allowed samples for each write API call.").Default("50000").Uint64Var(&rc.samplesLimitPerEachWrite)

	rc.forwardTimeout = extkingpin.ModelDuration(cmd.Flag("receive-forward-timeout", "Timeout for each forward request.").Default("5s").Hidden())

	rc.tsdbMinBlockDuration = extkingpin.ModelDuration(cmd.Flag("tsdb.min-block-duration", "Min duration for local TSDB blocks").Default("2h").Hidden())

	rc.tsdbMaxBlockDuration = extkingpin.ModelDuration(cmd.Flag("tsdb.max-block-duration", "Max duration for local TSDB blocks").Default("2h").Hidden())

	cmd.Flag("tsdb.allow-overlapping-blocks", "Allow overlapping blocks, which in turn enables vertical compaction and vertical query merge.").Default("false").BoolVar(&rc.tsdbAllowOverlappingBlocks)

	cmd.Flag("tsdb.wal-compression", "Compress the tsdb WAL.").Default("true").BoolVar(&rc.walCompression)

	cmd.Flag("tsdb.no-lockfile", "Do not create lockfile in TSDB data directory. In any case, the lockfiles will be deleted on next startup.").Default("false").BoolVar(&rc.noLockFile)

	cmd.Flag("tsdb.max-exemplars",
		"Enables support for ingesting exemplars and sets the maximum number of exemplars that will be stored per tenant."+
			" In case the exemplar storage becomes full (number of stored exemplars becomes equal to max-exemplars),"+
			" ingesting a new exemplar will evict the oldest exemplar from storage. 0 (or less) value of this flag disables exemplars storage.").
		Default("0").IntVar(&rc.tsdbMaxExemplars)

	cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").EnumVar(&rc.hashFunc, "SHA256", "")

	cmd.Flag("shipper.ignore-unequal-block-size", "If true receive will not require min and max block size flags to be set to the same value. Only use this if you want to keep long retention and compaction enabled, as in the worst case it can result in ~2h data loss for your Thanos bucket storage.").Default("false").Hidden().BoolVar(&rc.ignoreBlockSize)

	cmd.Flag("shipper.allow-out-of-order-uploads",
		"If true, shipper will skip failed block uploads in the given iteration and retry later. This means that some newer blocks might be uploaded sooner than older blocks."+
			"This can trigger compaction without those blocks and as a result will create an overlap situation. Set it to true if you have vertical compaction enabled and wish to upload blocks as soon as possible without caring"+
			"about order.").
		Default("false").Hidden().BoolVar(&rc.allowOutOfOrderUpload)

	rc.reqLogConfig = extkingpin.RegisterRequestLoggingFlags(cmd)
}
