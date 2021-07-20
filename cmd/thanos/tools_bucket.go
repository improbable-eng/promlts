// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/olekukonko/tablewriter"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	extflag "github.com/efficientgo/tools/extkingpin"
	v1 "github.com/thanos-io/thanos/pkg/api/blocks"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/compactv2"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/replicate"
	"github.com/thanos-io/thanos/pkg/runutil"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/ui"
	"github.com/thanos-io/thanos/pkg/verifier"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"gopkg.in/yaml.v3"
)

const extpromPrefix = "thanos_bucket_"

var (
	issuesVerifiersRegistry = verifier.Registry{
		Verifiers: []verifier.Verifier{verifier.OverlappedBlocksIssue{}},
		VerifierRepairers: []verifier.VerifierRepairer{
			verifier.IndexKnownIssues{},
			verifier.DuplicatedCompactionBlocks{},
		},
	}
	inspectColumns = []string{"ULID", "FROM", "UNTIL", "RANGE", "UNTIL-DOWN", "#SERIES", "#SAMPLES", "#CHUNKS", "COMP-LEVEL", "COMP-FAILED", "LABELS", "RESOLUTION", "SOURCE"}
)

func registerBucket(app extkingpin.AppClause) {
	cmd := app.Command("bucket", "Bucket utility commands")

	objStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "", true)
	registerBucketVerify(cmd, objStoreConfig)
	registerBucketLs(cmd, objStoreConfig)
	registerBucketInspect(cmd, objStoreConfig)
	registerBucketWeb(cmd, objStoreConfig)
	registerBucketReplicate(cmd, objStoreConfig)
	registerBucketDownsample(cmd, objStoreConfig)
	registerBucketCleanup(cmd, objStoreConfig)
	registerBucketMarkBlock(cmd, objStoreConfig)
	registerBucketRewrite(cmd, objStoreConfig)
	registerBucketRetention(cmd, objStoreConfig)
}

func registerBucketVerify(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("verify", "Verify all blocks in the bucket against specified issues. NOTE: Depending on issue this might take time and will need downloading all specified blocks to disk.")
	objStoreBackupConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "-backup", false, "Used for repair logic to backup blocks before removal.")
	repair := cmd.Flag("repair", "Attempt to repair blocks for which issues were detected").
		Short('r').Default("false").Bool()
	issuesToVerify := cmd.Flag("issues", fmt.Sprintf("Issues to verify (and optionally repair). "+
		"Possible issue to verify, without repair: %v; Possible issue to verify and repair: %v",
		issuesVerifiersRegistry.VerifiersIDs(), issuesVerifiersRegistry.VerifierRepairersIDs())).
		Short('i').Default(verifier.IndexKnownIssues{}.IssueID(), verifier.OverlappedBlocksIssue{}.IssueID()).Strings()
	ids := cmd.Flag("id", "Block IDs to verify (and optionally repair) only. "+
		"If none is specified, all blocks will be verified. Repeated field").Strings()
	deleteDelay := extkingpin.ModelDuration(cmd.Flag("delete-delay", "Duration after which blocks marked for deletion would be deleted permanently from source bucket by compactor component. "+
		"If delete-delay is non zero, blocks will be marked for deletion and compactor component is required to delete blocks from source bucket. "+
		"If delete-delay is 0, blocks will be deleted straight away. Use this if you want to get rid of or move the block immediately. "+
		"Note that deleting blocks immediately can cause query failures, if store gateway still has the block loaded, "+
		"or compactor is ignoring the deletion because it's compacting the block at the same time.").
		Default("0s"))
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Bucket.String())
		if err != nil {
			return err
		}
		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		backupconfContentYaml, err := objStoreBackupConfig.Content()
		if err != nil {
			return err
		}

		var backupBkt objstore.Bucket
		if len(backupconfContentYaml) == 0 {
			if *repair {
				return errors.New("repair is specified, so backup client is required")
			}
		} else {
			// nil Prometheus registerer: don't create conflicting metrics.
			backupBkt, err = client.NewBucket(logger, backupconfContentYaml, nil, component.Bucket.String())
			if err != nil {
				return err
			}

			defer runutil.CloseWithLogOnErr(logger, backupBkt, "backup bucket client")
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		r, err := issuesVerifiersRegistry.SubstractByIDs(*issuesToVerify, *repair)
		if err != nil {
			return err
		}

		fetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), nil, nil)
		if err != nil {
			return err
		}

		var idMatcher func(ulid.ULID) bool = nil
		if len(*ids) > 0 {
			idsMap := map[string]struct{}{}
			for _, bid := range *ids {
				id, err := ulid.Parse(bid)
				if err != nil {
					return errors.Wrap(err, "invalid ULID found in --id flag")
				}
				idsMap[id.String()] = struct{}{}
			}

			idMatcher = func(id ulid.ULID) bool {
				if _, ok := idsMap[id.String()]; !ok {
					return false
				}
				return true
			}
		}

		v := verifier.NewManager(reg, logger, bkt, backupBkt, fetcher, time.Duration(*deleteDelay), r)
		if *repair {
			return v.VerifyAndRepair(context.Background(), idMatcher)
		}

		return v.Verify(context.Background(), idMatcher)
	})
}

func registerBucketLs(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("ls", "List all blocks in the bucket.")
	output := cmd.Flag("output", "Optional format in which to print each block's information. Options are 'json', 'wide' or a custom template.").
		Short('o').Default("").String()
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Bucket.String())
		if err != nil {
			return err
		}

		fetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), nil, nil)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		var (
			format     = *output
			objects    = 0
			printBlock func(m *metadata.Meta) error
		)

		switch format {
		case "":
			printBlock = func(m *metadata.Meta) error {
				fmt.Fprintln(os.Stdout, m.ULID.String())
				return nil
			}
		case "wide":
			printBlock = func(m *metadata.Meta) error {
				minTime := time.Unix(m.MinTime/1000, 0)
				maxTime := time.Unix(m.MaxTime/1000, 0)

				if _, err = fmt.Fprintf(os.Stdout, "%s -- %s - %s Diff: %s, Compaction: %d, Downsample: %d, Source: %s\n",
					m.ULID, minTime.Format("2006-01-02 15:04"), maxTime.Format("2006-01-02 15:04"), maxTime.Sub(minTime),
					m.Compaction.Level, m.Thanos.Downsample.Resolution, m.Thanos.Source); err != nil {
					return err
				}
				return nil
			}
		case "json":
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "\t")

			printBlock = func(m *metadata.Meta) error {
				return enc.Encode(&m)
			}
		default:
			tmpl, err := template.New("").Parse(format)
			if err != nil {
				return errors.Wrap(err, "invalid template")
			}
			printBlock = func(m *metadata.Meta) error {
				if err := tmpl.Execute(os.Stdout, &m); err != nil {
					return errors.Wrap(err, "execute template")
				}
				fmt.Fprintln(os.Stdout, "")
				return nil
			}
		}

		metas, _, err := fetcher.Fetch(ctx)
		if err != nil {
			return err
		}

		for _, meta := range metas {
			objects++
			if err := printBlock(meta); err != nil {
				return errors.Wrap(err, "iter")
			}
		}
		level.Info(logger).Log("msg", "ls done", "objects", objects)
		return nil
	})
}

func registerBucketInspect(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("inspect", "Inspect all blocks in the bucket in detailed, table-like way.")
	selector := cmd.Flag("selector", "Selects blocks based on label, e.g. '-l key1=\\\"value1\\\" -l key2=\\\"value2\\\"'. All key value pairs must match.").Short('l').
		PlaceHolder("<name>=\\\"<value>\\\"").Strings()
	sortBy := cmd.Flag("sort-by", "Sort by columns. It's also possible to sort by multiple columns, e.g. '--sort-by FROM --sort-by UNTIL'. I.e., if the 'FROM' value is equal the rows are then further sorted by the 'UNTIL' value.").
		Default("FROM", "UNTIL").Enums(inspectColumns...)
	timeout := cmd.Flag("timeout", "Timeout to download metadata from remote storage").Default("5m").Duration()

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {

		// Parse selector.
		selectorLabels, err := parseFlagLabels(*selector)
		if err != nil {
			return errors.Wrap(err, "error parsing selector flag")
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Bucket.String())
		if err != nil {
			return err
		}

		fetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg), nil, nil)
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()

		// Getting Metas.
		metas, _, err := fetcher.Fetch(ctx)
		if err != nil {
			return err
		}

		blockMetas := make([]*metadata.Meta, 0, len(metas))
		for _, meta := range metas {
			blockMetas = append(blockMetas, meta)
		}

		return printTable(blockMetas, selectorLabels, *sortBy)
	})
}

// registerBucketWeb exposes a web interface for the state of remote store like `pprof web`.
func registerBucketWeb(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("web", "Web interface for remote storage bucket.")
	httpBindAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)
	webRoutePrefix := cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. Defaults to the value of --web.external-prefix. This option is analogous to --web.route-prefix of Prometheus.").Default("").String()
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the bucket web UI interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos bucket web UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()
	webDisableCORS := cmd.Flag("web.disable-cors", "Whether to disable CORS headers to be set by Thanos. By default Thanos sets CORS headers to be allowed by all.").Default("false").Bool()

	interval := cmd.Flag("refresh", "Refresh interval to download metadata from remote storage").Default("30m").Duration()
	timeout := cmd.Flag("timeout", "Timeout to download metadata from remote storage").Default("5m").Duration()
	label := cmd.Flag("label", "Prometheus label to use as timeline title").String()
	filterConf := &store.FilterConfig{}
	cmd.Flag("min-time", "Start of time range limit to serve. Thanos tool bucket web will serve only blocks, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z").SetValue(&filterConf.MinTime)
	cmd.Flag("max-time", "End of time range limit to serve. Thanos tool bucket web will serve only blocks, which happened earlier than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z").SetValue(&filterConf.MaxTime)
	selectorRelabelConf := *extkingpin.RegisterSelectorRelabelFlags(cmd)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		comp := component.Bucket
		httpProbe := prober.NewHTTP()
		statusProber := prober.Combine(
			httpProbe,
			prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
		)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(*httpBindAddr),
			httpserver.WithGracePeriod(time.Duration(*httpGracePeriod)),
			httpserver.WithTLSConfig(*httpTLSConfig),
		)

		if *webRoutePrefix == "" {
			*webRoutePrefix = *webExternalPrefix
		}

		if *webRoutePrefix != *webExternalPrefix {
			level.Warn(logger).Log("msg", "different values for --web.route-prefix and --web.external-prefix detected, web UI may not work without a reverse-proxy.")
		}

		router := route.New()

		// RoutePrefix must always start with '/'.
		*webRoutePrefix = "/" + strings.Trim(*webRoutePrefix, "/")

		// Redirect from / to /webRoutePrefix.
		if *webRoutePrefix != "/" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, *webRoutePrefix+"/", http.StatusFound)
			})
			router.Get(*webRoutePrefix, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, *webRoutePrefix+"/", http.StatusFound)
			})
			router = router.WithPrefix(*webRoutePrefix)
		}

		ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)

		bucketUI := ui.NewBucketUI(logger, *label, *webExternalPrefix, *webPrefixHeaderName, "", component.Bucket)
		bucketUI.Register(router, true, ins)

		flagsMap := getFlagsMap(cmd.Flags())

		api := v1.NewBlocksAPI(logger, *webDisableCORS, *label, flagsMap)

		// Configure Request Logging for HTTP calls.
		opts := []logging.Option{logging.WithDecider(func(_ string, _ error) logging.Decision {
			return logging.NoLogCall
		})}
		logMiddleware := logging.NewHTTPServerMiddleware(logger, opts...)

		api.Register(router.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

		srv.Handle("/", router)

		if *interval < 5*time.Minute {
			level.Warn(logger).Log("msg", "Refreshing more often than 5m could lead to large data transfers")
		}

		if *timeout < time.Minute {
			level.Warn(logger).Log("msg", "Timeout less than 1m could lead to frequent failures")
		}

		if *interval < (*timeout * 2) {
			level.Warn(logger).Log("msg", "Refresh interval should be at least 2 times the timeout")
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Bucket.String())
		if err != nil {
			return errors.Wrap(err, "bucket client")
		}

		relabelContentYaml, err := selectorRelabelConf.Content()
		if err != nil {
			return errors.Wrap(err, "get content of relabel configuration")
		}

		relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
		if err != nil {
			return err
		}
		// TODO(bwplotka): Allow Bucket UI to visualize the state of block as well.
		fetcher, err := block.NewMetaFetcher(logger, block.FetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg),
			[]block.MetadataFilter{
				block.NewTimePartitionMetaFilter(filterConf.MinTime, filterConf.MaxTime),
				block.NewLabelShardedMetaFilter(relabelConfig),
				block.NewDeduplicateFilter(),
			}, nil)
		if err != nil {
			return err
		}
		fetcher.UpdateOnChange(func(blocks []metadata.Meta, err error) {
			bucketUI.Set(blocks, err)
			api.SetGlobal(blocks, err)
		})

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			statusProber.Ready()
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			return runutil.Repeat(*interval, ctx.Done(), func() error {
				return runutil.RetryWithLog(logger, time.Minute, ctx.Done(), func() error {
					iterCtx, iterCancel := context.WithTimeout(ctx, *timeout)
					defer iterCancel()

					_, _, err := fetcher.Fetch(iterCtx)
					return err
				})
			})
		}, func(error) {
			cancel()
		})

		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})

		return nil
	})
}

// Provide a list of resolution, can not use Enum directly, since string does not implement int64 function.
func listResLevel() []string {
	return []string{
		time.Duration(downsample.ResLevel0).String(),
		time.Duration(downsample.ResLevel1).String(),
		time.Duration(downsample.ResLevel2).String()}
}

func registerBucketReplicate(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command("replicate", fmt.Sprintf("Replicate data from one object storage to another. NOTE: Currently it works only with Thanos blocks (%v has to have Thanos metadata).", block.MetaFilename))
	httpBindAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)
	toObjStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "-to", false, "The object storage which replicate data to.")
	resolutions := cmd.Flag("resolution", "Only blocks with these resolutions will be replicated. Repeated flag.").Default("0s", "5m", "1h").HintAction(listResLevel).DurationList()
	compactions := cmd.Flag("compaction", "Only blocks with these compaction levels will be replicated. Repeated flag.").Default("1", "2", "3", "4").Ints()
	matcherStrs := cmd.Flag("matcher", "Only blocks whose external labels exactly match this matcher will be replicated.").PlaceHolder("key=\"value\"").Strings()
	singleRun := cmd.Flag("single-run", "Run replication only one time, then exit.").Default("false").Bool()
	minTime := model.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to replicate. Thanos Replicate will replicate only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))
	maxTime := model.TimeOrDuration(cmd.Flag("max-time", "End of time range limit to replicate. Thanos Replicate will replicate only metrics, which happened earlier than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("9999-12-31T23:59:59Z"))
	ids := cmd.Flag("id", "Block to be replicated to the destination bucket. IDs will be used to match blocks and other matchers will be ignored. When specified, this command will be run only once after successful replication. Repeated field").Strings()

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		matchers, err := replicate.ParseFlagMatchers(*matcherStrs)
		if err != nil {
			return errors.Wrap(err, "parse block label matchers")
		}

		var resolutionLevels []compact.ResolutionLevel
		for _, lvl := range *resolutions {
			resolutionLevels = append(resolutionLevels, compact.ResolutionLevel(lvl.Milliseconds()))
		}

		blockIDs := make([]ulid.ULID, 0, len(*ids))
		for _, id := range *ids {
			bid, err := ulid.Parse(id)
			if err != nil {
				return errors.Wrap(err, "invalid ULID found in --id flag")
			}
			blockIDs = append(blockIDs, bid)
		}

		return replicate.RunReplicate(
			g,
			logger,
			reg,
			tracer,
			*httpBindAddr,
			*httpTLSConfig,
			time.Duration(*httpGracePeriod),
			matchers,
			resolutionLevels,
			*compactions,
			objStoreConfig,
			toObjStoreConfig,
			*singleRun,
			minTime,
			maxTime,
			blockIDs,
		)
	})
}

func registerBucketDownsample(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command(component.Downsample.String(), "Continuously downsamples blocks in an object store bucket.")
	httpAddr, httpGracePeriod, httpTLSConfig := extkingpin.RegisterHTTPFlags(cmd)
	downsampleConcurrency := cmd.Flag("downsample.concurrency", "Number of goroutines to use when downsampling blocks.").
		Default("1").Int()
	dataDir := cmd.Flag("data-dir", "Data directory in which to cache blocks and process downsamplings.").
		Default("./data").String()
	hashFunc := cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").Enum("SHA256", "")

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		return RunDownsample(g, logger, reg, *httpAddr, *httpTLSConfig, time.Duration(*httpGracePeriod), *dataDir, *downsampleConcurrency, objStoreConfig, component.Downsample, metadata.HashFunc(*hashFunc))
	})
}

func registerBucketCleanup(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command(component.Cleanup.String(), "Cleans up all blocks marked for deletion.")
	deleteDelay := cmd.Flag("delete-delay", "Time before a block marked for deletion is deleted from bucket.").Default("48h").Duration()
	consistencyDelay := cmd.Flag("consistency-delay", fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %v will be removed.", compact.PartialUploadThresholdAge)).
		Default("30m").Duration()
	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing block metadata from object storage.").
		Default("20").Int()
	selectorRelabelConf := extkingpin.RegisterSelectorRelabelFlags(cmd)
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		relabelContentYaml, err := selectorRelabelConf.Content()
		if err != nil {
			return errors.Wrap(err, "get content of relabel configuration")
		}

		relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Cleanup.String())
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		stubCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

		// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
		// The delay of deleteDelay/2 is added to ensure we fetch blocks that are meant to be deleted but do not have a replacement yet.
		// This is to make sure compactor will not accidentally perform compactions with gap instead.
		ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, *deleteDelay/2, block.FetcherConcurrency)
		duplicateBlocksFilter := block.NewDeduplicateFilter()
		blocksCleaner := compact.NewBlocksCleaner(logger, bkt, ignoreDeletionMarkFilter, *deleteDelay, stubCounter, stubCounter)

		ctx := context.Background()

		var sy *compact.Syncer
		{
			baseMetaFetcher, err := block.NewBaseFetcher(logger, block.FetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg))
			if err != nil {
				return errors.Wrap(err, "create meta fetcher")
			}
			cf := baseMetaFetcher.NewMetaFetcher(
				extprom.WrapRegistererWithPrefix(extpromPrefix, reg), []block.MetadataFilter{
					block.NewLabelShardedMetaFilter(relabelConfig),
					block.NewConsistencyDelayMetaFilter(logger, *consistencyDelay, extprom.WrapRegistererWithPrefix(extpromPrefix, reg)),
					ignoreDeletionMarkFilter,
					duplicateBlocksFilter,
				}, []block.MetadataModifier{block.NewReplicaLabelRemover(logger, make([]string, 0))},
			)
			sy, err = compact.NewMetaSyncer(
				logger,
				reg,
				bkt,
				cf,
				duplicateBlocksFilter,
				ignoreDeletionMarkFilter,
				stubCounter,
				stubCounter,
				*blockSyncConcurrency)
			if err != nil {
				return errors.Wrap(err, "create syncer")
			}
		}

		level.Info(logger).Log("msg", "syncing blocks metadata")
		if err := sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync blocks")
		}

		level.Info(logger).Log("msg", "synced blocks done")

		compact.BestEffortCleanAbortedPartialUploads(ctx, logger, sy.Partial(), bkt, stubCounter, stubCounter, stubCounter)
		if err := blocksCleaner.DeleteMarkedBlocks(ctx); err != nil {
			return errors.Wrap(err, "error cleaning blocks")
		}

		level.Info(logger).Log("msg", "cleanup done")
		return nil
	})
}

func printTable(blockMetas []*metadata.Meta, selectorLabels labels.Labels, sortBy []string) error {
	header := inspectColumns

	var lines [][]string
	p := message.NewPrinter(language.English)

	for _, blockMeta := range blockMetas {
		if !matchesSelector(blockMeta, selectorLabels) {
			continue
		}

		timeRange := time.Duration((blockMeta.MaxTime - blockMeta.MinTime) * int64(time.Millisecond))

		untilDown := "-"
		if until, err := compact.UntilNextDownsampling(blockMeta); err == nil {
			untilDown = until.String()
		}
		var labels []string
		for _, key := range getKeysAlphabetically(blockMeta.Thanos.Labels) {
			labels = append(labels, fmt.Sprintf("%s=%s", key, blockMeta.Thanos.Labels[key]))
		}

		var line []string
		line = append(line,
			blockMeta.ULID.String(),
			time.Unix(blockMeta.MinTime/1000, 0).Format("02-01-2006 15:04:05"),
			time.Unix(blockMeta.MaxTime/1000, 0).Format("02-01-2006 15:04:05"),
			timeRange.String(),
			untilDown,
			p.Sprintf("%d", blockMeta.Stats.NumSeries),
			p.Sprintf("%d", blockMeta.Stats.NumSamples),
			p.Sprintf("%d", blockMeta.Stats.NumChunks),
			p.Sprintf("%d", blockMeta.Compaction.Level),
			p.Sprintf("%t", blockMeta.Compaction.Failed),
			strings.Join(labels, ","),
			time.Duration(blockMeta.Thanos.Downsample.Resolution*int64(time.Millisecond)).String(),
			string(blockMeta.Thanos.Source))

		lines = append(lines, line)
	}

	var sortByColNum []int
	for _, col := range sortBy {
		index := getIndex(header, col)
		if index == -1 {
			return errors.Errorf("column %s not found", col)
		}
		sortByColNum = append(sortByColNum, index)
	}

	t := Table{Header: header, Lines: lines, SortIndices: sortByColNum}
	sort.Sort(t)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(t.Header)
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")
	table.SetAutoWrapText(false)
	table.SetReflowDuringAutoWrap(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.AppendBulk(t.Lines)
	table.Render()

	return nil
}

func getKeysAlphabetically(labels map[string]string) []string {
	var keys []string
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// matchesSelector checks if blockMeta contains every label from
// the selector with the correct value.
func matchesSelector(blockMeta *metadata.Meta, selectorLabels labels.Labels) bool {
	for _, l := range selectorLabels {
		if v, ok := blockMeta.Thanos.Labels[l.Name]; !ok || v != l.Value {
			return false
		}
	}
	return true
}

// getIndex calculates the index of s in strs.
func getIndex(strs []string, s string) int {
	for i, col := range strs {
		if col == s {
			return i
		}
	}
	return -1
}

type Table struct {
	Header      []string
	Lines       [][]string
	SortIndices []int
}

func (t Table) Len() int { return len(t.Lines) }

func (t Table) Swap(i, j int) { t.Lines[i], t.Lines[j] = t.Lines[j], t.Lines[i] }

func (t Table) Less(i, j int) bool {
	for _, index := range t.SortIndices {
		if t.Lines[i][index] == t.Lines[j][index] {
			continue
		}
		return compare(t.Lines[i][index], t.Lines[j][index])
	}
	return compare(t.Lines[i][0], t.Lines[j][0])
}

func compare(s1, s2 string) bool {
	// Values can be either Time, Duration, comma-delimited integers or strings.
	s1Time, s1Err := time.Parse("02-01-2006 15:04:05", s1)
	s2Time, s2Err := time.Parse("02-01-2006 15:04:05", s2)
	if s1Err != nil || s2Err != nil {
		s1Duration, s1Err := time.ParseDuration(s1)
		s2Duration, s2Err := time.ParseDuration(s2)
		if s1Err != nil || s2Err != nil {
			s1Int, s1Err := strconv.ParseUint(strings.ReplaceAll(s1, ",", ""), 10, 64)
			s2Int, s2Err := strconv.ParseUint(strings.ReplaceAll(s2, ",", ""), 10, 64)
			if s1Err != nil || s2Err != nil {
				return s1 < s2
			}
			return s1Int < s2Int
		}
		return s1Duration < s2Duration
	}
	return s1Time.Before(s2Time)
}

func registerBucketMarkBlock(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command(component.Mark.String(), "Mark block for deletion or no-compact in a safe way. NOTE: If the compactor is currently running compacting same block, this operation would be potentially a noop.")
	blockIDs := cmd.Flag("id", "ID (ULID) of the blocks to be marked for deletion (repeated flag)").Required().Strings()
	marker := cmd.Flag("marker", "Marker to be put.").Required().Enum(metadata.DeletionMarkFilename, metadata.NoCompactMarkFilename)
	details := cmd.Flag("details", "Human readable details to be put into marker.").Required().String()

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Mark.String())
		if err != nil {
			return err
		}

		var ids []ulid.ULID
		for _, id := range *blockIDs {
			u, err := ulid.Parse(id)
			if err != nil {
				return errors.Errorf("block.id is not a valid UUID, got: %v", id)
			}
			ids = append(ids, u)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		g.Add(func() error {
			for _, id := range ids {
				switch *marker {
				case metadata.DeletionMarkFilename:
					if err := block.MarkForDeletion(ctx, logger, bkt, id, *details, promauto.With(nil).NewCounter(prometheus.CounterOpts{})); err != nil {
						return errors.Wrapf(err, "mark %v for %v", id, *marker)
					}
				case metadata.NoCompactMarkFilename:
					if err := block.MarkForNoCompact(ctx, logger, bkt, id, metadata.ManualNoCompactReason, *details, promauto.With(nil).NewCounter(prometheus.CounterOpts{})); err != nil {
						return errors.Wrapf(err, "mark %v for %v", id, *marker)
					}
				default:
					return errors.Errorf("not supported marker %v", *marker)
				}
			}
			level.Info(logger).Log("msg", "marking done", "marker", *marker, "IDs", strings.Join(*blockIDs, ","))
			return nil
		}, func(err error) {
			cancel()
		})
		return nil
	})
}

func registerBucketRewrite(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	cmd := app.Command(component.Rewrite.String(), "Rewrite chosen blocks in the bucket, while deleting or modifying series "+
		"Resulted block has modified stats in meta.json. Additionally compaction.sources are altered to not confuse readers of meta.json. "+
		"Instead thanos.rewrite section is added with useful info like old sources and deletion requests. "+
		"NOTE: It's recommended to turn off compactor while doing this operation. If the compactor is running and touching exactly same block that "+
		"is being rewritten, the resulted rewritten block might only cause overlap (mitigated by marking overlapping block manually for deletion) "+
		"and the data you wanted to rewrite could already part of bigger block.\n\n"+
		"Use FILESYSTEM type of bucket to rewrite block on disk (suitable for vanilla Prometheus) "+
		"After rewrite, it's caller responsibility to delete or mark source block for deletion to avoid overlaps. "+
		"WARNING: This procedure is *IRREVERSIBLE* after certain time (delete delay), so do backup your blocks first.")
	blockIDs := cmd.Flag("id", "ID (ULID) of the blocks for rewrite (repeated flag).").Required().Strings()
	tmpDir := cmd.Flag("tmp.dir", "Working directory for temporary files").Default(filepath.Join(os.TempDir(), "thanos-rewrite")).String()
	hashFunc := cmd.Flag("hash-func", "Specify which hash function to use when calculating the hashes of produced files. If no function has been specified, it does not happen. This permits avoiding downloading some files twice albeit at some performance cost. Possible values are: \"\", \"SHA256\".").
		Default("").Enum("SHA256", "")
	dryRun := cmd.Flag("dry-run", "Prints the series changes instead of doing them. Defaults to true, for user to double check. (: Pass --no-dry-run to skip this.").Default("true").Bool()
	toDelete := extflag.RegisterPathOrContent(cmd, "rewrite.to-delete-config", "YAML file that contains []metadata.DeletionRequest that will be applied to blocks", extflag.WithEnvSubstitution())
	toRelabel := extflag.RegisterPathOrContent(cmd, "rewrite.to-relabel-config", "YAML file that contains relabel configs that will be applied to blocks", extflag.WithEnvSubstitution())
	provideChangeLog := cmd.Flag("rewrite.add-change-log", "If specified, all modifications are written to new block directory. Disable if latency is to high.").Default("true").Bool()
	promBlocks := cmd.Flag("prom-blocks", "If specified, we assume the blocks to be uploaded are only used with Prometheus so we don't check external labels in this case.").Default("false").Bool()
	deleteBlocks := cmd.Flag("delete-blocks", "Whether to delete the original blocks after rewriting blocks successfully. Available in non dry-run mode only.").Default("false").Bool()
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Rewrite.String())
		if err != nil {
			return err
		}

		var modifiers []compactv2.Modifier

		relabelYaml, err := toRelabel.Content()
		if err != nil {
			return err
		}
		var relabels []*relabel.Config
		if len(relabelYaml) > 0 {
			relabels, err = block.ParseRelabelConfig(relabelYaml, nil)
			if err != nil {
				return err
			}
			modifiers = append(modifiers, compactv2.WithRelabelModifier(relabels...))
		}

		deletionsYaml, err := toDelete.Content()
		if err != nil {
			return err
		}
		var deletions []metadata.DeletionRequest
		if len(deletionsYaml) > 0 {
			if err := yaml.Unmarshal(deletionsYaml, &deletions); err != nil {
				return err
			}
			modifiers = append(modifiers, compactv2.WithDeletionModifier(deletions...))
		}

		if len(modifiers) == 0 {
			return errors.New("rewrite configuration should be provided")
		}

		var ids []ulid.ULID
		for _, id := range *blockIDs {
			u, err := ulid.Parse(id)
			if err != nil {
				return errors.Errorf("id is not a valid block ULID, got: %v", id)
			}
			ids = append(ids, u)
		}

		if err := os.RemoveAll(*tmpDir); err != nil {
			return err
		}
		if err := os.MkdirAll(*tmpDir, os.ModePerm); err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			chunkPool := chunkenc.NewPool()
			changeLog := compactv2.NewChangeLog(ioutil.Discard)
			stubCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			for _, id := range ids {
				// Delete series from block & modify.
				level.Info(logger).Log("msg", "downloading block", "source", id)
				if err := block.Download(ctx, logger, bkt, id, filepath.Join(*tmpDir, id.String())); err != nil {
					return errors.Wrapf(err, "download %v", id)
				}

				meta, err := metadata.ReadFromDir(filepath.Join(*tmpDir, id.String()))
				if err != nil {
					return errors.Wrapf(err, "read meta of %v", id)
				}
				b, err := tsdb.OpenBlock(logger, filepath.Join(*tmpDir, id.String()), chunkPool)
				if err != nil {
					return errors.Wrapf(err, "open block %v", id)
				}

				p := compactv2.NewProgressLogger(logger, int(b.Meta().Stats.NumSeries))
				newID := ulid.MustNew(ulid.Now(), rand.Reader)
				meta.ULID = newID
				meta.Thanos.Rewrites = append(meta.Thanos.Rewrites, metadata.Rewrite{
					Sources:          meta.Compaction.Sources,
					DeletionsApplied: deletions,
					RelabelsApplied:  relabels,
				})
				meta.Compaction.Sources = []ulid.ULID{newID}
				meta.Thanos.Source = metadata.BucketRewriteSource

				if err := os.MkdirAll(filepath.Join(*tmpDir, newID.String()), os.ModePerm); err != nil {
					return err
				}

				if *provideChangeLog {
					f, err := os.OpenFile(filepath.Join(*tmpDir, newID.String(), "change.log"), os.O_CREATE|os.O_WRONLY, os.ModePerm)
					if err != nil {
						return err
					}
					defer runutil.CloseWithLogOnErr(logger, f, "close changelog")

					changeLog = compactv2.NewChangeLog(f)
					level.Info(logger).Log("msg", "changelog will be available", "file", filepath.Join(*tmpDir, newID.String(), "change.log"))
				}

				d, err := block.NewDiskWriter(ctx, logger, filepath.Join(*tmpDir, newID.String()))
				if err != nil {
					return err
				}

				var comp *compactv2.Compactor
				if *dryRun {
					comp = compactv2.NewDryRun(*tmpDir, logger, changeLog, chunkPool)
				} else {
					comp = compactv2.New(*tmpDir, logger, changeLog, chunkPool)
				}

				level.Info(logger).Log("msg", "starting rewrite for block", "source", id, "new", newID, "toDelete", string(deletionsYaml), "toRelabel", string(relabelYaml))
				if err := comp.WriteSeries(ctx, []block.Reader{b}, d, p, modifiers...); err != nil {
					return errors.Wrapf(err, "writing series from %v to %v", id, newID)
				}

				if *dryRun {
					level.Info(logger).Log("msg", "dry run finished. Changes should be printed to stderr")
					return nil
				}

				level.Info(logger).Log("msg", "wrote new block after modifications; flushing", "source", id, "new", newID)
				meta.Stats, err = d.Flush()
				if err != nil {
					return errors.Wrap(err, "flush")
				}
				if err := meta.WriteToDir(logger, filepath.Join(*tmpDir, newID.String())); err != nil {
					return err
				}

				level.Info(logger).Log("msg", "uploading new block", "source", id, "new", newID)
				if *promBlocks {
					if err := block.UploadPromBlock(ctx, logger, bkt, filepath.Join(*tmpDir, newID.String()), metadata.HashFunc(*hashFunc)); err != nil {
						return errors.Wrap(err, "upload")
					}
				} else {
					if err := block.Upload(ctx, logger, bkt, filepath.Join(*tmpDir, newID.String()), metadata.HashFunc(*hashFunc)); err != nil {
						return errors.Wrap(err, "upload")
					}
				}
				level.Info(logger).Log("msg", "uploaded", "source", id, "new", newID)

				if !*dryRun && *deleteBlocks {
					if err := block.MarkForDeletion(ctx, logger, bkt, id, "block rewritten", stubCounter); err != nil {
						level.Error(logger).Log("msg", "failed to mark block for deletion", "id", id.String(), "err", err)
					}
				}
			}
			level.Info(logger).Log("msg", "rewrite done", "IDs", strings.Join(*blockIDs, ","))
			return nil
		}, func(err error) {
			cancel()
		})
		return nil
	})
}

func registerBucketRetention(app extkingpin.AppClause, objStoreConfig *extflag.PathOrContent) {
	var (
		retentionRaw, retentionFiveMin, retentionOneHr prommodel.Duration
	)

	cmd := app.Command("retention", "Retention applies retention policies on the given bucket. Please make sure no compactor is running on the same bucket at the same time.")
	deleteDelay := cmd.Flag("delete-delay", "Time before a block marked for deletion is deleted from bucket.").Default("48h").Duration()
	consistencyDelay := cmd.Flag("consistency-delay", fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %v will be removed.", compact.PartialUploadThresholdAge)).
		Default("30m").Duration()
	blockSyncConcurrency := cmd.Flag("block-sync-concurrency", "Number of goroutines to use when syncing block metadata from object storage.").
		Default("20").Int()
	selectorRelabelConf := extkingpin.RegisterSelectorRelabelFlags(cmd)
	cmd.Flag("retention.resolution-raw",
		"How long to retain raw samples in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&retentionRaw)
	cmd.Flag("retention.resolution-5m", "How long to retain samples of resolution 1 (5 minutes) in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&retentionFiveMin)
	cmd.Flag("retention.resolution-1h", "How long to retain samples of resolution 2 (1 hour) in bucket. Setting this to 0d will retain samples of this resolution forever").
		Default("0d").SetValue(&retentionOneHr)
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		retentionByResolution := map[compact.ResolutionLevel]time.Duration{
			compact.ResolutionLevelRaw: time.Duration(retentionRaw),
			compact.ResolutionLevel5m:  time.Duration(retentionFiveMin),
			compact.ResolutionLevel1h:  time.Duration(retentionOneHr),
		}

		if retentionByResolution[compact.ResolutionLevelRaw].Seconds() != 0 {
			level.Info(logger).Log("msg", "retention policy of raw samples is enabled", "duration", retentionByResolution[compact.ResolutionLevelRaw])
		}
		if retentionByResolution[compact.ResolutionLevel5m].Seconds() != 0 {
			level.Info(logger).Log("msg", "retention policy of 5 min aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel5m])
		}
		if retentionByResolution[compact.ResolutionLevel1h].Seconds() != 0 {
			level.Info(logger).Log("msg", "retention policy of 1 hour aggregated samples is enabled", "duration", retentionByResolution[compact.ResolutionLevel1h])
		}

		confContentYaml, err := objStoreConfig.Content()
		if err != nil {
			return err
		}

		relabelContentYaml, err := selectorRelabelConf.Content()
		if err != nil {
			return errors.Wrap(err, "get content of relabel configuration")
		}

		relabelConfig, err := block.ParseRelabelConfig(relabelContentYaml, block.SelectorSupportedRelabelActions)
		if err != nil {
			return err
		}

		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Rewrite.String())
		if err != nil {
			return err
		}

		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

		// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
		// The delay of deleteDelay/2 is added to ensure we fetch blocks that are meant to be deleted but do not have a replacement yet.
		// This is to make sure compactor will not accidentally perform compactions with gap instead.
		ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, *deleteDelay/2, block.FetcherConcurrency)
		duplicateBlocksFilter := block.NewDeduplicateFilter()
		stubCounter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

		var sy *compact.Syncer
		{
			baseMetaFetcher, err := block.NewBaseFetcher(logger, block.FetcherConcurrency, bkt, "", extprom.WrapRegistererWithPrefix(extpromPrefix, reg))
			if err != nil {
				return errors.Wrap(err, "create meta fetcher")
			}
			cf := baseMetaFetcher.NewMetaFetcher(
				extprom.WrapRegistererWithPrefix(extpromPrefix, reg), []block.MetadataFilter{
					block.NewLabelShardedMetaFilter(relabelConfig),
					block.NewConsistencyDelayMetaFilter(logger, *consistencyDelay, extprom.WrapRegistererWithPrefix(extpromPrefix, reg)),
					duplicateBlocksFilter,
					ignoreDeletionMarkFilter,
				}, []block.MetadataModifier{block.NewReplicaLabelRemover(logger, make([]string, 0))},
			)
			sy, err = compact.NewMetaSyncer(
				logger,
				reg,
				bkt,
				cf,
				duplicateBlocksFilter,
				ignoreDeletionMarkFilter,
				stubCounter,
				stubCounter,
				*blockSyncConcurrency)
			if err != nil {
				return errors.Wrap(err, "create syncer")
			}
		}

		ctx := context.Background()
		level.Info(logger).Log("msg", "syncing blocks metadata")
		if err := sy.SyncMetas(ctx); err != nil {
			return errors.Wrap(err, "sync blocks")
		}

		level.Info(logger).Log("msg", "synced blocks done")

		level.Warn(logger).Log("msg", "GLOBAL COMPACTOR SHOULD __NOT__ BE RUNNING ON THE SAME BUCKET")

		if err := compact.ApplyRetentionPolicyByResolution(ctx, logger, bkt, sy.Metas(), retentionByResolution, stubCounter); err != nil {
			return errors.Wrap(err, "retention failed")
		}
		return nil
	})
}
