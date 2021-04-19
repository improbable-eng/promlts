// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package replicate

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"time"

	thanosmodel "github.com/thanos-io/thanos/pkg/model"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	thanosblock "github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/server/http"
)

const (
	// Labels for metrics.
	labelSuccess = "success"
	labelError   = "error"
)

// ParseFlagMatchers parse flag into matchers.
func ParseFlagMatchers(s []string) ([]*labels.Matcher, error) {
	matchers := make([]*labels.Matcher, 0, len(s))

	for _, l := range s {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return nil, errors.Errorf("unrecognized label %q", l)
		}

		labelName := parts[0]
		if !model.LabelName.IsValid(model.LabelName(labelName)) {
			return nil, errors.Errorf("unsupported format for label %s", l)
		}

		labelValue, err := strconv.Unquote(parts[1])
		if err != nil {
			return nil, errors.Wrap(err, "unquote label value")
		}
		newEqualMatcher, err := labels.NewMatcher(labels.MatchEqual, labelName, labelValue)
		if err != nil {
			return nil, errors.Wrap(err, "new equal matcher")
		}
		matchers = append(matchers, newEqualMatcher)
	}

	return matchers, nil
}

// RunReplicate replicate data based on config.
func RunReplicate(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	_ opentracing.Tracer,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	labelSelector labels.Selector,
	resolutions []compact.ResolutionLevel,
	compactions []int,
	fromObjStoreConfig *extflag.PathOrContent,
	toObjStoreConfig *extflag.PathOrContent,
	singleRun bool,
	minTime, maxTime *thanosmodel.TimeOrDurationValue,
	blockIDs []ulid.ULID,
) error {
	logger = log.With(logger, "component", "replicate")

	level.Debug(logger).Log("msg", "setting up http listen-group")

	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		prober.NewInstrumentation(component.Replicate, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	s := http.New(logger, reg, component.Replicate, httpProbe,
		http.WithListen(httpBindAddr),
		http.WithGracePeriod(httpGracePeriod),
	)

	g.Add(func() error {
		level.Info(logger).Log("msg", "Listening for http service", "address", httpBindAddr)

		statusProber.Healthy()

		return s.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		s.Shutdown(err)
	})

	fromConfContentYaml, err := fromObjStoreConfig.Content()
	if err != nil {
		return err
	}
	fromConfPathYaml, err := fromObjStoreConfig.GetPath()
	if len(fromConfContentYaml) == 0 {
		return errors.New("No supported bucket was configured to replicate from")
	}

	fromBkt, err := client.NewBucket(
		logger,
		fromConfContentYaml,
		prometheus.WrapRegistererWith(prometheus.Labels{"replicate": "from"}, reg),
		component.Replicate.String(),
		fromConfPathYaml...,
	)
	if err != nil {
		return err
	}

	toConfContentYaml, err := toObjStoreConfig.Content()
	if err != nil {
		return err
	}
	toConfPathYaml, err := toObjStoreConfig.GetPath()
	if len(toConfContentYaml) == 0 {
		return errors.New("No supported bucket was configured to replicate to")
	}

	toBkt, err := client.NewBucket(
		logger,
		toConfContentYaml,
		prometheus.WrapRegistererWith(prometheus.Labels{"replicate": "to"}, reg),
		component.Replicate.String(),
		toConfPathYaml...,
	)
	if err != nil {
		return err
	}

	replicationRunCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_replicate_replication_runs_total",
		Help: "The number of replication runs split by success and error.",
	}, []string{"result"})
	replicationRunCounter.WithLabelValues(labelSuccess)
	replicationRunCounter.WithLabelValues(labelError)

	replicationRunDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "thanos_replicate_replication_run_duration_seconds",
		Help: "The Duration of replication runs split by success and error.",
	}, []string{"result"})
	replicationRunDuration.WithLabelValues(labelSuccess)
	replicationRunDuration.WithLabelValues(labelError)

	fetcher, err := thanosblock.NewMetaFetcher(
		logger,
		32,
		fromBkt,
		"",
		reg,
		[]thanosblock.MetadataFilter{thanosblock.NewTimePartitionMetaFilter(*minTime, *maxTime)},
		nil,
	)
	if err != nil {
		return errors.Wrapf(err, "create meta fetcher with bucket %v", fromBkt)
	}

	blockFilter := NewBlockFilter(
		logger,
		labelSelector,
		resolutions,
		compactions,
		blockIDs,
	).Filter
	metrics := newReplicationMetrics(reg)
	ctx, cancel := context.WithCancel(context.Background())

	replicateFn := func() error {
		timestamp := time.Now()
		entropy := ulid.Monotonic(rand.New(rand.NewSource(timestamp.UnixNano())), 0)

		runID, err := ulid.New(ulid.Timestamp(timestamp), entropy)
		if err != nil {
			return errors.Wrap(err, "generate replication run-id")
		}

		logger := log.With(logger, "replication-run-id", runID.String())
		level.Info(logger).Log("msg", "running replication attempt")

		if err := newReplicationScheme(logger, metrics, blockFilter, fetcher, fromBkt, toBkt, reg).execute(ctx); err != nil {
			return errors.Wrap(err, "replication execute")
		}

		return nil
	}

	g.Add(func() error {
		defer runutil.CloseWithLogOnErr(logger, fromBkt, "from bucket client")
		defer runutil.CloseWithLogOnErr(logger, toBkt, "to bucket client")

		if singleRun || len(blockIDs) > 0 {
			return replicateFn()
		}

		return runutil.Repeat(time.Minute, ctx.Done(), func() error {
			start := time.Now()
			if err := replicateFn(); err != nil {
				level.Error(logger).Log("msg", "running replication failed", "err", err)
				replicationRunCounter.WithLabelValues(labelError).Inc()
				replicationRunDuration.WithLabelValues(labelError).Observe(time.Since(start).Seconds())

				// No matter the error we want to repeat indefinitely.
				return nil
			}
			replicationRunCounter.WithLabelValues(labelSuccess).Inc()
			replicationRunDuration.WithLabelValues(labelSuccess).Observe(time.Since(start).Seconds())
			level.Info(logger).Log("msg", "ran replication successfully")

			return nil
		})
	}, func(error) {
		cancel()
	})

	level.Info(logger).Log("msg", "starting replication")

	return nil
}
