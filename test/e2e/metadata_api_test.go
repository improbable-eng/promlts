// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestMetricMetadataAPI_Fanout(t *testing.T) {
	t.Parallel()

	netName := "e2e_test_metric_metadata_fanout"

	s, err := e2e.NewScenario(netName)
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	// 2x Prometheus.
	// Each Prometheus scrapes its own metrics and Sidecar's metrics.
	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom1",
		defaultPromConfig("ha", 0, "", "", "localhost:9090", "sidecar-prom1:8080"),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)

	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom2",
		defaultPromConfig("ha", 1, "", "", "localhost:9090", "sidecar-prom2:8080"),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	stores := []string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint()}
	q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "query", stores).
		WithMetadataAddresses(stores).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))
	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_query_metadata_apis_dns_provider_results"}, e2e.WaitMissingMetrics))

	var promMeta map[string][]metadatapb.Meta
	// Wait metadata response to be ready as Prometheus gets metadata after scrape.
	testutil.Ok(t, runutil.Retry(3*time.Second, ctx.Done(), func() error {
		promMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+prom1.HTTPEndpoint()), "", -1)
		testutil.Ok(t, err)
		if len(promMeta) > 0 {
			return nil
		}
		return fmt.Errorf("empty metadata response from Prometheus")
	}))
	time.Sleep(time.Second * 5)
	thanosMeta, err := promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", -1)
	testutil.Ok(t, err)
	testutil.Assert(t, len(thanosMeta) > 0, "got empty metadata response from Thanos")

	// Metadata response from Prometheus and Thanos Querier should be the same after deduplication.
	metricMetadataEqual(t, thanosMeta, promMeta)

	// We only expect to see one metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 1)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 1)

	// We only expect to see ten metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 10)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 10)

	// No metadata returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", 0)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 0)

	// Only prometheus_build_info metric will be returned.
	thanosMeta, err = promclient.NewDefaultClient().MetricMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "prometheus_build_info", -1)
	testutil.Ok(t, err)
	testutil.Assert(t, len(thanosMeta) == 1 && len(thanosMeta["prometheus_build_info"]) > 0, "expected one prometheus_build_info metadata from Thanos, got %v", thanosMeta)
}

func metricMetadataEqual(t *testing.T, meta1, meta2 map[string][]metadatapb.Meta) {
	// The two responses should have equal # of entries.
	testutil.Equals(t, len(meta1), len(meta2))

	for metric := range meta1 {
		// Get metadata for the metric.
		meta1MetricMeta := meta1[metric]
		meta2MetricMeta, ok := meta2[metric]
		testutil.Assert(t, ok)

		sort.Slice(meta1MetricMeta, func(i, j int) bool {
			return meta1MetricMeta[i].Help < meta1MetricMeta[j].Help
		})
		sort.Slice(meta2MetricMeta, func(i, j int) bool {
			return meta2MetricMeta[i].Help < meta2MetricMeta[j].Help
		})
		testutil.Equals(t, meta1MetricMeta, meta2MetricMeta)
	}
}

func TestTargetMetadataAPI_Fanout(t *testing.T) {
	t.Parallel()
	netName := "e2e_test_target_metadata_fanout"

	s, err := e2e.NewScenario(netName)
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	// 2x Prometheus.
	// Each Prometheus scrapes its own metrics and Sidecar's metrics.
	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom1",
		defaultPromConfig("ha", 0, "", "", "localhost:9090", "sidecar-prom1:8080"),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)

	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom2",
		defaultPromConfig("ha", 1, "", "", "localhost:9090", "sidecar-prom2:8080"),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	stores := []string{sidecar2.NetworkEndpoint(9091), sidecar1.NetworkEndpoint(9091)}
	q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "query", stores).
		WithMetadataAddresses(stores).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	thanosCtx, thanosCancel := context.WithTimeout(context.Background(), time.Second*10)
	t.Cleanup(thanosCancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))
	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_query_metadata_apis_dns_provider_results"}, e2e.WaitMissingMetrics))

	var (
		promOneMeta []*metadatapb.TargetMetadata
		promTwoMeta []*metadatapb.TargetMetadata
		thanosMeta  []*metadatapb.TargetMetadata
	)

	// Wait metadata response to be ready as Prometheus gets metadata after scrape.
	testutil.Ok(t, runutil.Retry(3*time.Second, ctx.Done(), func() error {
		promOneMeta, err = promclient.NewDefaultClient().TargetMetadataInGRPC(ctx, mustURLParse(t, "http://"+prom1.HTTPEndpoint()), "", "", -1)
		testutil.Ok(t, err)
		if len(promOneMeta) > 0 {
			return nil
		}
		return fmt.Errorf("empty metadata response from Prometheus 1")
	}))

	testutil.Ok(t, runutil.Retry(3*time.Second, ctx.Done(), func() error {
		promTwoMeta, err = promclient.NewDefaultClient().TargetMetadataInGRPC(ctx, mustURLParse(t, "http://"+prom2.HTTPEndpoint()), "", "", -1)
		testutil.Ok(t, err)
		if len(promTwoMeta) > 0 {
			return nil
		}
		return fmt.Errorf("empty metadata response from Prometheus 2")
	}))

	testutil.Ok(t, runutil.Retry(3*time.Second, thanosCtx.Done(), func() error {
		thanosMeta, err = promclient.NewDefaultClient().TargetMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", "", -1)
		testutil.Ok(t, err)
		if len(thanosMeta) > 0 {
			return nil
		}
		return fmt.Errorf("empty metadata response from Thanos querier")
	}))

	verifyTargetMetadataResponses(t, promOneMeta, promTwoMeta, thanosMeta)

	//matchTargetSelector := "instance=localhost:9090"
	// We expect no metadata when we pass an invalid target
	thanosMeta, err = promclient.NewDefaultClient().TargetMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), `{instance="i-dont-exist"}`, "", -1)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 0)

	//matchTargetSelector := "instance=localhost:9090"
	// We expect no metadata when we pass an invalid target
	thanosMeta, err = promclient.NewDefaultClient().TargetMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), `{instance="localhost:9090"}`, "", -1)
	testutil.Ok(t, err)
	// extract the instance meta without sidecar metrics
	meta := extractPromMetadata(t, promOneMeta)
	// expect the same from thanos after deduplication
	testutil.Equals(t, len(meta), len(thanosMeta))

	// Only prometheus_build_info metric will be returned and the metric name should not be included
	thanosMeta, err = promclient.NewDefaultClient().TargetMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", "prometheus_build_info", 1)
	testutil.Ok(t, err)
	testutil.Assert(t, len(thanosMeta) == 1 && thanosMeta[0].Metric == "")

	// No metadata returned because the limit is set to zero
	thanosMeta, err = promclient.NewDefaultClient().TargetMetadataInGRPC(ctx, mustURLParse(t, "http://"+q.HTTPEndpoint()), "", "", 0)
	testutil.Ok(t, err)
	testutil.Equals(t, len(thanosMeta), 0)

}

func verifyTargetMetadataResponses(t *testing.T, promOneMeta, promTwoMeta, thanosMeta []*metadatapb.TargetMetadata) {
	promTwoSidecarMeta := extractSidecarMetadata(t, promTwoMeta)
	expect := append(promOneMeta, promTwoSidecarMeta...)
	// The aggregated data should be the same length as thanos data after deduplication
	testutil.Equals(t, len(expect), len(thanosMeta))

	// sort the metadata prior to comparison
	expect = sortMetadata(t, expect)
	thanosMeta = sortMetadata(t, thanosMeta)

	assertMetaIsEqual(t, expect, thanosMeta)
}

func assertMetaIsEqual(t *testing.T, compare, to []*metadatapb.TargetMetadata) {
	for i := 0; i < len(compare); i++ {
		compareThis := compare[i]
		toThat := to[i]
		testutil.Equals(t, compareThis.Help, toThat.Help)
		testutil.Equals(t, compareThis.Metric, toThat.Metric)
		testutil.Equals(t, compareThis.Unit, toThat.Unit)
		testutil.Equals(t, compareThis.Target.Job, toThat.Target.Job)
		testutil.Equals(t, compareThis.Target.Instance, toThat.Target.Instance)
	}
}

func extractSidecarMetadata(t *testing.T, meta []*metadatapb.TargetMetadata) []*metadatapb.TargetMetadata {
	t.Helper()
	var sidecarMeta []*metadatapb.TargetMetadata
	for _, m := range meta {
		if strings.HasPrefix(m.Target.Instance, "sidecar") {
			sidecarMeta = append(sidecarMeta, m)
		}
	}
	return sidecarMeta
}

func extractPromMetadata(t *testing.T, meta []*metadatapb.TargetMetadata) []*metadatapb.TargetMetadata {
	t.Helper()
	var promMeta []*metadatapb.TargetMetadata
	for _, m := range meta {
		if !strings.HasPrefix(m.Target.Instance, "sidecar") {
			promMeta = append(promMeta, m)
		}
	}
	return promMeta
}

// sortMetadata sorts the slice is a consistent fashion based on struct contents.
func sortMetadata(t *testing.T, meta []*metadatapb.TargetMetadata) []*metadatapb.TargetMetadata {
	t.Helper()
	if meta == nil {
		return meta
	}

	sort.Slice(meta, func(i, j int) bool {
		if meta[i].Target.Instance < meta[j].Target.Instance {
			return true
		}
		if meta[i].Target.Instance > meta[j].Target.Instance {
			return false
		}
		if meta[i].Metric < meta[j].Metric {
			return true
		}
		if meta[i].Metric > meta[j].Metric {
			return false
		}
		return meta[i].Help < meta[j].Help && meta[i].Metric < meta[j].Metric
	})
	return meta
}
