// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestQueryFrontend(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_query_frontend")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	prom, sidecar, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), s.NetworkName(), "1", defaultPromConfig("test", 0, "", ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom, sidecar))

	q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{sidecar.GRPCNetworkEndpoint()}, nil, nil, "", "")
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	queryFrontend, err := e2ethanos.NewQueryFrontend(s.SharedDir(), "1", "http://"+q.NetworkHTTPEndpoint())
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(queryFrontend))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"replica":    "0",
		},
	})

	now := time.Now()

	t.Run("query frontend works for instant query", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, queryFrontend.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "test",
				"replica":    "0",
			},
		})

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "op", "query"))),
		)
	})

	t.Run("query frontend works for labels APIs", func(t *testing.T) {
		// LabelNames and LabelValues API should still work via query frontend.
		labelNames(t, ctx, queryFrontend.HTTPEndpoint(), timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) > 0
		})
		labelValues(t, ctx, queryFrontend.HTTPEndpoint(), "instance", timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) > 0
		})
	})

	t.Run("query frontend works for range query and it can cache results", func(t *testing.T) {
		rangeQuery(
			t,
			ctx,
			queryFrontend.HTTPEndpoint(),
			queryUpWithoutInstance,
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(time.Hour)),
			14,
			promclient.QueryOptions{},
			func(res model.Matrix) bool {
				return len(res) > 0
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "op", "query_range"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "cortex_cache_fetched_keys"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(0), "cortex_cache_hits"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_misses_total"))

		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "cortex_frontend_split_queries_total"))
	})

	t.Run("same range query results can be retrieved from cache directly", func(t *testing.T) {
		// Run the same range query again, the result can be retrieved from cache directly.
		rangeQuery(
			t,
			ctx,
			queryFrontend.HTTPEndpoint(),
			queryUpWithoutInstance,
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(time.Hour)),
			14,
			promclient.QueryOptions{},
			func(res model.Matrix) bool {
				return len(res) > 0
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "op", "query_range"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "cortex_cache_fetched_keys"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "cortex_cache_hits"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_misses_total"))

		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "cortex_frontend_split_queries_total"))
	})

	t.Run("range query > 24h should be split", func(t *testing.T) {
		rangeQuery(
			t,
			ctx,
			queryFrontend.HTTPEndpoint(),
			queryUpWithoutInstance,
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(24*time.Hour)),
			14,
			promclient.QueryOptions{},
			func(res model.Matrix) bool {
				return len(res) > 0
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(3),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "op", "query_range"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(3), "cortex_cache_fetched_keys"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "cortex_cache_hits"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(3), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(3), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_misses_total"))

		// Query is 25h so it will be split to 2 requests.
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(4), "cortex_frontend_split_queries_total"))
	})
}
