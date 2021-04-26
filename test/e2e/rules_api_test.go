// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"

	http_util "github.com/thanos-io/thanos/pkg/http"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestRulesAPI_Fanout(t *testing.T) {
	t.Parallel()

	netName := "e2e_test_rules_fanout"

	s, err := e2e.NewScenario(netName)
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	promRulesSubDir := filepath.Join("rules")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), promRulesSubDir), os.ModePerm))
	// Create the abort_on_partial_response alert for Prometheus.
	// We don't create the warn_on_partial_response alert as Prometheus has strict yaml unmarshalling.
	createRuleFile(t, filepath.Join(s.SharedDir(), promRulesSubDir, "rules.yaml"), testAlertRuleAbortOnPartialResponse)

	thanosRulesSubDir := filepath.Join("thanos-rules")
	testutil.Ok(t, os.MkdirAll(filepath.Join(s.SharedDir(), thanosRulesSubDir), os.ModePerm))
	createRuleFiles(t, filepath.Join(s.SharedDir(), thanosRulesSubDir))

	// 2x Prometheus.
	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom1",
		defaultPromConfig("ha", 0, "", filepath.Join(e2e.ContainerSharedDir, promRulesSubDir, "*.yaml")),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom2",
		defaultPromConfig("ha", 1, "", filepath.Join(e2e.ContainerSharedDir, promRulesSubDir, "*.yaml")),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	// 2x Rulers.
	r1, err := e2ethanos.NewTSDBRuler(s.SharedDir(), "rule1", thanosRulesSubDir, nil, nil)
	testutil.Ok(t, err)
	r2, err := e2ethanos.NewTSDBRuler(s.SharedDir(), "rule2", thanosRulesSubDir, nil, nil)
	testutil.Ok(t, err)

	stores := []string{sidecar1.GRPCNetworkEndpoint(), sidecar2.GRPCNetworkEndpoint(), r1.NetworkEndpointFor(s.NetworkName(), 9091), r2.NetworkEndpointFor(s.NetworkName(), 9091)}
	q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "query", stores).
		WithRuleAddresses(stores).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	queryCfg := []query.Config{
		{
			EndpointsConfig: http_util.EndpointsConfig{
				StaticAddresses: []string{q.NetworkHTTPEndpoint()},
				Scheme:          "http",
			},
		},
	}

	// Recreate rulers with the corresponding query config.
	r1, err = e2ethanos.NewRuler(s.SharedDir(), "rule1", thanosRulesSubDir, nil, queryCfg)
	testutil.Ok(t, err)
	r2, err = e2ethanos.NewRuler(s.SharedDir(), "rule2", thanosRulesSubDir, nil, queryCfg)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(r1, r2))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(4), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

	ruleAndAssert(t, ctx, q.HTTPEndpoint(), "", []*rulespb.RuleGroup{
		{
			Name: "example_abort",
			File: "/shared/rules/rules.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_AbortOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "prometheus", Value: "ha"},
						{Name: "severity", Value: "page"},
					}},
				}),
			},
		},
		{
			Name: "example_abort",
			File: "/shared/thanos-rules/rules-0.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_AbortOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "page"},
					}},
				}),
			},
		},
		{
			Name: "example_warn",
			File: "/shared/thanos-rules/rules-1.yaml",
			Rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "TestAlert_WarnOnPartialResponse",
					State: rulespb.AlertState_FIRING,
					Query: "absent(some_metric)",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "page"},
					}},
				}),
			},
		},
	})
}

func ruleAndAssert(t *testing.T, ctx context.Context, addr string, typ string, want []*rulespb.RuleGroup) {
	t.Helper()

	fmt.Println("ruleAndAssert: Waiting for results for rules type", typ)
	var result []*rulespb.RuleGroup
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		res, err := promclient.NewDefaultClient().RulesInGRPC(ctx, mustURLParse(t, "http://"+addr), typ)
		if err != nil {
			return err
		}

		if len(result) != len(res) {
			fmt.Println("ruleAndAssert: New result:", res)
		}

		if len(res) != len(want) {
			return errors.Errorf("unexpected result size, want %d; got: %d result: %v", len(want), len(res), res)
		}

		for ig, g := range res {
			res[ig].LastEvaluation = time.Time{}
			res[ig].EvaluationDurationSeconds = 0
			res[ig].Interval = 0
			res[ig].PartialResponseStrategy = 0

			sort.Slice(g.Rules, func(i, j int) bool { return g.Rules[i].Compare(g.Rules[j]) < 0 })

			for ir, r := range g.Rules {
				if alert := r.GetAlert(); alert != nil {
					res[ig].Rules[ir] = rulespb.NewAlertingRule(&rulespb.Alert{
						Name:   alert.Name,
						State:  alert.State,
						Query:  alert.Query,
						Labels: alert.Labels,
					})
				} else if rec := r.GetRecording(); rec != nil {
					res[ig].Rules[ir] = rulespb.NewAlertingRule(&rulespb.Alert{
						Name:   rec.Name,
						Query:  rec.Query,
						Labels: rec.Labels,
					})
				}
			}
		}

		if !reflect.DeepEqual(want, res) {
			return errors.Errorf("unexpected result\nwant %v\ngot: %v", want, res)
		}

		return nil
	}))
}
