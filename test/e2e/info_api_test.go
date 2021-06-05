// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestInfo(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_info")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "e2e_test_info", "alone1", defaultPromConfig("prom-alone1", 0, "", ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "e2e_test_info", "alone2", defaultPromConfig("prom-alone2", 0, "", ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	prom3, sidecar3, err := e2ethanos.NewPrometheusWithSidecar(s.SharedDir(), "e2e_test_info", "alone3", defaultPromConfig("prom-alone3", 0, "", ""), e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2, prom3, sidecar3))

	m := e2edb.NewMinio(8080, "thanos")
	testutil.Ok(t, s.StartAndWaitReady(m))
	str, err := e2ethanos.NewStoreGW(s.SharedDir(), "1", client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    "thanos",
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	})
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(str))

	// Register 1 sidecar using `--store`.
	// Register 2 sidecars and 1 storeGW using `--endpoint`.
	// Register `sidecar3` twice to verify it is deduplicated.
	q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{sidecar1.GRPCNetworkEndpoint()}).
		WithEndpoints([]string{
			sidecar2.GRPCNetworkEndpoint(),
			sidecar3.GRPCNetworkEndpoint(),
			sidecar3.GRPCNetworkEndpoint(),
			str.GRPCNetworkEndpoint(),
		}).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	expected := map[string][]query.StoreStatus{
		"sidecar": {
			{
				Name: "e2e_test_info-sidecar-alone1:9091",
				LabelSets: []labels.Labels{{
					{
						Name:  "prometheus",
						Value: "prom-alone1",
					},
					{
						Name:  "replica",
						Value: "0",
					},
				}},
			},
			{
				Name: "e2e_test_info-sidecar-alone2:9091",
				LabelSets: []labels.Labels{{
					{
						Name:  "prometheus",
						Value: "prom-alone2",
					},
					{
						Name:  "replica",
						Value: "0",
					},
				}},
			},
			{
				Name: "e2e_test_info-sidecar-alone3:9091",
				LabelSets: []labels.Labels{{
					{
						Name:  "prometheus",
						Value: "prom-alone3",
					},
					{
						Name:  "replica",
						Value: "0",
					},
				}},
			},
		},
		"store": {
			{
				Name:      "e2e_test_info-store-gw-1:9091",
				LabelSets: []labels.Labels{},
			},
		},
	}

	url := "http://" + path.Join(q.HTTPEndpoint(), "/api/v1/stores")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	err = runutil.Retry(time.Second, ctx.Done(), func() error {

		resp, err := http.Get(url)
		testutil.Ok(t, err)

		body, err := ioutil.ReadAll(resp.Body)
		testutil.Ok(t, err)

		var res struct {
			Data map[string][]query.StoreStatus `json:"data"`
		}

		if err = json.Unmarshal(body, &res); err != nil {
			t.Fatalf("Error unmarshaling JSON body: %s", err)
		}

		if err = assertStoreStatus(t, "sidecar", res.Data, expected); err != nil {
			return err
		}

		if err = assertStoreStatus(t, "store", res.Data, expected); err != nil {
			return err
		}

		return nil
	})
	testutil.Ok(t, err)
}

func assertStoreStatus(t *testing.T, component string, res map[string][]query.StoreStatus, expected map[string][]query.StoreStatus) error {
	t.Helper()

	if len(res[component]) != len(expected[component]) {
		return fmt.Errorf("Expected %d %s, got: %d", len(expected[component]), component, len(res[component]))
	}

	for i, v := range res[component] {
		// Set value of the fields which keep changing in every test run to their default value.
		v.MaxTime = 0
		v.MinTime = 0
		v.LastCheck = time.Time{}

		testutil.Equals(t, expected[component][i], v)
	}

	return nil
}
