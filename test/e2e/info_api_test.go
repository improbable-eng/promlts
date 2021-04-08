// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"testing"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
	"google.golang.org/grpc"
)

func TestInfoAPI_WithSidecar(t *testing.T) {
	t.Parallel()

	netName := "e2e_test_info_with_sidecar"

	s, err := e2e.NewScenario(netName)
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	prom, sidecar, err := e2ethanos.NewPrometheusWithSidecar(
		s.SharedDir(),
		netName,
		"prom",
		defaultPromConfig("ha", 0, "", ""),
		e2ethanos.DefaultPrometheusImage(),
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(prom, sidecar))

	// Create grpc Client
	conn, err := grpc.Dial(sidecar.GRPCEndpoint(), grpc.WithInsecure())
	testutil.Ok(t, err)
	defer conn.Close()

	client := infopb.NewInfoClient(conn)

	res, err := client.Info(context.Background(), &infopb.InfoRequest{})
	testutil.Ok(t, err)
	testutil.Equals(t, res, infopb.InfoResponse{
		LabelSets:     []labelpb.ZLabelSet{},
		ComponentType: infopb.ComponentType_SIDECAR,
		StoreInfo: &infopb.StoreInfo{
			MinTime: -62167219200000,
			MaxTime: 9223372036854775807,
		},
		ExemplarsInfo: &infopb.ExemplarsInfo{
			MinTime: -9223309901257974,
			MaxTime: 9223309901257974,
		},
	})
}
