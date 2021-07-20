// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

type ReverseProxyConfig struct {
	tenantId string
	port     string
	target   string
}

type DebugTransport struct{}

func (DebugTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	_, err := httputil.DumpRequestOut(r, false)
	if err != nil {
		return nil, err
	}
	return http.DefaultTransport.RoundTrip(r)
}

func generateProxy(conf ReverseProxyConfig) {
	targetURL, _ := url.Parse(conf.target)
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	d := proxy.Director
	proxy.Director = func(r *http.Request) {
		d(r) // call default director
		r.Header.Add("THANOS-TENANT", conf.tenantId)
	}
	proxy.ErrorHandler = ErrorHandler
	proxy.Transport = DebugTransport{}
	log.Fatal(http.ListenAndServe(conf.port, proxy))
}

func ErrorHandler(_ http.ResponseWriter, _ *http.Request, err error) {
	log.Print("Response from receiver")
	log.Print(err)
}

func TestReceive(t *testing.T) {
	t.Parallel()

	t.Run("single_ingestor", func(t *testing.T) {
		/*
			The single_ingestor suite represents the simplest possible configuration of Thanos Receive.
			 ┌──────────┐
			 │  Prom    │
			 └────┬─────┘
			      │
			 ┌────▼─────┐
			 │ Ingestor │
			 └────┬─────┘
			      │
			 ┌────▼─────┐
			 │  Query   │
			 └──────────┘
			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/

		t.Parallel()
		s, err := e2e.NewScenario("e2e_receive_single_ingestor")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// Setup Router Ingestor.
		i, err := e2ethanos.NewIngestingReceiver(s.SharedDir(), "ingestor")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(i))

		// Setup Prometheus
		prom, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(i.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom))

		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{i.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		// We expect the data from each Prometheus instance to be replicated twice across our ingesting instances
		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "ingestor",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("router_replication", func(t *testing.T) {
		/*
			The router_replication suite configures separate routing and ingesting components.
			It verifies that data ingested from Prometheus instances through the router is successfully replicated twice
			across the ingestors.

			  ┌───────┐       ┌───────┐      ┌───────┐
			  │       │       │       │      │       │
			  │ Prom1 │       │ Prom2 │      │ Prom3 │
			  │       │       │       │      │       │
			  └───┬───┘       └───┬───┘      └──┬────┘
			      │           ┌───▼────┐        │
			      └───────────►        ◄────────┘
			                  │ Router │
			      ┌───────────┤        ├──────────┐
			      │           └───┬────┘          │
			┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
			│           │   │           │   │           │
			│ Ingestor1 │   │ Ingestor2 │   │ Ingestor3 │
			│           │   │           │   │           │
			└─────┬─────┘   └─────┬─────┘   └─────┬─────┘
			      │           ┌───▼───┐           │
			      │           │       │           │
			      └───────────► Query ◄───────────┘
			                  │       │
			                  └───────┘

			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/

		t.Parallel()
		s, err := e2e.NewScenario("e2e_receive_router_replication")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// Setup 3 ingestors.
		i1, err := e2ethanos.NewIngestingReceiver(s.SharedDir(), "i1")
		testutil.Ok(t, err)
		i2, err := e2ethanos.NewIngestingReceiver(s.SharedDir(), "i2")
		testutil.Ok(t, err)
		i3, err := e2ethanos.NewIngestingReceiver(s.SharedDir(), "i3")
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				i1.GRPCNetworkEndpointFor(s.NetworkName()),
				i2.GRPCNetworkEndpointFor(s.NetworkName()),
				i3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Setup 1 distributor
		r1, err := e2ethanos.NewRoutingReceiver(s.SharedDir(), "r1", 2, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(i1, i2, i3, r1))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom2, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "2", defaultPromConfig(
			"prom2",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom3, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "3", defaultPromConfig(
			"prom3",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1, prom2, prom3))

		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{i1.GRPCNetworkEndpoint(), i2.GRPCNetworkEndpoint(), i3.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		expectedReplicationFactor := 2.0

		queryAndAssert(t, ctx, q.HTTPEndpoint(), "count(up) by (prometheus)", promclient.QueryOptions{
			Deduplicate: false,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom1",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom2",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom3",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
		})
	})

	t.Run("routing_tree", func(t *testing.T) {
		/*
			The routing_tree suite configures a valid and plausible, but non-trivial topology of receiver components.
			Crucially, the first router routes to both a routing component, and a receiving component. This demonstrates
			Receiver's ability to handle arbitrary depth receiving trees.

			Router1 is configured to duplicate data twice, once to Ingestor1, and once to Router2,
			Router2 is also configured to duplicate data twice, once to Ingestor2, and once to Ingestor3.

			           ┌───────┐         ┌───────┐
			           │       │         │       │
			           │ Prom1 ├──┐   ┌──┤ Prom2 │
			           │       │  │   │  │       │
			           └───────┘  │   │  └───────┘
			                   ┌──▼───▼──┐
			                   │         │
			                   │ Router1 │
			              ┌────┤         ├───────┐
			              │    └─────────┘       │
			          ┌───▼─────┐          ┌─────▼─────┐
			          │         │          │           │
			          │ Router2 │          │ Ingestor1 │
			      ┌───┤         ├───┐      │           │
			      │   └─────────┘   │      └─────┬─────┘
			┌─────▼─────┐      ┌────▼──────┐     │
			│           │      │           │     │
			│ Ingestor2 │      │ Ingestor3 │     │
			│           │      │           │     │
			└─────┬─────┘      └─────┬─────┘     │
			      │             ┌────▼────┐      │
			      │             │         │      │
			      └─────────────►  Query  ◄──────┘
			                    │         │
			                    └─────────┘

			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/

		t.Parallel()
		s, err := e2e.NewScenario("e2e_receive_routing_tree")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// Setup ingestors.
		i1, err := e2ethanos.NewIngestingReceiver(s.SharedDir(), "i1")
		testutil.Ok(t, err)
		i2, err := e2ethanos.NewIngestingReceiver(s.SharedDir(), "i2")
		testutil.Ok(t, err)
		i3, err := e2ethanos.NewIngestingReceiver(s.SharedDir(), "i3")
		testutil.Ok(t, err)

		// Setup distributors
		r2, err := e2ethanos.NewRoutingReceiver(s.SharedDir(), "r2", 2, receive.HashringConfig{
			Endpoints: []string{
				i2.GRPCNetworkEndpointFor(s.NetworkName()),
				i3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		})
		testutil.Ok(t, err)

		r1, err := e2ethanos.NewRoutingReceiver(s.SharedDir(), "r1", 2, receive.HashringConfig{
			Endpoints: []string{
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				i1.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		})
		testutil.Ok(t, err)

		testutil.Ok(t, s.StartAndWaitReady(i1, i2, i3, r1, r2))

		//Setup Prometheuses
		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom2, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "2", defaultPromConfig(
			"prom2",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1, prom2))

		//Setup Querier
		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{i1.GRPCNetworkEndpoint(), i2.GRPCNetworkEndpoint(), i3.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		expectedReplicationFactor := 3.0

		queryAndAssert(t, ctx, q.HTTPEndpoint(), "count(up) by (prometheus)", promclient.QueryOptions{
			Deduplicate: false,
		}, model.Vector{
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom1",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
			&model.Sample{
				Metric: model.Metric{
					"prometheus": "prom2",
				},
				Value: model.SampleValue(expectedReplicationFactor),
			},
		})
	})

	t.Run("hashring", func(t *testing.T) {
		/*
			The hashring suite creates three receivers, each with a Prometheus
			remote-writing data to it. However, due to the hashing of the labels,
			the time series from the Prometheus is forwarded to a different
			receiver in the hashring than the one handling the request.
			The querier queries all the receivers and the test verifies
			the time series are forwarded to the correct receive node.

			                      ┌───────┐
			                      │       │
			                      │ Prom2 │
			                      │       │
			                      └───┬───┘
			                          │
			                          │
			    ┌────────┐      ┌─────▼─────┐     ┌───────┐
			    │        │      │           │     │       │
			    │ Prom1  │      │ Router    │     │ Prom3 │
			    │        │      │ Ingestor2 │     │       │
			    └───┬────┘      │           │     └───┬───┘
			        │           └──▲──┬──▲──┘         │
			        │              │  │  │            │
			   ┌────▼──────┐       │  │  │       ┌────▼──────┐
			   │           ◄───────┘  │  └───────►           │
			   │ Router    │          │          │ Router    │
			   │ Ingestor1 ◄──────────┼──────────► Ingestor3 │
			   │           │          │          │           │
			   └─────┬─────┘          │          └────┬──────┘
			         │                │               │
			         │            ┌───▼───┐           │
			         │            │       │           │
			         └────────────► Query ◄───────────┘
			                      │       │
			                      └───────┘
		*/
		t.Parallel()
		s, err := e2e.NewScenario("e2e_test_receive_hashring")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		r1, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 1)
		testutil.Ok(t, err)
		r2, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "2", 1)
		testutil.Ok(t, err)
		r3, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "3", 1)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				r3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		r1, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 1, h)
		testutil.Ok(t, err)
		r2, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "2", 1, h)
		testutil.Ok(t, err)
		r3, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "3", 1, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1, r2, r3))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom2, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "2", defaultPromConfig(
			"prom2",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom3, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "3", defaultPromConfig(
			"prom3",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1, prom2, prom3))

		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint(), r3.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom2",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom3",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("hashring with config watcher", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_receive_hashring_config_watcher")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		r1, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 1)
		testutil.Ok(t, err)
		r2, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "2", 1)
		testutil.Ok(t, err)
		r3, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "3", 1)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				r3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		// TODO(kakkoyun): Update config file and wait config watcher to reconcile hashring.
		r1, err = e2ethanos.NewRoutingAndIngestingReceiverWithConfigWatcher(s.SharedDir(), s.NetworkName(), "1", 1, h)
		testutil.Ok(t, err)
		r2, err = e2ethanos.NewRoutingAndIngestingReceiverWithConfigWatcher(s.SharedDir(), s.NetworkName(), "2", 1, h)
		testutil.Ok(t, err)
		r3, err = e2ethanos.NewRoutingAndIngestingReceiverWithConfigWatcher(s.SharedDir(), s.NetworkName(), "3", 1, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1, r2, r3))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom2, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "2", defaultPromConfig(
			"prom2",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom3, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "3", defaultPromConfig(
			"prom3",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1, prom2, prom3))

		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint(), r3.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom2",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom3",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("replication", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_receive_replication")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// The replication suite creates three receivers but only one
		// receives Prometheus remote-written data. The querier queries all
		// receivers and the test verifies that the time series are
		// replicated to all of the nodes.
		r1, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 3)
		testutil.Ok(t, err)
		r2, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "2", 3)
		testutil.Ok(t, err)
		r3, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "3", 3)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				r3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		r1, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 3, h)
		testutil.Ok(t, err)
		r2, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "2", 3, h)
		testutil.Ok(t, err)
		r3, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "3", 3, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1, r2, r3))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1))

		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint(), r3.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "3",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("replication_with_outage", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_receive_replication_with_outage")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// The replication suite creates a three-node hashring but one of the
		// receivers is dead. In this case, replication should still
		// succeed and the time series should be replicated to the other nodes.
		r1, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 3)
		testutil.Ok(t, err)
		r2, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "2", 3)
		testutil.Ok(t, err)
		notRunningR3, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "3", 3)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
				r2.GRPCNetworkEndpointFor(s.NetworkName()),
				notRunningR3.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		r1, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 3, h)
		testutil.Ok(t, err)
		r2, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "2", 3, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1, r2))

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			defaultRemoteWriteEntry(e2ethanos.RemoteWriteEndpoint(r1.NetworkEndpoint(8081))), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1))

		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint(), r2.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "2",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})

	t.Run("multitenancy", func(t *testing.T) {
		t.Parallel()

		s, err := e2e.NewScenario("e2e_test_for_multitenancy")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		// The replication suite creates a three-node hashring but one of the
		// receivers is dead. In this case, replication should still
		// succeed and the time series should be replicated to the other nodes.
		r1, err := e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 1)
		testutil.Ok(t, err)

		h := receive.HashringConfig{
			Endpoints: []string{
				r1.GRPCNetworkEndpointFor(s.NetworkName()),
			},
		}

		// Recreate again, but with hashring config.
		r1, err = e2ethanos.NewRoutingAndIngestingReceiver(s.SharedDir(), s.NetworkName(), "1", 1, h)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(r1))
		testutil.Ok(t, err)

		conf1 := ReverseProxyConfig{
			tenantId: "tenant-1",
			port:     ":9097",
			target:   "http://" + r1.Endpoint(8081),
		}
		conf2 := ReverseProxyConfig{
			tenantId: "tenant-2",
			port:     ":9098",
			target:   "http://" + r1.Endpoint(8081),
		}

		go generateProxy(conf1)
		go generateProxy(conf2)

		prom1, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			defaultRemoteWriteEntry("http://172.17.0.1:9097/api/v1/receive"), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		prom2, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "2", defaultPromConfig(
			"prom2",
			0,
			defaultRemoteWriteEntry("http://172.17.0.1:9098/api/v1/receive"), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom1))
		testutil.Ok(t, s.StartAndWaitReady(prom2))

		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{r1.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))
		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "tenant-1",
			},
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "1",
				"replica":    "0",
				"tenant_id":  "tenant-2",
			},
		})
	})

	// TODO(bwplotka): Add TLS options
	t.Run("mtls_single_ingestor", func(t *testing.T) {
		/*
			The single_ingestor suite represents the simplest possible configuration of Thanos Receive.
			 ┌──────────┐
			 │  Prom    │
			 └────┬─────┘
			      │ mTLS
			 ┌────▼─────┐
			 │ Ingestor │
			 └────┬─────┘
			      │
			 ┌────▼─────┐
			 │  Query   │
			 └──────────┘
			NB: Made with asciiflow.com - you can copy & paste the above there to modify.

			Self signed certs created using following commands: create.sh

		*/

		t.Parallel()
		s, err := e2e.NewScenario("e2e_receive_mtls_single_ingestor")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, s))

		receiveLocalDirSuffix := e2ethanos.ReceiveRelLocalDir("ingestor")
		testutil.Ok(t, cpyDir("./certs", filepath.Join(s.SharedDir(), receiveLocalDirSuffix)))
		receiveRWCertsFlags := []string{
			"--remote-write.server-tls-cert=" + filepath.Join(e2e.ContainerSharedDir, receiveLocalDirSuffix, "e2e_receive_mtls_single_ingestor_server.crt"),
			"--remote-write.server-tls-key=" + filepath.Join(e2e.ContainerSharedDir, receiveLocalDirSuffix, "testserver.key"),
			"--remote-write.server-tls-client-ca=" + filepath.Join(e2e.ContainerSharedDir, receiveLocalDirSuffix, "testca.crt"),
		}

		// Setup Router Ingestor.
		i, err := e2ethanos.NewIngestingReceiver(s.SharedDir(), "ingestor", receiveRWCertsFlags...)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(i))

		promLocalDirSuffix := e2ethanos.PrometheusRelLocalDir("1")
		testutil.Ok(t, cpyDir("./certs", filepath.Join(s.SharedDir(), promLocalDirSuffix)))

		// Setup Prometheus
		prom, _, err := e2ethanos.NewPrometheus(s.SharedDir(), "1", defaultPromConfig(
			"prom1",
			0,
			tlsRemoteWriteEntry(
				e2ethanos.RemoteWriteEndpointHTTPS(i.NetworkEndpoint(8081)),
				filepath.Join(e2e.ContainerSharedDir, promLocalDirSuffix, "testca.crt"),
				filepath.Join(e2e.ContainerSharedDir, promLocalDirSuffix, "testclient.key"),
				filepath.Join(e2e.ContainerSharedDir, promLocalDirSuffix, "e2e_receive_mtls_single_ingestor_client.crt"),
			), "",
		), e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(prom))

		q, err := e2ethanos.NewQuerierBuilder(s.SharedDir(), "1", []string{i.GRPCNetworkEndpoint()}).Build()
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics))

		// We expect the data from each Prometheus instance to be replicated twice across our ingesting instances
		queryAndAssertSeries(t, ctx, q.HTTPEndpoint(), queryUpWithoutInstance, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "prom1",
				"receive":    "ingestor",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})
	})
}

func cpyDir(scrDir, dest string) error {
	entries, err := ioutil.ReadDir(scrDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(scrDir, entry.Name())
		destPath := filepath.Join(dest, entry.Name())

		fileInfo, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		stat, ok := fileInfo.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("failed to get raw syscall.Stat_t data for '%s'", sourcePath)
		}

		switch fileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err := os.MkdirAll(destPath, 0755); err != nil {
				return err
			}
			if err := cpyDir(sourcePath, destPath); err != nil {
				return err
			}
		case os.ModeSymlink:
			return errors.New("symlink copy is not supported")
		default:
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return err
			}
			if err := cpyFile(sourcePath, destPath); err != nil {
				return err
			}
		}
		if err := os.Lchown(destPath, int(stat.Uid), int(stat.Gid)); err != nil {
			return err
		}
	}
	return nil
}

func cpyFile(srcFile, dstFile string) (err error) {
	out, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	defer runutil.CloseWithErrCapture(&err, out, "close dst")

	in, err := os.Open(srcFile)
	defer runutil.CloseWithErrCapture(&err, in, "close src")
	if err != nil {
		return err
	}

	_, err = io.Copy(out, in)
	return err
}
