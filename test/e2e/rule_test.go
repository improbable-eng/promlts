package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	yaml "gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/promclient"
	rapi "github.com/thanos-io/thanos/pkg/rule/api"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

const (
	testAlertRuleAbortOnPartialResponse = `
groups:
- name: example
  # Abort should be a default: partial_response_strategy: "ABORT"
  rules:
  - alert: TestAlert_AbortOnPartialResponse
    # It must be based on actual metrics otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain, but I don't allow partial response in query."
`
	testAlertRuleWarnOnPartialResponse = `
groups:
- name: example
  partial_response_strategy: "WARN"
  rules:
  - alert: TestAlert_WarnOnPartialResponse
    # It must be based on actual metric, otherwise call to StoreAPI would be not involved.
    expr: absent(some_metric)
    labels:
      severity: page
    annotations:
      summary: "I always complain and allow partial response in query."
`
)

func createRuleFiles(t *testing.T, dir string) {
	t.Helper()

	for i, rule := range []string{testAlertRuleAbortOnPartialResponse, testAlertRuleWarnOnPartialResponse} {
		err := ioutil.WriteFile(filepath.Join(dir, fmt.Sprintf("rules-%d.yaml", i)), []byte(rule), 0666)
		testutil.Ok(t, err)
	}
}

func serializeAlertingConfiguration(t *testing.T, cfg ...alert.AlertmanagerConfig) []byte {
	t.Helper()
	amCfg := alert.AlertingConfig{
		Alertmanagers: cfg,
	}
	b, err := yaml.Marshal(&amCfg)
	if err != nil {
		t.Errorf("failed to serialize alerting configuration: %v", err)
	}
	return b
}

func writeAlertmanagerFileSD(t *testing.T, path string, addrs ...string) {
	group := targetgroup.Group{Targets: []model.LabelSet{}}
	for _, addr := range addrs {
		group.Targets = append(group.Targets, model.LabelSet{model.LabelName(model.AddressLabel): model.LabelValue(addr)})
	}

	b, err := yaml.Marshal([]*targetgroup.Group{&group})
	if err != nil {
		t.Errorf("failed to serialize file SD configuration: %v", err)
		return
	}

	err = ioutil.WriteFile(path+".tmp", b, 0660)
	if err != nil {
		t.Errorf("failed to write file SD configuration: %v", err)
		return
	}

	err = os.Rename(path+".tmp", path)
	testutil.Ok(t, err)
}

type mockAlertmanager struct {
	path      string
	token     string
	mtx       sync.Mutex
	alerts    []*model.Alert
	lastError error
}

func newMockAlertmanager(path string, token string) *mockAlertmanager {
	return &mockAlertmanager{
		path:   path,
		token:  token,
		alerts: make([]*model.Alert, 0),
	}
}

func (m *mockAlertmanager) setLastError(err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.lastError = err
}

func (m *mockAlertmanager) LastError() error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.lastError
}

func (m *mockAlertmanager) Alerts() []*model.Alert {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.alerts
}

func (m *mockAlertmanager) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		m.setLastError(errors.Errorf("invalid method: %s", req.Method))
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.URL.Path != m.path {
		m.setLastError(errors.Errorf("invalid path: %s", req.URL.Path))
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	if m.token != "" {
		auth := req.Header.Get("Authorization")
		if auth != fmt.Sprintf("Bearer %s", m.token) {
			m.setLastError(errors.Errorf("invalid auth: %s", req.URL.Path))
			resp.WriteHeader(http.StatusForbidden)
			return
		}
	}

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		m.setLastError(err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	var alerts []*model.Alert
	if err := json.Unmarshal(b, &alerts); err != nil {
		m.setLastError(err)
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}

	m.mtx.Lock()
	m.alerts = append(m.alerts, alerts...)
	m.mtx.Unlock()
}

func TestRuleAlertmanagerHTTPClient(t *testing.T) {
	a := newLocalAddresser()

	// Plain HTTP with a prefix.
	handler1 := newMockAlertmanager("/prefix/api/v1/alerts", "")
	srv1 := httptest.NewServer(handler1)
	defer srv1.Close()
	// HTTPS with authentication.
	handler2 := newMockAlertmanager("/api/v1/alerts", "secret")
	srv2 := httptest.NewTLSServer(handler2)
	defer srv2.Close()

	// Write the server's certificate to disk for the alerting configuration.
	tlsDir, err := ioutil.TempDir("", "tls")
	defer os.RemoveAll(tlsDir)
	testutil.Ok(t, err)
	var out bytes.Buffer
	err = pem.Encode(&out, &pem.Block{Type: "CERTIFICATE", Bytes: srv2.TLS.Certificates[0].Certificate[0]})
	testutil.Ok(t, err)
	caFile := filepath.Join(tlsDir, "ca.crt")
	err = ioutil.WriteFile(caFile, out.Bytes(), 0640)
	testutil.Ok(t, err)

	amCfg := serializeAlertingConfiguration(
		t,
		alert.AlertmanagerConfig{
			StaticAddresses: []string{srv1.Listener.Addr().String()},
			Scheme:          "http",
			Timeout:         model.Duration(time.Second),
			PathPrefix:      "/prefix/",
		},
		alert.AlertmanagerConfig{
			HTTPClientConfig: alert.HTTPClientConfig{
				TLSConfig: alert.TLSConfig{
					CAFile: caFile,
				},
				BearerToken: "secret",
			},
			StaticAddresses: []string{srv2.Listener.Addr().String()},
			Scheme:          "https",
			Timeout:         model.Duration(time.Second),
		},
	)

	rulesDir, err := ioutil.TempDir("", "rules")
	defer os.RemoveAll(rulesDir)
	testutil.Ok(t, err)
	createRuleFiles(t, rulesDir)

	qAddr := a.New()
	r := rule(a.New(), a.New(), rulesDir, amCfg, []address{qAddr}, nil)
	q := querier(qAddr, a.New(), []address{r.GRPC}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	exit, err := e2eSpinup(t, ctx, q, r)
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		cancel()
		return
	}

	defer func() {
		cancel()
		<-exit
	}()

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		for i, am := range []*mockAlertmanager{handler1, handler2} {
			if len(am.Alerts()) == 0 {
				return errors.Errorf("no alert received from handler%d, last error: %v", i, am.LastError())
			}
		}

		return nil
	}))
}

func TestRuleAlertmanagerFileSD(t *testing.T) {
	a := newLocalAddresser()

	am := alertManager(a.New())
	amDir, err := ioutil.TempDir("", "am")
	defer os.RemoveAll(amDir)
	testutil.Ok(t, err)
	amCfg := serializeAlertingConfiguration(
		t,
		alert.AlertmanagerConfig{
			FileSDConfigs: []alert.FileSDConfig{
				alert.FileSDConfig{
					Files:           []string{filepath.Join(amDir, "*.yaml")},
					RefreshInterval: model.Duration(time.Hour),
				},
			},
			Scheme:  "http",
			Timeout: model.Duration(time.Second),
		},
	)

	rulesDir, err := ioutil.TempDir("", "rules")
	defer os.RemoveAll(rulesDir)
	testutil.Ok(t, err)
	createRuleFiles(t, rulesDir)

	qAddr := a.New()
	r := rule(a.New(), a.New(), rulesDir, amCfg, []address{qAddr}, nil)
	q := querier(qAddr, a.New(), []address{r.GRPC}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	exit, err := e2eSpinup(t, ctx, am, q, r)
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		cancel()
		return
	}

	defer func() {
		cancel()
		<-exit
	}()

	// Wait for a couple of evaluations.
	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		// The time series written for the firing alerting rule must be queryable.
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "max(count_over_time(ALERTS[1m])) > 2", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}
		if len(warnings) > 0 {
			return errors.Errorf("unexpected warnings %s", warnings)
		}
		if len(res) == 0 {
			return errors.Errorf("empty result")
		}

		alrts, err := queryAlertmanagerAlerts(ctx, am.HTTP.URL())
		if err != nil {
			return err
		}
		if len(alrts) != 0 {
			return errors.Errorf("unexpected alerts length %d", len(alrts))
		}

		return nil
	}))

	// Update the Alertmanager file service discovery configuration.
	writeAlertmanagerFileSD(t, filepath.Join(amDir, "targets.yaml"), am.HTTP.HostPort())

	// Verify that alerts are received by Alertmanager.
	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}
		alrts, err := queryAlertmanagerAlerts(ctx, am.HTTP.URL())
		if err != nil {
			return err
		}
		if len(alrts) == 0 {
			return errors.Errorf("expecting alerts")
		}

		return nil
	}))
}

func TestRule(t *testing.T) {
	a := newLocalAddresser()

	am := alertManager(a.New())
	amCfg := serializeAlertingConfiguration(
		t,
		alert.AlertmanagerConfig{
			StaticAddresses: []string{am.HTTP.HostPort()},
			Scheme:          "http",
			Timeout:         model.Duration(time.Second),
		},
	)

	qAddr := a.New()

	rulesDir, err := ioutil.TempDir("", "rules")
	defer os.RemoveAll(rulesDir)
	testutil.Ok(t, err)
	createRuleFiles(t, rulesDir)

	r1 := rule(a.New(), a.New(), rulesDir, amCfg, []address{qAddr}, nil)
	r2 := rule(a.New(), a.New(), rulesDir, amCfg, nil, []address{qAddr})

	q := querier(qAddr, a.New(), []address{r1.GRPC, r2.GRPC}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)

	exit, err := e2eSpinup(t, ctx, q, r1, r2, am)
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		cancel()
		return
	}

	defer func() {
		cancel()
		<-exit
	}()

	expMetrics := []model.Metric{
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_AbortOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r1.HTTP.Port),
		},
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_AbortOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r2.HTTP.Port),
		},
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_WarnOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r1.HTTP.Port),
		},
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_WarnOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r2.HTTP.Port),
		},
	}
	expAlertLabels := []model.LabelSet{
		{
			"severity":  "page",
			"alertname": "TestAlert_AbortOnPartialResponse",
			"replica":   model.LabelValue(r1.HTTP.Port),
		},
		{
			"severity":  "page",
			"alertname": "TestAlert_AbortOnPartialResponse",
			"replica":   model.LabelValue(r2.HTTP.Port),
		},
		{
			"severity":  "page",
			"alertname": "TestAlert_WarnOnPartialResponse",
			"replica":   model.LabelValue(r1.HTTP.Port),
		},
		{
			"severity":  "page",
			"alertname": "TestAlert_WarnOnPartialResponse",
			"replica":   model.LabelValue(r2.HTTP.Port),
		},
	}

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		qtime := time.Now()

		// The time series written for the firing alerting rule must be queryable.
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "ALERTS", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(res) != len(expMetrics) {
			return errors.Errorf("unexpected result %v, expected %d", res, len(expMetrics))
		}

		for i, r := range res {
			if !r.Metric.Equal(expMetrics[i]) {
				return errors.Errorf("unexpected metric %s", r.Metric)
			}
			if int64(r.Timestamp) != timestamp.FromTime(qtime) {
				return errors.Errorf("unexpected timestamp %d", r.Timestamp)
			}
			if r.Value != 1 {
				return errors.Errorf("unexpected value %f", r.Value)
			}
		}

		// A notification must be sent to Alertmanager.
		alrts, err := queryAlertmanagerAlerts(ctx, am.HTTP.URL())
		if err != nil {
			return err
		}
		if len(alrts) != len(expAlertLabels) {
			return errors.Errorf("unexpected alerts length %d", len(alrts))
		}
		for i, a := range alrts {
			if !a.Labels.Equal(expAlertLabels[i]) {
				return errors.Errorf("unexpected labels %s", a.Labels)
			}
		}
		return nil
	}))

	// The checks counter ensures that we are not missing metrics.
	checks := 0
	// Check metrics to make sure we report correct ones that allow handling the AlwaysFiring not being triggered because of query issue.
	testutil.Ok(t, promclient.MetricValues(ctx, nil, urlParse(t, r1.HTTP.URL()), func(lset labels.Labels, val float64) error {
		switch lset.Get("__name__") {
		case "prometheus_rule_group_rules":
			checks++
			if val != 1 {
				return errors.Errorf("expected 1 loaded groups for strategy %s but found %v", lset.Get("strategy"), val)
			}
		}

		return nil
	}))
	testutil.Equals(t, 2, checks)

	// Verify the rules API endpoint.
	for _, r := range []*serverScheduler{r1, r2} {
		rgs, err := queryRules(ctx, r.HTTP.URL())
		testutil.Ok(t, err)
		testutil.Equals(t, 2, len(rgs))
		for i := range rgs {
			testutil.Equals(t, filepath.Join(rulesDir, fmt.Sprintf("rules-%d.yaml", i)), rgs[i].File)
			testutil.Equals(t, "example", rgs[i].Name)
		}
	}

	// Verify the alerts API endpoint.
	for _, r := range []*serverScheduler{r1, r2} {
		code, _, err := getAPIEndpoint(ctx, r.HTTP.URL()+"/api/v1/alerts")
		testutil.Ok(t, err)
		testutil.Equals(t, 200, code)
	}
}

type failingStoreAPI struct{}

func (a *failingStoreAPI) Info(context.Context, *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return &storepb.InfoResponse{
		MinTime: math.MinInt64,
		MaxTime: math.MaxInt64,
		Labels: []storepb.Label{
			{
				Name:  "magic",
				Value: "store_api",
			},
		},
		LabelSets: []storepb.LabelSet{
			{
				Labels: []storepb.Label{
					{
						Name:  "magic",
						Value: "store_api",
					},
				},
			},
			{
				Labels: []storepb.Label{
					{
						Name:  "magicmarker",
						Value: "store_api",
					},
				},
			},
		},
	}, nil
}

func (a *failingStoreAPI) Series(_ *storepb.SeriesRequest, _ storepb.Store_SeriesServer) error {
	return errors.New("I always fail. No reason. I am just offended StoreAPI. Don't touch me")
}

func (a *failingStoreAPI) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return &storepb.LabelNamesResponse{}, nil
}

func (a *failingStoreAPI) LabelValues(context.Context, *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{}, nil
}

// Test Ruler behaviour on different storepb.PartialResponseStrategy when having partial response from single `failingStoreAPI`.
func TestRulePartialResponse(t *testing.T) {
	a := newLocalAddresser()
	qAddr := a.New()

	f := fakeStoreAPI(a.New(), &failingStoreAPI{})
	am := alertManager(a.New())
	amCfg := serializeAlertingConfiguration(
		t,
		alert.AlertmanagerConfig{
			StaticAddresses: []string{am.HTTP.HostPort()},
			Scheme:          "http",
			Timeout:         model.Duration(time.Second),
		},
	)

	rulesDir, err := ioutil.TempDir("", "rules")
	defer os.RemoveAll(rulesDir)
	testutil.Ok(t, err)

	r := rule(a.New(), a.New(), rulesDir, amCfg, []address{qAddr}, nil)
	q := querier(qAddr, a.New(), []address{r.GRPC, f.GRPC}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	exit, err := e2eSpinup(t, ctx, am, f, q, r)
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		cancel()
		return
	}

	defer func() {
		cancel()
		<-exit
	}()

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		// The time series written for the firing alerting rule must be queryable.
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "ALERTS", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) != 1 {
			// We do expect warnings.
			return errors.Errorf("unexpected number of warnings, expected 1, got %s", warnings)
		}

		// This is tricky as for initial time (1 rule eval, we will have both alerts, as "No store match queries" will be there.
		if len(res) != 0 {
			return errors.Errorf("unexpected result length. expected %v, got %v", 0, res)
		}
		return nil
	}))

	// Add alerts to ruler, we want to add it only when Querier is rdy, otherwise we will get "no store match the query".
	createRuleFiles(t, rulesDir)

	resp, err := http.Post(r.HTTP.URL()+"/-/reload", "", nil)
	testutil.Ok(t, err)
	defer func() { _, _ = ioutil.ReadAll(resp.Body); _ = resp.Body.Close() }()
	testutil.Equals(t, http.StatusOK, resp.StatusCode)

	// We don't expect `AlwaysFiring` as it does NOT allow PartialResponse, so it will trigger `prometheus_rule_evaluation_failures_total` instead.
	expMetrics := []model.Metric{
		{
			"__name__":   "ALERTS",
			"severity":   "page",
			"alertname":  "TestAlert_WarnOnPartialResponse",
			"alertstate": "firing",
			"replica":    model.LabelValue(r.HTTP.Port),
		},
	}
	expAlertLabels := []model.LabelSet{
		{
			"severity":  "page",
			"alertname": "TestAlert_WarnOnPartialResponse",
			"replica":   model.LabelValue(r.HTTP.Port),
		},
	}

	expectedWarning := "receive series from Addr: " + f.GRPC.HostPort() + " LabelSets: [name:\"magic\" value:\"store_api\" ][name:\"magicmarker\" value:\"store_api\" ] Mint: -9223372036854775808 Maxt: 9223372036854775807: rpc error: code = Unknown desc = I always fail. No reason. I am just offended StoreAPI. Don't touch me"

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() (err error) {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		qtime := time.Now()

		// The time series written for the firing alerting rule must be queryable.
		res, warnings, err := promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), "ALERTS", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) != 1 {
			// We do expect warnings.
			return errors.Errorf("unexpected number of warnings, expected 1, got %s", warnings)
		}

		if warnings[0] != expectedWarning {
			return errors.Errorf("unexpected warning, expected %s, got %s", expectedWarning, warnings[0])
		}

		// This is tricky as for initial time (1 rule eval, we will have both alerts, as "No store match queries" will be there.
		if len(res) != len(expMetrics) {
			return errors.Errorf("unexpected result length. expected %v, got %v", len(expMetrics), res)
		}

		for i, r := range res {
			if !r.Metric.Equal(expMetrics[i]) {
				return errors.Errorf("unexpected metric %s, expected %s", r.Metric, expMetrics[i])
			}
			if int64(r.Timestamp) != timestamp.FromTime(qtime) {
				return errors.Errorf("unexpected timestamp %d", r.Timestamp)
			}
			if r.Value != 1 {
				return errors.Errorf("unexpected value %f", r.Value)
			}
		}

		// A notification must be sent to Alertmanager.
		alrts, err := queryAlertmanagerAlerts(ctx, am.HTTP.URL())
		if err != nil {
			return err
		}
		if len(alrts) != len(expAlertLabels) {
			return errors.Errorf("unexpected alerts length %d", len(alrts))
		}
		for i, a := range alrts {
			if !a.Labels.Equal(expAlertLabels[i]) {
				return errors.Errorf("unexpected labels %s", a.Labels)
			}
		}
		return nil
	}))

	// checks counter ensures we are not missing metrics.
	checks := 0
	// Check metrics to make sure we report correct ones that allow handling the AlwaysFiring not being triggered because of query issue.
	testutil.Ok(t, promclient.MetricValues(ctx, nil, urlParse(t, r.HTTP.URL()), func(lset labels.Labels, val float64) error {
		switch lset.Get("__name__") {
		case "prometheus_rule_group_rules":
			checks++
			if val != 1 {
				return errors.Errorf("expected 1 loaded groups for strategy %s but found %v", lset.Get("strategy"), val)
			}
		case "prometheus_rule_evaluation_failures_total":
			if lset.Get("strategy") == "abort" {
				checks++
				if val <= 0 {
					return errors.Errorf("expected rule eval failures for abort strategy rule as we have failing storeAPI but found %v", val)
				}
			} else if lset.Get("strategy") == "warn" {
				checks++
				if val > 0 {
					return errors.Errorf("expected no rule eval failures for warm strategy rule but found %v", val)
				}
			}
		case "thanos_rule_evaluation_with_warnings_total":
			if lset.Get("strategy") == "warn" {
				checks++
				if val <= 0 {
					return errors.Errorf("expected rule eval with warnings for warn strategy rule as we have failing storeAPI but found %v", val)
				}
			} else if lset.Get("strategy") == "abort" {
				checks++
				if val > 0 {
					return errors.Errorf("expected rule eval with warnings 0 for abort strategy rule but found %v", val)
				}
			}
		}
		return nil
	}))
	testutil.Equals(t, 6, checks)
}

// TODO(bwplotka): Move to promclient.
func queryAlertmanagerAlerts(ctx context.Context, url string) ([]*model.Alert, error) {
	code, body, err := getAPIEndpoint(ctx, url+"/api/v1/alerts")
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, errors.Errorf("expected 200 response, got %d", code)
	}

	var v struct {
		Data []*model.Alert `json:"data"`
	}
	if err = json.Unmarshal(body, &v); err != nil {
		return nil, err
	}

	sort.Slice(v.Data, func(i, j int) bool {
		return v.Data[i].Labels.Before(v.Data[j].Labels)
	})
	return v.Data, nil
}

func queryRules(ctx context.Context, url string) ([]*rapi.RuleGroup, error) {
	code, body, err := getAPIEndpoint(ctx, url+"/api/v1/rules")
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, errors.Errorf("expected 200 response, got %d", code)
	}

	var resp struct {
		Data rapi.RuleDiscovery
	}
	if err = json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	sort.Slice(resp.Data.RuleGroups, func(i, j int) bool {
		return resp.Data.RuleGroups[i].File < resp.Data.RuleGroups[j].File
	})
	return resp.Data.RuleGroups, nil
}

func getAPIEndpoint(ctx context.Context, url string) (int, []byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, nil, err
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer runutil.CloseWithLogOnErr(nil, resp.Body, "%s: close body", req.URL.String())
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}
