package alert

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/discovery/cache"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	defaultAlertmanagerPort = 9093
	alertPushEndpoint       = "/api/v1/alerts"
	contentTypeJSON         = "application/json"
)

var userAgent = fmt.Sprintf("Thanos/%s", version.Version)

type AlertingConfig struct {
	Alertmanagers []AlertmanagerConfig `yaml:"alertmanagers"`
}

// AlertmanagerConfig represents a client to a cluster of Alertmanager endpoints.
// TODO(simonpasquier): add support for API version (v1 or v2).
type AlertmanagerConfig struct {
	// HTTP client configuration.
	HTTPClientConfig HTTPClientConfig `yaml:"http_config"`

	// List of addresses with DNS prefixes.
	StaticAddresses []string `yaml:"static_configs"`
	// List of file  configurations (our FileSD supports different DNS lookups).
	FileSDConfigs []FileSDConfig `yaml:"file_sd_configs"`

	// The URL scheme to use when talking to Alertmanagers.
	Scheme string `yaml:"scheme"`

	// Path prefix to add in front of the push endpoint path.
	PathPrefix string `yaml:"path_prefix"`

	// The timeout used when sending alerts (default: 10s).
	Timeout model.Duration `yaml:"timeout"`
}

type HTTPClientConfig struct {
	// The HTTP basic authentication credentials for the targets.
	BasicAuth BasicAuth `yaml:"basic_auth"`
	// The bearer token for the targets.
	BearerToken string `yaml:"bearer_token"`
	// The bearer token file for the targets.
	BearerTokenFile string `yaml:"bearer_token_file"`
	// HTTP proxy server to use to connect to the targets.
	ProxyURL string `yaml:"proxy_url"`
	// TLSConfig to use to connect to the targets.
	TLSConfig TLSConfig `yaml:"tls_config"`
}

type TLSConfig struct {
	// The CA cert to use for the targets.
	CAFile string `yaml:"ca_file"`
	// The client cert file for the targets.
	CertFile string `yaml:"cert_file"`
	// The client key file for the targets.
	KeyFile string `yaml:"key_file"`
	// Used to verify the hostname for the targets.
	ServerName string `yaml:"server_name"`
	// Disable target certificate validation.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

type BasicAuth struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	PasswordFile string `yaml:"password_file"`
}

func (b BasicAuth) IsZero() bool {
	return b.Username == "" && b.Password == "" && b.PasswordFile == ""
}

func (c HTTPClientConfig) convert() (config_util.HTTPClientConfig, error) {
	httpClientConfig := config_util.HTTPClientConfig{
		BearerToken:     config_util.Secret(c.BearerToken),
		BearerTokenFile: c.BearerTokenFile,
		TLSConfig: config_util.TLSConfig{
			CAFile:             c.TLSConfig.CAFile,
			CertFile:           c.TLSConfig.CertFile,
			KeyFile:            c.TLSConfig.KeyFile,
			ServerName:         c.TLSConfig.ServerName,
			InsecureSkipVerify: c.TLSConfig.InsecureSkipVerify,
		},
	}
	if c.ProxyURL != "" {
		var proxy config_util.URL
		err := yaml.Unmarshal([]byte(c.ProxyURL), &proxy)
		if err != nil {
			return httpClientConfig, err
		}
		httpClientConfig.ProxyURL = proxy
	}
	if !c.BasicAuth.IsZero() {
		httpClientConfig.BasicAuth = &config_util.BasicAuth{
			Username:     c.BasicAuth.Username,
			Password:     config_util.Secret(c.BasicAuth.Password),
			PasswordFile: c.BasicAuth.PasswordFile,
		}
	}
	return httpClientConfig, httpClientConfig.Validate()
}

type FileSDConfig struct {
	Files           []string       `yaml:"files"`
	RefreshInterval model.Duration `yaml:"refresh_interval"`
}

func (c FileSDConfig) convert() (file.SDConfig, error) {
	var fileSDConfig file.SDConfig
	b, err := yaml.Marshal(c)
	if err != nil {
		return fileSDConfig, err
	}
	err = yaml.Unmarshal(b, &fileSDConfig)
	if err != nil {
		return fileSDConfig, err
	}
	return fileSDConfig, nil
}

func DefaultAlertmanagerConfig() AlertmanagerConfig {
	return AlertmanagerConfig{
		Scheme:          "http",
		Timeout:         model.Duration(time.Second * 10),
		StaticAddresses: []string{},
		FileSDConfigs:   []FileSDConfig{},
	}
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *AlertmanagerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultAlertmanagerConfig()
	type plain AlertmanagerConfig
	return unmarshal((*plain)(c))
}

type AddressProvider interface {
	Resolve(context.Context, []string)
	Addresses() []string
}

// Alertmanager represents an HTTP client that can send alerts to a cluster of Alertmanager endpoints.
type Alertmanager struct {
	logger log.Logger

	client  *http.Client
	timeout time.Duration
	scheme  string
	prefix  string

	staticAddresses []string
	fileSDCache     *cache.Cache
	fileDiscoverers []*file.Discovery

	provider AddressProvider
}

// NewAlertmanager returns a new Alertmanager client.
func NewAlertmanager(logger log.Logger, cfg AlertmanagerConfig, provider AddressProvider) (*Alertmanager, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	httpClientConfig, err := cfg.HTTPClientConfig.convert()
	if err != nil {
		return nil, err
	}
	client, err := config_util.NewClientFromConfig(httpClientConfig, "alertmanager", false)
	if err != nil {
		return nil, err
	}

	var discoverers []*file.Discovery
	for _, sdCfg := range cfg.FileSDConfigs {
		fileSDCfg, err := sdCfg.convert()
		if err != nil {
			return nil, err
		}
		discoverers = append(discoverers, file.NewDiscovery(&fileSDCfg, logger))
	}
	return &Alertmanager{
		logger:          logger,
		client:          client,
		scheme:          cfg.Scheme,
		prefix:          cfg.PathPrefix,
		timeout:         time.Duration(cfg.Timeout),
		staticAddresses: cfg.StaticAddresses,
		fileSDCache:     cache.New(),
		fileDiscoverers: discoverers,
		provider:        provider,
	}, nil
}

// LoadAlertmanagerConfigs loads a list of AlertmanagerConfig from YAML data.
func LoadAlertingConfig(confYaml []byte) (AlertingConfig, error) {
	var cfg AlertingConfig
	if err := yaml.UnmarshalStrict(confYaml, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// BuildAlertmanagerConfig initializes and returns an Alertmanager client configuration from a static address.
func BuildAlertmanagerConfig(logger log.Logger, address string, timeout time.Duration) (AlertmanagerConfig, error) {
	parsed, err := url.Parse(address)
	if err != nil {
		return AlertmanagerConfig{}, err
	}

	scheme := parsed.Scheme
	host := parsed.Host
	for _, qType := range []dns.QType{dns.A, dns.SRV, dns.SRVNoA} {
		prefix := string(qType) + "+"
		if strings.HasPrefix(strings.ToLower(scheme), prefix) {
			// Scheme is of the form "<dns type>+<http scheme>".
			scheme = strings.TrimPrefix(scheme, prefix)
			host = prefix + parsed.Host
			if qType == dns.A {
				if _, _, err := net.SplitHostPort(parsed.Host); err != nil {
					// The host port could be missing. Append the defaultAlertmanagerPort.
					host = host + ":" + strconv.Itoa(defaultAlertmanagerPort)
				}
			}
			break
		}
	}
	var basicAuth BasicAuth
	if parsed.User != nil && parsed.User.String() != "" {
		basicAuth.Username = parsed.User.Username()
		pw, _ := parsed.User.Password()
		basicAuth.Password = pw
	}

	return AlertmanagerConfig{
		PathPrefix:      parsed.Path,
		Scheme:          scheme,
		StaticAddresses: []string{host},
		Timeout:         model.Duration(timeout),
		HTTPClientConfig: HTTPClientConfig{
			BasicAuth: basicAuth,
		},
	}, nil
}

// Endpoints returns the list of known Alertmanager endpoints.
func (a *Alertmanager) Endpoints() []*url.URL {
	var urls []*url.URL
	for _, addr := range a.provider.Addresses() {
		urls = append(urls,
			&url.URL{
				Scheme: a.scheme,
				Host:   addr,
				Path:   path.Join("/", a.prefix, alertPushEndpoint),
			},
		)
	}
	return urls
}

// Do sends a POST request to the given URL.
func (a *Alertmanager) Do(ctx context.Context, u *url.URL, r io.Reader) error {
	req, err := http.NewRequest("POST", u.String(), r)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", contentTypeJSON)
	req.Header.Set("User-Agent", userAgent)

	resp, err := a.client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "send request to %q", u)
	}
	defer runutil.ExhaustCloseWithLogOnErr(a.logger, resp.Body, "send one alert")

	if resp.StatusCode/100 != 2 {
		return errors.Errorf("bad response status %v from %q", resp.Status, u)
	}
	return nil
}

// Discover runs the service to discover target endpoints.
func (a *Alertmanager) Discover(ctx context.Context) {
	var wg sync.WaitGroup
	ch := make(chan []*targetgroup.Group)

	for _, d := range a.fileDiscoverers {
		wg.Add(1)
		go func(d *file.Discovery) {
			d.Run(ctx, ch)
			wg.Done()
		}(d)
	}

	func() {
		for {
			select {
			case update := <-ch:
				// Discoverers sometimes send nil updates so need to check for it to avoid panics.
				if update == nil {
					continue
				}
				a.fileSDCache.Update(update)
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
}

// Resolve refreshes and resolves the list of Alertmanager targets.
func (a *Alertmanager) Resolve(ctx context.Context) {
	a.provider.Resolve(ctx, append(a.fileSDCache.Addresses(), a.staticAddresses...))
}
