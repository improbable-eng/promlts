package provider

import (
	"context"
	"io"
	"io/ioutil"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/jaeger"
	"github.com/improbable-eng/thanos/pkg/tracing/provider/stackdriver"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type TracingProvider string

const (
	STACKDRIVER TracingProvider = "STACKDRIVER"
	JAEGER      TracingProvider = "JAEGER"
)

type TracingConfig struct {
	Type   TracingProvider `yaml:"type"`
	Config interface{}     `yaml:"config"`
}

func NewTracer(ctx context.Context, logger log.Logger, confContentYaml []byte) (opentracing.Tracer, io.Closer, error) {
	level.Info(logger).Log("msg", "loading tracing configuration")
	tracingConf := &TracingConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, tracingConf); err != nil {
		return &opentracing.NoopTracer{}, ioutil.NopCloser(nil), errors.Wrap(err, "parsing config YAML file")
	}

	config, err := yaml.Marshal(tracingConf.Config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshal content of tracing configuration")
	}
	switch strings.ToUpper(string(tracingConf.Type)) {
	case string(STACKDRIVER):
		return stackdriver.NewTracer(ctx, logger, config)
	case string(JAEGER):
		return jaeger.NewTracer(ctx, logger, config)
	default:
		return nil, nil, errors.Errorf("tracing with type %s is not supported", tracingConf.Type)
	}
	return nil, nil, errors.Errorf("tracing bla bla bla")
}
