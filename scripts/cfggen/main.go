// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/prometheus/prometheus/discovery/file"
	"github.com/thanos-io/thanos/pkg/store"

	"github.com/fatih/structtag"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	http_util "github.com/thanos-io/thanos/pkg/http"
	"github.com/thanos-io/thanos/pkg/objstore/azure"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/cos"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	"github.com/thanos-io/thanos/pkg/objstore/oss"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/objstore/swift"
	"github.com/thanos-io/thanos/pkg/query"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	trclient "github.com/thanos-io/thanos/pkg/tracing/client"
	"github.com/thanos-io/thanos/pkg/tracing/elasticapm"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/pkg/tracing/lightstep"
	"github.com/thanos-io/thanos/pkg/tracing/stackdriver"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

var (
	bucketConfigs = map[client.ObjProvider]interface{}{
		client.AZURE:      azure.Config{},
		client.GCS:        gcs.Config{},
		client.S3:         s3.DefaultConfig,
		client.SWIFT:      swift.SwiftConfig{},
		client.COS:        cos.Config{},
		client.ALIYUNOSS:  oss.Config{},
		client.FILESYSTEM: filesystem.Config{},
	}
	tracingConfigs = map[trclient.TracingProvider]interface{}{
		trclient.JAEGER:      jaeger.Config{},
		trclient.STACKDRIVER: stackdriver.Config{},
		trclient.ELASTIC_APM: elasticapm.Config{},
		trclient.LIGHTSTEP:   lightstep.Config{},
	}
	indexCacheConfigs = map[storecache.IndexCacheProvider]interface{}{
		storecache.INMEMORY:  storecache.InMemoryIndexCacheConfig{},
		storecache.MEMCACHED: cacheutil.MemcachedClientConfig{},
	}
)

func main() {
	app := kingpin.New(filepath.Base(os.Args[0]), "Thanos config examples generator.")
	app.HelpFlag.Short('h')
	outputDir := app.Flag("output-dir", "Output directory for generated examples.").String()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	if _, err := app.Parse(os.Args[1:]); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	for typ, config := range bucketConfigs {
		if err := generate(client.BucketConfig{Type: typ, Config: config}, generateName("bucket_", string(typ)), *outputDir); err != nil {
			level.Error(logger).Log("msg", "failed to generate", "type", typ, "err", err)
			os.Exit(1)
		}
	}

	for typ, config := range tracingConfigs {
		if err := generate(trclient.TracingConfig{Type: typ, Config: config}, generateName("tracing_", string(typ)), *outputDir); err != nil {
			level.Error(logger).Log("msg", "failed to generate", "type", typ, "err", err)
			os.Exit(1)
		}
	}

	for typ, config := range indexCacheConfigs {
		if err := generate(storecache.IndexCacheConfig{Type: typ, Config: config}, generateName("index_cache_", string(typ)), *outputDir); err != nil {
			level.Error(logger).Log("msg", "failed to generate", "type", typ, "err", err)
			os.Exit(1)
		}
	}

	alertmgrCfg := alert.DefaultAlertmanagerConfig()
	alertmgrCfg.EndpointsConfig.FileSDConfigs = []http_util.FileSDConfig{http_util.FileSDConfig{}}
	if err := generate(alert.AlertingConfig{Alertmanagers: []alert.AlertmanagerConfig{alertmgrCfg}}, "rule_alerting", *outputDir); err != nil {
		level.Error(logger).Log("msg", "failed to generate", "type", "rule_alerting", "err", err)
		os.Exit(1)
	}

	queryCfg := query.DefaultConfig()
	queryCfg.EndpointsConfig.FileSDConfigs = []http_util.FileSDConfig{http_util.FileSDConfig{}}
	if err := generate([]query.Config{queryCfg}, "rule_query", *outputDir); err != nil {
		level.Error(logger).Log("msg", "failed to generate", "type", "rule_query", "err", err)
		os.Exit(1)
	}

	storeCfg := store.DefaultConfig()
	storeCfg.EndpointsConfig.FileSDConfigs = []file.SDConfig{file.SDConfig{}}
	storeCfg.TlsConfig = &store.TlsConfig{}
	if err := generate([]store.Config{storeCfg}, "query_store", *outputDir); err != nil {
		level.Error(logger).Log("msg", "failed to generate", "type", "query_store", "err", err)
		os.Exit(1)
	}
	logger.Log("msg", "success")
}

func generate(obj interface{}, typ string, outputDir string) error {
	// We forbid omitempty option. This is for simplification for doc generation.
	if err := checkForOmitEmptyTagOption(obj); err != nil {
		return errors.Wrap(err, "invalid type")
	}

	out, err := yaml.Marshal(obj)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(filepath.Join(outputDir, fmt.Sprintf("config_%s.txt", typ)), out, os.ModePerm)
}

func generateName(prefix, typ string) string {
	return prefix + strings.ReplaceAll(strings.ToLower(string(typ)), "-", "_")
}

func checkForOmitEmptyTagOption(obj interface{}) error {
	return checkForOmitEmptyTagOptionRec(reflect.ValueOf(obj))
}

func checkForOmitEmptyTagOptionRec(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			tags, err := structtag.Parse(string(v.Type().Field(i).Tag))
			if err != nil {
				return errors.Wrapf(err, "%s: failed to parse tag %q", v.Type().Field(i).Name, v.Type().Field(i).Tag)
			}

			tag, err := tags.Get("yaml")
			if err != nil {
				return errors.Wrapf(err, "%s: failed to get tag %q", v.Type().Field(i).Name, v.Type().Field(i).Tag)
			}

			for _, opts := range tag.Options {
				if opts == "omitempty" {
					return errors.Errorf("omitempty is forbidden for config, but spotted on field '%s'", v.Type().Field(i).Name)
				}
			}

			if err := checkForOmitEmptyTagOptionRec(v.Field(i)); err != nil {
				return errors.Wrapf(err, "%s", v.Type().Field(i).Name)
			}
		}

	case reflect.Ptr:
		return errors.New("nil pointers are not allowed in configuration")

	case reflect.Interface:
		return checkForOmitEmptyTagOptionRec(v.Elem())
	}

	return nil
}
