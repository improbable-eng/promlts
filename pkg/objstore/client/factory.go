// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/azure"
	"github.com/thanos-io/thanos/pkg/objstore/cos"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	"github.com/thanos-io/thanos/pkg/objstore/oss"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/objstore/swift"
	yaml "gopkg.in/yaml.v2"
)

type ObjProvider string

const (
	FILESYSTEM ObjProvider = "FILESYSTEM"
	GCS        ObjProvider = "GCS"
	S3         ObjProvider = "S3"
	AZURE      ObjProvider = "AZURE"
	SWIFT      ObjProvider = "SWIFT"
	COS        ObjProvider = "COS"
	ALIYUNOSS  ObjProvider = "ALIYUNOSS"
)

type BucketConfig struct {
	Type   ObjProvider `yaml:"type"`
	Config interface{} `yaml:"config"`
}

type TestBucketConfig struct {
	Type   ObjProvider `yaml:"type"`
	Config struct {
		Bucket    string `yaml:"bucket"`
		AccessKey string `yaml:"access_key"`
		SecretKey string `yaml:"secret_key"`
		Endpoint  string `yaml:"endpoint"`
		Insecure  bool   `yaml:"insecure"`
	}
}

// NewBucket initializes and returns new object storage clients.
// NOTE: confContentYaml can contain secrets.
func NewBucket(logger log.Logger, confContentYaml []byte, reg prometheus.Registerer, component string) (objstore.InstrumentedBucket, error) {
	level.Info(logger).Log("msg", "loading bucket configuration")
	bucketConf := &BucketConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, bucketConf); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	config, err := yaml.Marshal(bucketConf.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of bucket configuration")
	}

	var bucket objstore.Bucket
	switch strings.ToUpper(string(bucketConf.Type)) {
	case string(GCS):
		bucket, err = gcs.NewBucket(context.Background(), logger, config, component)
	case string(S3):
		bucket, err = s3.NewBucket(logger, config, component)
	case string(AZURE):
		bucket, err = azure.NewBucket(logger, config, component)
	case string(SWIFT):
		bucket, err = swift.NewContainer(logger, config)
	case string(COS):
		bucket, err = cos.NewBucket(logger, config, component)
	case string(ALIYUNOSS):
		bucket, err = oss.NewBucket(logger, config, component)
	case string(FILESYSTEM):
		bucket, err = filesystem.NewBucketFromConfig(config)
	default:
		return nil, errors.Errorf("bucket with type %s is not supported", bucketConf.Type)
	}
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s client", bucketConf.Type))
	}
	return objstore.NewTracingBucket(objstore.BucketWithMetrics(bucket.Name(), bucket, reg)), nil
}
