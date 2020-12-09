// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package config

import (
	"strings"

	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/objstore/azure"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/cos"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	"github.com/thanos-io/thanos/pkg/objstore/oss"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/objstore/swift"

	"gopkg.in/yaml.v2"
)

// Config stores the configuration for storing and accessing blobs in filesystem.
type fileConfig struct {
	Type   client.ObjProvider
	Config filesystem.Config
}

// Config stores the configuration for oss bucket.
type aliConfig struct {
	Type   client.ObjProvider
	Config oss.Config
}

// Config stores the configuration for gcs bucket.
type gcsConfig struct {
	Type   client.ObjProvider
	Config gcs.Config
}

// Config encapsulates the necessary config values to instantiate an cos client.
type cosConfig struct {
	Type   client.ObjProvider
	Config cos.Config
}

// Config Azure storage configuration.
type azureConfig struct {
	Type   client.ObjProvider
	Config azure.Config
}

// Config Swift storage configuration.
type swiftConfig struct {
	Type   client.ObjProvider
	Config swift.SwiftConfig
}

//Config S3 storage configuration.
type s3Config struct {
	Type   client.ObjProvider
	Config s3.Config
}

func ReplaceSecret(confContentYaml []byte) ([]byte, error) {
	var str []byte
	conf := &client.BucketConfig{}

	err := yaml.UnmarshalStrict(confContentYaml, conf)
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(string(conf.Type)) {
	case string(client.GCS):
		gcsConf := &gcsConfig{}
		str, _ = mashedYaml(confContentYaml, gcsConf)
	case string(client.S3):
		s3Conf := &s3Config{}
		str, _ = mashedYaml(confContentYaml, s3Conf)
	case string(client.AZURE):
		azConf := &azureConfig{}
		str, _ = mashedYaml(confContentYaml, azConf)
	case string(client.SWIFT):
		swiftConf := &swiftConfig{}
		str, _ = mashedYaml(confContentYaml, swiftConf)
	case string(client.COS):
		cosConf := &cosConfig{}
		str, _ = mashedYaml(confContentYaml, cosConf)
	case string(client.ALIYUNOSS):
		aliConf := &aliConfig{}
		str, _ = mashedYaml(confContentYaml, aliConf)
	case string(client.FILESYSTEM):
		fileConf := &fileConfig{}
		str, _ = mashedYaml(confContentYaml, fileConf)
	default:
		return nil, errors.Errorf("bucket with type %s is not supported", conf.Type)
	}
	return str, err
}

func mashedYaml(confContent []byte, conf interface{}) ([]byte, error) {
	var mashedStr []byte
	err := yaml.UnmarshalStrict(confContent, conf)
	if err != nil {
		return nil, err
	}

	mashedStr, _ = yaml.Marshal(conf)

	return mashedStr, err
}
