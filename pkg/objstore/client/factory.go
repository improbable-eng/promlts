package client

import (
	"context"
	"fmt"
	"runtime"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"google.golang.org/api/option"
)

// NewBucket initializes and returns new object storage clients.
func NewBucket(logger log.Logger, bucketConf *objstore.BucketConfig, reg *prometheus.Registry, component string) (objstore.Bucket, error) {
	err := bucketConf.Validate()
	if err != nil {
		return nil, err
	}
	switch bucketConf.Provider {
	case objstore.GCS:
		gcsOptions := option.WithUserAgent(fmt.Sprintf("thanos-%s/%s (%s)", component, version.Version, runtime.Version()))
		gcsClient, err := storage.NewClient(context.Background(), gcsOptions)
		if err != nil {
			return nil, errors.Wrap(err, "create GCS client")
		}
		return objstore.BucketWithMetrics(bucketConf.Bucket, gcs.NewBucket(bucketConf.Bucket, gcsClient, reg), reg), nil
	case objstore.S3:
		b, err := s3.NewBucket(logger, bucketConf, reg, component)
		if err != nil {
			return nil, errors.Wrap(err, "create s3 client")
		}
		return objstore.BucketWithMetrics(bucketConf.Bucket, b, reg), nil
	}
	return nil, objstore.ErrUnsupported
}
