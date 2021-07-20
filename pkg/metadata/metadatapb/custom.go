// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadatapb

import (
	"unsafe"
)

func NewMetricMetadataResponse(metadata *MetricMetadata) *MetricMetadataResponse {
	return &MetricMetadataResponse{
		Result: &MetricMetadataResponse_Metadata{
			Metadata: metadata,
		},
	}
}

func NewTargetMetadataResponse(metadata *TargetMetadata) *TargetMetadataResponse {
	return &TargetMetadataResponse{
		Result: &TargetMetadataResponse_Data{
			Data: metadata,
		},
	}
}

func NewWarningMetricMetadataResponse(warning error) *MetricMetadataResponse {
	return &MetricMetadataResponse{
		Result: &MetricMetadataResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

func NewWarningTargetMetadataResponse(warning error) *TargetMetadataResponse {
	return &TargetMetadataResponse{
		Result: &TargetMetadataResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

func FromMetadataMap(m map[string][]Meta) *MetricMetadata {
	return &MetricMetadata{Metadata: *(*map[string]MetricMetadataEntry)(unsafe.Pointer(&m))}
}
