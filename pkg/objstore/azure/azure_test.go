// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package azure

import (
	"testing"

	"github.com/prometheus/common/config"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestConfig_validate(t *testing.T) {
	type fields struct {
		StorageAccountName string
		StorageAccountKey  string
		ContainerName      string
		Endpoint           string
		MaxRetries         int
	}
	tests := []struct {
		name         string
		fields       fields
		wantErr      bool
		wantEndpoint string
	}{
		{
			name: "valid global configuration",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "bar",
				ContainerName:      "roo",
				MaxRetries:         3,
			},
			wantErr:      false,
			wantEndpoint: azureDefaultEndpoint,
		},
		{
			name: "valid custom endpoint",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "bar",
				ContainerName:      "roo",
				Endpoint:           "blob.core.chinacloudapi.cn",
			},
			wantErr:      false,
			wantEndpoint: "blob.core.chinacloudapi.cn",
		},
		{
			name: "no account key but account name",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "",
				ContainerName:      "roo",
			},
			wantErr: true,
		},
		{
			name: "no account name but account key",
			fields: fields{
				StorageAccountName: "",
				StorageAccountKey:  "bar",
				ContainerName:      "roo",
			},
			wantErr: true,
		},
		{
			name: "no container name",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "bar",
				ContainerName:      "",
			},
			wantErr: true,
		},
		{
			name: "invalid max retries (negative)",
			fields: fields{
				StorageAccountName: "foo",
				StorageAccountKey:  "bar",
				ContainerName:      "roo",
				MaxRetries:         -3,
			},
			wantErr:      true,
			wantEndpoint: azureDefaultEndpoint,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &Config{
				StorageAccountName: tt.fields.StorageAccountName,
				StorageAccountKey:  config.Secret(tt.fields.StorageAccountKey),
				ContainerName:      tt.fields.ContainerName,
				Endpoint:           tt.fields.Endpoint,
				MaxRetries:         tt.fields.MaxRetries,
			}
			err := conf.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.validate() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				testutil.Equals(t, tt.wantEndpoint, conf.Endpoint)
			}
		})
	}
}
