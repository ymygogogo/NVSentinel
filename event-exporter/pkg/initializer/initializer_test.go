// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package initializer

import (
	"testing"

	"github.com/nvidia/nvsentinel/event-exporter/pkg/config"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/sink"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

func TestTokenDatabaseCertMountPath_NoTLSReturnsEmptyPath(t *testing.T) {
	dsConfig := &datastore.DataStoreConfig{
		Provider: datastore.ProviderMongoDB,
	}

	if got := tokenDatabaseCertMountPath(dsConfig); got != "" {
		t.Fatalf("expected empty cert mount path when TLS is disabled, got %q", got)
	}
}

func TestTokenDatabaseCertMountPath_TLSConfigReturnsCADirectory(t *testing.T) {
	dsConfig := &datastore.DataStoreConfig{
		Provider: datastore.ProviderMongoDB,
		Connection: datastore.ConnectionConfig{
			TLSConfig: &datastore.TLSConfig{
				CAPath: "/tmp/mongo-certs/ca.crt",
			},
		},
	}

	if got := tokenDatabaseCertMountPath(dsConfig); got != "/tmp/mongo-certs" {
		t.Fatalf("expected CA cert directory, got %q", got)
	}
}

func TestInitializeOIDC_DisabledDoesNotReadSecret(t *testing.T) {
	cfg := &config.Config{}
	enabled := false
	cfg.Exporter.OIDC.Enabled = &enabled

	provider, err := initializeOIDC(cfg, "/path/that/does/not/exist")
	if err != nil {
		t.Fatalf("initializeOIDC() error = %v", err)
	}
	if provider != nil {
		t.Fatalf("provider = %#v, want nil", provider)
	}
}

func TestInitializeSinkDefaultsToHTTP(t *testing.T) {
	cfg := &config.Config{}
	cfg.Exporter.Sink.Endpoint = "https://events.example.com"

	eventSink, err := initializeSink(cfg, nil, 1)
	if err != nil {
		t.Fatalf("initializeSink() error = %v", err)
	}
	if _, ok := eventSink.(*sink.HTTPSink); !ok {
		t.Fatalf("sink type = %T, want *sink.HTTPSink", eventSink)
	}
}

func TestInitializeSinkBuildsKafkaSink(t *testing.T) {
	cfg := &config.Config{}
	cfg.Exporter.Sink.Type = config.SinkTypeKafka
	cfg.Exporter.Sink.Kafka.Brokers = []string{"kafka:9092"}
	cfg.Exporter.Sink.Kafka.Topic = "nvsentinel.health-events"

	eventSink, err := initializeSink(cfg, nil, 1)
	if err != nil {
		t.Fatalf("initializeSink() error = %v", err)
	}
	defer eventSink.Close(t.Context())
	if _, ok := eventSink.(*sink.KafkaSink); !ok {
		t.Fatalf("sink type = %T, want *sink.KafkaSink", eventSink)
	}
}
