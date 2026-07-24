package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeKafkaConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func baseKafkaConfig(extra string) string {
	return `[exporter]
client_name = "event-exporter"

[exporter.metadata]
cluster = "test-cluster"

[exporter.sink]
type = "kafka"

[exporter.sink.kafka]
brokers = ["kafka-0.kafka:9092"]
topic = "nvsentinel.health-events"
client_id = "event-exporter"
required_acks = "all"
compression = "snappy"
batch_timeout = "10ms"
write_timeout = "30s"
` + extra + `
[exporter.resume_token]
collection = "tokens"
database = "nvsentinel"
`
}

func TestKafkaSinkConfigValidatesWithoutOIDC(t *testing.T) {
	cfg, err := Load(writeKafkaConfig(t, baseKafkaConfig("")))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Exporter.Sink.Type != SinkTypeKafka {
		t.Fatalf("Sink.Type = %q, want %q", cfg.Exporter.Sink.Type, SinkTypeKafka)
	}
	if got := cfg.Exporter.Sink.Kafka.Topic; got != "nvsentinel.health-events" {
		t.Fatalf("Kafka topic = %q", got)
	}
}

func TestKafkaSinkConfigRequiresBrokersAndTopic(t *testing.T) {
	cfg := Config{Exporter: ExporterConfig{Sink: SinkConfig{Type: SinkTypeKafka}, ResumeToken: ResumeTokenConfig{Collection: "tokens", Database: "nvsentinel"}}}

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "kafka brokers") {
		t.Fatalf("Validate() error = %v, want kafka brokers error", err)
	}

	cfg.Exporter.Sink.Kafka.Brokers = []string{"kafka:9092"}
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "kafka topic") {
		t.Fatalf("Validate() error = %v, want kafka topic error", err)
	}
}

func TestKafkaSinkConfigRejectsInvalidEnums(t *testing.T) {
	tests := []struct {
		name  string
		extra string
		want  string
	}{
		{name: "acks", extra: "required_acks = \"invalid\"\n", want: "required_acks"},
		{name: "compression", extra: "compression = \"brotli\"\n", want: "compression"},
		{name: "sasl mechanism", extra: "[exporter.sink.kafka.sasl]\nenabled = true\nmechanism = \"OAUTHBEARER\"\nusername = \"u\"\npassword_file = \"/tmp/p\"\n", want: "sasl mechanism"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeKafkaConfig(t, baseKafkaConfig(tt.extra)))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestKafkaSASLRequiresCredentials(t *testing.T) {
	_, err := Load(writeKafkaConfig(t, baseKafkaConfig(`[exporter.sink.kafka.sasl]
enabled = true
mechanism = "SCRAM-SHA-512"
username = "event-exporter"
`)))
	if err == nil || !strings.Contains(err.Error(), "password_file") {
		t.Fatalf("Load() error = %v, want password_file error", err)
	}
}
