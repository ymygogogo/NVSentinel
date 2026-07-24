package sink

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
	"github.com/twmb/franz-go/pkg/kgo"
)

type recordingKafkaProducer struct {
	record *kgo.Record
	err    error
	closed bool
}

func (p *recordingKafkaProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	if len(records) > 0 {
		p.record = records[0]
	}
	return kgo.ProduceResults{{Record: p.record, Err: p.err}}
}

func (p *recordingKafkaProducer) Close() {
	p.closed = true
}

func TestKafkaSinkPublishesCloudEventRecord(t *testing.T) {
	producer := &recordingKafkaProducer{}
	sink := NewKafkaSinkWithProducer("nvsentinel.health-events", producer)
	event := &transformer.CloudEvent{
		SpecVersion: "1.0",
		Type:        "com.nvidia.nvsentinel.health.v1",
		Source:      "nvsentinel://test/healthevents",
		ID:          "event-123",
		Time:        "2026-07-24T01:02:03Z",
		Data:        map[string]any{"enrichment": map[string]any{"status": "matched"}},
	}

	if err := sink.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if producer.record == nil {
		t.Fatal("producer record was nil")
	}
	if got := producer.record.Topic; got != "nvsentinel.health-events" {
		t.Fatalf("Topic = %q", got)
	}
	if got := string(producer.record.Key); got != "event-123" {
		t.Fatalf("Key = %q", got)
	}

	var decoded transformer.CloudEvent
	if err := json.Unmarshal(producer.record.Value, &decoded); err != nil {
		t.Fatalf("record value is not CloudEvent JSON: %v", err)
	}
	if decoded.ID != event.ID {
		t.Fatalf("decoded ID = %q", decoded.ID)
	}

	headers := map[string]string{}
	for _, h := range producer.record.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["content-type"] != "application/cloudevents+json" {
		t.Fatalf("content-type header = %q", headers["content-type"])
	}
	if headers["ce_id"] != "event-123" || headers["ce_type"] != event.Type || headers["ce_source"] != event.Source || headers["ce_specversion"] != "1.0" {
		t.Fatalf("CloudEvent headers = %#v", headers)
	}
}

func TestKafkaSinkPublishesWrappedRecord(t *testing.T) {
	producer := &recordingKafkaProducer{}
	sink := NewKafkaSinkWithProducer("dingo_command_ai_instance_status_change_topic", producer)
	sink.payloadFormat = KafkaPayloadFormatWrapped
	sink.wrapper = KafkaWrapperConfig{
		EventType:         "nvsentinel_health_event",
		ResourceID:        "{{ .Node }}",
		ResourceStatus:    "",
		ResourceSubStatus: "{{ .CheckName }}",
		ExtraRawField:     "raw",
	}
	event := &transformer.CloudEvent{
		SpecVersion: "1.0",
		Type:        "com.nvidia.nvsentinel.health.v1",
		Source:      "nvsentinel://test-cluster/healthevents",
		ID:          "event-123",
		Time:        "2026-07-24T01:02:03Z",
		Data: map[string]any{
			"metadata": map[string]string{"cluster": "test-cluster"},
			"healthEvent": map[string]any{
				"nodeName":          "gpu-node-1",
				"checkName":         "MountPointUnavailable",
				"recommendedAction": "CONTACT_SUPPORT",
				"isHealthy":         false,
			},
			"enrichment": map[string]any{"status": "empty"},
		},
	}

	if err := sink.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(producer.record.Value, &decoded); err != nil {
		t.Fatalf("record value is not JSON: %v", err)
	}
	if decoded["event_type"] != "nvsentinel_health_event" {
		t.Fatalf("event_type = %#v", decoded["event_type"])
	}
	if decoded["resource_id"] != "gpu-node-1" {
		t.Fatalf("resource_id = %#v", decoded["resource_id"])
	}
	if decoded["resource_status"] != "" {
		t.Fatalf("resource_status = %#v", decoded["resource_status"])
	}
	if decoded["resource_sub_status"] != "MountPointUnavailable" {
		t.Fatalf("resource_sub_status = %#v", decoded["resource_sub_status"])
	}
	if decoded["update_time"] != event.Time {
		t.Fatalf("update_time = %#v", decoded["update_time"])
	}
	extra, ok := decoded["extra"].(map[string]any)
	if !ok {
		t.Fatalf("extra = %#v", decoded["extra"])
	}
	if extra["event_id"] != event.ID || extra["node"] != "gpu-node-1" || extra["enrichment_status"] != "empty" {
		t.Fatalf("extra summary = %#v", extra)
	}
	if _, ok := extra["raw"].(map[string]any); !ok {
		t.Fatalf("extra.raw = %#v", extra["raw"])
	}
}

func TestKafkaTLSConfigLoadsFiles(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(ca, testCACertPEM(t), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	cfg, err := kafkaTLSConfig(KafkaTLSConfig{Enabled: true, CAFile: ca})
	if err != nil {
		t.Fatalf("kafkaTLSConfig() error = %v", err)
	}
	if cfg == nil || cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLS config = %#v", cfg)
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs was nil")
	}
}

func TestKafkaSASLConfigReadsPasswordFile(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte("secret\n"), 0o600); err != nil {
		t.Fatalf("write password: %v", err)
	}

	opts, err := kafkaSASLOpts(KafkaSASLConfig{Enabled: true, Mechanism: "SCRAM-SHA-512", Username: "user", PasswordFile: passwordFile})
	if err != nil {
		t.Fatalf("kafkaSASLOpts() error = %v", err)
	}
	if len(opts) == 0 {
		t.Fatal("expected SASL opts")
	}
}

func testCACertPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
