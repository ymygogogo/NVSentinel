package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadParsesEnrichmentConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	contents := `
[exporter]
client_name = "event-exporter-alerting"

[exporter.metadata]
cluster = "dks01"

[exporter.sink]
endpoint = "https://sink.example.com/events"

[exporter.oidc]
token_url = "https://auth.example.com/token"
client_id = "event-exporter"
scope = "events:write"

[exporter.resume_token]
database = "nvsentinel"
collection = "resumetokens"

[exporter.enrichment]
enabled = true
failure_policy = "require_pod_context"
current_cache_max_event_age = "5m"
clock_skew_tolerance = "2m"

[exporter.enrichment.pod_metadata]
enabled = true
realtime_source = "kubernetes-watch-cache"
realtime_fallback_source = "prometheus"
historical_source = "prometheus"
namespace_include_regex = "^cci-.*"
namespace_exclude_regex = "^(kube-system|nvsentinel)$"
label_selector = "platform.example.com/managed=true"
label_allowlist = ["tenant_id", "task_id"]
annotation_allowlist = []
max_pods_per_node = 500
max_payload_bytes = 1048576
cache_sync_timeout = "10s"

[exporter.enrichment.prometheus]
enabled = true
endpoint = "http://prometheus.monitoring.svc:9090"
timeout = "3s"
query_lookback = "5m"
query_range_step = "15s"
max_concurrent_queries = 5
cache_ttl = "60s"

[exporter.enrichment.missing_pod_context_alert]
enabled = true
webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=redacted"
timeout = "3s"
max_retries = 2
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Exporter.ClientName != "event-exporter-alerting" {
		t.Fatalf("ClientName = %q, want event-exporter-alerting", cfg.Exporter.ClientName)
	}

	enrichment := cfg.Exporter.Enrichment
	if !enrichment.Enabled {
		t.Fatal("enrichment.Enabled = false, want true")
	}
	if enrichment.FailurePolicy != "require_pod_context" {
		t.Fatalf("FailurePolicy = %q", enrichment.FailurePolicy)
	}
	if enrichment.PodMetadata.NamespaceIncludeRegex != "^cci-.*" {
		t.Fatalf("NamespaceIncludeRegex = %q", enrichment.PodMetadata.NamespaceIncludeRegex)
	}
	if got := enrichment.PodMetadata.LabelAllowlist; len(got) != 2 || got[0] != "tenant_id" || got[1] != "task_id" {
		t.Fatalf("LabelAllowlist = %#v", got)
	}
	if enrichment.Prometheus.Endpoint != "http://prometheus.monitoring.svc:9090" {
		t.Fatalf("Prometheus endpoint = %q", enrichment.Prometheus.Endpoint)
	}
	if got := enrichment.Prometheus.GetQueryRangeStep().String(); got != "15s" {
		t.Fatalf("Prometheus query range step = %q, want 15s", got)
	}
	if enrichment.MissingPodContextAlert.WebhookURL == "" {
		t.Fatal("WebhookURL should be parsed")
	}
}

func TestLoadAllowsOIDCDisabledWithoutTokenSettings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	contents := `
[exporter.metadata]
cluster = "dks01"

[exporter.sink]
endpoint = "https://sink.example.com/events"

[exporter.oidc]
enabled = false

[exporter.resume_token]
database = "nvsentinel"
collection = "resumetokens"
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Exporter.OIDC.IsEnabled() {
		t.Fatal("OIDC IsEnabled() = true, want false")
	}
}

func TestOIDCDefaultsToDisabledWhenEnabledIsOmitted(t *testing.T) {
	cfg := OIDCConfig{}
	if cfg.IsEnabled() {
		t.Fatal("OIDC IsEnabled() = true, want false when enabled is omitted")
	}
}

func TestExporterClientNameDefaultsToEventExporter(t *testing.T) {
	cfg := Config{}
	cfg.Exporter.Sink.Endpoint = "https://sink.example.com/events"
	cfg.Exporter.ResumeToken.Database = "nvsentinel"
	cfg.Exporter.ResumeToken.Collection = "resumetokens"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.Exporter.ClientName != "event-exporter" {
		t.Fatalf("ClientName = %q, want event-exporter", cfg.Exporter.ClientName)
	}
}
