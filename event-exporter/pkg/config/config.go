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

package config

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nvidia/nvsentinel/commons/pkg/configmanager"
)

const (
	defaultSinkTimeout    = 30 * time.Second
	defaultBackfillMaxAge = 720 * time.Hour
	defaultInitialBackoff = 1 * time.Second
	defaultMaxBackoff     = 60 * time.Second
	defaultCacheMaxAge    = 5 * time.Minute
	defaultClockSkew      = 2 * time.Minute
	defaultCacheSync      = 10 * time.Second
	defaultPromTimeout    = 3 * time.Second
	defaultQueryLookback  = 5 * time.Minute
	defaultPromCacheTTL   = 60 * time.Second
	defaultAlertTimeout   = 3 * time.Second
)

type Config struct {
	Exporter ExporterConfig `toml:"exporter"`
}

type ExporterConfig struct {
	ClientName      string                `toml:"client_name"`
	Metadata        MetadataConfig        `toml:"metadata"`
	Sink            SinkConfig            `toml:"sink"`
	OIDC            OIDCConfig            `toml:"oidc"`
	Backfill        BackfillConfig        `toml:"backfill"`
	ResumeToken     ResumeTokenConfig     `toml:"resume_token"`
	FailureHandling FailureHandlingConfig `toml:"failure_handling"`
	Enrichment      EnrichmentConfig      `toml:"enrichment"`
}

type MetadataConfig map[string]string

type SinkConfig struct {
	Endpoint           string `toml:"endpoint"`
	Timeout            string `toml:"timeout"`
	InsecureSkipVerify bool   `toml:"insecure_skip_verify"`
}

type OIDCConfig struct {
	Enabled            *bool  `toml:"enabled"`
	TokenURL           string `toml:"token_url"`
	ClientID           string `toml:"client_id"`
	Scope              string `toml:"scope"`
	InsecureSkipVerify bool   `toml:"insecure_skip_verify"`
}

type BackfillConfig struct {
	Enabled   bool   `toml:"enabled"`
	MaxAge    string `toml:"max_age"`
	MaxEvents int    `toml:"max_events"`
	BatchSize int    `toml:"batch_size"`
	RateLimit int    `toml:"rate_limit"`
}

type ResumeTokenConfig struct {
	Collection string `toml:"collection"`
	Database   string `toml:"database"`
}

type FailureHandlingConfig struct {
	MaxRetries        int     `toml:"max_retries"`
	InitialBackoff    string  `toml:"initial_backoff"`
	MaxBackoff        string  `toml:"max_backoff"`
	BackoffMultiplier float64 `toml:"backoff_multiplier"`
}

type EnrichmentConfig struct {
	Enabled                 bool                         `toml:"enabled"`
	FailurePolicy           string                       `toml:"failure_policy"`
	CurrentCacheMaxEventAge string                       `toml:"current_cache_max_event_age"`
	ClockSkewTolerance      string                       `toml:"clock_skew_tolerance"`
	PodMetadata             PodMetadataEnrichmentConfig  `toml:"pod_metadata"`
	Prometheus              PrometheusEnrichmentConfig   `toml:"prometheus"`
	MissingPodContextAlert  MissingPodContextAlertConfig `toml:"missing_pod_context_alert"`
}

type PodMetadataEnrichmentConfig struct {
	Enabled                bool     `toml:"enabled"`
	RealtimeSource         string   `toml:"realtime_source"`
	RealtimeFallbackSource string   `toml:"realtime_fallback_source"`
	HistoricalSource       string   `toml:"historical_source"`
	NamespaceIncludeRegex  string   `toml:"namespace_include_regex"`
	NamespaceExcludeRegex  string   `toml:"namespace_exclude_regex"`
	LabelSelector          string   `toml:"label_selector"`
	LabelAllowlist         []string `toml:"label_allowlist"`
	AnnotationAllowlist    []string `toml:"annotation_allowlist"`
	MaxPodsPerNode         int      `toml:"max_pods_per_node"`
	MaxPayloadBytes        int      `toml:"max_payload_bytes"`
	CacheSyncTimeout       string   `toml:"cache_sync_timeout"`
}

type PrometheusEnrichmentConfig struct {
	Enabled              bool   `toml:"enabled"`
	Endpoint             string `toml:"endpoint"`
	Timeout              string `toml:"timeout"`
	QueryLookback        string `toml:"query_lookback"`
	MaxConcurrentQueries int    `toml:"max_concurrent_queries"`
	CacheTTL             string `toml:"cache_ttl"`
}

type MissingPodContextAlertConfig struct {
	Enabled    bool   `toml:"enabled"`
	WebhookURL string `toml:"webhook_url"`
	Timeout    string `toml:"timeout"`
	MaxRetries int    `toml:"max_retries"`
}

func (c *SinkConfig) GetTimeout() time.Duration {
	if c.Timeout == "" {
		slog.Error("Sink timeout not configured, using default 30 seconds")
		return defaultSinkTimeout
	}

	d, err := time.ParseDuration(c.Timeout)
	if err != nil {
		slog.Error("Failed to parse sink timeout, using default 30 seconds", "error", err)
		return defaultSinkTimeout
	}

	return d
}

func (c *OIDCConfig) IsEnabled() bool {
	if c.Enabled == nil {
		return false
	}
	return *c.Enabled
}

func (c *BackfillConfig) GetMaxAge() time.Duration {
	if c.MaxAge == "" {
		slog.Error("Backfill max age not configured, using default 720 hours")
		return defaultBackfillMaxAge
	}

	d, err := time.ParseDuration(c.MaxAge)
	if err != nil {
		slog.Error("Failed to parse backfill max age, using default 720 hours", "error", err)
		return defaultBackfillMaxAge
	}

	return d
}

func (c *FailureHandlingConfig) GetInitialBackoff() time.Duration {
	if c.InitialBackoff == "" {
		slog.Error("Initial backoff not configured, using default 1 second")
		return defaultInitialBackoff
	}

	d, err := time.ParseDuration(c.InitialBackoff)
	if err != nil {
		slog.Error("Failed to parse initial backoff, using default 1 second", "error", err)
		return defaultInitialBackoff
	}

	return d
}

func (c *FailureHandlingConfig) GetMaxBackoff() time.Duration {
	if c.MaxBackoff == "" {
		slog.Error("Max backoff not configured, using default 60 seconds")
		return defaultMaxBackoff
	}

	d, err := time.ParseDuration(c.MaxBackoff)
	if err != nil {
		slog.Error("Failed to parse max backoff, using default 60 seconds", "error", err)
		return defaultMaxBackoff
	}

	return d
}

func (c *EnrichmentConfig) GetCurrentCacheMaxEventAge() time.Duration {
	return parseDurationOrDefault(c.CurrentCacheMaxEventAge, defaultCacheMaxAge, "enrichment current cache max event age")
}

func (c *EnrichmentConfig) GetClockSkewTolerance() time.Duration {
	return parseDurationOrDefault(c.ClockSkewTolerance, defaultClockSkew, "enrichment clock skew tolerance")
}

func (c *PodMetadataEnrichmentConfig) GetCacheSyncTimeout() time.Duration {
	return parseDurationOrDefault(c.CacheSyncTimeout, defaultCacheSync, "enrichment cache sync timeout")
}

func (c *PrometheusEnrichmentConfig) GetTimeout() time.Duration {
	return parseDurationOrDefault(c.Timeout, defaultPromTimeout, "prometheus enrichment timeout")
}

func (c *PrometheusEnrichmentConfig) GetQueryLookback() time.Duration {
	return parseDurationOrDefault(c.QueryLookback, defaultQueryLookback, "prometheus enrichment query lookback")
}

func (c *PrometheusEnrichmentConfig) GetCacheTTL() time.Duration {
	return parseDurationOrDefault(c.CacheTTL, defaultPromCacheTTL, "prometheus enrichment cache ttl")
}

func (c *MissingPodContextAlertConfig) GetTimeout() time.Duration {
	return parseDurationOrDefault(c.Timeout, defaultAlertTimeout, "missing pod context alert timeout")
}

func parseDurationOrDefault(raw string, fallback time.Duration, name string) time.Duration {
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		slog.Error("Failed to parse duration, using default", "name", name, "value", raw, "default", fallback, "error", err)
		return fallback
	}
	return d
}

func (c *Config) Validate() error {
	if c.Exporter.ClientName == "" {
		c.Exporter.ClientName = "event-exporter"
	}

	if c.Exporter.Sink.Endpoint == "" {
		return fmt.Errorf("sink endpoint is required")
	}

	if c.Exporter.OIDC.IsEnabled() {
		if c.Exporter.OIDC.TokenURL == "" {
			return fmt.Errorf("OIDC token_url is required")
		}

		if c.Exporter.OIDC.ClientID == "" {
			return fmt.Errorf("OIDC client_id is required")
		}

		if c.Exporter.OIDC.Scope == "" {
			return fmt.Errorf("OIDC scope is required")
		}
	}

	if c.Exporter.ResumeToken.Collection == "" {
		return fmt.Errorf("resume_token collection is required")
	}

	if c.Exporter.ResumeToken.Database == "" {
		return fmt.Errorf("resume_token database is required")
	}

	if c.Exporter.Enrichment.Enabled {
		policy := c.Exporter.Enrichment.FailurePolicy
		if policy != "" && policy != "best_effort" && policy != "require_pod_context" {
			return fmt.Errorf("unsupported enrichment failure_policy %q", policy)
		}
		if c.Exporter.Enrichment.PodMetadata.Enabled && c.Exporter.Enrichment.PodMetadata.HistoricalSource == "prometheus" && c.Exporter.Enrichment.Prometheus.Enabled && c.Exporter.Enrichment.Prometheus.Endpoint == "" {
			return fmt.Errorf("prometheus endpoint is required when prometheus enrichment is enabled")
		}
	}

	return nil
}

func Load(path string) (*Config, error) {
	var cfg Config

	if err := configmanager.LoadTOMLConfig(path, &cfg); err != nil {
		slog.Error("Failed to load config", "error", err)
		return nil, fmt.Errorf("load config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		slog.Error("Config validation failed", "error", err)
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}
