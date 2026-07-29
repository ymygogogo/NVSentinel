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
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nvidia/nvsentinel/event-exporter/pkg/auth"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/config"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/enrichment"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/exporter"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/sink"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	storeconfig "github.com/nvidia/nvsentinel/store-client/pkg/config"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/factory"
	"github.com/nvidia/nvsentinel/store-client/pkg/helper"
)

type Params struct {
	ConfigPath     string
	OIDCSecretPath string
	Workers        int
}

type Components struct {
	Exporter        *exporter.HealthEventsExporter
	DatastoreBundle *helper.DatastoreClientBundle
	BackfillEnabled bool
}

func InitializeAll(ctx context.Context, params Params) (*Components, error) {
	cfg, err := loadConfig(params.ConfigPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	slog.Info("Publish workers configured", "workers", params.Workers)

	tokenProvider, err := initializeOIDC(cfg, params.OIDCSecretPath)
	if err != nil {
		slog.Error("Failed to initialize OIDC", "error", err)
		return nil, fmt.Errorf("failed to initialize OIDC: %w", err)
	}

	eventSink, err := initializeSink(cfg, tokenProvider, params.Workers)
	if err != nil {
		slog.Error("Failed to initialize sink", "error", err)
		return nil, fmt.Errorf("failed to initialize sink: %w", err)
	}

	cloudEventsTransformer := transformer.NewCloudEventsTransformer(cfg.Exporter.Metadata)

	alerter := initializeEnrichmentAlerter(cfg)

	eventEnricher, err := initializeEnricher(ctx, cfg, alerter)
	if err != nil {
		slog.Error("Failed to initialize enrichment", "error", err)
		return nil, fmt.Errorf("failed to initialize enrichment: %w", err)
	}

	datastoreBundle, hasResumeToken, err := initializeDatastore(ctx, cfg.Exporter.ClientName)
	if err != nil {
		slog.Error("Failed to initialize datastore", "error", err)
		return nil, fmt.Errorf("failed to initialize datastore: %w", err)
	}

	exp := exporter.New(
		cfg,
		datastoreBundle.DatabaseClient,
		datastoreBundle.ChangeStreamWatcher,
		cloudEventsTransformer,
		eventEnricher,
		alerter,
		eventSink,
		hasResumeToken,
		params.Workers,
	)

	return &Components{
		Exporter:        exp,
		DatastoreBundle: datastoreBundle,
		BackfillEnabled: cfg.Exporter.Backfill.Enabled,
	}, nil
}

func initializeSink(cfg *config.Config, tokenProvider *auth.TokenProvider, workers int) (sink.EventSink, error) {
	switch cfg.Exporter.Sink.SinkType() {
	case config.SinkTypeHTTP:
		return sink.NewHTTPSink(
			cfg.Exporter.Sink.Endpoint,
			cfg.Exporter.Sink.GetTimeout(),
			tokenProvider,
			cfg.Exporter.Sink.InsecureSkipVerify,
			workers,
		), nil
	case config.SinkTypeKafka:
		kafkaCfg := cfg.Exporter.Sink.Kafka
		return sink.NewKafkaSink(sink.KafkaConfig{
			Brokers:       kafkaCfg.Brokers,
			Topic:         kafkaCfg.Topic,
			ClientID:      kafkaCfg.ClientID,
			RequiredAcks:  kafkaCfg.RequiredAcks,
			Compression:   kafkaCfg.Compression,
			BatchTimeout:  kafkaCfg.GetBatchTimeout(),
			WriteTimeout:  kafkaCfg.GetWriteTimeout(),
			PayloadFormat: kafkaCfg.PayloadFormat,
			Wrapper: sink.KafkaWrapperConfig{
				EventType:         kafkaCfg.Wrapper.EventType,
				ResourceID:        kafkaCfg.Wrapper.ResourceID,
				ResourceStatus:    kafkaCfg.Wrapper.ResourceStatus,
				ResourceSubStatus: kafkaCfg.Wrapper.ResourceSubStatus,
				ExtraRawField:     kafkaCfg.Wrapper.ExtraRawField,
			},
			TLS: sink.KafkaTLSConfig{
				Enabled:            kafkaCfg.TLS.Enabled,
				CAFile:             kafkaCfg.TLS.CAFile,
				CertFile:           kafkaCfg.TLS.CertFile,
				KeyFile:            kafkaCfg.TLS.KeyFile,
				InsecureSkipVerify: kafkaCfg.TLS.InsecureSkipVerify,
			},
			SASL: sink.KafkaSASLConfig{
				Enabled:      kafkaCfg.SASL.Enabled,
				Mechanism:    kafkaCfg.SASL.Mechanism,
				Username:     kafkaCfg.SASL.Username,
				PasswordFile: kafkaCfg.SASL.PasswordFile,
			},
		})
	default:
		return nil, fmt.Errorf("unsupported sink type %q", cfg.Exporter.Sink.SinkType())
	}
}

func loadConfig(configPath string) (*config.Config, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		return nil, fmt.Errorf("failed to load config from %s: %w", configPath, err)
	}

	slog.Info("Configuration loaded", "path", configPath)

	return cfg, nil
}

func initializeOIDC(cfg *config.Config, secretPath string) (*auth.TokenProvider, error) {
	if !cfg.Exporter.OIDC.IsEnabled() {
		slog.Info("OIDC token provider disabled")
		return nil, nil
	}

	clientSecretBytes, err := os.ReadFile(secretPath)
	if err != nil {
		slog.Error("Failed to read OIDC client secret from file", "path", secretPath, "error", err)
		return nil, fmt.Errorf("failed to read OIDC client secret from file %s: %w", secretPath, err)
	}

	tokenProvider := auth.NewTokenProvider(
		cfg.Exporter.OIDC.TokenURL,
		cfg.Exporter.OIDC.ClientID,
		strings.TrimSpace(string(clientSecretBytes)),
		cfg.Exporter.OIDC.Scope,
		cfg.Exporter.OIDC.InsecureSkipVerify,
	)

	slog.Info("OIDC token provider initialized",
		"tokenURL", cfg.Exporter.OIDC.TokenURL,
		"clientID", cfg.Exporter.OIDC.ClientID,
		"scope", cfg.Exporter.OIDC.Scope)

	return tokenProvider, nil
}

func initializeDatastore(ctx context.Context, clientName string) (*helper.DatastoreClientBundle, bool, error) {
	datastoreConfig, err := datastore.LoadDatastoreConfig()
	if err != nil {
		slog.Error("Failed to load datastore config", "error", err)
		return nil, false, fmt.Errorf("failed to load datastore config: %w", err)
	}

	builder := client.GetPipelineBuilder()
	pipeline := builder.BuildAllHealthEventInsertsPipeline()

	bundle, err := helper.NewDatastoreClientFromConfig(ctx, clientName, *datastoreConfig, pipeline)
	if err != nil {
		slog.Error("Failed to create datastore client", "error", err)
		return nil, false, fmt.Errorf("failed to create datastore client: %w", err)
	}

	slog.Info("Datastore client initialized", "provider", datastoreConfig.Provider, "clientName", clientName)

	hasResumeToken, err := checkResumeTokenExists(ctx, clientName)
	if err != nil {
		slog.Warn("Failed to check resume token, assuming false", "error", err)

		hasResumeToken = false
	}

	return bundle, hasResumeToken, nil
}

func tokenDatabaseCertMountPath(datastoreConfig *datastore.DataStoreConfig) string {
	if datastoreConfig == nil || datastoreConfig.Connection.TLSConfig == nil {
		return ""
	}

	if datastoreConfig.Connection.TLSConfig.CAPath == "" {
		return ""
	}

	return filepath.Dir(datastoreConfig.Connection.TLSConfig.CAPath)
}

func checkResumeTokenExists(ctx context.Context, clientName string) (bool, error) {
	tokenConfig, err := storeconfig.TokenConfigFromEnv(clientName)
	if err != nil {
		return false, fmt.Errorf("failed to get token config: %w", err)
	}

	slog.Info("Checking for existing resume token",
		"database", tokenConfig.TokenDatabase,
		"collection", tokenConfig.TokenCollection,
		"clientName", tokenConfig.ClientName)

	datastoreConfig, err := datastore.LoadDatastoreConfig()
	if err != nil {
		return false, fmt.Errorf("failed to load datastore config for token lookup: %w", err)
	}

	certMountPath := tokenDatabaseCertMountPath(datastoreConfig)

	dbConfig, err := storeconfig.NewDatabaseConfigForCollectionType(
		certMountPath,
		storeconfig.CollectionTypeTokens,
	)
	if err != nil {
		return false, fmt.Errorf("failed to create token database config: %w", err)
	}

	clientFactory := factory.NewClientFactory(dbConfig)

	tokenClient, err := clientFactory.CreateDatabaseClient(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to create token client: %w", err)
	}

	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tokenClient.Close(closeCtx)
	}()

	filter := map[string]any{
		"clientName": tokenConfig.ClientName,
	}

	var tokenDoc struct {
		ResumeToken any `bson:"resumeToken"`
	}

	result, err := tokenClient.FindOne(ctx, filter, nil)
	if err != nil {
		return false, fmt.Errorf("failed to query resume token: %w", err)
	}

	if err := result.Decode(&tokenDoc); err != nil {
		if client.IsNoDocumentsError(err) {
			slog.Info("No resume token found - this appears to be first deployment")

			return false, nil
		}

		return false, fmt.Errorf("failed to decode token document: %w", err)
	}

	hasToken := tokenDoc.ResumeToken != nil
	if hasToken {
		slog.Info("Resume token exists - skipping backfill on this restart")
	}

	return hasToken, nil
}

func initializeEnrichmentAlerter(cfg *config.Config) enrichment.Alerter {
	alertCfg := cfg.Exporter.Enrichment.MissingPodContextAlert
	if !alertCfg.Enabled {
		return nil
	}
	return enrichment.NewWebhookAlerter(
		alertCfg.WebhookURL,
		alertCfg.GetTimeout(),
		alertCfg.MaxRetries,
	)
}

func initializeEnricher(ctx context.Context, cfg *config.Config, alerter enrichment.Alerter) (enrichment.EventEnricher, error) {
	enrichmentCfg := cfg.Exporter.Enrichment
	if !enrichmentCfg.Enabled || !enrichmentCfg.PodMetadata.Enabled {
		return nil, nil
	}

	podCfg := enrichmentCfg.PodMetadata
	promCfg := enrichmentCfg.Prometheus

	var realtimeProvider enrichment.Provider
	if podCfg.RealtimeSource == "kubernetes-watch-cache" {
		provider, err := enrichment.NewKubernetesPodWatchCache(ctx, enrichment.PodCacheConfig{
			LabelSelector:    podCfg.LabelSelector,
			CacheSyncTimeout: podCfg.GetCacheSyncTimeout(),
		})
		if err != nil {
			return nil, err
		}
		realtimeProvider = provider
	}

	var prometheusProvider enrichment.Provider
	if promCfg.Enabled {
		prometheusProvider = enrichment.NewPrometheusProvider(enrichment.PrometheusConfig{
			Endpoint:             promCfg.Endpoint,
			Timeout:              promCfg.GetTimeout(),
			QueryLookback:        promCfg.GetQueryLookback(),
			QueryRangeStep:       promCfg.GetQueryRangeStep(),
			QueryTemplate:        promCfg.QueryTemplate,
			LabelAllowlist:       podCfg.LabelAllowlist,
			AnnotationAllowlist:  podCfg.AnnotationAllowlist,
			MaxConcurrentQueries: promCfg.MaxConcurrentQueries,
			CacheTTL:             promCfg.GetCacheTTL(),
		})
	}

	return enrichment.NewPipeline(enrichment.Config{
		Enabled:                  true,
		FailurePolicy:            enrichmentCfg.FailurePolicy,
		CurrentCacheMaxEventAge:  enrichmentCfg.GetCurrentCacheMaxEventAge(),
		ClockSkewTolerance:       enrichmentCfg.GetClockSkewTolerance(),
		NamespaceIncludeRegex:    podCfg.NamespaceIncludeRegex,
		NamespaceExcludeRegex:    podCfg.NamespaceExcludeRegex,
		LabelAllowlist:           podCfg.LabelAllowlist,
		AnnotationAllowlist:      podCfg.AnnotationAllowlist,
		MaxPodsPerNode:           podCfg.MaxPodsPerNode,
		MaxPayloadBytes:          podCfg.MaxPayloadBytes,
		RealtimeProvider:         realtimeProvider,
		RealtimeFallbackProvider: prometheusProvider,
		HistoricalProvider:       prometheusProvider,
		Alerter:                  alerter,
	})
}
