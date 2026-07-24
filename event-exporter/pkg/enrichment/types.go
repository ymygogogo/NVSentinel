package enrichment

import (
	"context"
	"time"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
)

const (
	SchemaVersion = "nvsentinel.event_exporter.enrichment.v1"

	StatusMatched = "matched"
	StatusEmpty   = "empty"
	StatusFailed  = "failed"

	PodSourceWatchCache = "kubernetes-watch-cache"
	PodSourcePrometheus = "prometheus"

	FailurePolicyBestEffort        = "best_effort"
	FailurePolicyRequirePodContext = "require_pod_context"

	ProcessingModeStream   = "stream"
	ProcessingModeBackfill = "backfill"
)

type PodSummary struct {
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	NodeName    string            `json:"nodeName,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type EnrichmentData struct {
	SchemaVersion      string       `json:"schemaVersion"`
	Status             string       `json:"status"`
	PodSource          string       `json:"podSource,omitempty"`
	EventTime          string       `json:"eventTime,omitempty"`
	PodResultTruncated bool         `json:"podResultTruncated"`
	PodCountTotal      int          `json:"podCountTotal"`
	PodCountReturned   int          `json:"podCountReturned"`
	Pods               []PodSummary `json:"pods"`
	Reason             string       `json:"reason"`
}

type Query struct {
	NodeName  string
	EventTime time.Time
}

type Result struct {
	Drop   bool
	Reason string
}

type EventEnricher interface {
	Enrich(ctx context.Context, event *pb.HealthEvent, cloudEvent *transformer.CloudEvent, mode string) (Result, error)
}

type Provider interface {
	GetPods(ctx context.Context, query Query) ([]PodSummary, error)
}

type Alerter interface {
	Alert(ctx context.Context, alert MissingPodContextAlert) error
}

type MissingPodContextAlert struct {
	Cluster     string
	NodeName    string
	CheckName   string
	EventTime   time.Time
	Reason      string
	EventID     string
	PodSource   string
	HealthEvent map[string]any
}
