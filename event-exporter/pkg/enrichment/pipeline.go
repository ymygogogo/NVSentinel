package enrichment

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
)

type Config struct {
	Enabled                  bool
	FailurePolicy            string
	CurrentCacheMaxEventAge  time.Duration
	ClockSkewTolerance       time.Duration
	NamespaceIncludeRegex    string
	NamespaceExcludeRegex    string
	LabelAllowlist           []string
	AnnotationAllowlist      []string
	MaxPodsPerNode           int
	MaxPayloadBytes          int
	RealtimeProvider         Provider
	RealtimeFallbackProvider Provider
	HistoricalProvider       Provider
	Alerter                  Alerter
	Now                      func() time.Time
}

type Pipeline struct {
	cfg    Config
	filter *filter
}

func NewPipeline(cfg Config) (*Pipeline, error) {
	if cfg.FailurePolicy == "" {
		cfg.FailurePolicy = FailurePolicyBestEffort
	}
	if cfg.CurrentCacheMaxEventAge == 0 {
		cfg.CurrentCacheMaxEventAge = 5 * time.Minute
	}
	if cfg.ClockSkewTolerance == 0 {
		cfg.ClockSkewTolerance = 2 * time.Minute
	}
	if cfg.MaxPodsPerNode == 0 {
		cfg.MaxPodsPerNode = 1000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.FailurePolicy != FailurePolicyBestEffort && cfg.FailurePolicy != FailurePolicyRequirePodContext {
		return nil, fmt.Errorf("unsupported enrichment failure policy %q", cfg.FailurePolicy)
	}

	f, err := newFilter(cfg.NamespaceIncludeRegex, cfg.NamespaceExcludeRegex, cfg.LabelAllowlist, cfg.AnnotationAllowlist)
	if err != nil {
		return nil, fmt.Errorf("compile enrichment filters: %w", err)
	}

	return &Pipeline{cfg: cfg, filter: f}, nil
}

func (p *Pipeline) Enrich(
	ctx context.Context,
	event *pb.HealthEvent,
	cloudEvent *transformer.CloudEvent,
	mode string,
) (Result, error) {
	if p == nil || !p.cfg.Enabled {
		return Result{}, nil
	}
	if cloudEvent.Data == nil {
		cloudEvent.Data = map[string]any{}
	}

	eventTime, ok := generatedTime(event)
	if !ok {
		return p.applyNoContext(ctx, event, cloudEvent, "missing_generated_timestamp", "")
	}

	query := Query{NodeName: event.GetNodeName(), EventTime: eventTime}
	pods, source, errors := p.findPods(ctx, query, mode)

	if len(pods) == 0 {
		reason := "missing_pod_context"
		if len(errors) > 0 {
			reason = strings.Join(errors, ",")
		}
		return p.applyNoContext(ctx, event, cloudEvent, reason, source)
	}

	truncated := false
	total := len(pods)
	if p.cfg.MaxPodsPerNode > 0 && len(pods) > p.cfg.MaxPodsPerNode {
		pods = pods[:p.cfg.MaxPodsPerNode]
		truncated = true
	}
	limitedPods, sizeTruncated := limitPayloadPods(pods, p.cfg.MaxPayloadBytes)
	if sizeTruncated {
		truncated = true
	}
	pods = limitedPods

	cloudEvent.Data["enrichment"] = EnrichmentData{
		SchemaVersion:      SchemaVersion,
		Status:             StatusMatched,
		PodSource:          source,
		EventTime:          eventTime.UTC().Format(time.RFC3339Nano),
		PodResultTruncated: truncated,
		PodCountTotal:      total,
		PodCountReturned:   len(pods),
		Pods:               pods,
		Reason:             "",
	}

	return Result{}, nil
}

func (p *Pipeline) findPods(ctx context.Context, query Query, mode string) ([]PodSummary, string, []string) {
	if mode == ProcessingModeBackfill || p.isHistorical(query.EventTime) {
		pods, source, errors, _ := p.queryProvider(ctx, p.cfg.HistoricalProvider, query, PodSourcePrometheus)
		return pods, source, errors
	}

	pods, source, errors, cacheAvailable := p.queryProvider(ctx, p.cfg.RealtimeProvider, query, PodSourceWatchCache)
	if len(pods) > 0 || cacheAvailable {
		return pods, source, errors
	}

	fallbackPods, fallbackSource, fallbackErrors, _ := p.queryProvider(ctx, p.cfg.RealtimeFallbackProvider, query, PodSourcePrometheus)
	if len(fallbackErrors) > 0 {
		errors = append(errors, fallbackErrors...)
	}
	if fallbackSource != "" {
		source = fallbackSource
	}
	return fallbackPods, source, errors
}

func (p *Pipeline) queryProvider(ctx context.Context, provider Provider, query Query, source string) ([]PodSummary, string, []string, bool) {
	if provider == nil {
		return nil, source, []string{source + "_provider_not_configured"}, false
	}
	pods, err := provider.GetPods(ctx, query)
	if err != nil {
		return nil, source, []string{source + "_query_failed"}, false
	}
	pods = p.filter.normalizePods(pods)
	if len(pods) == 0 {
		return nil, source, []string{source + "_empty"}, true
	}
	return pods, source, nil, true
}

func (p *Pipeline) isHistorical(eventTime time.Time) bool {
	now := p.cfg.Now().UTC()
	if eventTime.After(now.Add(p.cfg.ClockSkewTolerance)) {
		return true
	}
	return now.Sub(eventTime) > p.cfg.CurrentCacheMaxEventAge
}

func (p *Pipeline) applyNoContext(
	ctx context.Context,
	event *pb.HealthEvent,
	cloudEvent *transformer.CloudEvent,
	reason string,
	source string,
) (Result, error) {
	status := StatusFailed
	if strings.Contains(reason, "_empty") {
		status = StatusEmpty
	}
	eventTime := eventTimeFromCloudEvent(cloudEvent)
	eventTimeText := ""
	if !eventTime.IsZero() {
		eventTimeText = eventTime.UTC().Format(time.RFC3339Nano)
	}

	cloudEvent.Data["enrichment"] = EnrichmentData{
		SchemaVersion:      SchemaVersion,
		Status:             status,
		PodSource:          source,
		EventTime:          eventTimeText,
		PodResultTruncated: false,
		PodCountTotal:      0,
		PodCountReturned:   0,
		Pods:               []PodSummary{},
		Reason:             reason,
	}

	if status == StatusEmpty {
		slog.InfoContext(ctx, "Publishing event without matching Pod context",
			"node", event.GetNodeName(),
			"checkName", event.GetCheckName(),
			"reason", reason)
		return Result{}, nil
	}

	slog.ErrorContext(ctx, "Publishing event with pod context enrichment failed",
		"node", event.GetNodeName(),
		"checkName", event.GetCheckName(),
		"reason", reason)

	if p.cfg.Alerter != nil {
		if err := p.cfg.Alerter.Alert(ctx, MissingPodContextAlert{
			Cluster:   clusterName(cloudEvent),
			NodeName:  event.GetNodeName(),
			CheckName: event.GetCheckName(),
			Reason:    reason,
			PodSource: source,
			EventTime: eventTime,
		}); err != nil {
			slog.WarnContext(ctx, "Failed to send pod context enrichment alert", "error", err)
		}
	}

	return Result{}, nil
}

func generatedTime(event *pb.HealthEvent) (time.Time, bool) {
	if event == nil || event.GeneratedTimestamp == nil {
		return time.Time{}, false
	}
	return event.GeneratedTimestamp.AsTime().UTC(), true
}

func clusterName(cloudEvent *transformer.CloudEvent) string {
	metadata, ok := cloudEvent.Data["metadata"].(map[string]string)
	if !ok {
		return ""
	}
	return metadata["cluster"]
}

func eventTimeFromCloudEvent(cloudEvent *transformer.CloudEvent) time.Time {
	if cloudEvent == nil || cloudEvent.Time == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, cloudEvent.Time)
	if err != nil {
		return time.Time{}
	}
	return parsed
}
