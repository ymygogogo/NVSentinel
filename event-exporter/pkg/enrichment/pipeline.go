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
	NodeLabelAllowlist       []string
	PodMetadataEnabled       bool
	MaxPodsPerNode           int
	MaxPayloadBytes          int
	NodeProvider             NodeProvider
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
	if !cfg.PodMetadataEnabled && (cfg.RealtimeProvider != nil || cfg.RealtimeFallbackProvider != nil || cfg.HistoricalProvider != nil) {
		cfg.PodMetadataEnabled = true
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
		node := p.findNode(ctx, event.GetNodeName())
		return p.applyNoContext(ctx, event, cloudEvent, "missing_generated_timestamp", "", nil, node)
	}

	if !p.cfg.PodMetadataEnabled {
		node := p.findNode(ctx, event.GetNodeName())
		status := StatusEmpty
		reason := "pod_metadata_disabled"
		if node != nil {
			status = StatusMatched
			reason = ""
		}
		cloudEvent.Data["enrichment"] = EnrichmentData{
			SchemaVersion:      SchemaVersion,
			Status:             status,
			EventTime:          eventTime.UTC().Format(time.RFC3339Nano),
			PodResultTruncated: false,
			PodCountTotal:      0,
			PodCountReturned:   0,
			Pods:               []PodSummary{},
			Node:               node,
			Reason:             reason,
		}
		return Result{}, nil
	}

	query := Query{NodeName: event.GetNodeName(), EventTime: eventTime}
	pods, source, errors, errorDetails := p.findPods(ctx, query, mode)

	if len(pods) == 0 {
		reason := "missing_pod_context"
		if len(errors) > 0 {
			reason = strings.Join(errors, ",")
		}
		node := p.findNode(ctx, event.GetNodeName())
		return p.applyNoContext(ctx, event, cloudEvent, reason, source, errorDetails, node)
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
	node := p.findNode(ctx, event.GetNodeName())

	cloudEvent.Data["enrichment"] = EnrichmentData{
		SchemaVersion:      SchemaVersion,
		Status:             StatusMatched,
		PodSource:          source,
		EventTime:          eventTime.UTC().Format(time.RFC3339Nano),
		PodResultTruncated: truncated,
		PodCountTotal:      total,
		PodCountReturned:   len(pods),
		Pods:               pods,
		Node:               node,
		Reason:             "",
	}

	return Result{}, nil
}

func (p *Pipeline) findPods(ctx context.Context, query Query, mode string) ([]PodSummary, string, []string, []string) {
	if mode == ProcessingModeBackfill || p.isHistorical(query.EventTime) {
		pods, source, errors, details, _ := p.queryProvider(ctx, p.cfg.HistoricalProvider, query, PodSourcePrometheus)
		return pods, source, errors, details
	}

	pods, source, errors, details, cacheAvailable := p.queryProvider(ctx, p.cfg.RealtimeProvider, query, PodSourceWatchCache)
	if len(pods) > 0 || cacheAvailable {
		return pods, source, errors, details
	}

	fallbackPods, fallbackSource, fallbackErrors, fallbackDetails, _ := p.queryProvider(ctx, p.cfg.RealtimeFallbackProvider, query, PodSourcePrometheus)
	if len(fallbackErrors) > 0 {
		errors = append(errors, fallbackErrors...)
	}
	if len(fallbackDetails) > 0 {
		details = append(details, fallbackDetails...)
	}
	if fallbackSource != "" {
		source = fallbackSource
	}
	return fallbackPods, source, errors, details
}

func (p *Pipeline) queryProvider(ctx context.Context, provider Provider, query Query, source string) ([]PodSummary, string, []string, []string, bool) {
	if provider == nil {
		reason := source + "_provider_not_configured"
		return nil, source, []string{reason}, []string{source + ": provider not configured"}, false
	}
	pods, err := provider.GetPods(ctx, query)
	if err != nil {
		return nil, source, []string{source + "_query_failed"}, []string{source + ": " + err.Error()}, false
	}
	pods = p.filter.normalizePods(pods)
	if len(pods) == 0 {
		return nil, source, []string{source + "_empty"}, nil, true
	}
	return pods, source, nil, nil, true
}

func (p *Pipeline) findNode(ctx context.Context, nodeName string) *NodeSummary {
	if p.cfg.NodeProvider == nil || len(p.cfg.NodeLabelAllowlist) == 0 {
		return nil
	}
	node, err := p.cfg.NodeProvider.GetNode(ctx, nodeName)
	if err != nil {
		slog.WarnContext(ctx, "Failed to enrich event with node labels", "node", nodeName, "error", err)
		return nil
	}
	return normalizeNode(node, stringSet(p.cfg.NodeLabelAllowlist))
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
	errorDetails []string,
	node *NodeSummary,
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
		Node:               node,
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
		"reason", reason,
		"error_details", strings.Join(errorDetails, "; "))

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
