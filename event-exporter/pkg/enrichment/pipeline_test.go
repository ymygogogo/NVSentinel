package enrichment

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
)

type fakeProvider struct {
	pods []PodSummary
	err  error
}

func (f fakeProvider) GetPods(context.Context, Query) ([]PodSummary, error) {
	return f.pods, f.err
}

type recordingAlerter struct {
	calls int
}

func (r *recordingAlerter) Alert(context.Context, MissingPodContextAlert) error {
	r.calls++
	return nil
}

func TestPipelineRealtimeUsesCacheAndNormalizesPayload(t *testing.T) {
	eventTime := time.Now().Add(-time.Minute).UTC()
	event := &pb.HealthEvent{
		NodeName:           "gpu-node-1",
		CheckName:          "SysLogsXIDError",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}
	cloudEvent := &transformer.CloudEvent{Data: map[string]any{}}

	pipeline, err := NewPipeline(Config{
		Enabled:                  true,
		FailurePolicy:            FailurePolicyBestEffort,
		CurrentCacheMaxEventAge:  5 * time.Minute,
		ClockSkewTolerance:       2 * time.Minute,
		NamespaceIncludeRegex:    "^cci-.*",
		NamespaceExcludeRegex:    "^(kube-system|nvsentinel)$",
		LabelAllowlist:           []string{"tenant_id", "task_id"},
		AnnotationAllowlist:      []string{"platform.example.com/order-id"},
		MaxPodsPerNode:           1000,
		RealtimeProvider:         fakeProvider{pods: []PodSummary{{Namespace: "cci-a", Name: "pod-a", NodeName: "gpu-node-1", Labels: map[string]string{"tenant_id": "t1", "secret": "drop"}, Annotations: map[string]string{"platform.example.com/order-id": "o1", "drop": "x"}}}},
		RealtimeFallbackProvider: fakeProvider{pods: []PodSummary{{Namespace: "cci-b", Name: "pod-b"}}},
		HistoricalProvider:       fakeProvider{pods: []PodSummary{{Namespace: "cci-c", Name: "pod-c"}}},
		Now:                      func() time.Time { return eventTime.Add(time.Minute) },
	})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	result, err := pipeline.Enrich(context.Background(), event, cloudEvent, ProcessingModeStream)
	if err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if result.Drop {
		t.Fatal("Drop = true, want false")
	}

	enrichment := cloudEvent.Data["enrichment"].(EnrichmentData)
	if enrichment.Status != StatusMatched {
		t.Fatalf("Status = %q, want %q", enrichment.Status, StatusMatched)
	}
	if enrichment.Reason != "" {
		t.Fatalf("Reason = %q, want empty", enrichment.Reason)
	}
	if enrichment.PodSource != PodSourceWatchCache {
		t.Fatalf("PodSource = %q, want %q", enrichment.PodSource, PodSourceWatchCache)
	}
	if enrichment.PodCountReturned != 1 {
		t.Fatalf("PodCountReturned = %d, want 1", enrichment.PodCountReturned)
	}
	gotPod := enrichment.Pods[0]
	if gotPod.Namespace != "cci-a" || gotPod.Name != "pod-a" || gotPod.NodeName != "gpu-node-1" {
		t.Fatalf("pod = %+v, want cci-a/pod-a on gpu-node-1", gotPod)
	}
	if gotPod.Labels["tenant_id"] != "t1" || gotPod.Labels["secret"] != "" {
		t.Fatalf("labels were not allowlist-filtered: %+v", gotPod.Labels)
	}
	if gotPod.Annotations["platform.example.com/order-id"] != "o1" || gotPod.Annotations["drop"] != "" {
		t.Fatalf("annotations were not allowlist-filtered: %+v", gotPod.Annotations)
	}
}

func TestPipelineStreamUsesHistoricalProviderWhenEventTimeIsOld(t *testing.T) {
	eventTime := time.Now().Add(-30 * time.Minute).UTC()
	event := &pb.HealthEvent{
		NodeName:           "gpu-node-1",
		CheckName:          "MountPointUnavailable",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}
	cloudEvent := &transformer.CloudEvent{Data: map[string]any{}}

	pipeline, err := NewPipeline(Config{
		Enabled:                 true,
		FailurePolicy:           FailurePolicyBestEffort,
		CurrentCacheMaxEventAge: 5 * time.Minute,
		MaxPodsPerNode:          1000,
		RealtimeProvider:        fakeProvider{pods: []PodSummary{{Namespace: "cci-a", Name: "pod-a", NodeName: "gpu-node-1"}}},
		HistoricalProvider:      fakeProvider{pods: []PodSummary{{Namespace: "cci-history", Name: "pod-history", NodeName: "gpu-node-1"}}},
		Now:                     func() time.Time { return eventTime.Add(30 * time.Minute) },
	})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	result, err := pipeline.Enrich(context.Background(), event, cloudEvent, ProcessingModeStream)
	if err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if result.Drop {
		t.Fatal("Drop = true, want false")
	}

	enrichment := cloudEvent.Data["enrichment"].(EnrichmentData)
	if enrichment.PodSource != PodSourcePrometheus {
		t.Fatalf("PodSource = %q, want %q", enrichment.PodSource, PodSourcePrometheus)
	}
	if got := enrichment.Pods[0].Name; got != "pod-history" {
		t.Fatalf("pod name = %q, want pod-history", got)
	}
}

func TestPipelineStreamDoesNotFallbackWhenWatchCacheIsEmpty(t *testing.T) {
	eventTime := time.Now().UTC()
	event := &pb.HealthEvent{
		NodeName:           "gpu-node-1",
		CheckName:          "MountPointUnavailable",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}
	cloudEvent := &transformer.CloudEvent{Data: map[string]any{}}

	pipeline, err := NewPipeline(Config{
		Enabled:                  true,
		FailurePolicy:            FailurePolicyBestEffort,
		CurrentCacheMaxEventAge:  5 * time.Minute,
		NamespaceIncludeRegex:    "^cci-.*",
		MaxPodsPerNode:           1000,
		RealtimeProvider:         fakeProvider{pods: []PodSummary{{Namespace: "kube-system", Name: "system-pod", NodeName: "gpu-node-1"}}},
		RealtimeFallbackProvider: fakeProvider{pods: []PodSummary{{Namespace: "cci-history", Name: "stale-pod", NodeName: "gpu-node-1"}}},
		Now:                      func() time.Time { return eventTime.Add(time.Minute) },
	})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	result, err := pipeline.Enrich(context.Background(), event, cloudEvent, ProcessingModeStream)
	if err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if result.Drop {
		t.Fatal("Drop = true, want false for best_effort")
	}

	enrichment := cloudEvent.Data["enrichment"].(EnrichmentData)
	if enrichment.Status != StatusEmpty {
		t.Fatalf("Status = %q, want %q", enrichment.Status, StatusEmpty)
	}
	if enrichment.PodSource != PodSourceWatchCache {
		t.Fatalf("PodSource = %q, want %q", enrichment.PodSource, PodSourceWatchCache)
	}
	if got := enrichment.Reason; got != "kubernetes-watch-cache_empty" {
		t.Fatalf("reason = %q, want kubernetes-watch-cache_empty", got)
	}
}

func TestPipelineBackfillUsesPrometheusProvider(t *testing.T) {
	eventTime := time.Now().Add(-time.Hour).UTC()
	event := &pb.HealthEvent{
		NodeName:           "gpu-node-1",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}
	cloudEvent := &transformer.CloudEvent{Data: map[string]any{}}

	pipeline, err := NewPipeline(Config{
		Enabled:                 true,
		FailurePolicy:           FailurePolicyBestEffort,
		CurrentCacheMaxEventAge: 5 * time.Minute,
		MaxPodsPerNode:          1000,
		RealtimeProvider:        fakeProvider{pods: []PodSummary{{Namespace: "cci-realtime", Name: "pod-realtime"}}},
		HistoricalProvider:      fakeProvider{pods: []PodSummary{{Namespace: "cci-history", Name: "pod-history", NodeName: "gpu-node-1"}}},
		Now:                     func() time.Time { return eventTime.Add(time.Hour) },
	})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	result, err := pipeline.Enrich(context.Background(), event, cloudEvent, ProcessingModeBackfill)
	if err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if result.Drop {
		t.Fatal("Drop = true, want false")
	}

	enrichment := cloudEvent.Data["enrichment"].(EnrichmentData)
	if enrichment.PodSource != PodSourcePrometheus {
		t.Fatalf("PodSource = %q, want %q", enrichment.PodSource, PodSourcePrometheus)
	}
	if got := enrichment.Pods[0].Name; got != "pod-history" {
		t.Fatalf("pod name = %q, want pod-history", got)
	}
}

func TestPipelineBackfillPrometheusEmptyPublishesWithoutAlert(t *testing.T) {
	eventTime := time.Now().Add(-time.Hour).UTC()
	event := &pb.HealthEvent{
		NodeName:           "gpu-node-1",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}
	cloudEvent := &transformer.CloudEvent{Data: map[string]any{}}
	alerter := &recordingAlerter{}

	pipeline, err := NewPipeline(Config{
		Enabled:            true,
		FailurePolicy:      FailurePolicyRequirePodContext,
		MaxPodsPerNode:     1000,
		HistoricalProvider: fakeProvider{},
		Alerter:            alerter,
		Now:                func() time.Time { return eventTime.Add(time.Hour) },
	})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	result, err := pipeline.Enrich(context.Background(), event, cloudEvent, ProcessingModeBackfill)
	if err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if result.Drop {
		t.Fatal("Drop = true, want false")
	}
	if alerter.calls != 0 {
		t.Fatalf("alert calls = %d, want 0", alerter.calls)
	}

	enrichment := cloudEvent.Data["enrichment"].(EnrichmentData)
	if enrichment.Status != StatusEmpty {
		t.Fatalf("Status = %q, want %q", enrichment.Status, StatusEmpty)
	}
	if enrichment.Reason != "prometheus_empty" {
		t.Fatalf("Reason = %q, want prometheus_empty", enrichment.Reason)
	}
	if enrichment.Pods == nil || len(enrichment.Pods) != 0 {
		t.Fatalf("Pods = %#v, want empty non-nil slice", enrichment.Pods)
	}
}

func TestPipelineMissingGeneratedTimestampPublishesAndAlerts(t *testing.T) {
	event := &pb.HealthEvent{
		NodeName:  "gpu-node-1",
		CheckName: "MountPointUnavailable",
	}
	cloudEvent := &transformer.CloudEvent{Data: map[string]any{"metadata": map[string]string{"cluster": "dks01"}}}
	alerter := &recordingAlerter{}

	pipeline, err := NewPipeline(Config{
		Enabled:       true,
		FailurePolicy: FailurePolicyRequirePodContext,
		Alerter:       alerter,
	})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	result, err := pipeline.Enrich(context.Background(), event, cloudEvent, ProcessingModeStream)
	if err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if result.Drop {
		t.Fatal("Drop = true, want false")
	}
	if alerter.calls != 1 {
		t.Fatalf("alert calls = %d, want 1", alerter.calls)
	}

	enrichment := cloudEvent.Data["enrichment"].(EnrichmentData)
	if enrichment.Status != StatusFailed {
		t.Fatalf("Status = %q, want %q", enrichment.Status, StatusFailed)
	}
	if enrichment.Reason != "missing_generated_timestamp" {
		t.Fatalf("Reason = %q, want missing_generated_timestamp", enrichment.Reason)
	}
}

func TestPipelineRequirePodContextPublishesWhenWatchCacheIsEmpty(t *testing.T) {
	eventTime := time.Now().UTC()
	event := &pb.HealthEvent{
		NodeName:           "gpu-node-1",
		CheckName:          "MountPointUnavailable",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}
	cloudEvent := &transformer.CloudEvent{Data: map[string]any{"metadata": map[string]string{"cluster": "dks01"}}}
	alerter := &recordingAlerter{}

	pipeline, err := NewPipeline(Config{
		Enabled:                 true,
		FailurePolicy:           FailurePolicyRequirePodContext,
		CurrentCacheMaxEventAge: 5 * time.Minute,
		NamespaceIncludeRegex:   "^cci-.*",
		MaxPodsPerNode:          1000,
		RealtimeProvider:        fakeProvider{pods: []PodSummary{{Namespace: "kube-system", Name: "system-pod", NodeName: "gpu-node-1"}}},
		Alerter:                 alerter,
		Now:                     func() time.Time { return eventTime.Add(time.Minute) },
	})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	result, err := pipeline.Enrich(context.Background(), event, cloudEvent, ProcessingModeStream)
	if err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if result.Drop {
		t.Fatal("Drop = true, want false when cache is available but no matching pods exist")
	}
	if alerter.calls != 0 {
		t.Fatalf("alert calls = %d, want 0", alerter.calls)
	}

	enrichment := cloudEvent.Data["enrichment"].(EnrichmentData)
	if enrichment.Status != StatusEmpty {
		t.Fatalf("Status = %q, want %q", enrichment.Status, StatusEmpty)
	}
	if enrichment.PodSource != PodSourceWatchCache {
		t.Fatalf("PodSource = %q, want %q", enrichment.PodSource, PodSourceWatchCache)
	}
	if got := enrichment.Reason; got != "kubernetes-watch-cache_empty" {
		t.Fatalf("reason = %q, want kubernetes-watch-cache_empty", got)
	}
}

func TestPipelineQueryProviderReturnsStableReasonAndErrorDetails(t *testing.T) {
	pipeline, err := NewPipeline(Config{Enabled: true})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	_, _, reasons, details, cacheAvailable := pipeline.queryProvider(
		context.Background(),
		fakeProvider{err: errors.New("prometheus returned status 400")},
		Query{NodeName: "gpu-node-1", EventTime: time.Now().UTC()},
		PodSourcePrometheus,
	)
	if cacheAvailable {
		t.Fatal("cacheAvailable = true, want false")
	}
	if len(reasons) != 1 || reasons[0] != "prometheus_query_failed" {
		t.Fatalf("reasons = %+v, want prometheus_query_failed", reasons)
	}
	if len(details) != 1 || details[0] != "prometheus: prometheus returned status 400" {
		t.Fatalf("details = %+v, want provider error detail", details)
	}
}

func TestPipelineRequirePodContextPublishesAndAlertsWhenProvidersFail(t *testing.T) {
	eventTime := time.Now().UTC()
	event := &pb.HealthEvent{
		NodeName:           "gpu-node-1",
		CheckName:          "SysLogsXIDError",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}
	cloudEvent := &transformer.CloudEvent{Data: map[string]any{"metadata": map[string]string{"cluster": "dks01"}}}
	alerter := &recordingAlerter{}

	pipeline, err := NewPipeline(Config{
		Enabled:                  true,
		FailurePolicy:            FailurePolicyRequirePodContext,
		CurrentCacheMaxEventAge:  5 * time.Minute,
		MaxPodsPerNode:           1000,
		RealtimeProvider:         fakeProvider{err: errors.New("cache unavailable")},
		RealtimeFallbackProvider: fakeProvider{err: errors.New("prometheus unavailable")},
		Alerter:                  alerter,
		Now:                      func() time.Time { return eventTime.Add(time.Minute) },
	})
	if err != nil {
		t.Fatalf("NewPipeline() error = %v", err)
	}

	result, err := pipeline.Enrich(context.Background(), event, cloudEvent, ProcessingModeStream)
	if err != nil {
		t.Fatalf("Enrich() error = %v", err)
	}
	if result.Drop {
		t.Fatal("Drop = true, want false")
	}
	enrichment := cloudEvent.Data["enrichment"].(EnrichmentData)
	if enrichment.Status != StatusFailed {
		t.Fatalf("Status = %q, want %q", enrichment.Status, StatusFailed)
	}
	if enrichment.Reason != "kubernetes-watch-cache_query_failed,prometheus_query_failed" {
		t.Fatalf("reason = %q, want combined provider failures", enrichment.Reason)
	}
	if alerter.calls != 1 {
		t.Fatalf("alert calls = %d, want 1", alerter.calls)
	}
}
