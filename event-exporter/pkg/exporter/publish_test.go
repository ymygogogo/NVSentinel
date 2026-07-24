package exporter

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/config"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/enrichment"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/metrics"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
)

type fakeSink struct {
	err       error
	calls     int
	published []*transformer.CloudEvent
}

func (f *fakeSink) Publish(_ context.Context, event *transformer.CloudEvent) error {
	f.calls++
	f.published = append(f.published, event)
	return f.err
}

func (f *fakeSink) Close(context.Context) error { return nil }

type recordingPublishAlerter struct {
	calls  int
	alerts []enrichment.MissingPodContextAlert
	err    error
}

func (r *recordingPublishAlerter) Alert(_ context.Context, alert enrichment.MissingPodContextAlert) error {
	r.calls++
	r.alerts = append(r.alerts, alert)
	return r.err
}

func sourceDocForPublishTest(event *pb.HealthEvent) healthEventSourceDocument {
	return healthEventSourceDocument{
		ID:                "63acf0813e9054394e1d167a",
		CreatedAt:         time.Date(2026, 7, 24, 7, 30, 49, 603674426, time.UTC),
		HealthEvent:       event,
		HealthEventStatus: &pb.HealthEventStatus{},
	}
}

func TestPublishWithRetryUsesDeterministicSourceDocumentID(t *testing.T) {
	sink := &fakeSink{}
	exporter := &HealthEventsExporter{
		cfg: &config.Config{Exporter: config.ExporterConfig{
			Metadata: config.MetadataConfig{"cluster": "dks01"},
			FailureHandling: config.FailureHandlingConfig{
				MaxRetries:        1,
				InitialBackoff:    "1ms",
				MaxBackoff:        "1ms",
				BackoffMultiplier: 1,
			},
		}},
		transformer: transformer.NewCloudEventsTransformer(map[string]string{"cluster": "dks01"}),
		sink:        sink,
	}
	sourceDoc := sourceDocForPublishTest(&pb.HealthEvent{
		NodeName:           "gpu-node-1",
		CheckName:          "MountPointUnavailable",
		GeneratedTimestamp: timestamppb.New(time.Date(2026, 7, 24, 7, 30, 49, 603674426, time.UTC)),
	})
	wantID, err := deterministicEventID(sourceDoc)
	if err != nil {
		t.Fatalf("deterministicEventID() error = %v", err)
	}

	if err := exporter.publishWithRetry(context.Background(), sourceDoc, enrichment.ProcessingModeStream); err != nil {
		t.Fatalf("publishWithRetry() error = %v, want nil", err)
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want 1", sink.calls)
	}
	if got := sink.published[0].ID; got != wantID {
		t.Fatalf("published event ID = %q, want deterministic ID %q", got, wantID)
	}
}

func TestPublishWithRetryAlertsWhenSinkRetriesExhausted(t *testing.T) {
	sinkErr := errors.New("sink unavailable")
	alerter := &recordingPublishAlerter{}
	exporter := &HealthEventsExporter{
		cfg: &config.Config{Exporter: config.ExporterConfig{
			Metadata: config.MetadataConfig{"cluster": "dks01"},
			FailureHandling: config.FailureHandlingConfig{
				MaxRetries:        1,
				InitialBackoff:    "1ms",
				MaxBackoff:        "1ms",
				BackoffMultiplier: 1,
			},
		}},
		transformer: transformer.NewCloudEventsTransformer(map[string]string{"cluster": "dks01"}),
		sink:        &fakeSink{err: sinkErr},
		alerter:     alerter,
	}

	eventTime := time.Now().UTC()
	if err := exporter.publishWithRetry(context.Background(), sourceDocForPublishTest(&pb.HealthEvent{
		NodeName:           "gpu-node-1",
		CheckName:          "MountPointUnavailable",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}), enrichment.ProcessingModeStream); err != nil {
		t.Fatalf("publishWithRetry() error = %v, want nil after alerting sink failure", err)
	}
	if alerter.calls != 1 {
		t.Fatalf("alert calls = %d, want 1", alerter.calls)
	}
	if got := alerter.alerts[0].Reason; got != "max_retries_exceeded" {
		t.Fatalf("alert reason = %q, want max_retries_exceeded", got)
	}
}

func TestPublishWithRetryDoesNotAlertWhenSinkEventuallySucceeds(t *testing.T) {
	alerter := &recordingPublishAlerter{}
	sink := &fakeSink{}
	exporter := &HealthEventsExporter{
		cfg: &config.Config{Exporter: config.ExporterConfig{
			Metadata: config.MetadataConfig{"cluster": "dks01"},
			FailureHandling: config.FailureHandlingConfig{
				MaxRetries:        1,
				InitialBackoff:    "1ms",
				MaxBackoff:        "1ms",
				BackoffMultiplier: 1,
			},
		}},
		transformer: transformer.NewCloudEventsTransformer(map[string]string{"cluster": "dks01"}),
		sink:        sink,
		alerter:     alerter,
	}

	eventTime := time.Now().UTC()
	if err := exporter.publishWithRetry(context.Background(), sourceDocForPublishTest(&pb.HealthEvent{
		NodeName:           "gpu-node-1",
		CheckName:          "MountPointUnavailable",
		GeneratedTimestamp: timestamppb.New(eventTime),
	}), enrichment.ProcessingModeStream); err != nil {
		t.Fatalf("publishWithRetry() error = %v, want nil", err)
	}
	if alerter.calls != 0 {
		t.Fatalf("alert calls = %d, want 0", alerter.calls)
	}
}

func TestPublishWithRetryRecordsFaultLastSeenMetric(t *testing.T) {
	sink := &fakeSink{}
	exporter := &HealthEventsExporter{
		cfg: &config.Config{Exporter: config.ExporterConfig{
			Metadata: config.MetadataConfig{"cluster": "dks01"},
			FailureHandling: config.FailureHandlingConfig{
				MaxRetries:        1,
				InitialBackoff:    "1ms",
				MaxBackoff:        "1ms",
				BackoffMultiplier: 1,
			},
		}},
		transformer: transformer.NewCloudEventsTransformer(map[string]string{"cluster": "dks01"}),
		sink:        sink,
	}

	eventTime := time.Date(2026, 6, 29, 11, 42, 37, 0, time.UTC)
	labels := []string{
		"gpu-node-1",
		"MountPointUnavailable",
		"mount-health-monitor",
		"false",
		"CONTACT_SUPPORT",
	}
	metrics.FaultLastSeenTimestampSeconds.DeleteLabelValues(labels...)

	if err := exporter.publishWithRetry(context.Background(), sourceDocForPublishTest(&pb.HealthEvent{
		NodeName:           labels[0],
		CheckName:          labels[1],
		Agent:              labels[2],
		IsHealthy:          false,
		RecommendedAction:  pb.RecommendedAction_CONTACT_SUPPORT,
		GeneratedTimestamp: timestamppb.New(eventTime),
	}), enrichment.ProcessingModeStream); err != nil {
		t.Fatalf("publishWithRetry() error = %v, want nil", err)
	}

	observer, err := metrics.FaultLastSeenTimestampSeconds.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("get fault last seen metric: %v", err)
	}
	dtoMetric := &dto.Metric{}
	if err := observer.Write(dtoMetric); err != nil {
		t.Fatalf("write fault last seen metric: %v", err)
	}
	if got, want := dtoMetric.GetGauge().GetValue(), float64(eventTime.Unix()); math.Abs(got-want) > 0.001 {
		t.Fatalf("fault last seen metric = %v, want %v", got, want)
	}
}

func TestPublishWithRetryRecordsFaultMetricWithUnknownLabelsAndCurrentTime(t *testing.T) {
	sink := &fakeSink{}
	exporter := &HealthEventsExporter{
		cfg: &config.Config{Exporter: config.ExporterConfig{
			Metadata: config.MetadataConfig{"cluster": "dks01"},
			FailureHandling: config.FailureHandlingConfig{
				MaxRetries:        1,
				InitialBackoff:    "1ms",
				MaxBackoff:        "1ms",
				BackoffMultiplier: 1,
			},
		}},
		transformer: transformer.NewCloudEventsTransformer(map[string]string{"cluster": "dks01"}),
		sink:        sink,
	}

	labels := []string{"unknown", "unknown", "unknown", "true", "NONE"}
	metrics.FaultLastSeenTimestampSeconds.DeleteLabelValues(labels...)
	before := time.Now().UTC().Unix()

	if err := exporter.publishWithRetry(context.Background(), sourceDocForPublishTest(&pb.HealthEvent{
		IsHealthy: true,
	}), enrichment.ProcessingModeStream); err != nil {
		t.Fatalf("publishWithRetry() error = %v, want nil", err)
	}
	after := time.Now().UTC().Unix()

	observer, err := metrics.FaultLastSeenTimestampSeconds.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("get fault last seen metric: %v", err)
	}
	dtoMetric := &dto.Metric{}
	if err := observer.Write(dtoMetric); err != nil {
		t.Fatalf("write fault last seen metric: %v", err)
	}
	got := int64(dtoMetric.GetGauge().GetValue())
	if got < before || got > after {
		t.Fatalf("fault last seen metric = %d, want between %d and %d", got, before, after)
	}
}
