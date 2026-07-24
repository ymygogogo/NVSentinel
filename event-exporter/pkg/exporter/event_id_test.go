package exporter

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

func TestDeterministicEventIDUsesCanonicalSourceDocument(t *testing.T) {
	createdAt := time.Date(2026, 7, 24, 7, 30, 49, 603674426, time.UTC)
	doc := healthEventSourceDocument{
		ID:        "63acf0813e9054394e1d167a",
		CreatedAt: createdAt,
		HealthEvent: &pb.HealthEvent{
			Version:            1,
			Agent:              "mount-health-monitor",
			ComponentClass:     "NODE_STORAGE",
			CheckName:          "MountPointUnavailable",
			IsFatal:            true,
			IsHealthy:          false,
			Message:            "mount /data not found in mountinfo",
			RecommendedAction:  pb.RecommendedAction_CONTACT_SUPPORT,
			ErrorCode:          []string{"missing"},
			GeneratedTimestamp: timestamppb.New(createdAt),
			NodeName:           "gsh-gpu-1",
		},
		HealthEventStatus: &pb.HealthEventStatus{
			NodeQuarantined: "",
		},
	}

	first, err := deterministicEventID(doc)
	if err != nil {
		t.Fatalf("deterministicEventID() error = %v", err)
	}
	second, err := deterministicEventID(doc)
	if err != nil {
		t.Fatalf("deterministicEventID() second call error = %v", err)
	}
	if first != second {
		t.Fatalf("deterministicEventID() = %q then %q, want stable ID", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("deterministicEventID() length = %d, want 64 hex chars", len(first))
	}
	if first[:7] == "sha256:" {
		t.Fatalf("deterministicEventID() = %q, must not include sha256 prefix", first)
	}

	statusChanged := doc
	statusChanged.HealthEventStatus = &pb.HealthEventStatus{NodeQuarantined: "Quarantined"}
	changed, err := deterministicEventID(statusChanged)
	if err != nil {
		t.Fatalf("deterministicEventID(statusChanged) error = %v", err)
	}
	if changed == first {
		t.Fatalf("deterministicEventID() did not change when source status changed: %q", changed)
	}

	idChanged := doc
	idChanged.ID = "63acf7893e9054394e1d167b"
	changed, err = deterministicEventID(idChanged)
	if err != nil {
		t.Fatalf("deterministicEventID(idChanged) error = %v", err)
	}
	if changed == first {
		t.Fatalf("deterministicEventID() did not change when source document id changed: %q", changed)
	}
}
