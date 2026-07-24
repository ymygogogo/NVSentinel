package exporter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

type healthEventSourceDocument struct {
	ID                string
	CreatedAt         time.Time
	HealthEvent       *pb.HealthEvent
	HealthEventStatus *pb.HealthEventStatus
}

type healthEventSourceDocumentRecord struct {
	ID                any                   `bson:"_id"`
	CreatedAt         time.Time             `bson:"createdAt"`
	HealthEvent       *pb.HealthEvent       `bson:"healthevent,omitempty"`
	HealthEventStatus *pb.HealthEventStatus `bson:"healtheventstatus"`
}

func sourceDocumentFromModel(id string, event model.HealthEventWithStatus) healthEventSourceDocument {
	return healthEventSourceDocument{
		ID:                id,
		CreatedAt:         event.CreatedAt,
		HealthEvent:       event.HealthEvent,
		HealthEventStatus: event.HealthEventStatus,
	}
}

func sourceDocumentFromRecord(record healthEventSourceDocumentRecord) (healthEventSourceDocument, error) {
	id, err := normalizeDocumentID(record.ID)
	if err != nil {
		return healthEventSourceDocument{}, err
	}

	return healthEventSourceDocument{
		ID:                id,
		CreatedAt:         record.CreatedAt,
		HealthEvent:       record.HealthEvent,
		HealthEventStatus: record.HealthEventStatus,
	}, nil
}

func normalizeDocumentID(id any) (string, error) {
	switch v := id.(type) {
	case string:
		if v == "" {
			return "", fmt.Errorf("document id is empty")
		}
		return v, nil
	case primitive.ObjectID:
		if v.IsZero() {
			return "", fmt.Errorf("document id is empty")
		}
		return v.Hex(), nil
	case []byte:
		if len(v) == 0 {
			return "", fmt.Errorf("document id is empty")
		}
		return hex.EncodeToString(v), nil
	default:
		return "", fmt.Errorf("unsupported document id type %T", id)
	}
}

func deterministicEventID(doc healthEventSourceDocument) (string, error) {
	if doc.ID == "" {
		return "", fmt.Errorf("document id is empty")
	}

	healthEvent, err := canonicalProto(doc.HealthEvent)
	if err != nil {
		return "", fmt.Errorf("canonicalize health event: %w", err)
	}

	healthEventStatus, err := canonicalProto(doc.HealthEventStatus)
	if err != nil {
		return "", fmt.Errorf("canonicalize health event status: %w", err)
	}

	canonicalDoc := struct {
		ID                string `json:"_id"`
		CreatedAt         string `json:"createdAt"`
		HealthEvent       any    `json:"healthevent"`
		HealthEventStatus any    `json:"healtheventstatus"`
	}{
		ID:                doc.ID,
		CreatedAt:         doc.CreatedAt.UTC().Format(time.RFC3339Nano),
		HealthEvent:       healthEvent,
		HealthEventStatus: healthEventStatus,
	}

	payload, err := json.Marshal(canonicalDoc)
	if err != nil {
		return "", fmt.Errorf("marshal canonical document: %w", err)
	}

	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalProto(message proto.Message) (any, error) {
	if message == nil {
		return nil, nil
	}

	payload, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}.Marshal(message)
	if err != nil {
		return nil, err
	}

	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}
