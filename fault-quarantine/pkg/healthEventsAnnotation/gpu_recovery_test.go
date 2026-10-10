// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
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

package healthEventsAnnotation

import (
	"encoding/json"
	"testing"

	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func gpuTestEvent(gpu, pci, uuid string, healthy bool) *protos.HealthEvent {
	event := &protos.HealthEvent{Version: 1, Agent: "test", ComponentClass: "GPU",
		CheckName: "GpuMemWatch", NodeName: "test-node", IsHealthy: healthy, IsFatal: !healthy,
		ErrorCode: []string{"A"}}
	for _, item := range []struct{ kind, value string }{{"GPU", gpu}, {"PCI", pci}, {"GPU_UUID", uuid}} {
		if item.value != "" {
			event.EntitiesImpacted = append(event.EntitiesImpacted,
				&protos.Entity{EntityType: item.kind, EntityValue: item.value})
		}
	}
	return event
}

// TestGPURecovery_ScopeMismatch_PreservesFault verifies recovery never crosses event scope.
func TestGPURecovery_ScopeMismatch_PreservesFault(t *testing.T) {
	mutations := map[string]func(*protos.HealthEvent){
		"agent":   func(e *protos.HealthEvent) { e.Agent = "other" },
		"check":   func(e *protos.HealthEvent) { e.CheckName = "other" },
		"node":    func(e *protos.HealthEvent) { e.NodeName = "other" },
		"version": func(e *protos.HealthEvent) { e.Version = 2 },
		"error":   func(e *protos.HealthEvent) { e.ErrorCode = []string{"B"} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			m := NewHealthEventsAnnotationMap()
			m.AddOrUpdateEvent(gpuTestEvent("5", "ba", "old", false))
			recovery := gpuTestEvent("5", "ba", "new", true)
			mutate(recovery)
			require.Zero(t, m.RemoveEvent(recovery))
			require.Equal(t, 3, m.Count())
		})
	}
}

// TestGPURecovery_CompoundEvent_PreservesUnassociatedUUID verifies conservative fallback.
func TestGPURecovery_CompoundEvent_PreservesUnassociatedUUID(t *testing.T) {
	m := NewHealthEventsAnnotationMap()
	fault := gpuTestEvent("5", "ba", "old", false)
	fault.EntitiesImpacted = append(fault.EntitiesImpacted, &protos.Entity{EntityType: "GPU", EntityValue: "6"})
	m.AddOrUpdateEvent(fault)
	recovery := gpuTestEvent("5", "ba", "new", true)
	require.Equal(t, 2, m.RemoveEvent(recovery))
	b, err := json.Marshal(m)
	require.NoError(t, err)
	restored := NewHealthEventsAnnotationMap()
	require.NoError(t, json.Unmarshal(b, restored))
	require.Equal(t, 2, restored.Count())
	recovery = proto.Clone(recovery).(*protos.HealthEvent)
	recovery.EntitiesImpacted = nil
	require.Equal(t, 2, restored.RemoveEvent(recovery))
}

// TestGPURecovery_SlotIdentity_RespectsConflicts verifies replacement and legacy identities.
func TestGPURecovery_SlotIdentity_RespectsConflicts(t *testing.T) {
	tests := []struct {
		name, gpu, pci, uuid, newGPU, newPCI, newUUID string
		clears                                        bool
	}{
		{"replacement", "5", "ba", "old", "5", "ba", "new", true},
		{"missing PCI", "5", "", "old", "5", "ba", "new", true},
		{"GPU only", "5", "", "", "5", "ba", "new", true},
		{"PCI only", "", "ba", "old", "5", "ba", "new", true},
		{"incoming GPU only", "5", "ba", "old", "5", "", "new", true},
		{"incoming PCI only", "5", "ba", "old", "", "ba", "new", true},
		{"PCI conflict", "5", "ba", "old", "5", "ca", "new", false},
		{"GPU conflict", "5", "ba", "old", "6", "ba", "new", false},
		{"other GPU", "5", "ba", "old", "6", "ca", "new", false},
		{"UUID only changed", "", "", "old", "", "", "new", false},
		{"UUID only exact", "", "", "old", "", "", "old", true},
		{"lost stable identity", "", "", "old", "5", "ba", "new", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewHealthEventsAnnotationMap()
			fault := gpuTestEvent(tc.gpu, tc.pci, tc.uuid, false)
			m.AddOrUpdateEvent(fault)
			count := m.Count()
			recovery := gpuTestEvent(tc.newGPU, tc.newPCI, tc.newUUID, true)
			_, matches := m.GetEvent(recovery)
			require.Equal(t, tc.clears, matches)
			removed := m.RemoveEvent(recovery)
			if tc.clears {
				require.Equal(t, count, removed)
				require.True(t, m.IsEmpty())
			} else {
				require.Zero(t, removed)
				require.Equal(t, count, m.Count())
			}
		})
	}
}

// TestGPURecovery_MultipleFaults_RoundTripPreservesRemaining verifies exact persisted scope.
func TestGPURecovery_MultipleFaults_RoundTripPreservesRemaining(t *testing.T) {
	m := NewHealthEventsAnnotationMap()
	first := gpuTestEvent("5", "ba", "old", false)
	first.ErrorCode = []string{"A", "B"}
	other := gpuTestEvent("6", "ca", "other", false)
	m.AddOrUpdateEvent(first)
	m.AddOrUpdateEvent(other)
	recovery := gpuTestEvent("5", "ba", "new", true)
	require.Equal(t, 3, m.RemoveEvent(recovery))
	b, err := json.Marshal(m)
	require.NoError(t, err)
	restored := NewHealthEventsAnnotationMap()
	require.NoError(t, json.Unmarshal(b, restored))
	require.Equal(t, 6, restored.Count())
	for key := range restored.Events {
		if key.EntityValue == "5" || key.EntityValue == "ba" || key.EntityValue == "old" {
			require.Equal(t, "B", key.ErrorCode)
		}
	}
	recovery.ErrorCode = []string{"B"}
	require.Equal(t, 3, restored.RemoveEvent(recovery))
	require.Equal(t, 3, restored.Count())
	recovery = gpuTestEvent("6", "ca", "replacement", true)
	recovery.ErrorCode = nil
	require.Equal(t, 3, restored.RemoveEvent(recovery))
	require.True(t, restored.IsEmpty())
	require.Len(t, first.EntitiesImpacted, 3)
	require.Len(t, first.ErrorCode, 2)
}

// TestGPURecovery_NonGPUAndPartialRemoval_PreserveExactEntities checks compatibility.
func TestGPURecovery_NonGPUAndPartialRemoval_PreserveExactEntities(t *testing.T) {
	m := NewHealthEventsAnnotationMap()
	fault := gpuTestEvent("5", "ba", "old", false)
	fault.ComponentClass = "NIC"
	m.AddOrUpdateEvent(fault)
	recovery := gpuTestEvent("5", "ba", "new", true)
	recovery.ComponentClass = "NIC"
	require.Equal(t, 2, m.RemoveEvent(recovery))
	b, err := json.Marshal(m)
	require.NoError(t, err)
	restored := NewHealthEventsAnnotationMap()
	require.NoError(t, json.Unmarshal(b, restored))
	require.Equal(t, 1, restored.Count())
	require.Equal(t, "GPU_UUID", func() string {
		for k := range restored.Events {
			return k.EntityType
		}
		return ""
	}())
}
