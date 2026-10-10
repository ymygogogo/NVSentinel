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
	"strings"

	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"google.golang.org/protobuf/proto"
)

// recoveryKeyMatchesEvent associates replacement UUIDs with the stored GPU slot.
// Fault insertion and lookup of unhealthy events keep the original semantics.
func recoveryKeyMatchesEvent(key HealthEventKey, stored, incoming *protos.HealthEvent) bool {
	if !incoming.IsHealthy || !strings.EqualFold(incoming.ComponentClass, "GPU") ||
		len(incoming.EntitiesImpacted) == 0 || !isGPUIdentity(key.EntityType) {
		return keyMatchesEvent(key, incoming)
	}

	scope := proto.Clone(incoming).(*protos.HealthEvent)

	scope.EntitiesImpacted = nil
	if !keyMatchesEvent(key, scope) {
		return false
	}

	matches, comparable := matchingGPUSlot(stored, incoming)
	if comparable {
		return matches
	}

	return entityInEvent(key, incoming)
}

func isGPUIdentity(entityType string) bool {
	return entityType == "GPU" || entityType == "PCI" || entityType == "GPU_UUID"
}

// matchingGPUSlot requires at least one shared stable identity and rejects
// conflicting or ambiguous stable identities. Missing historical fields are OK.
func matchingGPUSlot(stored, incoming *protos.HealthEvent) (bool, bool) {
	// Compound events do not encode which UUID belongs to which slot. Keep
	// exact entity matching instead of inferring an association for them.
	for _, kind := range []string{"GPU", "PCI"} {
		_, _, oldAmbiguous := singleEntityValue(stored, kind)

		_, _, newAmbiguous := singleEntityValue(incoming, kind)
		if oldAmbiguous || newAmbiguous {
			return false, false
		}
	}

	shared := false
	hasStable := false

	for _, kind := range []string{"GPU", "PCI"} {
		oldValue, oldPresent, _ := singleEntityValue(stored, kind)
		newValue, newPresent, _ := singleEntityValue(incoming, kind)

		hasStable = hasStable || newPresent
		if oldPresent && newPresent {
			if oldValue != newValue {
				return false, true
			}

			shared = true
		}
	}

	if shared {
		return true, true
	}
	// With a stable recovery identity, UUID alone cannot establish slot ownership.
	return false, hasStable
}

func singleEntityValue(event *protos.HealthEvent, kind string) (string, bool, bool) {
	value := ""

	present := false
	if event == nil {
		return value, false, false
	}

	for _, entity := range event.EntitiesImpacted {
		if entity == nil || entity.EntityType != kind {
			continue
		}

		if present && entity.EntityValue != value {
			return "", true, true
		}

		value, present = entity.EntityValue, true
	}

	return value, present, false
}

// remainingEvents preserves exactly the remaining (entity, errorCode) pairs
// across annotation serialization without mutating the original audit events.
func (he *HealthEventsAnnotationMap) remainingEvents() []*protos.HealthEvent {
	type groupKey struct {
		event *protos.HealthEvent
		code  string
	}

	groups := make(map[groupKey]*protos.HealthEvent)

	events := make([]*protos.HealthEvent, 0, len(he.Events))
	for key, original := range he.Events {
		group := groupKey{original, key.ErrorCode}

		event, exists := groups[group]
		if !exists {
			event = proto.Clone(original).(*protos.HealthEvent)
			event.EntitiesImpacted = nil

			event.ErrorCode = nil
			if key.ErrorCode != "" {
				event.ErrorCode = []string{key.ErrorCode}
			}

			groups[group] = event
			events = append(events, event)
		}

		if key.EntityType != "" {
			event.EntitiesImpacted = append(event.EntitiesImpacted, &protos.Entity{
				EntityType: key.EntityType, EntityValue: key.EntityValue,
			})
		}
	}

	return events
}
