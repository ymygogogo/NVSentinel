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

// Package condition converts between the proto Condition message (used in
// ExternalRemediationRequest's status conditions, wire format) and
// metav1.Condition (the standard Kubernetes representation that
// controller-runtime helpers like meta.SetStatusCondition and
// meta.IsStatusConditionTrue operate on).
//
// Reconcilers work in metav1.Condition for ergonomics; the conversion at the
// controller boundary keeps the proto wire format authoritative on the API
// server while letting controller code use the standard helpers. See ADR-040.
package condition

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	protos "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// ToMetav1 converts a single proto Condition to metav1.Condition.
//
// The proto Condition's status is a string (e.g. "True"), so the conversion
// preserves whatever value is there. Callers that want to constrain the value
// to the metav1.ConditionStatus enum should validate at a higher layer.
func ToMetav1(c *protos.Condition) metav1.Condition {
	if c == nil {
		return metav1.Condition{}
	}

	var lastTransition metav1.Time
	if c.LastTransitionTime != nil {
		lastTransition = metav1.NewTime(c.LastTransitionTime.AsTime())
	}

	return metav1.Condition{
		Type:               c.Type,
		Status:             metav1.ConditionStatus(c.Status),
		ObservedGeneration: c.ObservedGeneration,
		LastTransitionTime: lastTransition,
		Reason:             c.Reason,
		Message:            c.Message,
	}
}

// FromMetav1 converts a metav1.Condition back to a proto Condition.
//
// Returns a new proto Condition value (not a pointer); callers store this
// directly in an ExternalRemediationRequestStatus.Conditions slice.
func FromMetav1(c metav1.Condition) *protos.Condition {
	var lastTransition *timestamppb.Timestamp
	if !c.LastTransitionTime.IsZero() {
		lastTransition = timestamppb.New(c.LastTransitionTime.Time)
	}

	return &protos.Condition{
		Type:               c.Type,
		Status:             string(c.Status),
		ObservedGeneration: c.ObservedGeneration,
		LastTransitionTime: lastTransition,
		Reason:             c.Reason,
		Message:            c.Message,
	}
}

// ToMetav1Slice converts a slice of proto Conditions to []metav1.Condition,
// suitable for passing into meta.SetStatusCondition and the other
// controller-runtime helpers.
func ToMetav1Slice(in []*protos.Condition) []metav1.Condition {
	if in == nil {
		return nil
	}

	out := make([]metav1.Condition, 0, len(in))
	for _, c := range in {
		if c == nil {
			continue
		}

		out = append(out, ToMetav1(c))
	}

	return out
}

// FromMetav1Slice converts a slice of metav1.Conditions back to proto
// Conditions, suitable for writing back into an
// ExternalRemediationRequestStatus.Conditions field.
func FromMetav1Slice(in []metav1.Condition) []*protos.Condition {
	if in == nil {
		return nil
	}

	out := make([]*protos.Condition, 0, len(in))
	for i := range in {
		out = append(out, FromMetav1(in[i]))
	}

	return out
}
