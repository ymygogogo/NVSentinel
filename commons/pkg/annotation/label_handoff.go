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

package annotation

import (
	"encoding/json"
	"fmt"
	"maps"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
)

const (
	// RuleLabelOwnershipKey records the live quarantine label owner on a Node.
	RuleLabelOwnershipKey = "nvsentinel.nvidia.com/quarantine-label-ownership"
	// RuleLabelHandoffKey records a snapshot on a ValidationRequest.
	RuleLabelHandoffKey = "nvsentinel.nvidia.com/quarantine-label-handoff"
	// RuleLabelBindingsKey retains handed-off labels until the validation session ends.
	RuleLabelBindingsKey = "nvsentinel.nvidia.com/quarantine-label-bindings"
)

// RuleLabelOwner distinguishes a new quarantine from an older same-value label.
type RuleLabelOwner struct {
	Value       string `json:"value"`
	Token       string `json:"token"`
	PreExisting *bool  `json:"preExisting"`
}

// RuleLabelSnapshot is an immutable per-node request snapshot.
type RuleLabelSnapshot struct {
	Version int                       `json:"version"`
	NodeUID string                    `json:"nodeUID"`
	Labels  map[string]RuleLabelOwner `json:"labels"`
}

// RuleLabelBinding binds a snapshot to a particular request incarnation.
type RuleLabelBinding struct {
	RequestName string            `json:"requestName"`
	Snapshot    RuleLabelSnapshot `json:"snapshot"`
}

type ruleLabelBindings struct {
	Version  int                         `json:"version"`
	Requests map[string]RuleLabelBinding `json:"requests"`
}

func readRuleLabels(node *corev1.Node) (RuleLabelSnapshot, error) {
	state := RuleLabelSnapshot{Version: 1, NodeUID: string(node.UID), Labels: map[string]RuleLabelOwner{}}
	if raw := node.Annotations[RuleLabelOwnershipKey]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			return state, fmt.Errorf("decode rule label ownership: %w", err)
		}

		if state.Version != 1 || state.NodeUID != string(node.UID) || state.Labels == nil {
			return state, fmt.Errorf("invalid rule label ownership version or node identity")
		}
	}

	return state, nil
}

func writeLabelAnnotation(node *corev1.Node, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode %s: %w", key, err)
	}

	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}

	node.Annotations[key] = string(data)

	return nil
}

// TrackRuleLabels runs before label application, inside the live Node mutation.
func TrackRuleLabels(node *corev1.Node, labels map[string]string, newSession bool) error {
	if len(labels) == 0 {
		return nil
	}

	state, err := readRuleLabels(node)
	if err != nil {
		return err
	}

	for key, value := range labels {
		owner, known := state.Labels[key]
		if newSession || !known {
			_, preExisting := node.Labels[key]
			if known && ownsRuleLabel(owner, node.Labels[key]) {
				preExisting = false
			}

			owner = RuleLabelOwner{Token: uuid.NewString(), PreExisting: &preExisting}
		}

		owner.Value = value
		state.Labels[key] = owner
	}

	return writeLabelAnnotation(node, RuleLabelOwnershipKey, state)
}

func ownsRuleLabel(owner RuleLabelOwner, value string) bool {
	return owner.Token != "" && owner.PreExisting != nil && !*owner.PreExisting && owner.Value == value
}

func canReleaseRuleLabel(owner, expected RuleLabelOwner, actual string) bool {
	return ownsRuleLabel(owner, actual) && ownsRuleLabel(expected, actual) && owner.Token == expected.Token
}

// ForgetRuleLabels invalidates ownership when fault-quarantine removes labels itself.
func ForgetRuleLabels(node *corev1.Node, keys []string) error {
	if node.Annotations[RuleLabelOwnershipKey] == "" || len(keys) == 0 {
		return nil
	}

	state, err := readRuleLabels(node)
	if err != nil {
		return err
	}

	for _, key := range keys {
		delete(state.Labels, key)
	}

	if len(state.Labels) == 0 {
		delete(node.Annotations, RuleLabelOwnershipKey)
		return nil
	}

	return writeLabelAnnotation(node, RuleLabelOwnershipKey, state)
}

// SnapshotRuleLabels selects only labels with proven ownership and the current value.
func SnapshotRuleLabels(node *corev1.Node) (string, error) {
	state, err := readRuleLabels(node)
	if err != nil {
		return "", err
	}

	state.Labels = maps.Clone(state.Labels)
	for key, owner := range state.Labels {
		if owner.PreExisting == nil || *owner.PreExisting || owner.Token == "" || node.Labels[key] != owner.Value {
			delete(state.Labels, key)
		}
	}

	if len(state.Labels) == 0 {
		return "", nil
	}

	data, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("encode rule label snapshot: %w", err)
	}

	return string(data), nil
}

func readLabelBindings(node *corev1.Node) (ruleLabelBindings, error) {
	bindings := ruleLabelBindings{Version: 1, Requests: map[string]RuleLabelBinding{}}
	if raw := node.Annotations[RuleLabelBindingsKey]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &bindings); err != nil {
			return bindings, fmt.Errorf("decode rule label bindings: %w", err)
		}

		if bindings.Version != 1 || bindings.Requests == nil {
			return bindings, fmt.Errorf("invalid rule label bindings version")
		}
	}

	return bindings, nil
}

// BindRuleLabels completes the handoff after the request exists in the API server.
func BindRuleLabels(node *corev1.Node, name, uid, raw string) error {
	var snapshot RuleLabelSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return fmt.Errorf("decode rule label handoff: %w", err)
	}

	if snapshot.Version != 1 || snapshot.NodeUID != string(node.UID) || uid == "" {
		return fmt.Errorf("invalid rule label handoff identity")
	}

	bindings, err := readLabelBindings(node)
	if err != nil {
		return err
	}

	for requestUID, binding := range bindings.Requests {
		if binding.RequestName == name && requestUID != uid {
			delete(bindings.Requests, requestUID)
		}
	}

	bindings.Requests[uid] = RuleLabelBinding{RequestName: name, Snapshot: snapshot}

	return writeLabelAnnotation(node, RuleLabelBindingsKey, bindings)
}

// CheckRuleLabelBinding blocks a fast request from releasing before handoff completes.
func CheckRuleLabelBinding(node *corev1.Node, name, uid, raw string) error {
	if raw == "" {
		return nil
	}

	bindings, err := readLabelBindings(node)
	if err != nil {
		return err
	}

	binding, exists := bindings.Requests[uid]
	if !exists || binding.RequestName != name || binding.Snapshot.NodeUID != string(node.UID) {
		return fmt.Errorf("label handoff for request %s is not bound to this node and request UID", name)
	}

	data, err := json.Marshal(binding.Snapshot)
	if err != nil {
		return fmt.Errorf("encode bound rule label snapshot: %w", err)
	}

	if string(data) != raw {
		return fmt.Errorf("label handoff snapshot differs from request %s", name)
	}

	return nil
}

// ReleaseRuleLabels runs only when the validation session can release its scheduling gates.
func ReleaseRuleLabels(node *corev1.Node, configured map[string]string) (bool, error) {
	bindings, err := readLabelBindings(node)
	if err != nil {
		return false, err
	}

	if len(bindings.Requests) == 0 {
		return false, nil
	}

	state, err := readRuleLabels(node)
	if err != nil {
		return false, err
	}

	for _, binding := range bindings.Requests {
		releaseBoundRuleLabels(node, state, binding.Snapshot, configured)
	}

	if len(state.Labels) == 0 {
		delete(node.Annotations, RuleLabelOwnershipKey)
	} else {
		if err := writeLabelAnnotation(node, RuleLabelOwnershipKey, state); err != nil {
			return false, err
		}
	}

	delete(node.Annotations, RuleLabelBindingsKey)

	return true, nil
}

func releaseBoundRuleLabels(node *corev1.Node, state, snapshot RuleLabelSnapshot, configured map[string]string) {
	if snapshot.NodeUID != string(node.UID) {
		return
	}

	for key, expected := range snapshot.Labels {
		owner := state.Labels[key]

		value, allowed := configured[key]
		if allowed && value == expected.Value && canReleaseRuleLabel(owner, expected, node.Labels[key]) {
			delete(node.Labels, key)
			delete(state.Labels, key)
		}
	}
}
