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
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReleaseRuleLabels_OwnershipBoundaries_PreservesUnownedLabels(t *testing.T) {
	tests := []struct {
		name    string
		before  bool
		mutate  func(*testing.T, *corev1.Node)
		allowed map[string]string
		remains bool
	}{
		{name: "owned label", allowed: map[string]string{"example.com/fault": "active"}},
		{name: "not configured", remains: true},
		{name: "different configured value", allowed: map[string]string{"example.com/fault": "other"}, remains: true},
		{name: "external value", mutate: func(_ *testing.T, n *corev1.Node) { n.Labels["example.com/fault"] = "external" }, allowed: map[string]string{"example.com/fault": "active"}, remains: true},
		{name: "same-value newer session", mutate: func(t *testing.T, n *corev1.Node) {
			require.NoError(t, TrackRuleLabels(n, map[string]string{"example.com/fault": "active"}, true))
		}, allowed: map[string]string{"example.com/fault": "active"}, remains: true},
		{name: "pre-existing label", before: true, allowed: map[string]string{"example.com/fault": "active"}, remains: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Labels: map[string]string{}}}
			if tt.before {
				node.Labels["example.com/fault"] = "active"
			}
			require.NoError(t, TrackRuleLabels(node, map[string]string{"example.com/fault": "active"}, true))
			node.Labels["example.com/fault"] = "active"
			raw, err := SnapshotRuleLabels(node)
			require.NoError(t, err)
			if tt.before {
				require.Empty(t, raw)
			} else {
				require.NoError(t, BindRuleLabels(node, "vr", "vr-uid", raw))
				require.NoError(t, CheckRuleLabelBinding(node, "vr", "vr-uid", raw))
			}
			if tt.mutate != nil {
				tt.mutate(t, node)
			}
			_, err = ReleaseRuleLabels(node, tt.allowed)
			require.NoError(t, err)
			_, exists := node.Labels["example.com/fault"]
			require.Equal(t, tt.remains, exists)
		})
	}
}

func TestRuleLabelHandoff_RequestAndNodeIdentity_RejectsStaleRequests(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Labels: map[string]string{}}}
	require.NoError(t, TrackRuleLabels(node, map[string]string{"fault": "active"}, true))
	node.Labels["fault"] = "active"
	raw, err := SnapshotRuleLabels(node)
	require.NoError(t, err)
	require.Error(t, CheckRuleLabelBinding(node, "vr", "vr-uid", raw))
	require.NoError(t, BindRuleLabels(node, "vr", "vr-uid", raw))
	require.NoError(t, CheckRuleLabelBinding(node, "vr", "vr-uid", raw))
	require.Error(t, CheckRuleLabelBinding(node, "vr", "new-request-uid", raw))
	require.Error(t, CheckRuleLabelBinding(node, "another-request", "vr-uid", raw))
	require.NoError(t, BindRuleLabels(node, "vr", "new-request-uid", raw))
	require.Error(t, CheckRuleLabelBinding(node, "vr", "vr-uid", raw))
	require.NoError(t, CheckRuleLabelBinding(node, "vr", "new-request-uid", raw))
	node.UID = "new-node"
	require.Error(t, CheckRuleLabelBinding(node, "vr", "vr-uid", raw))
	require.Error(t, BindRuleLabels(node, "vr", "vr-uid", raw))
}

func TestTrackRuleLabels_RepeatedSession_PreservesToken(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Labels: map[string]string{}}}
	require.NoError(t, TrackRuleLabels(node, map[string]string{"fault": "active"}, true))
	node.Labels["fault"] = "active"
	first, err := SnapshotRuleLabels(node)
	require.NoError(t, err)
	require.NoError(t, TrackRuleLabels(node, map[string]string{"fault": "active"}, false))
	second, err := SnapshotRuleLabels(node)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestRuleLabelOwnership_InvalidAndUnknownRecords_FailsClosed(t *testing.T) {
	for _, raw := range []string{"{", `{"version":2,"nodeUID":"node-uid","labels":{}}`} {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Annotations: map[string]string{RuleLabelOwnershipKey: raw}}}
		_, err := SnapshotRuleLabels(node)
		require.Error(t, err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		UID: "node-uid", Labels: map[string]string{"fault": "active"},
		Annotations: map[string]string{RuleLabelOwnershipKey: `{"version":1,"nodeUID":"node-uid","labels":{"fault":{"value":"active","token":"unknown"}}}`},
	}}
	raw, err := SnapshotRuleLabels(node)
	require.NoError(t, err)
	require.Empty(t, raw)
}

// TestForgetRuleLabels_NormalAndManualCleanup_InvalidatesOldAuthority checks operator ownership transfer.
func TestForgetRuleLabels_NormalAndManualCleanup_InvalidatesOldAuthority(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Labels: map[string]string{}}}
	require.NoError(t, TrackRuleLabels(node, map[string]string{"fault": "active"}, true))
	node.Labels["fault"] = "active"
	raw, err := SnapshotRuleLabels(node)
	require.NoError(t, err)
	require.NoError(t, BindRuleLabels(node, "vr", "uid", raw))
	require.NoError(t, ForgetRuleLabels(node, []string{"fault"}))
	_, err = ReleaseRuleLabels(node, map[string]string{"fault": "active"})
	require.NoError(t, err)
	require.Equal(t, "active", node.Labels["fault"])
	require.NoError(t, TrackRuleLabels(node, map[string]string{"fault": "active"}, true))
	raw, err = SnapshotRuleLabels(node)
	require.NoError(t, err)
	require.Empty(t, raw, "a preserved operator label must not regain FQ ownership")
}

// TestRuleLabelBindings_InvalidRecords_PreventsRelease checks unsupported persisted formats.
func TestRuleLabelBindings_InvalidRecords_PreventsRelease(t *testing.T) {
	for _, raw := range []string{"{", `{"version":2,"requests":{}}`, `{"version":1,"requests":null}`} {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Annotations: map[string]string{RuleLabelBindingsKey: raw}}}
		_, err := ReleaseRuleLabels(node, nil)
		require.Error(t, err)
		require.Error(t, CheckRuleLabelBinding(node, "vr", "uid", "snapshot"))
		require.Equal(t, raw, node.Annotations[RuleLabelBindingsKey])
	}
}
