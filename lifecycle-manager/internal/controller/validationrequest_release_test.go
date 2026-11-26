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

package controller

import (
	"testing"

	annotationutil "github.com/nvidia/nvsentinel/commons/pkg/annotation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nvidia/nvsentinel/lifecycle-manager/api/v1alpha1"
)

// TestReleaseNodeFromSession_LabelHandoff_ReleasesOnlyAtSessionEnd checks atomic gate mutation and retries.
func TestReleaseNodeFromSession_LabelHandoff_ReleasesOnlyAtSessionEnd(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "failed"}[failed], func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Labels: map[string]string{}},
				Spec: corev1.NodeSpec{Unschedulable: true, Taints: []corev1.Taint{{Key: "fault", Value: "active", Effect: corev1.TaintEffectNoSchedule}}}}
			require.NoError(t, annotationutil.TrackRuleLabels(node, map[string]string{"fault": "active"}, true))
			node.Labels["fault"] = "active"
			raw, err := annotationutil.SnapshotRuleLabels(node)
			require.NoError(t, err)
			require.NoError(t, annotationutil.BindRuleLabels(node, "first", "first-uid", raw))
			session, err := annotationutil.MarshalValidationSession([]annotationutil.ValidationSessionEntry{
				{Name: "first"}, {Name: "second", Failed: failed},
			})
			require.NoError(t, err)
			node.Annotations[annotationutil.ValidationSessionKey] = session
			gate := &v1alpha1.SchedulingGateConfig{
				Labels: []v1alpha1.LabelConfig{{Key: "fault", Value: "active", Remove: true}},
				Taints: []v1alpha1.TaintConfig{{Key: "fault", Value: "active", Effect: corev1.TaintEffectNoSchedule, Remove: true}},
			}
			gate.Cordon.Remove = true
			request := &v1alpha1.ValidationRequest{ObjectMeta: metav1.ObjectMeta{Name: "first", UID: "first-uid", Annotations: map[string]string{annotationutil.RuleLabelHandoffKey: raw}}}
			require.NoError(t, checkRuleLabelHandoff(node, request))
			_, err = releaseNodeFromSession(node, "first", nil, gate)
			require.NoError(t, err)
			require.Equal(t, "active", node.Labels["fault"])
			require.True(t, node.Spec.Unschedulable)
			require.Len(t, node.Spec.Taints, 1)
			// The failed entry is released only through the existing explicit deletion/supersession policy.
			_, err = releaseNodeFromSession(node, "second", nil, gate)
			require.NoError(t, err)
			require.NotContains(t, node.Labels, "fault")
			require.False(t, node.Spec.Unschedulable)
			require.Empty(t, node.Spec.Taints)
			require.NoError(t, checkRuleLabelHandoff(node, request))
			changed, err := releaseNodeFromSession(node, "first", nil, gate)
			require.NoError(t, err)
			require.False(t, changed)
		})
	}
}

// TestCheckRuleLabelHandoff_UnboundRequest_BlocksActiveSession checks interrupted handoff recovery.
func TestCheckRuleLabelHandoff_UnboundRequest_BlocksActiveSession(t *testing.T) {
	session, err := annotationutil.MarshalValidationSession([]annotationutil.ValidationSessionEntry{{Name: "request"}})
	require.NoError(t, err)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{annotationutil.ValidationSessionKey: session}}}
	request := &v1alpha1.ValidationRequest{ObjectMeta: metav1.ObjectMeta{Name: "request", UID: "request-uid", Annotations: map[string]string{annotationutil.RuleLabelHandoffKey: "snapshot"}}}
	require.Error(t, checkRuleLabelHandoff(node, request))
}

func TestReadinessViolations_Message_NamesEveryViolationInOrder(t *testing.T) {
	tests := []struct {
		name       string
		violations []readinessViolation
		want       string
	}{
		{
			name:       "single node",
			violations: []readinessViolation{{node: "node-a", criterion: "not-under-quarantine"}},
			want:       `node "node-a" failed readiness criterion "not-under-quarantine"`,
		},
		{
			name: "several nodes keep request order",
			violations: []readinessViolation{
				{node: "node-b", criterion: "gpu-allocatable"},
				{node: "node-a", criterion: "not-under-quarantine"},
			},
			want: `node "node-b" failed readiness criterion "gpu-allocatable"; ` +
				`node "node-a" failed readiness criterion "not-under-quarantine"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, readinessViolations(tt.violations).message())
		})
	}
}

func TestSetReleaseBlockedCondition_RecordsReasonAndMessage(t *testing.T) {
	vr := &v1alpha1.ValidationRequest{ObjectMeta: metav1.ObjectMeta{Name: "vr", Generation: 3}}
	setReleaseBlockedCondition(vr, readinessViolations{{node: "node-a", criterion: "node-ready"}})

	cond := meta.FindStatusCondition(vr.Status.Conditions, conditionTypeReleaseBlocked)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Equal(t, string(v1alpha1.FailureReasonNodeReadinessViolation), cond.Reason)
	require.Equal(t, `node "node-a" failed readiness criterion "node-ready"`, cond.Message)
	require.Equal(t, int64(3), cond.ObservedGeneration)
	require.False(t, cond.LastTransitionTime.IsZero())

	// A second evaluation replaces the message instead of adding a condition.
	setReleaseBlockedCondition(vr, readinessViolations{{node: "node-a", criterion: "not-under-quarantine"}})
	require.Len(t, vr.Status.Conditions, 1)
	require.Equal(t, `node "node-a" failed readiness criterion "not-under-quarantine"`, vr.Status.Conditions[0].Message)
}
