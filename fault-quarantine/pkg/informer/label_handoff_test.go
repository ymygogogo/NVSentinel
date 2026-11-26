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

package informer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	annotationutil "github.com/nvidia/nvsentinel/commons/pkg/annotation"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
)

// TestCreateValidationRequestResource_LabelHandoff_BindsRealRequestUID verifies API-backed handoff retries.
func TestCreateValidationRequestResource_LabelHandoff_BindsRealRequestUID(t *testing.T) {
	_, err := envtest.InstallCRDs(testConfig, envtest.CRDInstallOptions{Paths: []string{
		"../../../distros/kubernetes/nvsentinel/charts/lifecycle-manager/crds",
	}})
	require.NoError(t, err)
	ctx := context.Background()
	node, err := testClient.CoreV1().Nodes().Create(ctx, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "label-handoff-"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testClient.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{}))
	})
	require.NoError(t, annotationutil.TrackRuleLabels(node, map[string]string{"example.com/fault": "active"}, true))
	node.Labels = map[string]string{"example.com/fault": "active"}
	node, err = testClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	dynamicClient, err := dynamic.NewForConfig(testConfig)
	require.NoError(t, err)
	c := &FaultQuarantineClient{Clientset: testClient, DynamicClient: dynamicClient}
	gvr := schema.GroupVersionResource{Group: "nvsentinel.nvidia.com", Version: "v1alpha1", Resource: "validationrequests"}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "nvsentinel.nvidia.com/v1alpha1", "kind": "ValidationRequest",
		"metadata": map[string]any{"name": node.Name},
		"spec":     map[string]any{"nodes": []any{map[string]any{"name": node.Name}}},
	}}
	obj.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID}})
	t.Cleanup(func() {
		require.NoError(t, dynamicClient.Resource(gvr).Delete(ctx, obj.GetName(), metav1.DeleteOptions{}))
	})
	for range 2 {
		require.NoError(t, c.CreateValidationRequestResource(ctx, gvr, obj.DeepCopy()))
	}
	created, err := dynamicClient.Resource(gvr).Get(ctx, obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	node, err = testClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	raw := created.GetAnnotations()[annotationutil.RuleLabelHandoffKey]
	require.NotEmpty(t, raw)
	require.NoError(t, annotationutil.CheckRuleLabelBinding(node, created.GetName(), string(created.GetUID()), raw))
	require.Equal(t, "active", node.Labels["example.com/fault"])
	// A new same-value fault must not be bound to an old request incarnation.
	require.NoError(t, annotationutil.TrackRuleLabels(node, map[string]string{"example.com/fault": "active"}, true))
	_, err = testClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.ErrorContains(t, c.CreateValidationRequestResource(ctx, gvr, obj.DeepCopy()), "different handoff")
	before := node.Annotations[annotationutil.RuleLabelOwnershipKey]
	c.DryRunMode = true
	require.NoError(t, c.CreateValidationRequestResource(ctx, gvr, obj.DeepCopy()))
	node, err = testClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, before, node.Annotations[annotationutil.RuleLabelOwnershipKey])
	// A create failure leaves both the label and its ownership unchanged.
	c.DryRunMode = false
	invalid := obj.DeepCopy()
	invalid.SetName("INVALID_NAME")
	require.Error(t, c.CreateValidationRequestResource(ctx, gvr, invalid))
	node, err = testClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, before, node.Annotations[annotationutil.RuleLabelOwnershipKey])
	require.Equal(t, "active", node.Labels["example.com/fault"])
}

// TestTrackAppliedRuleLabels_DryRunAndManualCleanup_PreservesOperatorLabels checks ownership boundaries.
func TestTrackAppliedRuleLabels_DryRunAndManualCleanup_PreservesOperatorLabels(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{UID: "node-uid", Labels: map[string]string{}, Annotations: map[string]string{
		common.QuarantineHealthEventAppliedLabelsAnnotationKey: `[{"key":"example.com/fault","value":"active"}]`,
	}}}
	c := &FaultQuarantineClient{DryRunMode: true}
	require.NoError(t, c.trackAppliedRuleLabels(node, true))
	require.NotContains(t, node.Annotations, annotationutil.RuleLabelOwnershipKey)
	c.DryRunMode = false
	require.NoError(t, c.trackAppliedRuleLabels(node, true))
	node.Labels["example.com/fault"] = "active"
	require.Contains(t, node.Annotations, annotationutil.RuleLabelOwnershipKey)
	c.updateNodeAnnotationsForManualUncordon(node, nil, nil)
	require.NotContains(t, node.Annotations, annotationutil.RuleLabelOwnershipKey)
	require.Equal(t, "active", node.Labels["example.com/fault"])
}
