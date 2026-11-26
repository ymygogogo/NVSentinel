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
	"encoding/json"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	annotationutil "github.com/nvidia/nvsentinel/commons/pkg/annotation"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/config"
)

func (c *FaultQuarantineClient) trackAppliedRuleLabels(node *corev1.Node, newSession bool) error {
	if c.DryRunMode {
		return nil
	}

	raw := node.Annotations[common.QuarantineHealthEventAppliedLabelsAnnotationKey]
	if raw == "" {
		return nil
	}

	var applied []config.AppliedLabel
	if err := json.Unmarshal([]byte(raw), &applied); err != nil {
		return fmt.Errorf("decode applied rule labels: %w", err)
	}

	labels := make(map[string]string, len(applied))
	for _, label := range applied {
		labels[label.Key] = label.Value
	}

	return annotationutil.TrackRuleLabels(node, labels, newSession)
}

func (c *FaultQuarantineClient) snapshotValidationRuleLabels(ctx context.Context,
	obj *unstructured.Unstructured) (string, string, error) {
	var nodeName, nodeUID string

	for _, owner := range obj.GetOwnerReferences() {
		if owner.Kind == "Node" && owner.APIVersion == "v1" {
			nodeName, nodeUID = owner.Name, string(owner.UID)
		}
	}

	if nodeName == "" {
		return "", "", nil
	}

	node, err := c.Clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return "", "", fmt.Errorf("read node for validation label handoff: %w", err)
	}

	if string(node.UID) != nodeUID {
		return "", "", fmt.Errorf("node identity changed before validation label handoff")
	}

	raw, err := annotationutil.SnapshotRuleLabels(node)

	return nodeName, raw, err
}

func (c *FaultQuarantineClient) createValidationRequestWithLabels(ctx context.Context,
	gvr schema.GroupVersionResource, obj *unstructured.Unstructured) error {
	nodeName, raw, err := c.snapshotValidationRuleLabels(ctx, obj)
	if err != nil {
		return err
	}

	if raw != "" {
		annotations := obj.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}

		annotations[annotationutil.RuleLabelHandoffKey] = raw
		obj.SetAnnotations(annotations)
	}

	created, err := c.ensureValidationRequest(ctx, gvr, obj, raw)
	if err != nil {
		return err
	}

	if raw == "" {
		return nil
	}

	return c.UpdateNode(ctx, nodeName, func(node *corev1.Node) error {
		return annotationutil.BindRuleLabels(node, created.GetName(), string(created.GetUID()), raw)
	})
}

func (c *FaultQuarantineClient) ensureValidationRequest(ctx context.Context, gvr schema.GroupVersionResource,
	obj *unstructured.Unstructured, raw string) (*unstructured.Unstructured, error) {
	resource := c.DynamicClient.Resource(gvr)

	created, err := resource.Create(ctx, obj, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = resource.Get(ctx, obj.GetName(), metav1.GetOptions{})
		if err == nil && (!reflect.DeepEqual(created.Object["spec"], obj.Object["spec"]) ||
			!reflect.DeepEqual(created.GetOwnerReferences(), obj.GetOwnerReferences()) ||
			created.GetAnnotations()[annotationutil.RuleLabelHandoffKey] != raw) {
			return nil, fmt.Errorf("existing ValidationRequest %s belongs to a different handoff", obj.GetName())
		}
	}

	if err != nil {
		return nil, fmt.Errorf("create or get ValidationRequest %s: %w", obj.GetName(), err)
	}

	return created, nil
}
