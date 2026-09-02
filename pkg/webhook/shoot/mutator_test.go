// Copyright 2025 Google LLC
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

package shoot

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMutateNetworkUnavailableNodeCondition(t *testing.T) {
	now := metav1.NewTime(time.Now())

	tests := []struct {
		name       string
		newNode    *corev1.Node
		oldNode    *corev1.Node
		wantStatus corev1.ConditionStatus
	}{
		{
			name:       "no data",
			newNode:    &corev1.Node{},
			oldNode:    &corev1.Node{},
			wantStatus: corev1.ConditionFalse,
		},
		{
			name: "partial data, only new",
			newNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionFalse},
					},
				},
			},
			oldNode:    &corev1.Node{},
			wantStatus: corev1.ConditionFalse,
		},
		{
			name:    "partial data, only old",
			newNode: &corev1.Node{},
			oldNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionFalse},
					},
				},
			},
			wantStatus: corev1.ConditionFalse,
		},
		{
			name: "full data with condition set to false",
			newNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionFalse},
					},
				},
			},
			oldNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionFalse},
					},
				},
			},
			wantStatus: corev1.ConditionFalse,
		},
		{
			name: "full data, updating condition set to true",
			newNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionTrue, Reason: noRouteCreated, Message: nodeCreatedWithoutRoute},
					},
				},
			},
			oldNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionFalse},
					},
				},
			},
			wantStatus: corev1.ConditionFalse,
		},
		{
			name: "full data, updating condition set to true without previous value",
			newNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionTrue, Reason: noRouteCreated, Message: nodeCreatedWithoutRoute},
					},
				},
			},
			oldNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{},
				},
			},
			wantStatus: corev1.ConditionFalse,
		},
		{
			name: "new node is deleted",
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node1",
					Namespace:         "default",
					DeletionTimestamp: &now,
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeNetworkUnavailable, Status: corev1.ConditionTrue, Reason: noRouteCreated, Message: nodeCreatedWithoutRoute},
					},
				},
			},
			oldNode: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{},
				},
			},
			wantStatus: corev1.ConditionTrue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutator := NewMutator()
			err := mutator.Mutate(context.TODO(), tt.newNode, tt.oldNode)

			if err != nil {
				t.Errorf("expected no error, but got %v", err)
			}

			if tt.newNode != nil {
				for _, c := range tt.newNode.Status.Conditions {
					if c.Type == corev1.NodeNetworkUnavailable {
						if diff := cmp.Diff(tt.wantStatus, c.Status); diff != "" {
							t.Errorf("unexpected condition status (-want +got):\n%s", diff)
						}
					}
				}
			}
		})
	}
}
