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

package daemonset

import (
	"context"
	"testing"

	gardenimagevector "github.com/gardener/gardener/imagevector"

	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const namespace = "test"

func TestEnsureKubeAPIServerdaemonset(t *testing.T) {
	c := fake.NewClientBuilder().WithObjects().Build()
	mutator := mutator{
		client: c,
		logger: logger,
	}
	ctx := context.Background()
	tests := []struct {
		name      string
		daemonset appsv1.DaemonSet
		want      appsv1.DaemonSet
	}{
		{
			name: "ensure fluent-bit daemonset success",
			daemonset: appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      v1beta1constants.DaemonSetNameFluentBit,
				},
				Spec: appsv1.DaemonSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: gardenimagevector.ContainerImageNameFluentBit,
								},
							},
						},
					},
				},
			},

			want: appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      v1beta1constants.DaemonSetNameFluentBit,
				},
				Spec: appsv1.DaemonSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: gardenimagevector.ContainerImageNameFluentBit,
									SecurityContext: &corev1.SecurityContext{
										SELinuxOptions: &corev1.SELinuxOptions{
											Type: "spc_t",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mutator.Mutate(ctx, &tt.daemonset, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.daemonset, tt.want); diff != "" {
				t.Errorf("expected values %v, but got %v, differences %v", tt.want, tt.daemonset, diff)
			}
		})
	}
}
