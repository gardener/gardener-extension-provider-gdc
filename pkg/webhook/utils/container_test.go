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

package utils

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
)

func TestEnsureSELinuxSPCT(t *testing.T) {
	var (
		runAsUser int64 = 1001
	)

	tests := []struct {
		name      string
		container corev1.Container
		want      corev1.Container
	}{
		{
			name: "should initialize security context and set selinux type when security context is nil",
			container: corev1.Container{
				Name: "fluent-bit",
			},
			want: corev1.Container{
				Name: "fluent-bit",
				SecurityContext: &corev1.SecurityContext{
					SELinuxOptions: &corev1.SELinuxOptions{
						Type: "spc_t",
					},
				},
			},
		},
		{
			name: "should initialize selinux options and set type when only security context exists",
			container: corev1.Container{
				Name: "fluent-bit",
				SecurityContext: &corev1.SecurityContext{
					RunAsUser: &runAsUser,
				},
			},
			want: corev1.Container{
				Name: "fluent-bit",
				SecurityContext: &corev1.SecurityContext{
					RunAsUser: &runAsUser,
					SELinuxOptions: &corev1.SELinuxOptions{
						Type: "spc_t",
					},
				},
			},
		},
		{
			name: "should overwrite existing selinux type",
			container: corev1.Container{
				Name: "fluent-bit",
				SecurityContext: &corev1.SecurityContext{
					SELinuxOptions: &corev1.SELinuxOptions{
						Type: "unconfined_t",
					},
				},
			},
			want: corev1.Container{
				Name: "fluent-bit",
				SecurityContext: &corev1.SecurityContext{
					SELinuxOptions: &corev1.SELinuxOptions{
						Type: "spc_t",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			EnsureSELinuxSPCT(&tt.container)

			if diff := cmp.Diff(tt.want, tt.container); diff != "" {
				t.Errorf("expected values %v, but got %v, differences %v", tt.want, tt.container, diff)
			}
		})
	}
}
