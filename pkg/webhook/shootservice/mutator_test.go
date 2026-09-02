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

package shootservice

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMutate(t *testing.T) {
	tests := []struct {
		name               string
		service            *corev1.Service
		expectedErr        error
		expectedAnnotation string
	}{
		{
			name: "External LB with internal subnet annotation",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:           "external",
						internalLBSubnetAnnotationKey: "subnet",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf("external LB service %s/%s must not have an internal subnet annotation %q", "", "", internalLBSubnetAnnotationKey),
		},
		{
			name: "External LB with IP address in annotation",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:                "external",
						externalLBIPAddressesAnnotationKey: "192.168.1.1",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf("external LB service %s/%s: annotation %q value must be a subnet name, not an IP address: got %q", "", "", externalLBIPAddressesAnnotationKey, "192.168.1.1"),
		},
		{
			name: "Internal LB with external IP annotation",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:                "internal",
						externalLBIPAddressesAnnotationKey: "value",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf("internal LB service %s/%s: must not have an external IP annotation %q", "", "", externalLBIPAddressesAnnotationKey),
		},
		{
			name: "Internal LB with IP address in annotation",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:           "internal",
						internalLBSubnetAnnotationKey: "192.168.1.1",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf("internal LB service %s/%s: annotation %q value must be a subnet name, not an IP address: got %q", "", "", internalLBSubnetAnnotationKey, "192.168.1.1"),
		},
		{
			name: "Unsupported LBType",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey: "unsupported",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf("service %s/%s: unsupported LBType %q. Supported values are %q or %q", "", "", "unsupported", "internal", "external"),
		},
		{
			name: "External LB with load-balancer-allow-projects annotation throws error",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:                  "external",
						internalLBAllowProjectsAnnotationKey: "project-a",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf("external LB service %s/%s must not have a load-balancer-allow-projects annotation %q", "", "", internalLBAllowProjectsAnnotationKey),
		},
		{
			name: "Internal LB with empty load-balancer-allow-projects annotation throws error",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						internalLBAllowProjectsAnnotationKey: "",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf("service %s/%s: annotation %q value cannot be empty", "", "", internalLBAllowProjectsAnnotationKey),
		},
		{
			name: "Internal LB with empty project in load-balancer-allow-projects annotation throws error",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						internalLBAllowProjectsAnnotationKey: "a,,b",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf("service %s/%s: annotation %q contains empty project name", "", "", internalLBAllowProjectsAnnotationKey),
		},
		{
			name: "Internal LB with project starting with number in load-balancer-allow-projects annotation throws error",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:                  "internal",
						internalLBAllowProjectsAnnotationKey: "111project",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: fmt.Errorf(`service %s/%s: annotation %q contains invalid project name %q: [a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')]`, "", "", internalLBAllowProjectsAnnotationKey, "111project"),
		},
		{
			name: "Internal LB with valid load-balancer-allow-projects list is successful",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:                  "internal",
						internalLBSubnetAnnotationKey:        "subnet",
						internalLBAllowProjectsAnnotationKey: "project-a,project-b",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr:        nil,
			expectedAnnotation: "project-a,project-b",
		},
		{
			name: "Internal LB with valid wildcard load-balancer-allow-projects is successful",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:                  "internal",
						internalLBSubnetAnnotationKey:        "subnet",
						internalLBAllowProjectsAnnotationKey: "*",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr:        nil,
			expectedAnnotation: "*",
		},
		{
			name: "Internal LB with duplicate projects is deduplicated",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:                  "internal",
						internalLBSubnetAnnotationKey:        "subnet",
						internalLBAllowProjectsAnnotationKey: "project-a,project-b,project-a",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr:        nil,
			expectedAnnotation: "project-a,project-b",
		},
		{
			name: "Internal LB with projects and wildcard is simplified to '*'",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:                  "internal",
						internalLBSubnetAnnotationKey:        "subnet",
						internalLBAllowProjectsAnnotationKey: "project-a, *, project-b",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr:        nil,
			expectedAnnotation: "*",
		},
		{
			name: "Valid internal LB",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey:           "internal",
						internalLBSubnetAnnotationKey: "subnet-name",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: nil,
		},
		{
			name: "Valid external LB",
			service: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						lbTypeAnnotationKey: "external",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
				},
			},
			expectedErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutator := &mutator{logger: logr.Discard()}
			err := mutator.Mutate(context.Background(), tc.service, nil)
			if (err != nil && tc.expectedErr == nil) || (err == nil && tc.expectedErr != nil) || (err != nil && tc.expectedErr != nil && err.Error() != tc.expectedErr.Error()) {
				t.Errorf("unexpected error: got %v, want %v", err, tc.expectedErr)
			}
			if tc.expectedErr == nil && tc.expectedAnnotation != "" {
				if val := tc.service.Annotations[internalLBAllowProjectsAnnotationKey]; val != tc.expectedAnnotation {
					t.Errorf("unexpected annotation value: got %q, want %q", val, tc.expectedAnnotation)
				}
			}
		})
	}
}
