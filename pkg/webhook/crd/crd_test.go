// Copyright 2026 Google LLC
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

package crd

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestMutate(t *testing.T) {
	tests := []struct {
		name     string
		obj      map[string]interface{}
		expected map[string]interface{}
		mutated  bool
	}{
		{
			name: "escape namespace keyword",
			obj: map[string]interface{}{
				"apiVersion": "apiextensions.k8s.io/v1",
				"kind":       "CustomResourceDefinition",
				"metadata": map[string]interface{}{
					"name": "test-crd",
				},
				"spec": map[string]interface{}{
					"versions": []interface{}{
						map[string]interface{}{
							"name": "v1",
							"schema": map[string]interface{}{
								"openAPIV3Schema": map[string]interface{}{
									"x-kubernetes-validations": []interface{}{
										map[string]interface{}{
											"rule": "self.namespace != 'default'",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: map[string]interface{}{
				"apiVersion": "apiextensions.k8s.io/v1",
				"kind":       "CustomResourceDefinition",
				"metadata": map[string]interface{}{
					"name": "test-crd",
				},
				"spec": map[string]interface{}{
					"versions": []interface{}{
						map[string]interface{}{
							"name": "v1",
							"schema": map[string]interface{}{
								"openAPIV3Schema": map[string]interface{}{
									"x-kubernetes-validations": []interface{}{
										map[string]interface{}{
											"rule": "self.__namespace__ != 'default'",
										},
									},
								},
							},
						},
					},
				},
			},
			mutated: true,
		},
		{
			name: "do not escape standalone true",
			obj: map[string]interface{}{
				"apiVersion": "apiextensions.k8s.io/v1",
				"kind":       "CustomResourceDefinition",
				"spec": map[string]interface{}{
					"versions": []interface{}{
						map[string]interface{}{
							"schema": map[string]interface{}{
								"openAPIV3Schema": map[string]interface{}{
									"x-kubernetes-validations": []interface{}{
										map[string]interface{}{
											"rule": "self.enabled == true",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: map[string]interface{}{
				"apiVersion": "apiextensions.k8s.io/v1",
				"kind":       "CustomResourceDefinition",
				"spec": map[string]interface{}{
					"versions": []interface{}{
						map[string]interface{}{
							"schema": map[string]interface{}{
								"openAPIV3Schema": map[string]interface{}{
									"x-kubernetes-validations": []interface{}{
										map[string]interface{}{
											"rule": "self.enabled == true",
										},
									},
								},
							},
						},
					},
				},
			},
			mutated: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			crd := &apiextensionsv1.CustomResourceDefinition{}
			jsonData, err := json.Marshal(tc.obj)
			if err != nil {
				t.Fatalf("failed to marshal test object: %v", err)
			}
			if err := json.Unmarshal(jsonData, crd); err != nil {
				t.Fatalf("failed to unmarshal test object to CRD: %v", err)
			}

			expectedCRD := &apiextensionsv1.CustomResourceDefinition{}
			expectedJSON, err := json.Marshal(tc.expected)
			if err != nil {
				t.Fatalf("failed to marshal expected object: %v", err)
			}
			if err := json.Unmarshal(expectedJSON, expectedCRD); err != nil {
				t.Fatalf("failed to unmarshal expected object to CRD: %v", err)
			}

			m := NewCRDMutator()
			err = m.Mutate(context.TODO(), crd, nil)
			if err != nil {
				t.Fatalf("Mutate failed: %v", err)
			}

			if diff := cmp.Diff(expectedCRD, crd); diff != "" {
				t.Errorf("Unexpected mutation (-want +got):\n%s", diff)
			}
		})
	}
}
