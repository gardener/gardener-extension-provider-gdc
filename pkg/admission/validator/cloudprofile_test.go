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

package validator

import (
	"context"
	"strings"
	"testing"

	"github.com/gardener/gardener/pkg/apis/core"
	"github.com/gardener/gardener/pkg/apis/core/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	fakemanager "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

func TestCloudProfileValidator(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)

	tests := []struct {
		name   string
		newObj client.Object
	}{
		{
			name: "valid configuration",
			newObj: &core.CloudProfile{
				Spec: core.CloudProfileSpec{
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
								Kind:       "CloudProfileConfig",
							},
							MachineImages: []apisgdc.MachineImages{
								{
									Name: "machine-img-1",
									Versions: []apisgdc.MachineImageVersion{
										{
											Version:      "1.2.3",
											Image:        "path/to/gdch/image",
											Architecture: ptr.To("amd64"),
										},
									},
								},
							},
							OrgConfig: &apisgdc.OrgConfig{
								GlobalManagementAPI: "test-global-api",
								Zones: []*apisgdc.ZoneEndpoints{
									{
										Name:              "test-region-1-zone-1",
										ManagementAPI:     "test-management-api",
										InfrastructureAPI: "test-infra-api",
									},
								},
								RegistryURL: "test-registry-url",
								CAData:      "test-ca-data",
							},
						}),
					},
					MachineImages: []core.MachineImage{
						{
							Name: "machine-img-1",
							Versions: []core.MachineImageVersion{
								{
									ExpirableVersion: core.ExpirableVersion{Version: "1.2.3"},
								},
							},
						},
					},
					Regions: []core.Region{
						{
							Name: "test-region-1",
							Zones: []core.AvailabilityZone{
								{
									Name: "test-region-1-zone-1",
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
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			validator := NewCloudProfileValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.newObj, nil)
			if err != nil {
				t.Fatalf("validator.Validate() error = %v", err.Error())
			}
		})
	}
}

func TestCloudProfileValidatorError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)

	tests := []struct {
		name    string
		newObj  client.Object
		wantErr string
	}{
		{
			name:    "invalid object",
			newObj:  &core.Project{},
			wantErr: "wrong object type *core.Project",
		},
		{
			name: "missing providerConfig",
			newObj: &core.CloudProfile{
				Spec: core.CloudProfileSpec{
					ProviderConfig: nil,
					MachineImages: []core.MachineImage{
						{
							Name: "machine-img-1",
							Versions: []core.MachineImageVersion{
								{
									ExpirableVersion: core.ExpirableVersion{Version: "1.2.3"},
								},
							},
						},
					},
				},
			},
			wantErr: "spec.providerConfig: Required value: providerConfig must be set for GDCH cloud profiles",
		},
		{
			name: "invalid providerConfig",
			newObj: &core.CloudProfile{
				Spec: core.CloudProfileSpec{
					ProviderConfig: &runtime.RawExtension{
						Raw: []byte(`malformed-provider-config`),
					},
				},
			},
			wantErr: "json parse error: json: cannot unmarshal",
		},
		{
			name: "unsupported machine image configuration",
			newObj: &core.CloudProfile{
				Spec: core.CloudProfileSpec{
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
								Kind:       "CloudProfileConfig",
							},
							MachineImages: []apisgdc.MachineImages{
								{
									Name: "machine-img-1",
									Versions: []apisgdc.MachineImageVersion{
										{
											Version:      "1.2.3",
											Image:        "",
											Architecture: ptr.To("foo"),
										},
									},
								},
							},
							OrgConfig: &apisgdc.OrgConfig{
								GlobalManagementAPI: "test-global-api",
								Zones: []*apisgdc.ZoneEndpoints{
									{
										Name:              "test-region-1-zone-1",
										ManagementAPI:     "test-management-api",
										InfrastructureAPI: "test-infra-api",
									},
								},
								RegistryURL: "test-registry-url",
								CAData:      "test-ca-data",
							},
						}),
					},
					MachineImages: []core.MachineImage{
						{
							Name: "machine-img-1",
							Versions: []core.MachineImageVersion{
								{
									ExpirableVersion: core.ExpirableVersion{Version: "1.2.3"},
								},
								{
									ExpirableVersion: core.ExpirableVersion{Version: "2.0.0"},
								},
							},
						},
					},
					Regions: []core.Region{
						{
							Name: "test-region-1",
							Zones: []core.AvailabilityZone{
								{
									Name: "test-region-1-zone-1",
								},
							},
						},
					},
				},
			},
			wantErr: `[spec.providerConfig.machineImages[0].versions[0].image: Required value: must provide an image, spec.providerConfig.machineImages[0].versions[0].architecture: Unsupported value: "foo": supported values: "amd64", "arm64", spec.providerConfig.machineImages[0].versions: Required value: must provide an image mapping for version "2.0.0"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			validator := NewCloudProfileValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.newObj, nil)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validator.Validate() error = %v, wantErrMsg %v", err.Error(), tt.wantErr)
			}
		})
	}
}
