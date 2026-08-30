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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gardener/gardener/pkg/apis/core"
	"github.com/gardener/gardener/pkg/apis/core/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	fakemanager "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

var (
	parentCloudProfile = &v1beta1.CloudProfile{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "core.gardener.cloud/v1beta1",
			Kind:       "CloudProfile",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cloud-profile",
		},
		Spec: v1beta1.CloudProfileSpec{
			ProviderConfig: &runtime.RawExtension{
				Raw: encode(&apisgdc.CloudProfileConfig{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
						Kind:       "CloudProfileConfig",
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
					MachineImages: []apisgdc.MachineImages{
						{
							Name:    "machine-img-1",
							Project: "test-project",
							Versions: []apisgdc.MachineImageVersion{
								{
									Version:      "1.2.3",
									Image:        "path/to/gdch/image",
									Architecture: ptr.To("amd64"),
								},
							},
						},
					},
				}),
			},
			Kubernetes: v1beta1.KubernetesSettings{
				Versions: []v1beta1.ExpirableVersion{
					{
						Version:        "1.23.4",
						Classification: ptr.To(v1beta1.ClassificationSupported),
					},
				},
			},
			MachineImages: []v1beta1.MachineImage{
				{
					Name: "machine-img-1",
					Versions: []v1beta1.MachineImageVersion{
						{
							Architectures: []string{
								"amd64",
							},
							ExpirableVersion: v1beta1.ExpirableVersion{
								Version: "1.2.3",
							},
						},
					},
				},
			},
			Regions: []v1beta1.Region{
				{
					Name: "test-region-1",
					Zones: []v1beta1.AvailabilityZone{
						{
							Name: "test-region-1-zone-1",
						},
					},
				},
			},
		},
	}
	validNCP = &core.NamespacedCloudProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-ncp",
			Namespace: "test-project",
		},
		Spec: core.NamespacedCloudProfileSpec{
			Parent: core.CloudProfileReference{
				Kind: "CloudProfile",
				Name: "test-cloud-profile",
			},
			ProviderConfig: &runtime.RawExtension{
				Raw: encode(&apisgdc.CloudProfileConfig{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
						Kind:       "CloudProfileConfig",
					},
					MachineImages: []apisgdc.MachineImages{
						{
							Name: "machine-img-2",
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
					Name: "machine-img-2",
					Versions: []core.MachineImageVersion{
						{
							ExpirableVersion: core.ExpirableVersion{Version: "1.2.3"},
						},
					},
				},
			},
			Kubernetes: &core.KubernetesSettings{
				Versions: []core.ExpirableVersion{
					{
						Version: "1.23.4",
						ExpirationDate: &metav1.Time{
							Time: metav1.Now().Time,
						},
					},
				},
			},
		},
	}
	shootWithValidNCP = &v1beta1.Shoot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shoot-ncp",
			Namespace: "test-project",
		},
		Spec: v1beta1.ShootSpec{
			CloudProfile: &v1beta1.CloudProfileReference{
				Kind: "NamespacedCloudProfile",
				Name: "my-ncp",
			},
			Region: "test-region-1",
			Networking: &v1beta1.Networking{
				Nodes: stringPtr("10.0.0.0/16"),
			},
			Kubernetes: v1beta1.Kubernetes{
				Version: "1.23.4",
			},
			Provider: v1beta1.Provider{
				Type: "gdch",
				InfrastructureConfig: &runtime.RawExtension{
					Raw: encode(&v1alpha1.InfrastructureConfig{
						TypeMeta: metav1.TypeMeta{
							Kind:       "InfrastructureConfig",
							APIVersion: v1alpha1.SchemeGroupVersion.String(),
						},
						Networks: v1alpha1.NetworkConfig{
							NodeCIDR:     "10.0.0.8/29",
							ParentSubnet: "test-parent-subnet",
							Zones:        []v1alpha1.Zone{{Name: "test-region-1-zone-1", CIDR: "10.0.0.8/30"}},
						},
					}),
				},
				Workers: []v1beta1.Worker{
					{
						CRI: &v1beta1.CRI{
							Name: "containerd",
						},
						Machine: v1beta1.Machine{
							Architecture: ptr.To("amd64"),
							Image: &v1beta1.ShootMachineImage{
								// use image is defined in NCP
								Name:    "machine-img-2",
								Version: ptr.To("1.2.3"),
							},
						},
					},
				},
			},
		}}
	providerConfigNoMachineImages = &runtime.RawExtension{
		Raw: encode(&apisgdc.CloudProfileConfig{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
				Kind:       "CloudProfileConfig",
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
	}
)

func TestNameSpacedCloudProfileCreateSuccess(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)

	tests := []struct {
		name        string
		newObj      client.Object
		existingObj client.Object
	}{
		{
			name:        "valid configuration with new machine image",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "test-cloud-profile",
					},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
								Kind:       "CloudProfileConfig",
							},
							MachineImages: []apisgdc.MachineImages{
								{
									Name: "machine-img-2",
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
							Name: "machine-img-2",
							Versions: []core.MachineImageVersion{
								{
									ExpirableVersion: core.ExpirableVersion{Version: "1.2.3"},
								},
							},
						},
					},
					Kubernetes: &core.KubernetesSettings{
						Versions: []core.ExpirableVersion{
							{
								Version: "1.23.4",
								ExpirationDate: &metav1.Time{
									Time: metav1.Now().Time,
								},
							},
						},
					},
				},
			},
		},
		{
			name:        "valid configuration with extending kubernete version exp date",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "test-cloud-profile",
					},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
								Kind:       "CloudProfileConfig",
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
					Kubernetes: &core.KubernetesSettings{
						Versions: []core.ExpirableVersion{
							{
								Version: "1.23.4",
								ExpirationDate: &metav1.Time{
									Time: metav1.Now().Time,
								},
							},
						},
					},
				},
			},
		},
		{
			name:        "minimal valid configuration",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "test-cloud-profile",
					},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
								Kind:       "CloudProfileConfig",
							},
							// No new MachineImages in ProviderConfig
							OrgConfig: &apisgdc.OrgConfig{ // OrgConfig still required to match
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
				},
			},
		},
		{
			name:        "valid configuration with multiple new machine images",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "test-cloud-profile",
					},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
								Kind:       "CloudProfileConfig",
							},
							MachineImages: []apisgdc.MachineImages{
								{
									Name: "new-img-A",
									Versions: []apisgdc.MachineImageVersion{
										{Version: "1.0.0", Image: "path/to/A", Architecture: ptr.To("amd64")},
									},
								},
								{
									Name: "new-img-B",
									Versions: []apisgdc.MachineImageVersion{
										{Version: "2.0.0", Image: "path/to/B", Architecture: ptr.To("amd64")},
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
							Name:     "new-img-A",
							Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0.0"}}},
						},
						{
							Name:     "new-img-B",
							Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "2.0.0"}}},
						},
					},
				},
			},
		},
		{
			name:        "valid configuration with multiple new versions for one machine image",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "test-cloud-profile",
					},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
								Kind:       "CloudProfileConfig",
							},
							MachineImages: []apisgdc.MachineImages{
								{
									Name: "new-img-C",
									Versions: []apisgdc.MachineImageVersion{
										{Version: "1.0.0", Image: "path/to/C1", Architecture: ptr.To("amd64")},
										{Version: "1.0.1", Image: "path/to/C2", Architecture: ptr.To("amd64")},
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
							Name: "new-img-C",
							Versions: []core.MachineImageVersion{
								{ExpirableVersion: core.ExpirableVersion{Version: "1.0.0"}},
								{ExpirableVersion: core.ExpirableVersion{Version: "1.0.1"}},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tt.existingObj).Build()
			validator := NewNamespacedCloudProfileValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.newObj, nil)
			if err != nil {
				t.Fatalf("validator.Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestNameSpacedCloudProfileCreateFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)

	tests := []struct {
		name          string
		newObj        client.Object
		existingObj   client.Object
		expectedError string // Substring to check for
	}{
		{
			name:        "invalid parent kind",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "Secret", // Invalid Kind
						Name: "test-cloud-profile",
					},
				},
			},
			expectedError: "spec.parent.kind: Invalid value: \"Secret\": parent reference must be of kind CloudProfile",
		},
		{
			name:        "parent not found",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "non-existent-profile", // Not Found
					},
				},
			},
			expectedError: "cloudprofiles.core.gardener.cloud \"non-existent-profile\" not found",
		},
		{
			name:        "machine image missing in providerConfig",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "test-cloud-profile",
					},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
								Kind:       "CloudProfileConfig",
							},
							OrgConfig: &apisgdc.OrgConfig{
								GlobalManagementAPI: "test-global-api",
								Zones: []*apisgdc.ZoneEndpoints{
									{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"},
								},
								RegistryURL: "test-registry-url", CAData: "test-ca-data",
							},
						}),
					},
					MachineImages: []core.MachineImage{
						{
							Name:     "machine-img-new",
							Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0.0"}}},
						},
					},
				},
			},
			expectedError: "Required value: machine image machine-img-new is not defined in the NamespacedCloudProfile providerConfig and parent CloudProfile",
		},
		{
			name:        "machine image already in parent",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "test-cloud-profile",
					},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal", Kind: "CloudProfileConfig"},
							MachineImages: []apisgdc.MachineImages{
								{
									Name: "machine-img-1", // Matches parent
									Versions: []apisgdc.MachineImageVersion{
										{Version: "1.2.3", Image: "path/to/gdch/image", Architecture: ptr.To("amd64")},
									},
								},
							},
							OrgConfig: &apisgdc.OrgConfig{
								GlobalManagementAPI: "test-global-api",
								Zones: []*apisgdc.ZoneEndpoints{
									{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"},
								},
								RegistryURL: "test-registry-url", CAData: "test-ca-data",
							},
						}),
					},
					MachineImages: []core.MachineImage{
						{
							Name: "machine-img-1",
							Versions: []core.MachineImageVersion{
								{ExpirableVersion: core.ExpirableVersion{Version: "1.2.3"}}, // Already in parent
							},
						},
					},
				},
			},
			expectedError: "is already defined in the parent CloudProfile",
		},
		{
			name:        "invalid zone in providerConfig",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{Kind: "CloudProfile", Name: "test-cloud-profile"},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal", Kind: "CloudProfileConfig"},
							OrgConfig: &apisgdc.OrgConfig{
								GlobalManagementAPI: "test-global-api",
								Zones: []*apisgdc.ZoneEndpoints{
									{Name: "invalid-zone", ManagementAPI: "test-api", InfrastructureAPI: "test-infra"}, // Invalid Zone
								},
								RegistryURL: "test-registry-url", CAData: "test-ca-data",
							},
						}),
					},
				},
			},
			expectedError: "Invalid value: \"invalid-zone\": zone invalid-zone is not a supported zone",
		},
		{
			name:        "nil orgCOnfig in providerConfig",
			existingObj: parentCloudProfile,
			newObj: &core.NamespacedCloudProfile{
				Spec: core.NamespacedCloudProfileSpec{
					Parent: core.CloudProfileReference{
						Kind: "CloudProfile",
						Name: "test-cloud-profile",
					},
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal", Kind: "CloudProfileConfig"},
						}),
					},
				},
			},
			expectedError: "spec.providerConfig.orgConfig: Required value: must provide an orgConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tt.existingObj).Build()
			validator := NewNamespacedCloudProfileValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.newObj, nil)

			if err == nil {
				if tt.expectedError != "" { // We expected an error, but got none
					t.Fatalf("validator.Validate() error = nil, want error containing %q", tt.expectedError)
				}
				// If expectedError is "", it means we didn't expect an error, so nil is fine.
				return
			}

			// We got an error, check if it's the one we expected.
			if tt.expectedError != "" && !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("validator.Validate() error = %v, want error containing %q", err, tt.expectedError)
			}
		})
	}
}

func TestNameSpacedCloudProfileUpdateSuccess(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)
	ncpK8sVersionRemoved := validNCP.DeepCopy()
	ncpK8sVersionRemoved.Spec.Kubernetes.Versions = []core.ExpirableVersion{}
	ncpMachineImageRemoved := validNCP.DeepCopy()
	ncpMachineImageRemoved.Spec.MachineImages = []core.MachineImage{}
	ncpMachineImageRemoved.Spec.ProviderConfig = providerConfigNoMachineImages
	shootWithValidNCP2 := shootWithValidNCP.DeepCopy()
	shootWithValidNCP2.Spec.CloudProfile = &v1beta1.CloudProfileReference{Kind: "NamespacedCloudProfile", Name: "my-ncp2"}

	tests := []struct {
		name    string
		oldObj  *core.NamespacedCloudProfile
		newObj  *core.NamespacedCloudProfile
		shoots  client.Object
		wantErr bool
	}{
		{
			name:   "no versions removed",
			oldObj: validNCP,
			newObj: validNCP,
			shoots: shootWithValidNCP,
		},
		{
			name:   "k8s version removed, not in use",
			oldObj: validNCP,
			newObj: ncpK8sVersionRemoved,
			shoots: &v1beta1.Shoot{},
		},
		{
			name:   "machine image version removed, not in use",
			oldObj: validNCP,
			newObj: ncpMachineImageRemoved,
			shoots: &v1beta1.Shoot{},
		},
		{
			name:   "version removed, but used by shoot referencing different NCP",
			oldObj: validNCP,
			newObj: ncpK8sVersionRemoved,
			shoots: shootWithValidNCP2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithIndex(&v1beta1.Shoot{}, ".spec.cloudProfile.name", indexShootCloudProfileName).WithRuntimeObjects(tt.shoots, parentCloudProfile.DeepCopy()).Build()
			validator := NewNamespacedCloudProfileValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.newObj, tt.oldObj)
			if err != nil {
				t.Errorf("validator.Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestNameSpacedCloudProfileUpdateFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)
	validNCPK8sVersionRemoved := validNCP.DeepCopy()
	validNCPK8sVersionRemoved.Spec.Kubernetes.Versions = []core.ExpirableVersion{}
	validNCPMachineImageRemoved := validNCP.DeepCopy()
	validNCPMachineImageRemoved.Spec.MachineImages = []core.MachineImage{}
	validNCPMachineImageRemoved.Spec.ProviderConfig = providerConfigNoMachineImages

	tests := []struct {
		name          string
		oldObj        *core.NamespacedCloudProfile
		newObj        *core.NamespacedCloudProfile
		shoots        client.Object
		expectedError string
	}{
		{
			name:          "k8s version removed, in use",
			oldObj:        validNCP,
			newObj:        validNCPK8sVersionRemoved,
			shoots:        shootWithValidNCP,
			expectedError: "Forbidden: kubernetes version \"1.23.4\" is being removed but is still in use by Shoot \"shoot-ncp\"",
		},
		{
			name:          "machine image version removed, in use",
			oldObj:        validNCP,
			newObj:        validNCPMachineImageRemoved,
			shoots:        shootWithValidNCP,
			expectedError: "Forbidden: machine image \"machine-img-2\" version \"1.2.3\" is being removed but is still in use by Shoot \"shoot-ncp\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithIndex(&v1beta1.Shoot{}, ".spec.cloudProfile.name", indexShootCloudProfileName).WithRuntimeObjects(tt.shoots, parentCloudProfile).Build()
			validator := NewNamespacedCloudProfileValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.newObj, tt.oldObj)

			if err == nil {
				t.Fatalf("validator.Validate() error = nil, want error containing %q", tt.expectedError)
			}
			if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("validator.Validate() error = %v, want error containing %q", err, tt.expectedError)
			}
		})
	}
}
func TestValidateNamespacedCloudProfileDeletionSuccess(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)

	const ncpName = "my-ncp"
	const ncpNamespace = "test-ns"

	tests := []struct {
		name            string
		ncpToDelete     client.Object
		existingObjects client.Object
	}{
		{
			name: "deletion success - no shoots",
			ncpToDelete: &core.NamespacedCloudProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:              ncpName,
					Namespace:         ncpNamespace,
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			existingObjects: &v1beta1.Shoot{},
		},
		{
			name: "deletion success - shoot references different NCP",
			ncpToDelete: &core.NamespacedCloudProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:              ncpName,
					Namespace:         ncpNamespace,
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			existingObjects: &v1beta1.Shoot{
				ObjectMeta: metav1.ObjectMeta{Name: "shoot1", Namespace: ncpNamespace},
				Spec: v1beta1.ShootSpec{
					CloudProfile: &v1beta1.CloudProfileReference{Kind: "NamespacedCloudProfile", Name: "other-ncp"},
				},
			},
		},
		{
			name: "deletion success - shoot references global CloudProfile",
			ncpToDelete: &core.NamespacedCloudProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:              ncpName,
					Namespace:         ncpNamespace,
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			existingObjects: &v1beta1.Shoot{
				ObjectMeta: metav1.ObjectMeta{Name: "shoot1", Namespace: ncpNamespace},
				Spec: v1beta1.ShootSpec{
					CloudProfile: &v1beta1.CloudProfileReference{Kind: "CloudProfile", Name: "global-cp"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithIndex(&v1beta1.Shoot{}, ".spec.cloudProfile.name", indexShootCloudProfileName).WithRuntimeObjects(tt.existingObjects).Build()
			validator := NewNamespacedCloudProfileValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.ncpToDelete, nil)

			if err != nil {
				t.Errorf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateNamespacedCloudProfileDeletionFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)

	const ncpName = "my-ncp"
	const ncpNamespace = "test-ns"

	tests := []struct {
		name             string
		ncpToDelete      client.Object
		existingObjects  client.Object
		expectedErrorMsg string
	}{
		{
			name: "deletion failure - shoot still references NCP",
			ncpToDelete: &core.NamespacedCloudProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:              ncpName,
					Namespace:         ncpNamespace,
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			existingObjects: &v1beta1.Shoot{
				ObjectMeta: metav1.ObjectMeta{Name: "shoot1", Namespace: ncpNamespace},
				Spec: v1beta1.ShootSpec{
					CloudProfile: &v1beta1.CloudProfileReference{Kind: "NamespacedCloudProfile", Name: ncpName},
				},
			},
			expectedErrorMsg: fmt.Sprintf("cannot delete namespaced cloud profile %q because it is still referenced by shoot %q", ncpName, "shoot1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithIndex(&v1beta1.Shoot{}, ".spec.cloudProfile.name", indexShootCloudProfileName).WithRuntimeObjects(tt.existingObjects).Build()
			validator := NewNamespacedCloudProfileValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.ncpToDelete, nil)

			if err == nil {
				t.Fatalf("Validate() succeeded, want error containing %q", tt.expectedErrorMsg)
			}
			if !strings.Contains(err.Error(), tt.expectedErrorMsg) {
				t.Errorf("Validate() error = %v, want error containing %q", err, tt.expectedErrorMsg)
			}
		})
	}
}

func indexShootCloudProfileName(rawObj client.Object) []string {
	shoot, ok := rawObj.(*v1beta1.Shoot)
	if !ok {
		return nil
	}
	if shoot.Spec.CloudProfile == nil {
		return nil
	}
	return []string{shoot.Spec.CloudProfile.Name}
}
