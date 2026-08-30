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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/gardener/gardener/pkg/apis/core"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
	fakemanager "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

var cloudProfileNameValue = "fake-cloud-profile"
var (
	validWorkerLancer = core.Worker{
		Name: "test-pool-1",
		Machine: core.Machine{
			Type: "test-machine-type-1",
			Image: &core.ShootMachineImage{
				Name:    "test-image-name-1",
				Version: "test-image-version-1",
			},
		},
		Volume: &core.Volume{
			Name:       ptr.To("test-volume-name-1"),
			VolumeSize: "test-volume-size-1",
			Type:       ptr.To("test-volume-type-1"),
		},
		Zones: []string{"gdch-zone"},
	}
)

func Test_ShootValidator(t *testing.T) {
	tests := []struct {
		name        string
		newObj      client.Object
		oldObj      client.Object
		withObjects []client.Object
	}{
		{
			name:   "workerless Shoot",
			newObj: &core.Shoot{},
			oldObj: &core.Shoot{},
		},
		{
			name: "normal Shoot",
			withObjects: []client.Object{
				&gardencorev1beta1.CloudProfile{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-cloud-profile",
					},
					Spec: gardencorev1beta1.CloudProfileSpec{
						Regions: []gardencorev1beta1.Region{{
							Zones: []gardencorev1beta1.AvailabilityZone{{Name: "gdch-zone"}},
						}},
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(createCloudProfileConfig()),
						},
					},
				},
				&extensionsv1alpha1.Infrastructure{
					ObjectMeta: metav1.ObjectMeta{
						Name: "existing-shoot",
					},
					Spec: extensionsv1alpha1.InfrastructureSpec{
						DefaultSpec: extensionsv1alpha1.DefaultSpec{
							ProviderConfig: &runtime.RawExtension{
								Raw: []byte("{}"),
							},
						},
						SecretRef: corev1.SecretReference{
							Name:      "my-secret",
							Namespace: "test-infrastructure-ns",
						},
					},
					Status: extensionsv1alpha1.InfrastructureStatus{
						NodesCIDR: stringPtr("10.0.0.0/29"),
					},
				},
			},
			newObj: &core.Shoot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "new-shoot",
				},
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					Region:           "gdch-zone",
					Networking: &core.Networking{
						Nodes: stringPtr("10.0.0.0/16"),
					},
					Provider: core.Provider{
						Type: "gdch",
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{
									NodeCIDR:     "10.0.0.8/29",
									ParentSubnet: "test-parent-subnet",
									Zones:        []v1alpha1.Zone{{Name: "gdch-zone", CIDR: "10.0.0.8/30"}},
								},
							}),
						},
						Workers: []core.Worker{validWorkerLancer},
					},
				}},
			oldObj: nil,
		},
		{
			name: "other shoot does not have infrastructure config (workerless shoot)",
			withObjects: []client.Object{
				&gardencorev1beta1.CloudProfile{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-cloud-profile",
					},
					Spec: gardencorev1beta1.CloudProfileSpec{
						Regions: []gardencorev1beta1.Region{{
							Zones: []gardencorev1beta1.AvailabilityZone{{Name: "gdch-zone"}},
						}},
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(createCloudProfileConfig()),
						},
					},
				},
				&gardencorev1beta1.Shoot{
					ObjectMeta: metav1.ObjectMeta{
						Name: "existing-shoot",
					},
					Spec: gardencorev1beta1.ShootSpec{
						CloudProfile: &gardencorev1beta1.CloudProfileReference{
							Name: cloudProfileNameValue,
						},
						Provider: gardencorev1beta1.Provider{},
					},
				},
			},
			newObj: &core.Shoot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "new-shoot",
				},
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					CloudProfile: &core.CloudProfileReference{
						Name: cloudProfileNameValue,
					},
					Region: "gdch-zone",
					Networking: &core.Networking{
						Nodes: stringPtr("10.0.0.0/16"),
					},
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{
									NodeCIDR:     "10.0.0.0/24",
									ParentSubnet: "test-parent-subnet",
									Zones:        []v1alpha1.Zone{{Name: "gdch-zone", CIDR: "10.0.0.0/25"}},
								},
							}),
						},
						Workers: []core.Worker{validWorkerLancer},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("Add v1alpha1 to scheme error = %v", err.Error())
			}
			if err := gdc.AddToScheme(scheme); err != nil {
				t.Fatalf("Add gdch to scheme error = %v", err.Error())
			}
			if err := extensionsv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("Add extensionsv1alpha1 to scheme error = %v", err.Error())
			}
			if err := gardencorev1beta1.AddToScheme(scheme); err != nil {
				t.Fatalf("Add gardencorev1beta1 to scheme error = %v", err.Error())
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.withObjects...).Build()
			shootValidator := NewShootValidator(fakemanager.NewManager(c))

			err := shootValidator.Validate(context.Background(), tt.newObj, tt.oldObj)
			if err != nil {
				t.Fatalf("shoot.Validate() error = %v", err.Error())
			}
		})
	}
}

func Test_ShootValidatorError(t *testing.T) {
	tests := []struct {
		name        string
		withObjects []client.Object
		newObj      client.Object
		oldObj      client.Object
		wantList    error
	}{
		{
			name: "old shoot has nil infrastructure config ",
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
						Workers: []core.Worker{
							{
								Name: "fakeworker",
							},
						},
					},
				},
			},
			oldObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{},
				},
			},
		},
		{
			name: "old shoot unable to decode infrastructure config",
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
						Workers: []core.Worker{
							{
								Name: "fakeworker",
							},
						},
					},
				},
			},
			oldObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: []byte(`invalid-infra-config`),
						},
					},
				},
			},
		},
		{
			name: "new shoot has nil infrastructure config",
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						Workers: []core.Worker{
							{
								Name: "fakeworker",
							},
						},
					},
				},
			},
			oldObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
					},
				},
			},
		},
		{
			name: "new shoot unable to decode infrastructure config",
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: []byte(`invalid-infra-config`),
						},
						Workers: []core.Worker{
							{
								Name: "fakeworker",
							},
						},
					},
				},
			},
			oldObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
					},
				},
			},
		},
		{
			name: "new shoot has invalid Infrastructure.Network.NodeCIDR",
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "invalid"},
							}),
						},
						Workers: []core.Worker{
							{
								Name: "fakeworker",
							},
						},
					},
				},
			},
			oldObj: &core.Shoot{
				Spec: core.ShootSpec{
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
					},
				},
			},
		},
		{
			name: "unsupported zone in control plane config",
			withObjects: []client.Object{
				&gardencorev1beta1.CloudProfile{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-cloud-profile",
					},
					Spec: gardencorev1beta1.CloudProfileSpec{
						Regions: []gardencorev1beta1.Region{{
							Zones: []gardencorev1beta1.AvailabilityZone{{Name: "gdch-zone"}},
						}},
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(createCloudProfileConfig()),
						},
					},
				},
			},
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					Region:           "gdch-zone",
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
						Workers: []core.Worker{validWorkerLancer},
					},
				},
			},
			oldObj: &core.Shoot{
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
					},
				},
			},
		},
		{
			name: "new shoot has the same node cidr with other shoots",
			withObjects: []client.Object{
				&gardencorev1beta1.CloudProfile{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-cloud-profile",
					},
					Spec: gardencorev1beta1.CloudProfileSpec{
						Regions: []gardencorev1beta1.Region{{
							Zones: []gardencorev1beta1.AvailabilityZone{{Name: "gdch-zone"}},
						}},
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(createCloudProfileConfig()),
						},
					},
				},
				&gardencorev1beta1.Shoot{
					ObjectMeta: metav1.ObjectMeta{
						Name: "existing-shoot",
					},
					Spec: gardencorev1beta1.ShootSpec{
						CloudProfile: &gardencorev1beta1.CloudProfileReference{
							Name: cloudProfileNameValue,
						},
						Provider: gardencorev1beta1.Provider{
							InfrastructureConfig: &runtime.RawExtension{
								Raw: encode(&v1alpha1.InfrastructureConfig{
									TypeMeta: metav1.TypeMeta{
										Kind:       "InfrastructureConfig",
										APIVersion: gdc.SchemeGroupVersion.String(),
									},
									Networks: v1alpha1.NetworkConfig{
										NodeCIDR:     "10.0.0.0/24",
										ParentSubnet: "test-ip-pool",
									},
								}),
							},
						},
					},
				},
			},
			newObj: &core.Shoot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "new-shoot",
				},
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					CloudProfile: &core.CloudProfileReference{
						Name: cloudProfileNameValue,
					},
					Region: "gdch-zone",
					Networking: &core.Networking{
						Nodes: stringPtr("10.0.0.0/16"),
					},
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{
									NodeCIDR:     "10.0.0.0/24",
									ParentSubnet: "test-parent-subnet",
									Zones:        []v1alpha1.Zone{{Name: "gdch-zone", CIDR: "10.0.0.0/25"}},
								},
							}),
						},
						Workers: []core.Worker{validWorkerLancer},
					},
				},
			},
			wantList: utilerrors.NewAggregate([]error{field.Invalid(
				infrastructureConfigPath.Child("nodecidr"),
				"10.0.0.0/24",
				fmt.Sprintf(
					"NodeCIDR \"%s\" overlaps with the nodeCIDR \"%s\" of shoot with name \"%q\"",
					"10.0.0.0/24",
					"10.0.0.0/24",
					"existing-shoot",
				),
			),
			}),
		},
		{
			name: "New shoot has a partially overlapping NodeCIDR with other shoots.",
			withObjects: []client.Object{
				&gardencorev1beta1.CloudProfile{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-cloud-profile",
					},
					Spec: gardencorev1beta1.CloudProfileSpec{
						Regions: []gardencorev1beta1.Region{{
							Zones: []gardencorev1beta1.AvailabilityZone{{Name: "gdch-zone"}},
						}},
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(createCloudProfileConfig()),
						},
					},
				},
				&gardencorev1beta1.Shoot{
					ObjectMeta: metav1.ObjectMeta{
						Name: "existing-shoot",
					},
					Spec: gardencorev1beta1.ShootSpec{
						CloudProfile: &gardencorev1beta1.CloudProfileReference{
							Name: cloudProfileNameValue,
						},
						Provider: gardencorev1beta1.Provider{
							InfrastructureConfig: &runtime.RawExtension{
								Raw: encode(&v1alpha1.InfrastructureConfig{
									TypeMeta: metav1.TypeMeta{
										Kind:       "InfrastructureConfig",
										APIVersion: gdc.SchemeGroupVersion.String(),
									},
									Networks: v1alpha1.NetworkConfig{
										NodeCIDR:     "10.0.2.0/23",
										ParentSubnet: "test-ip-pool",
									},
								}),
							},
						},
					},
				},
			},
			newObj: &core.Shoot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "new-shoot",
				},
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					CloudProfile: &core.CloudProfileReference{
						Name: cloudProfileNameValue,
					},
					Region: "gdch-zone",
					Networking: &core.Networking{
						Nodes: stringPtr("10.0.0.0/16"),
					},
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{
									NodeCIDR:     "10.0.0.0/22",
									ParentSubnet: "test-parent-subnet",
									Zones:        []v1alpha1.Zone{{Name: "gdch-zone", CIDR: "10.0.0.0/23"}},
								},
							}),
						},
						Workers: []core.Worker{validWorkerLancer},
					},
				},
			},
			wantList: utilerrors.NewAggregate([]error{field.Invalid(
				infrastructureConfigPath.Child("nodecidr"),
				"10.0.0.0/22",
				fmt.Sprintf(
					"NodeCIDR \"%s\" overlaps with the nodeCIDR \"%s\" of shoot with name \"%q\"",
					"10.0.0.0/22",
					"10.0.2.0/23",
					"existing-shoot",
				),
			),
			}),
		},
		{
			name: "unable to find cloud profile",
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					Region:           "gdch-zone",
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
						Workers: []core.Worker{
							{
								Name: "fakeworker",
							},
						},
					},
				},
			},
		},
		{
			name: "cloud profile with empty config",
			withObjects: []client.Object{
				&gardencorev1beta1.CloudProfile{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-cloud-profile",
					},
				},
			},
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					Region:           "gdch-zone",
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
						Workers: []core.Worker{
							{
								Name: "fakeworker",
							},
						},
					},
				},
			},
		},
		{
			name: "cloud profile with incorrect config type",
			withObjects: []client.Object{
				&gardencorev1beta1.CloudProfile{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-cloud-profile",
					},
					Spec: gardencorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{}),
						},
					},
				},
			},
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					Region:           "gdch-zone",
					Provider: core.Provider{
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
								Networks: v1alpha1.NetworkConfig{NodeCIDR: "10.0.212.1/21"},
							}),
						},
						Workers: []core.Worker{
							{
								Name: "fakeworker",
							},
						},
					},
				},
			},
		},
		{
			name: "unable to decode WorkerConfig",
			withObjects: []client.Object{
				&gardencorev1beta1.CloudProfile{
					TypeMeta: metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-cloud-profile",
					},
					Spec: gardencorev1beta1.CloudProfileSpec{
						Regions: []gardencorev1beta1.Region{{
							Zones: []gardencorev1beta1.AvailabilityZone{{Name: "gdch-zone"}},
						}},
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(createCloudProfileConfig()),
						},
					},
				},
			},
			newObj: &core.Shoot{
				Spec: core.ShootSpec{
					CloudProfileName: &cloudProfileNameValue,
					Region:           "gdch-zone",
					Provider: core.Provider{
						Type: "gdch",
						InfrastructureConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									Kind:       "InfrastructureConfig",
									APIVersion: gdc.SchemeGroupVersion.String(),
								},
							}),
						},
						Workers: []core.Worker{{
							ProviderConfig: &runtime.RawExtension{Raw: []byte("invalid-config")},
						}},
					},
				}},
			oldObj: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("Add v1alpha1 to scheme error = %v", err.Error())
			}
			if err := gdc.AddToScheme(scheme); err != nil {
				t.Fatalf("Add gdch to scheme error = %v", err.Error())
			}
			if err := extensionsv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("Add extensionsv1alpha1 to scheme error = %v", err.Error())
			}

			if err := gdc.AddToScheme(scheme); err != nil {
				t.Fatalf("Add gdch to scheme error = %v", err.Error())
			}

			if err := gardencorev1beta1.AddToScheme(scheme); err != nil {
				t.Fatalf("Add gardencorev1beta1 to scheme error = %v", err.Error())
			}

			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.withObjects...).Build()
			shootValidator := NewShootValidator(fakemanager.NewManager(c))

			gotList := shootValidator.Validate(context.Background(), tt.newObj, tt.oldObj)
			if gotList == nil {
				t.Fatal("shoot.Validate() want error, got nil")
			}

			if tt.wantList == nil {
				return
			}

			if diff := cmp.Diff(tt.wantList, gotList); diff != "" {
				t.Errorf("Unexpected error list (-want +got):\n%s", diff)
			}
		})
	}
}

func encode(obj runtime.Object) []byte {
	data, _ := json.Marshal(obj)
	return data
}

func stringPtr(value string) *string {
	return &value
}

func createCloudProfileConfig() *gdc.CloudProfileConfig {
	return &gdc.CloudProfileConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: gdc.SchemeGroupVersion.String(),
			Kind:       "CloudProfileConfig",
		},
		OrgConfig: &gdc.OrgConfig{
			OrgName:             "test-org",
			GlobalManagementAPI: "test-global-api",
			Zones: []*gdc.ZoneEndpoints{{
				Name:              "test-region-1-zone-1",
				ManagementAPI:     "test-management-api",
				InfrastructureAPI: "test-infra-api",
			}},
		},
	}
}
