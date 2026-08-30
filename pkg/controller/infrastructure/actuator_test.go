// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infrastructure

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/gardener/gardener/extensions/pkg/controller"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ipamglobalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/ipam/v1"
	ipamv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/ipam/v1"
	networkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	gdcapis "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
	gdcfake "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

var (
	infraUID            = "test-infra-uid"
	ownershipLabelKey   = "shootcontrolplane-test-project-test-infrastructure"
	ownershipLabelValue = "zone1-test-seed"
	testInfraSubnet     = &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure",
			Namespace: "test-project",
			UID:       types.UID(infraUID),
			Labels: map[string]string{
				"ipam.gdc.goog/vpc": "default-vpc",
				ownershipLabelKey:   ownershipLabelValue,
				"infra-uid":         infraUID,
			},
		},
		Spec: ipamglobalv1.SubnetSpec{
			Type: ipamv1.Branch,
			IPv4Request: &ipamv1.SubnetRequest{
				CIDR: ptr.To("10.0.212.1/29"),
			},
			ParentReference: &ipamv1.SubnetReference{
				Name:      "test-ip-pool",
				Namespace: ptr.To("platform"),
			},
		},
		Status: ipamglobalv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	testEgressSubnetZone1 = &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure-ns-zone1",
			Namespace: "test-project",
		},
		Status: ipamv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
			IPv4Allocation: &ipamv1.SubnetAllocation{
				CIDR: "136.125.35.138/32",
			},
		},
	}

	testEgressSubnetZone2 = &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure-ns-zone2",
			Namespace: "test-project",
		},
		Status: ipamv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
			IPv4Allocation: &ipamv1.SubnetAllocation{
				CIDR: "136.125.35.139/32",
			},
		},
	}

	testInfraZone1Subnet = &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure-zone1",
			Namespace: "test-project",
			Labels: map[string]string{
				"ipam.gdc.goog/vpc": "default-vpc",
			},
		},
		Spec: ipamglobalv1.SubnetSpec{
			Type: ipamv1.Branch,
			IPv4Request: &ipamv1.SubnetRequest{
				CIDR: ptr.To("10.0.212.1/30"),
			},
			ParentReference: &ipamv1.SubnetReference{
				Name:      "test-infrastructure",
				Namespace: ptr.To("test-project"),
			},
		},
		Status: ipamglobalv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	testInfraZone2Subnet = &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure-zone2",
			Namespace: "test-project",
			Labels: map[string]string{
				"ipam.gdc.goog/vpc": "default-vpc",
			},
		},
		Spec: ipamglobalv1.SubnetSpec{
			Type: ipamv1.Branch,
			IPv4Request: &ipamv1.SubnetRequest{
				CIDR: ptr.To("10.0.212.5/30"),
			},
			ParentReference: &ipamv1.SubnetReference{
				Name:      "test-infrastructure",
				Namespace: ptr.To("test-project"),
			},
		},
		Status: ipamglobalv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	zTestInfraZone1Subnet = &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "z-test-infrastructure-zone1",
			Namespace: "test-project",
			Labels: map[string]string{
				"ipam.gdc.goog/vpc": "default-vpc",
			},
		},
		Spec: ipamv1.SubnetSpec{
			Type: ipamv1.Branch,
			IPv4Request: &ipamv1.SubnetRequest{
				CIDR: ptr.To("10.0.212.1/30"),
			},
			ParentReference: &ipamv1.SubnetReference{
				Name:      "test-infrastructure-zone1",
				Namespace: ptr.To("test-project"),
			},
			NetworkSpec: &ipamv1.NetworkSpec{
				EnableGateway: true,
			},
		},
		Status: ipamv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	zTestInfraZone2Subnet = &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "z-test-infrastructure-zone2",
			Namespace: "test-project",
			Labels: map[string]string{
				"ipam.gdc.goog/vpc": "default-vpc",
			},
		},
		Spec: ipamv1.SubnetSpec{
			Type: ipamv1.Branch,
			IPv4Request: &ipamv1.SubnetRequest{
				CIDR: ptr.To("10.0.212.5/30"),
			},
			ParentReference: &ipamv1.SubnetReference{
				Name:      "test-infrastructure-zone2",
				Namespace: ptr.To("test-project"),
			},
			NetworkSpec: &ipamv1.NetworkSpec{
				EnableGateway: true,
			},
		},
		Status: ipamv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}
	zTestInfraZone1ParentSubnet = &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure-zone1",
			Namespace: "test-project",
		},
		Status: ipamv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	zTestInfraZone2ParentSubnet = &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure-zone2",
			Namespace: "test-project",
		},
		Status: ipamv1.SubnetStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}
)

func Test_Reconcile_Success(t *testing.T) {
	testSubnetParentInProjectNS := testInfraSubnet.DeepCopy()
	testSubnetParentInProjectNS.Spec.ParentReference.Namespace = ptr.To("test-project")
	testSubnetParentSubnetGroup := testInfraSubnet.DeepCopy()
	testSubnetParentSubnetGroup.Spec.ParentReference.Type = ipamv1.SubnetGroup
	testSubnetParentSubnetGroup.Spec.ParentReference.Namespace = ptr.To("test-project")

	tests := []struct {
		name                string
		infra               *extensionsv1alpha1.Infrastructure
		cluster             *controller.Cluster
		objects             []client.Object
		wantStatus          *v1alpha1.InfrastructureStatus
		wantState           *FlowState
		expectedSubnet      *ipamglobalv1.Subnet
		expectedEgressCIDRs []string
	}{
		{
			name: "Reconcile infrastructure in Lancer Evo",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
					UID:       types.UID(infraUID),
					Labels: map[string]string{
						ownershipLabelKey: ownershipLabelValue,
						"infra-uid":       infraUID,
					},
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentSubnet:        "test-ip-pool",
									ParentSubnetProject: "platform",
									NodeCIDR:            "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{
											Name: "zone1",
											CIDR: "10.0.212.1/30",
										},
									},
								},
								EnableEgress: ptr.To(false),
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
				testInfraSubnet,
				testInfraZone1Subnet,
				zTestInfraZone1Subnet,
				zTestInfraZone1ParentSubnet,
			},
			wantStatus: &v1alpha1.InfrastructureStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "InfrastructureStatus",
				},
				EnableEgress: ptr.To(false),
				Networks: v1alpha1.NetworkStatus{
					NodeCIDR: "10.0.212.1/29",
					Zones: []v1alpha1.Zones{
						{Name: "zone1", Subnet: "z-test-infrastructure-zone1"},
					},
				},
			},
			wantState: &FlowState{
				TypeMeta: metav1.TypeMeta{
					Kind:       FlowStateKind,
					APIVersion: SchemeGroupVersion.String(),
				},
				Data: map[string]string{},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
			expectedSubnet: &ipamglobalv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-project",
					Labels: map[string]string{
						"ipam.gdc.goog/vpc": "default-vpc",
						ownershipLabelKey:   ownershipLabelValue,
						"infra-uid":         infraUID,
					},
				},
				Spec: ipamglobalv1.SubnetSpec{
					Type: ipamv1.Branch,
					IPv4Request: &ipamv1.SubnetRequest{
						CIDR: ptr.To("10.0.212.1/29"),
					},
					ParentReference: &ipamv1.SubnetReference{
						Name:      "test-ip-pool",
						Namespace: ptr.To("platform"),
					},
				},
			},
		},
		{
			name: "Reconcile infrastructure in Lancer Evo - default project namespace",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
					UID:       types.UID(infraUID),
					Labels: map[string]string{
						ownershipLabelKey: ownershipLabelValue,
						"infra-uid":       infraUID,
					},
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentSubnet: "test-ip-pool",
									NodeCIDR:     "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{
											Name: "zone1",
											CIDR: "10.0.212.1/30",
										},
									},
								},
								EnableEgress: ptr.To(false),
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
				testSubnetParentInProjectNS,
				testInfraZone1Subnet,
				zTestInfraZone1Subnet,
				zTestInfraZone1ParentSubnet,
			},
			wantStatus: &v1alpha1.InfrastructureStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "InfrastructureStatus",
				},
				EnableEgress: ptr.To(false),
				Networks: v1alpha1.NetworkStatus{
					NodeCIDR: "10.0.212.1/29",
					Zones: []v1alpha1.Zones{
						{Name: "zone1", Subnet: "z-test-infrastructure-zone1"},
					},
				},
			},
			wantState: &FlowState{
				TypeMeta: metav1.TypeMeta{
					Kind:       FlowStateKind,
					APIVersion: SchemeGroupVersion.String(),
				},
				Data: map[string]string{},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
			expectedSubnet: &ipamglobalv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-project",
					Labels: map[string]string{
						"ipam.gdc.goog/vpc": "default-vpc",
						ownershipLabelKey:   ownershipLabelValue,
						"infra-uid":         infraUID,
					},
				},
				Spec: ipamglobalv1.SubnetSpec{
					Type: ipamv1.Branch,
					IPv4Request: &ipamv1.SubnetRequest{
						CIDR: ptr.To("10.0.212.1/29"),
					},
					ParentReference: &ipamv1.SubnetReference{
						Name:      "test-ip-pool",
						Namespace: ptr.To("test-project"),
					},
				},
			},
		},
		{
			name: "Reconcile infrastructure in Lancer Evo - by default egress is not enabled",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
					UID:       types.UID(infraUID),
					Labels: map[string]string{
						ownershipLabelKey: ownershipLabelValue,
						"infra-uid":       infraUID,
					},
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentSubnet: "test-ip-pool",
									NodeCIDR:     "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{
											Name: "zone1",
											CIDR: "10.0.212.1/30",
										},
									},
								},
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
				testSubnetParentInProjectNS,
				testInfraZone1Subnet,
				zTestInfraZone1Subnet,
				zTestInfraZone1ParentSubnet,
			},
			wantStatus: &v1alpha1.InfrastructureStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "InfrastructureStatus",
				},
				EnableEgress: nil,
				Networks: v1alpha1.NetworkStatus{
					NodeCIDR: "10.0.212.1/29",
					Zones: []v1alpha1.Zones{
						{Name: "zone1", Subnet: "z-test-infrastructure-zone1"},
					},
				},
			},
			wantState: &FlowState{
				TypeMeta: metav1.TypeMeta{
					Kind:       FlowStateKind,
					APIVersion: SchemeGroupVersion.String(),
				},
				Data: map[string]string{},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
			expectedSubnet: &ipamglobalv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-project",
					Labels: map[string]string{
						"ipam.gdc.goog/vpc": "default-vpc",
						ownershipLabelKey:   ownershipLabelValue,
						"infra-uid":         infraUID,
					},
				},
				Spec: ipamglobalv1.SubnetSpec{
					Type: ipamv1.Branch,
					IPv4Request: &ipamv1.SubnetRequest{
						CIDR: ptr.To("10.0.212.1/29"),
					},
					ParentReference: &ipamv1.SubnetReference{
						Name:      "test-ip-pool",
						Namespace: ptr.To("test-project"),
					},
				},
			},
		},
		{
			name: "Reconcile infrastructure in Lancer Evo - enable egress",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
					UID:       types.UID(infraUID),
					Labels: map[string]string{
						ownershipLabelKey: ownershipLabelValue,
						"infra-uid":       infraUID,
					},
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentSubnet: "test-ip-pool",
									NodeCIDR:     "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{
											Name: "zone1",
											CIDR: "10.0.212.1/30",
										},
									},
								},
								EnableEgress: ptr.To(true),
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
				testSubnetParentInProjectNS,
				testInfraZone1Subnet,
				zTestInfraZone1Subnet,
				zTestInfraZone1ParentSubnet,
				testEgressSubnetZone1,
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
					Status: networkingv1.CloudNATGatewayStatus{
						Conditions: []metav1.Condition{{
							Type:               "Ready",
							Status:             metav1.ConditionTrue,
							Reason:             "Succeeded",
							Message:            "CloudNATGateway is ready",
							LastTransitionTime: metav1.Now(),
						}},
					},
				},
			},
			wantStatus: &v1alpha1.InfrastructureStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "InfrastructureStatus",
				},
				EnableEgress: ptr.To(true),
				Networks: v1alpha1.NetworkStatus{
					NodeCIDR: "10.0.212.1/29",
					Zones: []v1alpha1.Zones{
						{Name: "zone1", Subnet: "z-test-infrastructure-zone1"},
					},
				},
			},
			wantState: &FlowState{
				TypeMeta: metav1.TypeMeta{
					Kind:       FlowStateKind,
					APIVersion: SchemeGroupVersion.String(),
				},
				Data: map[string]string{},
			},
			expectedEgressCIDRs: []string{"136.125.35.138/32"},
			cluster:             gdcfake.CreateClusterWithCloudProfile(),
			expectedSubnet: &ipamglobalv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-project",
					Labels: map[string]string{
						"ipam.gdc.goog/vpc": "default-vpc",
						ownershipLabelKey:   ownershipLabelValue,
						"infra-uid":         infraUID,
					},
				},
				Spec: ipamglobalv1.SubnetSpec{
					Type: ipamv1.Branch,
					IPv4Request: &ipamv1.SubnetRequest{
						CIDR: ptr.To("10.0.212.1/29"),
					},
					ParentReference: &ipamv1.SubnetReference{
						Name:      "test-ip-pool",
						Namespace: ptr.To("test-project"),
					},
				},
			},
		},
		{
			name: "Reconcile infrastructure - parent subnet is SubnetGroup",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
					UID:       types.UID(infraUID),
					Labels: map[string]string{
						ownershipLabelKey: ownershipLabelValue,
						"infra-uid":       infraUID,
					},
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentReference: &v1alpha1.SubnetReference{
										Name:      "test-ip-pool",
										Namespace: ptr.To("test-project"),
										Type:      v1alpha1.SubnetGroup,
									},
									NodeCIDR: "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{
											Name: "zone1",
											CIDR: "10.0.212.1/30",
										},
									},
								},
								EnableEgress: ptr.To(false),
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
				testSubnetParentSubnetGroup,
				testInfraZone1Subnet,
				zTestInfraZone1Subnet,
				zTestInfraZone1ParentSubnet,
			},
			wantStatus: &v1alpha1.InfrastructureStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "InfrastructureStatus",
				},
				EnableEgress: ptr.To(false),
				Networks: v1alpha1.NetworkStatus{
					NodeCIDR: "10.0.212.1/29",
					Zones: []v1alpha1.Zones{
						{Name: "zone1", Subnet: "z-test-infrastructure-zone1"},
					},
				},
			},
			wantState: &FlowState{
				TypeMeta: metav1.TypeMeta{
					Kind:       FlowStateKind,
					APIVersion: SchemeGroupVersion.String(),
				},
				Data: map[string]string{},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
			expectedSubnet: &ipamglobalv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-project",
					Labels: map[string]string{
						"ipam.gdc.goog/vpc": "default-vpc",
						ownershipLabelKey:   ownershipLabelValue,
						"infra-uid":         infraUID,
					},
				},
				Spec: ipamglobalv1.SubnetSpec{
					Type: ipamv1.Branch,
					IPv4Request: &ipamv1.SubnetRequest{
						CIDR: ptr.To("10.0.212.1/29"),
					},
					ParentReference: &ipamv1.SubnetReference{
						Name:      "test-ip-pool",
						Namespace: ptr.To("test-project"),
						Type:      ipamv1.SubnetGroup,
					},
				},
			},
		},
		{
			name: "Reconcile infrastructure - parent subnet is SubnetGroup - default project namespace",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
					UID:       types.UID(infraUID),
					Labels: map[string]string{
						ownershipLabelKey: ownershipLabelValue,
						"infra-uid":       infraUID,
					},
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentReference: &v1alpha1.SubnetReference{
										Name: "test-ip-pool",
										Type: v1alpha1.SubnetGroup,
										// no Namespace defined, should use serviceaccount.project
									},
									NodeCIDR: "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{
											Name: "zone1",
											CIDR: "10.0.212.1/30",
										},
									},
								},
								EnableEgress: ptr.To(false),
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
				testSubnetParentSubnetGroup,
				testInfraZone1Subnet,
				zTestInfraZone1Subnet,
				zTestInfraZone1ParentSubnet,
			},
			wantStatus: &v1alpha1.InfrastructureStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "InfrastructureStatus",
				},
				EnableEgress: ptr.To(false),
				Networks: v1alpha1.NetworkStatus{
					NodeCIDR: "10.0.212.1/29",
					Zones: []v1alpha1.Zones{
						{Name: "zone1", Subnet: "z-test-infrastructure-zone1"},
					},
				},
			},
			wantState: &FlowState{
				TypeMeta: metav1.TypeMeta{
					Kind:       FlowStateKind,
					APIVersion: SchemeGroupVersion.String(),
				},
				Data: map[string]string{},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
			expectedSubnet: &ipamglobalv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-project",
					Labels: map[string]string{
						"ipam.gdc.goog/vpc": "default-vpc",
						ownershipLabelKey:   ownershipLabelValue,
						"infra-uid":         infraUID,
					},
				},
				Spec: ipamglobalv1.SubnetSpec{
					Type: ipamv1.Branch,
					IPv4Request: &ipamv1.SubnetRequest{
						CIDR: ptr.To("10.0.212.1/29"),
					},
					ParentReference: &ipamv1.SubnetReference{
						Name:      "test-ip-pool",
						Namespace: ptr.To("test-project"),
						Type:      ipamv1.SubnetGroup,
					},
				},
			},
		},
		{
			name: "Reconcile infrastructure - adopt legacy unlabelled subnet (ProviderStatus is NOT nil)",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
					UID:       types.UID(infraUID),
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentSubnet:        "test-ip-pool",
									ParentSubnetProject: "platform",
									NodeCIDR:            "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{Name: "zone1", CIDR: "10.0.212.1/30"},
									},
								},
								EnableEgress: ptr.To(false),
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
				Status: extensionsv1alpha1.InfrastructureStatus{
					DefaultStatus: extensionsv1alpha1.DefaultStatus{
						// NOT NIL, This tells the controller it is a legacy shoot, triggering the patch
						ProviderStatus: &runtime.RawExtension{Raw: []byte(`{}`)},
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
				// EXISTING SUBNET WITHOUT LABELS
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure",
						Namespace: "test-project",
						UID:       "old-subnet-uid",
						Labels: map[string]string{
							"ipam.gdc.goog/vpc": "default-vpc",
							// Missing "infra-uid" and ownership labels here
						},
					},
					Spec: ipamglobalv1.SubnetSpec{
						Type: ipamv1.Branch,
						IPv4Request: &ipamv1.SubnetRequest{
							CIDR: ptr.To("10.0.212.1/29"),
						},
						ParentReference: &ipamv1.SubnetReference{
							Name:      "test-ip-pool",
							Namespace: ptr.To("platform"),
						},
					},
					Status: ipamglobalv1.SubnetStatus{
						Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
					},
				},
				testInfraZone1Subnet,
				zTestInfraZone1Subnet,
				zTestInfraZone1ParentSubnet,
			},
			wantStatus: &v1alpha1.InfrastructureStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "InfrastructureStatus",
				},
				EnableEgress: ptr.To(false),
				Networks: v1alpha1.NetworkStatus{
					NodeCIDR: "10.0.212.1/29",
					Zones: []v1alpha1.Zones{
						{Name: "zone1", Subnet: "z-test-infrastructure-zone1"},
					},
				},
			},
			wantState: &FlowState{
				TypeMeta: metav1.TypeMeta{
					Kind:       FlowStateKind,
					APIVersion: SchemeGroupVersion.String(),
				},
				Data: map[string]string{},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
			expectedSubnet: &ipamglobalv1.Subnet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-project",
					Labels: map[string]string{
						"ipam.gdc.goog/vpc": "default-vpc",
						ownershipLabelKey:   ownershipLabelValue,
						"infra-uid":         infraUID,
					},
				},
				Spec: ipamglobalv1.SubnetSpec{
					Type: ipamv1.Branch,
					IPv4Request: &ipamv1.SubnetRequest{
						CIDR: ptr.To("10.0.212.1/29"),
					},
					ParentReference: &ipamv1.SubnetReference{
						Name:      "test-ip-pool",
						Namespace: ptr.To("platform"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allObjects := append(tt.objects, tt.infra)

			c := createFakeClient(t, allObjects, tt.infra)

			var loader = func(config *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.WithWatch, error) {
				return c, nil
			}
			m := gdcfake.NewManager(c)
			a := NewActuator(m, withKubeClientGetter(loader))
			actuator := a.(*actuator)
			err := actuator.Reconcile(context.Background(), logr.Logger{}, tt.infra, tt.cluster)
			if err != nil {
				if strings.Contains(err.Error(), "not implemented") {
					return
				}
				t.Fatalf("actuator.Reconcile() error = %v", err.Error())
			}

			subnet := &ipamglobalv1.Subnet{}
			err = c.Get(context.Background(), client.ObjectKey{
				Name:      "test-infrastructure",
				Namespace: "test-project",
			}, subnet)
			if err != nil {
				t.Fatalf("failed to retrieve subnet: %v", err)
			}
			if !reflect.DeepEqual(subnet.Spec, tt.expectedSubnet.Spec) {
				t.Fatalf("actuator.Reconcile() subnet, want %+v, got %+v", tt.expectedSubnet.Spec, subnet.Spec)
			}
			if !reflect.DeepEqual(subnet.Labels, tt.expectedSubnet.Labels) {
				t.Fatalf("actuator.Reconcile() subnet labels, want %+v, got %+v", tt.expectedSubnet.Labels, subnet.Labels)
			}
			got := new(v1alpha1.InfrastructureStatus)
			if err := json.Unmarshal(tt.infra.Status.ProviderStatus.Raw, got); err != nil {
				t.Fatalf("actuator.Reconcile() unable to unmarsh the ProviderStatus: %v", err.Error())
			}
			if diff := cmp.Diff(tt.wantStatus, got); diff != "" {
				t.Fatalf("actuator.Reconcile() ProviderStatus want %v, got %v, diff: %s", tt.wantStatus, got, diff)
			}
			if diff := cmp.Diff(tt.expectedEgressCIDRs, tt.infra.Status.EgressCIDRs); diff != "" {
				t.Fatalf("actuator.Reconcile() EgressCIDRs want %v, got %v, diff: %s", tt.expectedEgressCIDRs, tt.infra.Status.EgressCIDRs, diff)
			}

			gotState := new(FlowState)
			if err := json.Unmarshal(tt.infra.Status.State.Raw, gotState); err != nil {
				t.Fatalf("actuator.Reconcile() unable to unmarshal the FlowState: %v", err.Error())
			}
			if diff := cmp.Diff(tt.wantState, gotState); diff != "" {
				t.Fatalf("actuator.Reconcile() State want %v, got %v, diff: %s", tt.wantState, gotState, diff)
			}
		})
	}
}

func Test_Reconcile_Multizone_Success(t *testing.T) {
	tests := []struct {
		name                string
		infra               *extensionsv1alpha1.Infrastructure
		cluster             *controller.Cluster
		objects             []client.Object
		wantStatus          *v1alpha1.InfrastructureStatus
		wantState           *FlowState
		expectedEgressCIDRs []string
	}{
		{
			name: "Reconcile infrastructure in Lancer Evo",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
					UID:       types.UID(infraUID),
					Labels: map[string]string{
						ownershipLabelKey: ownershipLabelValue,
						"infra-uid":       infraUID,
					},
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentSubnet:        "test-ip-pool",
									ParentSubnetProject: "platform",
									NodeCIDR:            "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{Name: "zone1", CIDR: "10.0.212.1/30"},
										{Name: "zone2", CIDR: "10.0.212.5/30"},
									},
								},
								EnableEgress: ptr.To(true),
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
				testInfraSubnet,
				testInfraZone1Subnet,
				testInfraZone2Subnet,
				zTestInfraZone1Subnet,
				zTestInfraZone2Subnet,
				zTestInfraZone1ParentSubnet,
				zTestInfraZone2ParentSubnet,
				testEgressSubnetZone1,
				testEgressSubnetZone2,
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
					Status: networkingv1.CloudNATGatewayStatus{
						Conditions: []metav1.Condition{{
							Type:               "Ready",
							Status:             metav1.ConditionTrue,
							Reason:             "Succeeded",
							Message:            "CloudNATGateway is ready",
							LastTransitionTime: metav1.Now(),
						}},
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
					Status: networkingv1.CloudNATGatewayStatus{
						Conditions: []metav1.Condition{{
							Type:               "Ready",
							Status:             metav1.ConditionTrue,
							Reason:             "Succeeded",
							Message:            "CloudNATGateway is ready",
							LastTransitionTime: metav1.Now(),
						}},
					},
				},
			},
			wantStatus: &v1alpha1.InfrastructureStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       "InfrastructureStatus",
				},
				EnableEgress: ptr.To(true),
				Networks: v1alpha1.NetworkStatus{
					NodeCIDR: "10.0.212.1/29",
					Zones: []v1alpha1.Zones{
						{Name: "zone1", Subnet: "z-test-infrastructure-zone1"},
						{Name: "zone2", Subnet: "z-test-infrastructure-zone2"},
					},
				},
			},
			wantState: &FlowState{
				TypeMeta: metav1.TypeMeta{
					Kind:       FlowStateKind,
					APIVersion: SchemeGroupVersion.String(),
				},
				Data: map[string]string{},
			},
			expectedEgressCIDRs: []string{"136.125.35.138/32", "136.125.35.139/32"},
			cluster:             gdcfake.CreateClusterWithCloudProfile(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allObjects := append(tt.objects, tt.infra)

			c := createFakeClient(t, allObjects, tt.infra)
			var loader = func(config *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.WithWatch, error) {
				return c, nil
			}
			m := gdcfake.NewManager(c)
			a := NewActuator(m, withKubeClientGetter(loader))
			actuator := a.(*actuator)
			err := actuator.Reconcile(context.Background(), logr.Logger{}, tt.infra, tt.cluster)
			if err != nil {
				if strings.Contains(err.Error(), "not implemented") {
					return
				}
				t.Fatalf("actuator.Reconcile() error = %v", err.Error())
			}

			got := new(v1alpha1.InfrastructureStatus)
			if err := json.Unmarshal(tt.infra.Status.ProviderStatus.Raw, got); err != nil {
				t.Fatalf("actuator.Reconcile() unable to unmarsh the ProviderStatus: %v", err.Error())
			}
			if !reflect.DeepEqual(got, tt.wantStatus) {
				t.Fatalf("actuator.Reconcile() ProviderStatus want %v, got %v", tt.wantStatus, got)
			}
			if diff := cmp.Diff(tt.expectedEgressCIDRs, tt.infra.Status.EgressCIDRs); diff != "" {
				t.Fatalf("actuator.Reconcile() EgressCIDRs want %v, got %v, diff: %s", tt.expectedEgressCIDRs, tt.infra.Status.EgressCIDRs, diff)
			}

			gotState := new(FlowState)
			if err := json.Unmarshal(tt.infra.Status.State.Raw, gotState); err != nil {
				t.Fatalf("actuator.Reconcile() unable to unmarshal the FlowState: %v", err.Error())
			}
			if !reflect.DeepEqual(gotState, tt.wantState) {
				t.Fatalf("actuator.Reconcile() State want %v, got %v", tt.wantStatus, got)
			}
		})
	}
}

func Test_Delete(t *testing.T) {
	tests := []struct {
		name          string
		infra         *extensionsv1alpha1.Infrastructure
		objects       []client.Object
		globalObjects []client.Object
		zone1Objects  []client.Object
		zone2Objects  []client.Object
		cluster       *controller.Cluster
	}{
		{
			name: "delete existing Subnets",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{
									Zones: []v1alpha1.Zone{
										{Name: "zone1"},
										{Name: "zone2"},
									},
								},
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			globalObjects: []client.Object{
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure",
						Namespace: "test-project",
					},
				},
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
			},
			zone1Objects: []client.Object{
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "z-test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
				},
			},
			zone2Objects: []client.Object{
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "z-test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
		},
		{
			name: "delete non-existent Subnet",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "InfrastructureConfig",
								},
								Networks: v1alpha1.NetworkConfig{
									ParentSubnet:        "test-ip-pool",
									ParentSubnetProject: "platform",
									NodeCIDR:            "10.0.212.1/29",
									Zones: []v1alpha1.Zone{
										{Name: "zone1"},
									},
								},
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
		},
		{
			name: "delete existing Subnets with egress disabled",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{
									Zones: []v1alpha1.Zone{
										{Name: "zone1"},
										{Name: "zone2"},
									},
								},
								EnableEgress: ptr.To(false),
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			globalObjects: []client.Object{
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure",
						Namespace: "test-project",
					},
				},
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
			},
			zone1Objects: []client.Object{
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "z-test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
				},
			},
			zone2Objects: []client.Object{
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "z-test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
		},
		{
			name: "delete existing Subnets with egress disabled",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								Networks: v1alpha1.NetworkConfig{
									Zones: []v1alpha1.Zone{
										{Name: "zone1"},
										{Name: "zone2"},
									},
								},
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			globalObjects: []client.Object{
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure",
						Namespace: "test-project",
					},
				},
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
			},
			zone1Objects: []client.Object{
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "z-test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
				},
			},
			zone2Objects: []client.Object{
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "z-test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
		},
		{
			name: "delete existing CloudNAT resources with egress disabled",
			infra: &extensionsv1alpha1.Infrastructure{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-infrastructure",
					Namespace: "test-infrastructure-ns",
				},
				Spec: extensionsv1alpha1.InfrastructureSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(&v1alpha1.InfrastructureConfig{
								EnableEgress: ptr.To(false),
								Networks: v1alpha1.NetworkConfig{
									Zones: []v1alpha1.Zone{
										{Name: "zone1"},
										{Name: "zone2"},
									},
								},
							}),
						},
					},
					SecretRef: corev1.SecretReference{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
				},
			},
			globalObjects: []client.Object{
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure",
						Namespace: "test-project",
					},
				},
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
			},
			zone1Objects: []client.Object{
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "z-test-infrastructure-zone1",
						Namespace: "test-project",
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone1"),
						Namespace: "test-project",
					},
				},
			},
			zone2Objects: []client.Object{
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "z-test-infrastructure-zone2",
						Namespace: "test-project",
					},
				},
				&networkingv1.CloudNATGateway{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
				},
				&ipamv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getEgressResourceName("test-infrastructure-ns", "zone2"),
						Namespace: "test-project",
					},
				},
			},
			objects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-secret",
						Namespace: "test-infrastructure-ns",
					},
					Data: map[string][]byte{
						"serviceaccount.json": []byte(`{"Project": "test-project"}`),
						"gdch-config":         []byte(`{}`),
					},
				},
			},
			cluster: gdcfake.CreateClusterWithCloudProfile(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allObjects := append(tt.objects, tt.infra)
			c := createFakeClient(t, allObjects, tt.infra)
			globalFakeClient := createFakeClient(t, tt.globalObjects, tt.infra)
			zone1FakeClient := createFakeClient(t, tt.zone1Objects, tt.infra)
			zone2FakeClient := createFakeClient(t, tt.zone2Objects, tt.infra)
			m := gdcfake.NewManager(c)

			var loader = func(config *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.WithWatch, error) {
				switch config.OrgClusterURL {
				case "test-global-url":
					return globalFakeClient, nil
				case "test-zone1-url":
					return zone1FakeClient, nil
				case "test-zone2-url":
					return zone2FakeClient, nil
				default:
					return c, nil
				}
			}

			actuator := NewActuator(m, withKubeClientGetter(loader))

			if err := actuator.Delete(context.Background(), logr.Logger{}, tt.infra, tt.cluster); err != nil {
				t.Fatalf("actuator.Delete() error = %v", err)
			}

			ctx := context.Background()
			namespace := "test-project"
			deletedGlobalSubnets := []string{
				"test-infrastructure",
				"test-infrastructure-zone1",
				"test-infrastructure-zone2",
			}
			verifyObjectsDeleted(ctx, t, globalFakeClient, namespace, deletedGlobalSubnets, &ipamglobalv1.Subnet{})
			deletedZonal1Subnets := []string{
				"test-infrastructure-zone1",
				"z-test-infrastructure-zone1",
			}
			verifyObjectsDeleted(ctx, t, zone1FakeClient, namespace, deletedZonal1Subnets, &ipamv1.Subnet{})
			deletedZonal2Subnets := []string{
				"test-infrastructure-zone2",
				"z-test-infrastructure-zone2",
			}
			verifyObjectsDeleted(ctx, t, zone2FakeClient, namespace, deletedZonal2Subnets, &ipamv1.Subnet{})

			// Verify egress resources are deleted
			verifyObjectsDeleted(ctx, t, zone1FakeClient, namespace, []string{getEgressResourceName("test-infrastructure-ns", "zone1")}, &networkingv1.CloudNATGateway{})
			verifyObjectsDeleted(ctx, t, zone1FakeClient, namespace, []string{getEgressResourceName("test-infrastructure-ns", "zone1")}, &ipamv1.Subnet{})
			verifyObjectsDeleted(ctx, t, zone2FakeClient, namespace, []string{getEgressResourceName("test-infrastructure-ns", "zone2")}, &networkingv1.CloudNATGateway{})
			verifyObjectsDeleted(ctx, t, zone2FakeClient, namespace, []string{getEgressResourceName("test-infrastructure-ns", "zone2")}, &ipamv1.Subnet{})
		})
	}
}

func encode(obj runtime.Object) []byte {
	data, _ := json.Marshal(obj)
	return data
}

func verifyObjectsDeleted[T client.Object](ctx context.Context, t *testing.T, c client.Client, namespace string, names []string, objectType T) {
	t.Helper()
	for _, name := range names {
		obj := objectType.DeepCopyObject().(T)
		key := client.ObjectKey{Name: name, Namespace: namespace}
		err := c.Get(ctx, key, obj)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err == nil {
			t.Errorf("Object %T with name %q in namespace %q should have been deleted but still exists", objectType, name, namespace)
			continue
		}
		t.Errorf("Expected a 'NotFound' error for %T %q, but got a different error: %v", objectType, name, err)
	}
}

func createFakeClient(t *testing.T, objects []client.Object, infra *extensionsv1alpha1.Infrastructure) client.WithWatch {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add corev1 to scheme: %v", err)
	}

	if err := extensionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add extensionsv1alpha1 to scheme: %v", err)
	}

	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add v1alpha1 to scheme: %v", err)
	}

	if err := ipamv1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add ipamv1 to scheme: %v", err)
	}
	if err := ipamglobalv1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add ipamglobal to scheme: %v", err)
	}

	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add networkingv1 to scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(infra).Build()
}

func TestDeleteEgressOnShootVMs(t *testing.T) {
	infra := &extensionsv1alpha1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure",
			Namespace: "test-infrastructure-ns",
		},
	}
	infraConfig := &gdcapis.InfrastructureConfig{
		Networks: gdcapis.NetworkConfig{
			Zones: []gdcapis.Zone{
				{Name: "zone1"},
			},
		},
	}
	serviceAccount := &auth.ServiceAccount{
		Project: "test-project",
	}
	s := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(s)
	_ = networkingv1.AddToScheme(s)
	_ = ipamv1.AddToScheme(s)

	tests := []struct {
		name      string
		deleteErr error
		wantErr   bool
	}{
		{
			name:      "should not return error if resource not found",
			deleteErr: apierrors.NewNotFound(schema.GroupResource{Group: "networking.gdc.goog", Resource: "cloudnatgateways"}, "test"),
			wantErr:   false,
		},
		{
			name:      "should not return error if permission denied",
			deleteErr: apierrors.NewForbidden(schema.GroupResource{Group: "networking.gdc.goog", Resource: "cloudnatgateways"}, "test", nil),
			wantErr:   false,
		},
		{
			name:      "should return error for other errors",
			deleteErr: fmt.Errorf("some other error"),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockZonalClient := &mockDeleteClient{
				WithWatch: fake.NewClientBuilder().WithScheme(s).Build(),
				err:       tt.deleteErr,
			}
			zonalKubeClients := map[string]client.WithWatch{
				"zone1": mockZonalClient,
			}

			err := deleteEgressOnShootVMs(context.Background(), infra, infraConfig, zonalKubeClients, serviceAccount)

			if (err != nil) != tt.wantErr {
				t.Errorf("deleteEgressOnShootVMs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type mockDeleteClient struct {
	client.WithWatch
	err error
}

func (c *mockDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return c.err
}

func Test_Reconcile_Error(t *testing.T) {
	// Base infrastructure object to deep copy for each test case
	baseInfra := &extensionsv1alpha1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-infrastructure",
			Namespace: "test-infrastructure-ns",
			UID:       types.UID(infraUID),
		},
		Spec: extensionsv1alpha1.InfrastructureSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(&v1alpha1.InfrastructureConfig{
						TypeMeta: metav1.TypeMeta{
							APIVersion: v1alpha1.SchemeGroupVersion.String(),
							Kind:       "InfrastructureConfig",
						},
						Networks: v1alpha1.NetworkConfig{
							ParentSubnet:        "test-ip-pool",
							ParentSubnetProject: "platform",
							NodeCIDR:            "10.0.212.1/29",
							Zones: []v1alpha1.Zone{
								{Name: "zone1", CIDR: "10.0.212.1/30"},
							},
						},
						EnableEgress: ptr.To(false),
					}),
				},
			},
			SecretRef: corev1.SecretReference{
				Name:      "my-secret",
				Namespace: "test-infrastructure-ns",
			},
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-secret",
			Namespace: "test-infrastructure-ns",
		},
		Data: map[string][]byte{
			"serviceaccount.json": []byte(`{"Project": "test-project"}`),
			"gdch-config":         []byte(`{}`),
		},
	}

	tests := []struct {
		name        string
		infra       *extensionsv1alpha1.Infrastructure
		cluster     *controller.Cluster
		objects     []client.Object
		wantErrCont string
	}{
		{
			name: "Reconcile error - root subnet occupied by another shoot (UID mismatch)",
			infra: func() *extensionsv1alpha1.Infrastructure {
				i := baseInfra.DeepCopy()
				// Non-nil ProviderStatus represents an existing/updating shoot
				i.Status.ProviderStatus = &runtime.RawExtension{Raw: []byte(`{}`)}
				return i
			}(),
			objects: []client.Object{
				secret,
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure",
						Namespace: "test-project",
						Labels: map[string]string{
							"shootcontrolplane-test-project-test-infrastructure": "zone1-test-seed",
							"infra-uid": "different-uid-67890", // Mismatch with infra-uid
						},
					},
				},
			},
			cluster:     gdcfake.CreateClusterWithCloudProfile(),
			wantErrCont: "terminal error: global root subnet is occupied by another shoot",
		},
		{
			name: "Reconcile error - legacy subnet name collision (nil ProviderStatus)",
			infra: func() *extensionsv1alpha1.Infrastructure {
				i := baseInfra.DeepCopy()
				// Nil ProviderStatus represents a brand-new shoot creation
				i.Status.ProviderStatus = nil
				return i
			}(),
			objects: []client.Object{
				secret,
				&ipamglobalv1.Subnet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-infrastructure",
						Namespace: "test-project",
						// Missing infra-uid label simulates an old legacy subnet
					},
				},
			},
			cluster:     gdcfake.CreateClusterWithCloudProfile(),
			wantErrCont: "already exists and belongs to an older legacy shoot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allObjects := append(tt.objects, tt.infra)

			c := createFakeClient(t, allObjects, tt.infra)

			var loader = func(config *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.WithWatch, error) {
				return c, nil
			}
			m := gdcfake.NewManager(c)
			a := NewActuator(m, withKubeClientGetter(loader))
			actuator := a.(*actuator)

			err := actuator.Reconcile(context.Background(), logr.Logger{}, tt.infra, tt.cluster)

			if err == nil {
				t.Fatalf("actuator.Reconcile() expected error containing %q, but got nil", tt.wantErrCont)
			}
			if !strings.Contains(err.Error(), tt.wantErrCont) {
				t.Fatalf("actuator.Reconcile() error = %v, want error containing %q", err.Error(), tt.wantErrCont)
			}
		})
	}
}
