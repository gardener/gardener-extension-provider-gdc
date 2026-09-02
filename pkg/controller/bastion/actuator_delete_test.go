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

package bastion

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	"github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gdchnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"
	vmv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/virtualmachine/v1"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"

	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
)

func Test_actuator_Delete(t *testing.T) {
	bastion := &extensionsv1alpha1.Bastion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bastion",
			Namespace: "garden",
		},
	}

	tests := []struct {
		name    string
		client  ctrlclient.Client
		ctx     context.Context
		log     logr.Logger
		bastion *extensionsv1alpha1.Bastion
		cluster *extensionscontroller.Cluster
	}{
		{
			name: "deletes a bastion cr",
			client: func() ctrlclient.Client {
				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &v1beta1.CloudProfile{
					Spec: v1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: func() []byte {
								cp := &apisgdc.CloudProfileConfig{OrgConfig: createOrgConfigStruct()}
								providerConfigBytes, err := json.Marshal(cp)
								if err != nil {
									t.Fatalf("error encoding provider config: %v", err)
								}
								return providerConfigBytes
							}(),
						}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &actuator{
				client:  tt.client,
				decoder: serializer.NewCodecFactory(tt.client.Scheme(), serializer.EnableStrict).UniversalDecoder(),
				getClientAndProject: func(ctx context.Context, c ctrlclient.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (ctrlclient.Client, string, error) {
					return tt.client, "garden", nil
				},
			}
			if err := a.Delete(tt.ctx, tt.log, tt.bastion, tt.cluster); err != nil {
				t.Fatalf("actuator.Delete() error = %v", err)
			}
		})
	}
}

func Test_actuator_DeleteErrors(t *testing.T) {
	bastion := &extensionsv1alpha1.Bastion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bastion",
			Namespace: "garden",
		},
	}

	cluster := &extensionscontroller.Cluster{
		Shoot: createShootStruct(),
		CloudProfile: &v1beta1.CloudProfile{
			Spec: v1beta1.CloudProfileSpec{
				ProviderConfig: &runtime.RawExtension{
					Raw: func() []byte {
						cp := &apisgdc.CloudProfileConfig{OrgConfig: createOrgConfigStruct()}
						bytes, err := json.Marshal(cp)
						if err != nil {
							t.Fatalf("error encoding provider config: %v", err)
						}
						return bytes
					}(),
				},
			},
		},
	}

	createFakeClientWithSchemes := func(objTypeToFail runtime.Object) ctrlclient.Client {
		scheme := runtime.NewScheme()
		runtimeutil.Must(corev1.AddToScheme(scheme))
		runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
		runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
		runtimeutil.Must(vmv1.AddToScheme(scheme))
		baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		return &fakeClientWithDeleteError{
			Client:        baseClient,
			objTypeToFail: objTypeToFail,
		}
	}

	tests := []struct {
		name          string
		objToFail     runtime.Object
		expectedError string
	}{
		{
			name:          "errors on deleting a project network policy",
			objToFail:     &gdchnetworkingv1.ProjectNetworkPolicy{},
			expectedError: "error deleting project network policy",
		},
		{
			name:          "errors on deleting a virtual machine disk",
			objToFail:     &vmv1.VirtualMachineDisk{},
			expectedError: "error deleting virtual machine",
		},
		{
			name:          "errors on deleting a virtual machine",
			objToFail:     &vmv1.VirtualMachine{},
			expectedError: "error deleting virtual machine",
		},
		{
			name:          "errors on deleting virtual machine external access",
			objToFail:     &vmv1.VirtualMachineExternalAccess{},
			expectedError: "error deleting virtual machine external access",
		},
		{
			name:          "errors on deleting the setup script secret",
			objToFail:     &corev1.Secret{},
			expectedError: "error deleting setup script secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := createFakeClientWithSchemes(tt.objToFail)
			a := &actuator{
				client:  client,
				decoder: serializer.NewCodecFactory(client.Scheme(), serializer.EnableStrict).UniversalDecoder(),
				getClientAndProject: func(_ context.Context, _ ctrlclient.Client, _ *gdcclient.OrgClusterConfig, _ corev1.SecretReference, _ *runtime.Scheme) (ctrlclient.Client, string, error) {
					return client, "garden", nil
				},
			}

			err := a.Delete(context.Background(), logr.Discard(), bastion, cluster)
			if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
				t.Fatalf("expected error: %s\n got: %v", tt.expectedError, err)
			}
		})
	}

	t.Run("error on creating bastion client", func(t *testing.T) {
		client := createFakeClientWithSchemes(nil)
		a := &actuator{
			client:  client,
			decoder: serializer.NewCodecFactory(client.Scheme(), serializer.EnableStrict).UniversalDecoder(),
			getClientAndProject: func(ctx context.Context, c ctrlclient.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (ctrlclient.Client, string, error) {
				return nil, "", fmt.Errorf("error creating bastion client")
			},
		}

		err := a.Delete(context.Background(), logr.Discard(), bastion, cluster)
		if err == nil || !strings.Contains(err.Error(), "error creating bastion client") {
			t.Fatalf("expected bastion client error, got: %v", err)
		}
	})
}

func createShootStruct() *v1beta1.Shoot {
	return &v1beta1.Shoot{
		Spec: v1beta1.ShootSpec{
			Region: "us-west",
			Provider: v1beta1.Provider{
				InfrastructureConfig: &runtime.RawExtension{Raw: encode(apisgdc.InfrastructureConfig{
					Networks: apisgdc.NetworkConfig{
						Zones: []apisgdc.Zone{
							{
								Name: "zone1",
								CIDR: "10.4.0.128/29",
							},
							{
								Name: "zone2",
								CIDR: "10.4.0.136/29",
							},
						},
					},
				})},
			},
		},
	}
}

func createOrgConfigStruct() *apisgdc.OrgConfig {
	return &apisgdc.OrgConfig{
		OrgName:     "test-org",
		RegistryURL: "test-registry-url",
		Zones: []*apisgdc.ZoneEndpoints{
			{
				Name:              "zone1",
				ManagementAPI:     "test-zone1-url",
				InfrastructureAPI: "https://zone1-infa-cluster-url",
			},
			{
				Name:              "zone2",
				ManagementAPI:     "test-zone2-url",
				InfrastructureAPI: "https://zone2-infa-cluster-url",
			},
			{
				Name:              "zone3",
				ManagementAPI:     "test-zone3-url",
				InfrastructureAPI: "https://zone3-infa-cluster-url",
			},
		},
	}
}

type fakeClientWithDeleteError struct {
	ctrlclient.Client
	objTypeToFail runtime.Object
}

func (f *fakeClientWithDeleteError) Delete(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.DeleteOption) error {
	switch o := obj.(type) {
	case *gdchnetworkingv1.ProjectNetworkPolicy:
		if _, ok := f.objTypeToFail.(*gdchnetworkingv1.ProjectNetworkPolicy); ok {
			return fmt.Errorf("injected delete error for ProjectNetworkPolicy %q", o.GetName())
		}
	case *vmv1.VirtualMachineExternalAccess:
		if _, ok := f.objTypeToFail.(*vmv1.VirtualMachineExternalAccess); ok {
			return fmt.Errorf("injected delete error for VirtualMachineExternalAccess %q", o.GetName())
		}
	case *vmv1.VirtualMachine:
		if _, ok := f.objTypeToFail.(*vmv1.VirtualMachine); ok {
			return fmt.Errorf("injected delete error for VirtualMachine %q", o.GetName())
		}
	case *vmv1.VirtualMachineDisk:
		if _, ok := f.objTypeToFail.(*vmv1.VirtualMachineDisk); ok {
			return fmt.Errorf("injected delete error for VirtualMachineDisk %q", o.GetName())
		}
	case *corev1.Secret:
		if _, ok := f.objTypeToFail.(*corev1.Secret); ok {
			return fmt.Errorf("injected delete error for Secret %q", o.GetName())
		}
	}
	return f.Client.Delete(ctx, obj, opts...)
}

func encode(obj any) []byte {
	data, _ := json.Marshal(obj)
	return data
}
