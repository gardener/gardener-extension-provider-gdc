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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	gardenercorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gdchnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"
	vmv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/virtualmachine/v1"

	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

func Test_actuator_Reconcile(t *testing.T) {
	providerConfig := &apisgdc.CloudProfileConfig{
		OrgConfig: createOrgConfigStruct(),
		MachineImages: []apisgdc.MachineImages{
			{
				Name:     "image-2",
				Project:  "garden-project",
				Versions: []apisgdc.MachineImageVersion{},
			},
			{
				Name:    "image",
				Project: "garden-project",
				Versions: []apisgdc.MachineImageVersion{
					{
						Version: "0.9.9",
						Image:   "garden-image",
					},
					{
						Version: "1.0.0",
						Image:   "garden-image",
					},
				},
			},
			{
				Name:     "cross-project-image",
				Versions: []apisgdc.MachineImageVersion{},
			},
		},
	}
	providerConfigBytes, err := json.Marshal(providerConfig)
	if err != nil {
		t.Fatalf("error encoding provider config: %v", err)
	}
	project := "garden"
	ingressIP := "1.2.3.4"
	bastion := &extensionsv1alpha1.Bastion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bastion",
			Namespace: project,
		},
		Spec: extensionsv1alpha1.BastionSpec{
			UserData: []byte(base64.StdEncoding.EncodeToString([]byte("data"))),
			Ingress: []extensionsv1alpha1.BastionIngressPolicy{
				{
					IPBlock: networkingv1.IPBlock{
						CIDR: "1.2.3.4/5",
					},
				},
			},
		},
	}

	tests := []struct {
		name    string
		client  client.Client
		ctx     context.Context
		log     logr.Logger
		bastion *extensionsv1alpha1.Bastion
		cluster *extensionscontroller.Cluster
	}{
		{
			name: "reconciles a bastion cr",
			client: func() client.Client {
				vm := &vmv1.VirtualMachine{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineNameSuffix),
						Namespace: project,
					},
					Status: vmv1.VirtualMachineStatus{
						State: vmv1.VirtualMachineStateRunning,
					},
				}
				vmdisk := &vmv1.VirtualMachineDisk{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineDiskNameSuffix),
						Namespace: project,
					},
					Status: vmv1.VirtualMachineDiskStatus{
						Phase: vmv1.DiskPhaseSucceeded,
					},
				}
				vmea := &vmv1.VirtualMachineExternalAccess{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineNameSuffix),
						Namespace: project,
					},
					Status: vmv1.VirtualMachineExternalAccessStatus{
						IngressIP: ingressIP,
					},
				}

				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion, vm, vmea, vmdisk).WithStatusSubresource(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &gardenercorev1beta1.CloudProfile{
					Spec: gardenercorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: providerConfigBytes,
						},
						MachineTypes: []gardenercorev1beta1.MachineType{
							{
								Name:         "machine-type",
								CPU:          resource.MustParse("12Gi"),
								Architecture: ptr.To("amd64"),
							},
							{
								Name: "machine-type-2",
								CPU:  resource.MustParse("12Gi"),
							},
						},
						MachineImages: []gardenercorev1beta1.MachineImage{
							{
								Name: "image",
								Versions: []gardenercorev1beta1.MachineImageVersion{
									{
										Architectures: []string{
											"amd64",
										},
										ExpirableVersion: gardenercorev1beta1.ExpirableVersion{
											Classification: ptr.To(gardenercorev1beta1.ClassificationSupported),
											Version:        "1.0.0",
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
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			a := &actuator{
				client:  tt.client,
				decoder: serializer.NewCodecFactory(tt.client.Scheme(), serializer.EnableStrict).UniversalDecoder(),
				getClientAndProject: func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
					return tt.client, project, nil
				},
			}
			if err := a.Reconcile(tt.ctx, tt.log, tt.bastion, tt.cluster); err != nil {
				t.Fatalf("actuator.Reconcile() error = %v", err)
			}

			pnp := &gdchnetworkingv1.ProjectNetworkPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionProjectNetworkPolicyNameSuffix),
					Namespace: project,
				},
			}
			err = tt.client.Get(context.Background(), client.ObjectKeyFromObject(pnp), pnp)
			if err != nil {
				t.Fatalf("failed to get project network policy: %v", err)
			}
			if pnp.Spec.Ingress[0].From[0].IPBlock.CIDR != bastion.Spec.Ingress[0].IPBlock.CIDR {
				t.Fatalf("expected project network policy ingress cidr to be %q but got %q", bastion.Spec.Ingress[0].IPBlock.CIDR, pnp.Spec.Ingress[0].From[0].IPBlock.CIDR)
			}

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionSetupScriptNameSuffix),
					Namespace: project,
				},
			}
			err = tt.client.Get(context.Background(), client.ObjectKeyFromObject(secret), secret)
			if err != nil {
				t.Fatalf("failed to get secret: %v", err)
			}
			if string(secret.Data["script"]) != string(bastion.Spec.UserData) {
				t.Fatalf("expected secret data to match bastion userdata")
			}

			disk := &vmv1.VirtualMachineDisk{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineDiskNameSuffix),
					Namespace: project,
				},
			}
			err = tt.client.Get(context.Background(), client.ObjectKeyFromObject(disk), disk)
			if err != nil {
				t.Fatalf("failed to get vmdisk: %v", err)
			}
			if disk.Spec.Source.Image.Name != providerConfig.MachineImages[1].Versions[0].Image {
				t.Fatalf("expected image name to be %q but got %q", providerConfig.MachineImages[1].Versions[0].Image, disk.Spec.Source.Image.Name)
			}
			if disk.Spec.Source.Image.Namespace != providerConfig.MachineImages[1].Project {
				t.Fatalf("expected image namespace to be %q but got %q", providerConfig.MachineImages[1].Project, disk.Spec.Source.Image.Namespace)
			}

			vm := &vmv1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineNameSuffix),
					Namespace: project,
				},
			}
			err = tt.client.Get(context.Background(), client.ObjectKeyFromObject(vm), vm)
			if err != nil {
				t.Fatalf("failed to get vm: %v", err)
			}
			if vm.Spec.Compute.VirtualMachineType != tt.cluster.CloudProfile.Spec.MachineTypes[0].Name {
				t.Fatalf("expected virtual machine type to be %q but got %q", tt.cluster.CloudProfile.Spec.MachineTypes[0].Name, vm.Spec.Compute.VirtualMachineType)
			}
			if vm.Spec.Disks[0].VirtualMachineDiskRef.Name != disk.Name {
				t.Fatalf("expected disk name to be %q but got %q", disk.Name, vm.Spec.Disks[0].VirtualMachineDiskRef.Name)
			}
			if vm.Spec.StartupScripts[0].ScriptSecretRef.Name != secret.Name {
				t.Fatalf("expected startup script name to be %q but got %q", secret.Name, vm.Spec.StartupScripts[0].ScriptSecretRef.Name)
			}

			bastionExternalAccess := &vmv1.VirtualMachineExternalAccess{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineNameSuffix),
					Namespace: project,
				},
			}
			err = tt.client.Get(context.Background(), client.ObjectKeyFromObject(bastionExternalAccess), bastionExternalAccess)
			if err != nil {
				t.Fatalf("failed to get vm external access: %v", err)
			}
			if bastionExternalAccess.Spec.Enabled != true {
				t.Fatalf("expected vm external access to be enabled")
			}
			if bastionExternalAccess.Spec.Ports[0].Port != 22 {
				t.Fatalf("expected vm external access to open port 22")
			}

			err = tt.client.Get(context.Background(), client.ObjectKeyFromObject(bastion), bastion)
			if err != nil {
				t.Fatalf("failed to get vm external access: %v", err)
			}
			if bastion.Status.Ingress.IP != ingressIP {
				t.Fatalf("expected bastion ingress ip to be %q but got %q", ingressIP, bastion.Status.Ingress.IP)
			}
		})
	}
}

func Test_actuator_ReconcileErrors(t *testing.T) {
	providerConfig := &apisgdc.CloudProfileConfig{
		OrgConfig: createOrgConfigStruct(),
		MachineImages: []apisgdc.MachineImages{
			{
				Name:    "image",
				Project: "garden-project",
				Versions: []apisgdc.MachineImageVersion{
					{
						Version: "1.0.0",
						Image:   "garden-image",
					},
				},
			},
		},
	}
	project := "garden"
	providerConfigBytes, err := json.Marshal(providerConfig)
	if err != nil {
		t.Fatalf("error encoding provider config: %v", err)
	}
	bastion := &extensionsv1alpha1.Bastion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bastion",
			Namespace: "garden",
		},
	}

	tests := []struct {
		name                string
		client              client.Client
		ctx                 context.Context
		log                 logr.Logger
		bastion             *extensionsv1alpha1.Bastion
		getClientAndProject func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error)
		cluster             *extensionscontroller.Cluster
		expectedError       string
	}{
		{
			name: "errors on creating client",
			client: func() client.Client {
				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion).WithStatusSubresource(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &gardenercorev1beta1.CloudProfile{
					Spec: gardenercorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: providerConfigBytes,
						},
						MachineTypes: []gardenercorev1beta1.MachineType{
							{
								Name:         "machine-type",
								CPU:          resource.MustParse("12Gi"),
								Architecture: ptr.To("amd64"),
							},
						},
						MachineImages: []gardenercorev1beta1.MachineImage{
							{
								Name: "image",
								Versions: []gardenercorev1beta1.MachineImageVersion{
									{
										Architectures: []string{
											"amd64",
										},
										ExpirableVersion: gardenercorev1beta1.ExpirableVersion{
											Classification: ptr.To(gardenercorev1beta1.ClassificationSupported),
											Version:        "1.0.0",
										},
									},
								},
							},
						},
					},
				},
			},
			getClientAndProject: func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return nil, "", fmt.Errorf("error creating client")
			},
			expectedError: "error creating client",
		},
		{
			name: "errors on creating virtual machine",
			client: func() client.Client {
				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion).WithStatusSubresource(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &gardenercorev1beta1.CloudProfile{
					Spec: gardenercorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: providerConfigBytes,
						},
						MachineTypes: []gardenercorev1beta1.MachineType{
							{
								Name:         "machine-type",
								CPU:          resource.MustParse("12Gi"),
								Architecture: ptr.To("amd64"),
							},
						},
						MachineImages: []gardenercorev1beta1.MachineImage{
							{
								Name: "image",
								Versions: []gardenercorev1beta1.MachineImageVersion{
									{
										Architectures: []string{
											"amd64",
										},
										ExpirableVersion: gardenercorev1beta1.ExpirableVersion{
											Classification: ptr.To(gardenercorev1beta1.ClassificationSupported),
											Version:        "1.0.0",
										},
									},
								},
							},
						},
					},
				},
			},
			getClientAndProject: func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return c, project, nil
			},
			expectedError: "error creating virtual machine",
		},
		{
			name: "errors when classification is nil",
			client: func() client.Client {
				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion).WithStatusSubresource(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &gardenercorev1beta1.CloudProfile{
					Spec: gardenercorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: providerConfigBytes,
						},
						MachineTypes: []gardenercorev1beta1.MachineType{
							{
								Name:         "machine-type",
								CPU:          resource.MustParse("12Gi"),
								Architecture: ptr.To("amd64"),
							},
						},
						MachineImages: []gardenercorev1beta1.MachineImage{
							{
								Name: "image",
								Versions: []gardenercorev1beta1.MachineImageVersion{
									{
										Architectures: []string{
											"amd64",
										},
										ExpirableVersion: gardenercorev1beta1.ExpirableVersion{
											Version: "1.0.0",
										},
									},
								},
							},
						},
					},
				},
			},
			getClientAndProject: func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return c, project, nil
			},
			expectedError: "could not find any supported bastion image for arch",
		},
		{
			name: "errors when arch is not supported",
			client: func() client.Client {
				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion).WithStatusSubresource(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &gardenercorev1beta1.CloudProfile{
					Spec: gardenercorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: providerConfigBytes,
						},
						MachineTypes: []gardenercorev1beta1.MachineType{
							{
								Name:         "machine-type",
								CPU:          resource.MustParse("12Gi"),
								Architecture: ptr.To("amd64"),
							},
						},
						MachineImages: []gardenercorev1beta1.MachineImage{
							{
								Name: "image",
								Versions: []gardenercorev1beta1.MachineImageVersion{
									{
										Architectures: []string{
											"arm64",
										},
										ExpirableVersion: gardenercorev1beta1.ExpirableVersion{
											Classification: ptr.To(gardenercorev1beta1.ClassificationSupported),
											Version:        "1.0.0",
										},
									},
								},
							},
						},
					},
				},
			},
			getClientAndProject: func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return c, project, nil
			},
			expectedError: "could not find any supported bastion image for arch",
		},
		{
			name: "errors when there are no images",
			client: func() client.Client {
				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion).WithStatusSubresource(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &gardenercorev1beta1.CloudProfile{
					Spec: gardenercorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: providerConfigBytes,
						},
						MachineTypes: []gardenercorev1beta1.MachineType{
							{
								Name:         "machine-type",
								CPU:          resource.MustParse("12Gi"),
								Architecture: ptr.To("amd64"),
							},
						},
						MachineImages: []gardenercorev1beta1.MachineImage{},
					},
				},
			},
			getClientAndProject: func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return c, project, nil
			},
			expectedError: "could not find any supported bastion image for arch",
		},
		{
			name: "errors when machine type does not exist",
			client: func() client.Client {
				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion).WithStatusSubresource(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &gardenercorev1beta1.CloudProfile{
					Spec: gardenercorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: providerConfigBytes,
						},
						MachineTypes: []gardenercorev1beta1.MachineType{},
						MachineImages: []gardenercorev1beta1.MachineImage{
							{
								Name: "image",
								Versions: []gardenercorev1beta1.MachineImageVersion{
									{
										Architectures: []string{
											"amd64",
										},
										ExpirableVersion: gardenercorev1beta1.ExpirableVersion{
											Classification: ptr.To(gardenercorev1beta1.ClassificationSupported),
											Version:        "1.0.0",
										},
									},
								},
							},
						},
					},
				},
			},
			getClientAndProject: func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return c, project, nil
			},
			expectedError: "no suitable machine found",
		},
		{
			name: "errors when images do not match",
			client: func() client.Client {
				scheme := runtime.NewScheme()
				runtimeutil.Must(corev1.AddToScheme(scheme))
				runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
				runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
				runtimeutil.Must(vmv1.AddToScheme(scheme))
				return fake.NewClientBuilder().WithScheme(scheme).WithObjects(bastion).WithStatusSubresource(bastion).Build()
			}(),
			ctx:     context.Background(),
			log:     logr.Logger{},
			bastion: bastion,
			cluster: &extensionscontroller.Cluster{
				Shoot: createShootStruct(),
				CloudProfile: &gardenercorev1beta1.CloudProfile{
					Spec: gardenercorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: providerConfigBytes,
						},
						MachineTypes: []gardenercorev1beta1.MachineType{
							{
								Name:         "machine-type",
								CPU:          resource.MustParse("12Gi"),
								Architecture: ptr.To("amd64"),
							},
						},
						MachineImages: []gardenercorev1beta1.MachineImage{
							{
								Name: "image-2",
								Versions: []gardenercorev1beta1.MachineImageVersion{
									{
										Architectures: []string{
											"amd64",
										},
										ExpirableVersion: gardenercorev1beta1.ExpirableVersion{
											Classification: ptr.To(gardenercorev1beta1.ClassificationSupported),
											Version:        "1.0.0",
										},
									},
								},
							},
						},
					},
				},
			},
			getClientAndProject: func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return c, project, nil
			},
			expectedError: "could not find any supported bastion image for arch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &actuator{
				client:              tt.client,
				decoder:             serializer.NewCodecFactory(tt.client.Scheme(), serializer.EnableStrict).UniversalDecoder(),
				getClientAndProject: tt.getClientAndProject,
			}
			err := a.Reconcile(tt.ctx, tt.log, tt.bastion, tt.cluster)
			if err == nil {
				t.Fatal("actuator.Reconcile() wants error but got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedError) {
				t.Fatalf("expected error: %s\n Got error: %v", tt.expectedError, err)
			}
		})
	}
}
