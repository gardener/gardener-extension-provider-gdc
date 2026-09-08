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

package worker

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	"github.com/gardener/gardener/extensions/pkg/controller/worker"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	v1betaconstants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	gardener "github.com/gardener/gardener/pkg/client/kubernetes"
	"github.com/gardener/gardener/pkg/extensions"
	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	apisgdcinstall "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/install"
)

var (
	lancer_cloudprofile = &gardencorev1beta1.CloudProfile{
		Spec: gardencorev1beta1.CloudProfileSpec{
			ProviderConfig: &runtime.RawExtension{
				Raw: func() []byte {
					spec := apisgdc.CloudProfileConfig{
						MachineImages: []apisgdc.MachineImages{
							{
								Name: "test-image",
								Versions: []apisgdc.MachineImageVersion{{
									Version:      "1.0",
									Image:        "ubuntu",
									Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
								}},
							},
						},
						OrgConfig: &apisgdc.OrgConfig{
							GlobalManagementAPI: "test-global-management-api",
							Zones: []*apisgdc.ZoneEndpoints{
								{
									Name:          "zone1",
									ManagementAPI: "test-management-api-zone1",
								},
							},
							CAData: "fake-certificate-data",
						},
					}
					raw, _ := json.Marshal(spec)
					return raw
				}(),
			},
		},
	}
)

func Test_workerDelegate_DeployMachineDeployments_Lancer(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}

	if err := apisgdc.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	if err := machinev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	type fields struct {
		client  client.Client
		worker  *extensionsv1alpha1.Worker
		cluster *extensionscontroller.Cluster
	}

	tests := []struct {
		name   string
		fields fields
		want   worker.MachineDeployments
	}{
		{
			name: "successfully generate machine deployments with multiple zones",
			fields: fields{
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-worker",
						Namespace: "test-worker-ns",
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								MachineType:    "test-machine-type",
								Maximum:        10,
								MaxSurge:       intstr.IntOrString{IntVal: 5},
								MaxUnavailable: intstr.IntOrString{IntVal: 3},
								Annotations:    map[string]string{"test-annotation": "true"},
								Labels:         map[string]string{"test-label": "true", "baremetal.cluster.gke.io/namespace": "test-project"},
								Taints:         []v1.Taint{{Key: "taint-key", Value: "taint-value"}},
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "test-machine-image",
									Version: "1.0",
								},
								Minimum:                          1,
								Name:                             "test-worker",
								MachineControllerManagerSettings: &gardencorev1beta1.MachineControllerManagerSettings{},
								NodeAgentSecretName:              ptr.To("test-secret-ref-name"),
								Zones: []string{
									"zone1",
									"zone2",
								},
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, nil),
					},
				},
				cluster: &extensions.Cluster{
					CloudProfile: lancer_cloudprofile,
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{
								Version: "1.24.3",
							},
						},
					},
				},
			},
			want: []worker.MachineDeployment{
				{
					Name:                 "test-worker-ns-test-worker-1",
					PoolName:             "test-worker",
					ClassName:            "test-worker-ns-test-worker-1-401ed",
					SecretName:           "test-worker-ns-test-worker-1-401ed",
					Minimum:              1,
					Maximum:              5,
					Labels:               map[string]string{"test-label": "true", "baremetal.cluster.gke.io/namespace": "test-project"},
					Annotations:          map[string]string{"test-annotation": "true"},
					Taints:               []v1.Taint{{Key: "taint-key", Value: "taint-value"}},
					MachineConfiguration: &machinev1alpha1.MachineConfiguration{},
					Strategy: machinev1alpha1.MachineDeploymentStrategy{
						Type: machinev1alpha1.RollingUpdateMachineDeploymentStrategyType,
						RollingUpdate: &machinev1alpha1.RollingUpdateMachineDeployment{
							UpdateConfiguration: machinev1alpha1.UpdateConfiguration{
								MaxSurge:       &intstr.IntOrString{Type: intstr.Int, IntVal: 3},
								MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 2},
							},
						}},
				},
				{
					Name:                 "test-worker-ns-test-worker-2",
					PoolName:             "test-worker",
					ClassName:            "test-worker-ns-test-worker-2-c8fd5",
					SecretName:           "test-worker-ns-test-worker-2-c8fd5",
					Minimum:              0,
					Maximum:              5,
					Labels:               map[string]string{"test-label": "true", "baremetal.cluster.gke.io/namespace": "test-project"},
					Annotations:          map[string]string{"test-annotation": "true"},
					Taints:               []v1.Taint{{Key: "taint-key", Value: "taint-value"}},
					MachineConfiguration: &machinev1alpha1.MachineConfiguration{},
					Strategy: machinev1alpha1.MachineDeploymentStrategy{
						Type: machinev1alpha1.RollingUpdateMachineDeploymentStrategyType,
						RollingUpdate: &machinev1alpha1.RollingUpdateMachineDeployment{
							UpdateConfiguration: machinev1alpha1.UpdateConfiguration{
								MaxSurge:       &intstr.IntOrString{Type: intstr.Int, IntVal: 2},
								MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
							},
						}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := apisgdc.AddToScheme(scheme); err != nil {
				t.Fatalf("unable to add to scheme %v", err)
			}
			delegate, err := newWorkerDelegate(tt.fields.client, scheme, nil, tt.fields.worker, tt.fields.cluster)
			if err != nil {
				t.Fatalf("NewWorkerDelegate error = %v", err.Error())
			}
			got, err := delegate.GenerateMachineDeployments(context.Background())
			if err != nil {
				t.Fatalf("workerDelegate.GenerateMachineDeployments() error = %v", err.Error())
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("workerDelegate.GenerateMachineDeployments() = %v, want %v", got, tt.want)
			}
		})
	}
}

type stubChartApplier struct {
	err  error
	opts []gardener.ApplyOption
}

func (a *stubChartApplier) ApplyFromEmbeddedFS(ctx context.Context, embeddedFS embed.FS, chartPath, namespace, name string, opts ...gardener.ApplyOption) error {
	if a.opts != nil {
		if len(a.opts) != len(opts) {
			return fmt.Errorf("expect %d options but got %d", len(a.opts), len(opts))
		}
		for i := range opts {
			if !reflect.DeepEqual(a.opts[i], opts[i]) {
				return fmt.Errorf("expected %v, got %v", a.opts[i], opts[i])
			}
		}
	}
	return a.err
}

func Test_workerDelegate_DeployMachineClasses_Lancer(t *testing.T) {
	name := "my-shoot"
	namespace := "shoot--foobar--gdch"
	machineClassZone1 := machineClass{
		Name:           name + "-1",
		ResourceLabels: map[string]string{},
		Project:        namespace,
		Annotations: map[string]string{
			"description": fmt.Sprintf("Machine of Shoot %s created by machine-controller-manager", name),
		},
		Labels: map[string]string{
			"name": name,
			fmt.Sprintf("kubernetes-io-cluster-%s", "shoot--foobar--gdch"): "true",
			"kubernetes-io-role-node":                                      "true",
		},
		Disks: []*disk{{
			AutoDelete: true,
			Boot:       true,
			SizeGB:     20,
			Labels: map[string]string{
				"name":             name,
				"k8s-cluster-name": "shoot--foobar--gdch",
			},
			Image:   "ubuntu",
			Project: "vm-system",
		}, {
			AutoDelete: false,
			Boot:       false,
			SizeGB:     20,
			Labels: map[string]string{
				"name":             name,
				"k8s-cluster-name": "shoot--foobar--gdch",
			},
		}},
		Secret: &secret{
			CloudConfig: "fake-cloudconfig",
		},
		CredentialsSecretRef: &credentialsSecretRef{
			Name:      "fake-credentialsSecretRef-name",
			Namespace: "fake-credentialsSecretRef-namespace",
		},
	}
	machineClassZone2 := machineClassZone1
	machineClassZone2.Name = name + "-2"

	machineClasses := map[string]interface{}{"machineClasses": []machineClass{
		machineClassZone1,
	}}
	charApplier := &stubChartApplier{
		opts: []gardener.ApplyOption{gardener.Values(machineClasses)},
	}
	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}

	apisgdcinstall.Install(scheme)

	if err := machinev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}

	type fields struct {
		client             client.Client
		decoder            runtime.Decoder
		seedChartApplier   EmbeddedChartApplier
		worker             *extensionsv1alpha1.Worker
		cluster            *extensionscontroller.Cluster
		cloudProfileConfig *apisgdc.CloudProfileConfig
		machineClasses     []machineClass
	}
	tests := []struct {
		name               string
		fields             fields
		wantMachineClasses []machineClass
	}{
		{
			name: "successfully deploy machine classes",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				seedChartApplier: charApplier,
				cloudProfileConfig: &apisgdc.CloudProfileConfig{
					OrgConfig: &apisgdc.OrgConfig{
						RegistryURL:         "foo.com",
						OrgName:             "org-name",
						GlobalManagementAPI: "global.com",
						CAData:              "cadata",
						Zones: []*apisgdc.ZoneEndpoints{
							{Name: "zone1", ManagementAPI: "zone1.com", InfrastructureAPI: "infra.com"},
						},
					},
					MachineImages: []apisgdc.MachineImages{
						{
							Name:    "test-machine-image",
							Project: "test-machine-image-project",
							Versions: []apisgdc.MachineImageVersion{
								{
									Version:      "v1",
									Image:        "test-machine-image-id",
									Architecture: ptr.To(v1betaconstants.ArchitectureAMD64)},
							},
						},
					},
				},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						Region: "europe-west3",
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								Name:        "pool1",
								MachineType: "large",
								Zones: []string{
									"zone1",
								},
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "test-machine-image",
									Version: "v1"},
								Volume: &extensionsv1alpha1.Volume{
									Size: "20",
									Type: ptr.To("pd-standard"),
								},
								UserDataSecretRef:   v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "test-secret-ref-name"}, Key: "serviceaccount.json"},
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
								ProviderConfig: &runtime.RawExtension{
									Raw: []byte(`{"apiVersion":"gdch.provider.extensions.gardener.gdc.goog/v1alpha1","kind":"WorkerConfig","nodeTemplate":{"capacity":{"cpu":"8","memory":"16Gi"},"virtualCapacity":{"gpu":"2"}}}`),
								},
								NodeTemplate: &extensionsv1alpha1.NodeTemplate{
									Capacity: v1.ResourceList{
										"cpu":    resource.MustParse("8"),
										"memory": resource.MustParse("16Gi"),
									},
									VirtualCapacity: v1.ResourceList{
										"gpu": resource.MustParse("2"),
									},
								},
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, nil)},
				},
				cluster: &extensions.Cluster{
					ObjectMeta: metav1.ObjectMeta{Name: "fake--shoot--foobar--gdch"},
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{Version: "1.27"}}},
				},
			},
			wantMachineClasses: []machineClass{{
				Name:           "foobar--gdch-pool1-1-401ed",
				ResourceLabels: map[string]string{"gardener.cloud/purpose": "machineclass"},
				Project:        "test-project",
				Annotations: map[string]string{
					"description": "Machine of Shoot  created by machine-controller-manager",
				},
				Labels: map[string]string{
					"kubernetes-io-cluster-shoot--foobar--gdch": "true",
					"kubernetes-io-role-node":                   "true",
					"name":                                      "",
					gdcconstants.WorkloadLabelSelectorKey:       "fake--shoot--foobar--gdch",
				},
				MachineType: "large",
				NodeTemplate: &machinev1alpha1.NodeTemplate{
					Capacity: v1.ResourceList{
						"cpu":    resource.MustParse("8"),
						"memory": resource.MustParse("16Gi"),
					},
					VirtualCapacity: v1.ResourceList{
						"gpu": resource.MustParse("2"),
					},
					InstanceType: "large",
					Region:       "europe-west3",
					Zone:         "zone1",
				},
				Disks: []*disk{
					{
						Image:      "test-machine-image-id",
						Project:    "test-machine-image-project",
						AutoDelete: true,
						Boot:       true,
						SizeGB:     20,
						Type:       "Standard",
						Labels: map[string]string{
							"kubernetes-io-cluster-shoot--foobar--gdch": "true",
							"kubernetes-io-role-node":                   "true",
							"name":                                      "",
							gdcconstants.WorkloadLabelSelectorKey:       "fake--shoot--foobar--gdch",
						}},
				},
				Secret: &secret{
					CloudConfig: string([]byte(`{"project": "test-project"}`)),
				},
				CredentialsSecretRef: &credentialsSecretRef{
					Name:      "test-secret-ref-name",
					Namespace: "test-secret-ref-namespace",
				},
				SubnetName:    "test-subnet",
				RegistryURL:   "foo.com",
				OrgClusterURL: "zone1.com",
				CAData:        "cadata",
			}},
		},
		{
			name: "successfully deploy machine classes when virtual capacity changes but hash remains the same",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				seedChartApplier: charApplier,
				cloudProfileConfig: &apisgdc.CloudProfileConfig{
					OrgConfig: &apisgdc.OrgConfig{
						RegistryURL:         "foo.com",
						OrgName:             "org-name",
						GlobalManagementAPI: "global.com",
						CAData:              "cadata",
						Zones: []*apisgdc.ZoneEndpoints{
							{Name: "zone1", ManagementAPI: "zone1.com", InfrastructureAPI: "infra.com"},
						},
					},
					MachineImages: []apisgdc.MachineImages{
						{
							Name:    "test-machine-image",
							Project: "test-machine-image-project",
							Versions: []apisgdc.MachineImageVersion{
								{
									Version:      "v1",
									Image:        "test-machine-image-id",
									Architecture: ptr.To(v1betaconstants.ArchitectureAMD64)},
							},
						},
					},
				},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						Region: "europe-west3",
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								Name:        "pool1",
								MachineType: "large",
								Zones: []string{
									"zone1",
								},
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "test-machine-image",
									Version: "v1"},
								Volume: &extensionsv1alpha1.Volume{
									Size: "20",
									Type: ptr.To("pd-standard"),
								},
								UserDataSecretRef:   v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "test-secret-ref-name"}, Key: "serviceaccount.json"},
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
								ProviderConfig: &runtime.RawExtension{
									Raw: []byte(`{"apiVersion":"gdch.provider.extensions.gardener.gdc.goog/v1alpha1","kind":"WorkerConfig","nodeTemplate":{"capacity":{"cpu":"8","memory":"16Gi"},"virtualCapacity":{"gpu":"4"}}}`),
								},
								NodeTemplate: &extensionsv1alpha1.NodeTemplate{
									Capacity: v1.ResourceList{
										"cpu":    resource.MustParse("8"),
										"memory": resource.MustParse("16Gi"),
									},
									VirtualCapacity: v1.ResourceList{
										"gpu": resource.MustParse("4"),
									},
								},
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, nil)},
				},
				cluster: &extensions.Cluster{
					ObjectMeta: metav1.ObjectMeta{Name: "fake--shoot--foobar--gdch"},
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{Version: "1.27"}}},
				},
			},
			wantMachineClasses: []machineClass{{
				Name:           "foobar--gdch-pool1-1-401ed",
				ResourceLabels: map[string]string{"gardener.cloud/purpose": "machineclass"},
				Project:        "test-project",
				Annotations: map[string]string{
					"description": "Machine of Shoot  created by machine-controller-manager",
				},
				Labels: map[string]string{
					"kubernetes-io-cluster-shoot--foobar--gdch": "true",
					"kubernetes-io-role-node":                   "true",
					"name":                                      "",
					gdcconstants.WorkloadLabelSelectorKey:       "fake--shoot--foobar--gdch",
				},
				MachineType: "large",
				NodeTemplate: &machinev1alpha1.NodeTemplate{
					Capacity: v1.ResourceList{
						"cpu":    resource.MustParse("8"),
						"memory": resource.MustParse("16Gi"),
					},
					VirtualCapacity: v1.ResourceList{
						"gpu": resource.MustParse("4"),
					},
					InstanceType: "large",
					Region:       "europe-west3",
					Zone:         "zone1",
				},
				Disks: []*disk{
					{
						Image:      "test-machine-image-id",
						Project:    "test-machine-image-project",
						AutoDelete: true,
						Boot:       true,
						SizeGB:     20,
						Type:       "Standard",
						Labels: map[string]string{
							"kubernetes-io-cluster-shoot--foobar--gdch": "true",
							"kubernetes-io-role-node":                   "true",
							"name":                                      "",
							gdcconstants.WorkloadLabelSelectorKey:       "fake--shoot--foobar--gdch",
						}},
				},
				Secret: &secret{
					CloudConfig: string([]byte(`{"project": "test-project"}`)),
				},
				CredentialsSecretRef: &credentialsSecretRef{
					Name:      "test-secret-ref-name",
					Namespace: "test-secret-ref-namespace",
				},
				SubnetName:    "test-subnet",
				RegistryURL:   "foo.com",
				OrgClusterURL: "zone1.com",
				CAData:        "cadata",
			}},
		},
		{
			name: "successfully deploy machine classes with enableEgress true",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				seedChartApplier: charApplier,
				cloudProfileConfig: &apisgdc.CloudProfileConfig{
					OrgConfig: &apisgdc.OrgConfig{
						RegistryURL:         "foo.com",
						OrgName:             "org-name",
						GlobalManagementAPI: "global.com",
						CAData:              "cadata",
						Zones: []*apisgdc.ZoneEndpoints{
							{Name: "zone1", ManagementAPI: "zone1.com", InfrastructureAPI: "infra.com"},
						},
					},
					MachineImages: []apisgdc.MachineImages{
						{
							Name:    "test-machine-image",
							Project: "test-machine-image-project",
							Versions: []apisgdc.MachineImageVersion{
								{
									Version:      "v1",
									Image:        "test-machine-image-id",
									Architecture: ptr.To(v1betaconstants.ArchitectureAMD64)},
							},
						},
					},
				},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								Name: "pool1",
								Zones: []string{
									"zone1",
								},
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "test-machine-image",
									Version: "v1"},
								Volume: &extensionsv1alpha1.Volume{
									Size: "20",
								},
								UserDataSecretRef:   v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "test-secret-ref-name"}, Key: "serviceaccount.json"},
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, ptr.To(true))},
				},
				cluster: &extensions.Cluster{
					ObjectMeta: metav1.ObjectMeta{Name: "fake--shoot--foobar--gdch"},
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{Version: "1.27"}}},
				},
			},
			wantMachineClasses: []machineClass{{
				Name:           "foobar--gdch-pool1-1-a32b3",
				ResourceLabels: map[string]string{"gardener.cloud/purpose": "machineclass"},
				Project:        "test-project",
				Annotations: map[string]string{
					"description": "Machine of Shoot  created by machine-controller-manager",
				},
				Labels: map[string]string{
					"kubernetes-io-cluster-shoot--foobar--gdch": "true",
					"kubernetes-io-role-node":                   "true",
					"name":                                      "",
					gdcconstants.WorkloadLabelSelectorKey:       "fake--shoot--foobar--gdch",
				},
				Disks: []*disk{
					{
						Image:      "test-machine-image-id",
						Project:    "test-machine-image-project",
						AutoDelete: true,
						Boot:       true,
						SizeGB:     20,
						Labels: map[string]string{
							"kubernetes-io-cluster-shoot--foobar--gdch": "true",
							"kubernetes-io-role-node":                   "true",
							"name":                                      "",
							gdcconstants.WorkloadLabelSelectorKey:       "fake--shoot--foobar--gdch",
						}},
				},
				Secret: &secret{
					CloudConfig: string([]byte(`{"project": "test-project"}`)),
				},
				CredentialsSecretRef: &credentialsSecretRef{
					Name:      "test-secret-ref-name",
					Namespace: "test-secret-ref-namespace",
				},
				SubnetName:    "test-subnet",
				RegistryURL:   "foo.com",
				OrgClusterURL: "zone1.com",
				CAData:        "cadata",
				EnableEgress:  ptr.To(true),
			}},
		},
		{
			name: "successfully deploy machine classes with enableEgress false",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				seedChartApplier: charApplier,
				cloudProfileConfig: &apisgdc.CloudProfileConfig{
					OrgConfig: &apisgdc.OrgConfig{
						RegistryURL:         "foo.com",
						OrgName:             "org-name",
						GlobalManagementAPI: "global.com",
						CAData:              "cadata",
						Zones: []*apisgdc.ZoneEndpoints{
							{Name: "zone1", ManagementAPI: "zone1.com", InfrastructureAPI: "infra.com"},
						},
					},
					MachineImages: []apisgdc.MachineImages{
						{
							Name:    "test-machine-image",
							Project: "test-machine-image-project",
							Versions: []apisgdc.MachineImageVersion{
								{
									Version:      "v1",
									Image:        "test-machine-image-id",
									Architecture: ptr.To(v1betaconstants.ArchitectureAMD64)},
							},
						},
					},
				},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								Name: "pool1",
								Zones: []string{
									"zone1",
								},
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "test-machine-image",
									Version: "v1"},
								Volume: &extensionsv1alpha1.Volume{
									Size: "20",
								},
								UserDataSecretRef:   v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "test-secret-ref-name"}, Key: "serviceaccount.json"},
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, ptr.To(false))},
				},
				cluster: &extensions.Cluster{
					ObjectMeta: metav1.ObjectMeta{Name: "fake--shoot--foobar--gdch"},
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{Version: "1.27"}}},
				},
			},
			wantMachineClasses: []machineClass{{
				Name:           "foobar--gdch-pool1-1-c5b68",
				ResourceLabels: map[string]string{"gardener.cloud/purpose": "machineclass"},
				Project:        "test-project",
				Annotations: map[string]string{
					"description": "Machine of Shoot  created by machine-controller-manager",
				},
				Labels: map[string]string{
					"kubernetes-io-cluster-shoot--foobar--gdch": "true",
					"kubernetes-io-role-node":                   "true",
					"name":                                      "",
					gdcconstants.WorkloadLabelSelectorKey:       "fake--shoot--foobar--gdch",
				},
				Disks: []*disk{
					{
						Image:      "test-machine-image-id",
						Project:    "test-machine-image-project",
						AutoDelete: true,
						Boot:       true,
						SizeGB:     20,
						Labels: map[string]string{
							"kubernetes-io-cluster-shoot--foobar--gdch": "true",
							"kubernetes-io-role-node":                   "true",
							"name":                                      "",
							gdcconstants.WorkloadLabelSelectorKey:       "fake--shoot--foobar--gdch",
						}},
				},
				Secret: &secret{
					CloudConfig: string([]byte(`{"project": "test-project"}`)),
				},
				CredentialsSecretRef: &credentialsSecretRef{
					Name:      "test-secret-ref-name",
					Namespace: "test-secret-ref-namespace",
				},
				SubnetName:    "test-subnet",
				RegistryURL:   "foo.com",
				OrgClusterURL: "zone1.com",
				CAData:        "cadata",
				EnableEgress:  ptr.To(false),
			}},
		},
		{
			name: "successfully deploy existing machine classes",
			fields: fields{
				decoder:          serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				seedChartApplier: charApplier,
				machineClasses:   []machineClass{machineClassZone1},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
					},
				},
			},
			wantMachineClasses: []machineClass{machineClassZone1},
		},
		{
			name: "successfully deploy machine classes with multiple zones",
			fields: fields{
				decoder:          serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				seedChartApplier: charApplier,
				machineClasses:   []machineClass{machineClassZone1, machineClassZone2},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								Name: "pool1",
								Zones: []string{
									"zone1",
									"zone2",
								},
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "test-machine-image",
									Version: "v1"},
								Volume: &extensionsv1alpha1.Volume{
									Size: "20",
								},
								UserDataSecretRef:   v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "test-secret-ref-name"}, Key: "serviceaccount.json"},
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, nil)},
				},
				cluster: &extensions.Cluster{
					ObjectMeta: metav1.ObjectMeta{Name: "fake--shoot--foobar--gdch"},
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{Version: "1.27"}}},
				},
			},
			wantMachineClasses: []machineClass{machineClassZone1, machineClassZone2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			machineClasses := map[string]interface{}{"machineClasses": tt.wantMachineClasses}
			charApplier := &stubChartApplier{
				opts: []gardener.ApplyOption{gardener.Values(machineClasses)},
			}
			w := &workerDelegate{
				client:             tt.fields.client,
				seedChartApplier:   charApplier,
				machineClasses:     tt.fields.machineClasses,
				worker:             tt.fields.worker,
				cluster:            tt.fields.cluster,
				cloudProfileConfig: tt.fields.cloudProfileConfig,
				decoder:            tt.fields.decoder,
			}
			if err := w.DeployMachineClasses(context.Background()); err != nil {
				t.Fatalf("workerDelegate.DeployMachineClasses() error = %v", err)
			}
		})
	}
}

func Test_workerDelegate_DeployMachineClassesErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	apisgdcinstall.Install(scheme)
	if err := machinev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}

	type fields struct {
		client  client.Client
		decoder runtime.Decoder
		scheme  *runtime.Scheme
		worker  *extensionsv1alpha1.Worker
		cluster *extensionscontroller.Cluster
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErrorMsg string
	}{
		{
			name: "should fail because Worker's service account cannot be found",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client:  fake.NewClientBuilder().WithScheme(scheme).Build(),
				worker: &extensionsv1alpha1.Worker{
					Spec: extensionsv1alpha1.WorkerSpec{
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, nil),
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
					},
				},
				cluster: &extensions.Cluster{
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{
								Version: "invalid",
							},
						},
					},
				},
			},
			wantErrorMsg: `secrets "test-secret-ref-name" not found`,
		},
		{
			name: "should fail because the machine image for given architecture cannot be found",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				worker: &extensionsv1alpha1.Worker{
					Spec: extensionsv1alpha1.WorkerSpec{
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "foo",
									Version: "1.0",
								},
								Architecture:        ptr.To(v1betaconstants.ArchitectureAMD64),
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, nil),
					},
				},
				cluster: &extensions.Cluster{
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{
								Version: "1.27.3",
							},
						},
					},
				},
			},
			wantErrorMsg: "could not find machine image with name",
		},
		{
			name: "should fail because the machine image cannot be found",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				worker: &extensionsv1alpha1.Worker{
					Spec: extensionsv1alpha1.WorkerSpec{
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "foo",
									Version: "1.0",
								},
								Architecture:        ptr.To(v1betaconstants.ArchitectureAMD64),
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, nil),
					},
					Status: extensionsv1alpha1.WorkerStatus{
						DefaultStatus: extensionsv1alpha1.DefaultStatus{
							ProviderStatus: &runtime.RawExtension{
								Raw: func() []byte {
									spec := apisgdc.WorkerStatus{
										MachineImages: []apisgdc.MachineImage{{
											Name:         "foo",
											Version:      "1.2",
											Image:        "bar",
											Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
										}},
									}
									raw, _ := json.Marshal(spec)
									return raw
								}(),
							},
						},
					},
				},
				cluster: &extensions.Cluster{
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{
								Version: "1.27.3",
							},
						},
					},
				},
			},
			wantErrorMsg: "could not find machine image with name",
		},

		{
			name: "should fail apply chart",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				worker: &extensionsv1alpha1.Worker{
					Spec: extensionsv1alpha1.WorkerSpec{
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "foo",
									Version: "1.0",
								},
								Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
								Volume: &extensionsv1alpha1.Volume{
									Name: ptr.To("root-vol"),
									Size: "20G",
								},
								UserDataSecretRef:   v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "test-secret-ref-name"}, Key: "serviceaccount.json"},
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
								DataVolumes: []extensionsv1alpha1.DataVolume{{
									Name: "data-vol",
									Size: "20G",
								}},
								NodeTemplate: &extensionsv1alpha1.NodeTemplate{
									Capacity: map[v1.ResourceName]resource.Quantity{
										"memory": func() resource.Quantity {
											q, _ := resource.ParseQuantity("8G")
											return q
										}(),
									},
								},
							},
						},
						InfrastructureProviderStatus: createInfrastructureProviderStatus(true, nil),
					},
					Status: extensionsv1alpha1.WorkerStatus{
						DefaultStatus: extensionsv1alpha1.DefaultStatus{
							ProviderStatus: &runtime.RawExtension{
								Raw: func() []byte {
									spec := apisgdc.WorkerStatus{
										MachineImages: []apisgdc.MachineImage{{
											Name:         "foo",
											Version:      "1.0",
											Image:        "bar",
											Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
										}},
									}
									raw, _ := json.Marshal(spec)
									return raw
								}(),
							},
						},
					},
				},
				cluster: &extensions.Cluster{
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{
								Version: "1.27.3",
							},
						},
					},
				},
			},
			wantErrorMsg: "unable to apply chart"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cpc := &apisgdc.CloudProfileConfig{
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-org-cluster",
				},
				MachineImages: []apisgdc.MachineImages{
					{
						Name: "test",
						Versions: []apisgdc.MachineImageVersion{
							{
								Version:      "1.2.0",
								Image:        "foo",
								Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
							},
						},
					},
				},
			}
			w := &workerDelegate{
				client:  tt.fields.client,
				decoder: tt.fields.decoder,
				scheme:  tt.fields.scheme,
				seedChartApplier: &stubChartApplier{
					err: errors.New("unable to apply chart"),
				},
				cloudProfileConfig: cpc,
				worker:             tt.fields.worker,
				cluster:            tt.fields.cluster,
			}
			err := w.DeployMachineClasses(tt.args.ctx)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrorMsg) {
				t.Fatalf("workerDelegate.GenerateMachineDeployments() error = %v, wantErrMsg %q", err.Error(), tt.wantErrorMsg)
			}
		})
	}
}

func TestNewWorkDelegate(t *testing.T) {
	cpc := &apisgdc.CloudProfileConfig{
		MachineImages: []apisgdc.MachineImages{
			{
				Name:    "test-image-name",
				Project: "test-image-project",
				Versions: []apisgdc.MachineImageVersion{
					{
						Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
						Image:        "test-image-name",
						Version:      "v1.2.3",
					},
					{
						Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
						Image:        "test-image-name",
						Version:      "v1.2.5",
					},
				},
			},
		},
	}

	bs, err := json.Marshal(cpc)
	if err != nil {
		t.Fatalf("json.Marshal(): %v", err)
	}
	cluster := &extensionscontroller.Cluster{
		CloudProfile: &gardencorev1beta1.CloudProfile{
			Spec: gardencorev1beta1.CloudProfileSpec{
				ProviderConfig: &runtime.RawExtension{
					Raw: bs,
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	wd, err := newWorkerDelegate(nil, scheme, nil, nil, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wdImpl, ok := wd.(*workerDelegate)
	if !ok {
		t.Fatalf("unexpected type: %T", wd)
	}

	if diff := cmp.Diff(cpc, wdImpl.cloudProfileConfig); diff != "" {
		t.Errorf("unexpected cloud profile config (-want +got):\n%s", diff)
	}
}

func TestNewWorkDelegateFailure(t *testing.T) {
	scheme := runtime.NewScheme()

	testCases := map[string]struct {
		cluster *extensionscontroller.Cluster
	}{
		"with nil cluster": {
			cluster: nil,
		},
		"with nil cloud profile": {
			cluster: &extensionscontroller.Cluster{
				CloudProfile: nil,
			},
		},
		"with nil cloudprofile.Spec.ProviderConfig.Raw": {
			cluster: &extensionscontroller.Cluster{
				CloudProfile: &gardencorev1beta1.CloudProfile{
					Spec: gardencorev1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: nil,
						},
					},
				},
			},
		},
	}
	expectedError := "no cloud profile config"

	for id, tc := range testCases {
		t.Run(id, func(t *testing.T) {
			switch _, err := newWorkerDelegate(nil, scheme, nil, nil, tc.cluster); {
			case err == nil:
				t.Error("expecting an error but got nil")
			case err.Error() != expectedError:
				t.Errorf("unexpected error: got %q but want %q", err.Error(), expectedError)
			}
		})
	}
}

func Test_workerDelegate_DeployMachineClassesErrors_Lancer(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	apisgdcinstall.Install(scheme)
	if err := machinev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}

	type fields struct {
		client  client.Client
		decoder runtime.Decoder
		scheme  *runtime.Scheme
		worker  *extensionsv1alpha1.Worker
		cluster *extensionscontroller.Cluster
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErrorMsg string
	}{
		{
			name: "should fail because Subnet name in infrastructure status is empty",
			fields: fields{
				decoder: serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder(),
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(&v1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace",
						},
						Data: map[string][]byte{
							"serviceaccount.json": []byte(`{"project": "test-project"}`),
						},
					}).Build(),
				worker: &extensionsv1alpha1.Worker{
					Spec: extensionsv1alpha1.WorkerSpec{
						SecretRef: v1.SecretReference{
							Name:      "test-secret-ref-name",
							Namespace: "test-secret-ref-namespace"},
						Pools: []extensionsv1alpha1.WorkerPool{
							{
								MachineImage: extensionsv1alpha1.MachineImage{
									Name:    "foo",
									Version: "1.2",
								},
								Architecture:        ptr.To(v1betaconstants.ArchitectureAMD64),
								Zones:               []string{"zone2"},
								UserDataSecretRef:   v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "test-secret-ref-name"}, Key: "serviceaccount.json"},
								NodeAgentSecretName: ptr.To("test-secret-ref-name"),
							},
						},
						InfrastructureProviderStatus: &runtime.RawExtension{
							Raw: encode(&apisgdc.InfrastructureStatus{
								Networks: apisgdc.NetworkStatus{
									Zones: []apisgdc.Zones{
										{
											Name:   "zone1",
											Subnet: "test-subnet",
										},
									},
								},
							}),
						},
					},
					Status: extensionsv1alpha1.WorkerStatus{
						DefaultStatus: extensionsv1alpha1.DefaultStatus{
							ProviderStatus: &runtime.RawExtension{
								Raw: func() []byte {
									spec := apisgdc.WorkerStatus{
										MachineImages: []apisgdc.MachineImage{{
											Name:         "foo",
											Version:      "1.2",
											Image:        "bar",
											Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
										}},
									}
									raw, _ := json.Marshal(spec)
									return raw
								}(),
							},
						},
					},
				},
				cluster: &extensions.Cluster{
					ObjectMeta: metav1.ObjectMeta{Name: "fake--shoot--foobar--gdch"},
					Shoot: &gardencorev1beta1.Shoot{
						Spec: gardencorev1beta1.ShootSpec{
							Kubernetes: gardencorev1beta1.Kubernetes{Version: "1.27"}}},
				},
			},
			wantErrorMsg: "cannot find zone network subnet for zone",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cpc := &apisgdc.CloudProfileConfig{
				OrgConfig: &apisgdc.OrgConfig{},
				MachineImages: []apisgdc.MachineImages{
					{
						Name: "test",
						Versions: []apisgdc.MachineImageVersion{
							{
								Version:      "1.2.0",
								Image:        "foo",
								Architecture: ptr.To(v1betaconstants.ArchitectureAMD64),
							},
						},
					},
				},
			}
			w := &workerDelegate{
				client:  tt.fields.client,
				decoder: tt.fields.decoder,
				scheme:  tt.fields.scheme,
				seedChartApplier: &stubChartApplier{
					err: errors.New("unable to apply chart"),
				},
				cloudProfileConfig: cpc,
				worker:             tt.fields.worker,
				cluster:            tt.fields.cluster,
			}
			err := w.DeployMachineClasses(tt.args.ctx)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrorMsg) {
				t.Fatalf("workerDelegate.GenerateMachineDeployments() error = %v, wantErrMsg %q", err.Error(), tt.wantErrorMsg)
			}
		})
	}
}

func createInfrastructureProviderStatus(isLancer bool, enableEgress *bool) *runtime.RawExtension {
	var infraStatus apisgdc.InfrastructureStatus
	if isLancer {
		infraStatus = apisgdc.InfrastructureStatus{
			EnableEgress: enableEgress,
			Networks: apisgdc.NetworkStatus{
				Zones: []apisgdc.Zones{
					{
						Name:   "zone1",
						Subnet: "test-subnet",
					},
				},
				NodeCIDR: "192.168.0.0/16",
			},
		}
	} else {
		infraStatus = apisgdc.InfrastructureStatus{
			EnableEgress: enableEgress,
			Networks: apisgdc.NetworkStatus{
				NodeCIDR: "192.168.0.0/16",
			},
		}
	}

	rawData, _ := json.Marshal(infraStatus)

	return &runtime.RawExtension{
		Raw: rawData,
	}
}

func encode(obj runtime.Object) []byte {
	data, _ := json.Marshal(obj)
	return data
}
