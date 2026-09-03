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
	"encoding/json"
	"strings"
	"testing"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
)

func Test_workerDelegate_UpdateMachineImagesStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := apisgdc.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	if err := extensionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}

	fakeWorker := &extensionsv1alpha1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fake-worker",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(fakeWorker).
		WithStatusSubresource(fakeWorker).Build()

	type fields struct {
		client             client.Client
		scheme             *runtime.Scheme
		machineImages      []apisgdc.MachineImage
		cloudProfileConfig *apisgdc.CloudProfileConfig
		worker             *extensionsv1alpha1.Worker
		cluster            *extensionscontroller.Cluster
	}

	tests := []struct {
		name             string
		fields           fields
		wantWorkerStatus *extensionsv1alpha1.WorkerStatus
	}{
		{
			name: "successfully update  machine images status",
			fields: fields{
				client:        c,
				scheme:        scheme,
				machineImages: nil,
				cloudProfileConfig: &apisgdc.CloudProfileConfig{
					MachineImages: []apisgdc.MachineImages{{
						Name:    "fake-machine-image",
						Project: "fake-machine-image-project",
						Versions: []apisgdc.MachineImageVersion{{
							Version:      "1",
							Image:        "fake-image",
							Architecture: ptr.To(v1beta1constants.ArchitectureAMD64),
						}},
					}, {
						Name: "fake-machine-image-2",
						Versions: []apisgdc.MachineImageVersion{{
							Version:      "1",
							Image:        "fake-image-2",
							Architecture: ptr.To(v1beta1constants.ArchitectureAMD64),
						}},
					}},
				},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-worker",
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						Pools: []extensionsv1alpha1.WorkerPool{{
							MachineImage: extensionsv1alpha1.MachineImage{
								Name:    "fake-machine-image",
								Version: "1",
							},
						}, {
							MachineImage: extensionsv1alpha1.MachineImage{
								Name:    "fake-machine-image-2",
								Version: "1",
							},
						}},
					},
				},
			},
			wantWorkerStatus: &extensionsv1alpha1.WorkerStatus{
				DefaultStatus: extensionsv1alpha1.DefaultStatus{
					ProviderStatus: &runtime.RawExtension{
						Raw: func() []byte {
							obj := &v1alpha1.WorkerStatus{
								MachineImages: []v1alpha1.MachineImage{
									{
										Name:         "fake-machine-image",
										Project:      "fake-machine-image-project",
										Version:      "1",
										Image:        "fake-image",
										Architecture: ptr.To(v1beta1constants.ArchitectureAMD64),
									},
									{
										Name:         "fake-machine-image-2",
										Project:      "vm-system",
										Version:      "1",
										Image:        "fake-image-2",
										Architecture: ptr.To(v1beta1constants.ArchitectureAMD64),
									},
								},
								TypeMeta: metav1.TypeMeta{
									APIVersion: v1alpha1.SchemeGroupVersion.String(),
									Kind:       "WorkerStatus",
								},
							}
							raw, _ := json.Marshal(obj)
							return raw
						}(),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerDelegate{
				client:             tt.fields.client,
				scheme:             tt.fields.scheme,
				machineImages:      tt.fields.machineImages,
				cloudProfileConfig: tt.fields.cloudProfileConfig,
				worker:             tt.fields.worker,
				cluster:            tt.fields.cluster,
			}
			if err := w.UpdateMachineImagesStatus(context.TODO()); err != nil {
				t.Errorf("workerDelegate.UpdateMachineImagesStatus() error = %v,", err)
			}
			if !cmp.Equal(w.worker.Status.ProviderStatus, tt.wantWorkerStatus.ProviderStatus,
				cmp.Transformer("AsObject", func(raw []byte) *v1alpha1.WorkerStatus {
					obj := &v1alpha1.WorkerStatus{}
					if err := json.Unmarshal(raw, obj); err != nil {
						t.Fatal(err)
					}
					return obj
				})) {
				t.Errorf("workerDelegate.worker = %v, want %v,", w.worker.Status.ProviderStatus, tt.wantWorkerStatus.ProviderStatus)
			}
		})
	}
}

func Test_workerDelegate_UpdateMachineImagesStatusErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := apisgdc.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	if err := extensionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to add to scheme %v", err)
	}
	decoder := serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder()

	fakeWorker := &extensionsv1alpha1.Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fake-worker",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(fakeWorker).
		WithStatusSubresource(fakeWorker).Build()

	type fields struct {
		client             client.Client
		scheme             *runtime.Scheme
		machineImages      []apisgdc.MachineImage
		cloudProfileConfig *apisgdc.CloudProfileConfig
		worker             *extensionsv1alpha1.Worker
	}

	tests := []struct {
		name         string
		fields       fields
		wantErrorMsg string
	}{
		{
			name: "should fail because machine image is not found",
			fields: fields{
				client:        c,
				scheme:        scheme,
				machineImages: nil,
				cloudProfileConfig: &apisgdc.CloudProfileConfig{
					MachineImages: []apisgdc.MachineImages{{
						Name:     "fake-machine-image",
						Versions: []apisgdc.MachineImageVersion{},
					}},
				},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-worker",
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						Pools: []extensionsv1alpha1.WorkerPool{{
							MachineImage: extensionsv1alpha1.MachineImage{
								Name:    "fake-machine-image",
								Version: "1",
							},
						}},
					},
				},
			},
			wantErrorMsg: "unable to generate the machine images",
		},
		{
			name: "should fail because worker status cannot be decoded",
			fields: fields{
				client:        c,
				scheme:        scheme,
				machineImages: nil,
				cloudProfileConfig: &apisgdc.CloudProfileConfig{
					MachineImages: []apisgdc.MachineImages{{
						Name: "fake-machine-image",
						Versions: []apisgdc.MachineImageVersion{{
							Version:      "1",
							Image:        "fake-image",
							Architecture: ptr.To(v1beta1constants.ArchitectureAMD64),
						}},
					}},
				},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-worker",
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						Pools: []extensionsv1alpha1.WorkerPool{{
							MachineImage: extensionsv1alpha1.MachineImage{
								Name:    "fake-machine-image",
								Version: "1",
							},
						}},
					},
					Status: extensionsv1alpha1.WorkerStatus{
						DefaultStatus: extensionsv1alpha1.DefaultStatus{
							ProviderStatus: &runtime.RawExtension{
								Raw: []byte("invalid-status"),
							},
						},
					},
				},
			},
			wantErrorMsg: "unable to decode the worker provider status",
		},
		{
			name: "should fail because worker status cannot be patched", fields: fields{
				client:        c,
				scheme:        scheme,
				machineImages: nil,
				cloudProfileConfig: &apisgdc.CloudProfileConfig{
					MachineImages: []apisgdc.MachineImages{{
						Name: "fake-machine-image",
						Versions: []apisgdc.MachineImageVersion{{
							Version:      "1",
							Image:        "fake-image",
							Architecture: ptr.To(v1beta1constants.ArchitectureAMD64),
						}},
					}},
				},
				worker: &extensionsv1alpha1.Worker{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fake-worker-not-found",
					},
					Spec: extensionsv1alpha1.WorkerSpec{
						Pools: []extensionsv1alpha1.WorkerPool{{
							MachineImage: extensionsv1alpha1.MachineImage{
								Name:    "fake-machine-image",
								Version: "1",
							},
						}},
					},
				},
			},
			wantErrorMsg: "unable to update worker provider status",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &workerDelegate{
				decoder:            decoder,
				client:             tt.fields.client,
				scheme:             tt.fields.scheme,
				machineImages:      tt.fields.machineImages,
				cloudProfileConfig: tt.fields.cloudProfileConfig,
				worker:             tt.fields.worker,
			}
			err := w.UpdateMachineImagesStatus(context.TODO())
			if err == nil {
				t.Errorf("workerDelegate.UpdateMachineImagesStatus() expected to return with an error")
				return
			}
			if !strings.Contains(err.Error(), tt.wantErrorMsg) {
				t.Errorf("workerDelegate.UpdateMachineImagesStatus() error = %v, wantErrMsg %v", err.Error(), tt.wantErrorMsg)
				return
			}
		})
	}
}
