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

package validation

import (
	"testing"

	"github.com/gardener/gardener/pkg/apis/core"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

func TestValidateWorkers(t *testing.T) {
	specPath := field.NewPath("spec")
	providerPath := specPath.Child("provider")
	workersPath := providerPath.Child("workers")

	type args struct {
		workers []core.Worker
		fldPath *field.Path
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "valid Workers for Lancer",
			args: args{
				workers: []core.Worker{
					validWorkerLancer,
				},
				fldPath: workersPath,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if errors := ValidateWorkers(tt.args.workers, allowedZonesTwoZones, infraConfig, tt.args.fldPath); len(errors) > 0 {
				t.Fatalf("ValidateWorkers() got %v, want nil", errors)
			}
		})
	}
}

func TestValidateWorkersLancerErrors(t *testing.T) {
	specPath := field.NewPath("spec")
	providerPath := specPath.Child("provider")
	workersPath := providerPath.Child("workers")
	workerTwoZones := *validWorkerLancer.DeepCopy()
	workerTwoZones.Zones = []string{"some-zone", "some-zone2"}
	workerNoZones := *validWorkerLancer.DeepCopy()
	workerNoZones.Zones = []string{}
	workerNoName := *validWorkerLancer.DeepCopy()
	workerNoName.Name = ""
	workerNoVolumeType := *validWorkerLancer.DeepCopy()
	workerNoVolumeType.Volume.Type = nil

	type args struct {
		workers []core.Worker
		fldPath *field.Path
	}
	tests := []struct {
		name   string
		args   args
		errors field.ErrorList
	}{
		{
			name: "worker has no name",
			args: args{
				workers: []core.Worker{
					workerNoName,
				},
				fldPath: workersPath,
			},
			errors: field.ErrorList{
				field.Required(workersPath.Index(0).Child("name"), "must specify a name for the worker pool"),
			},
		},
		{
			name: "workers have duplicate names",
			args: args{
				workers: []core.Worker{
					validWorkerLancer,
					validWorkerLancer,
				},
				fldPath: workersPath,
			},
			errors: field.ErrorList{
				field.Duplicate(workersPath.Index(1).Child("name"), "test-pool-1"),
			},
		},
		{
			name: "worker has no volume type",
			args: args{
				workers: []core.Worker{
					workerNoVolumeType,
				},
				fldPath: workersPath,
			},
			errors: field.ErrorList{
				field.Required(workersPath.Index(0).Child("volume").Child("type"), "must specify volume type"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateWorkers(tt.args.workers, allowedZonesTwoZones, infraConfig, tt.args.fldPath)
			if !cmp.Equal(errors, tt.errors) {
				t.Fatalf("ValidateWorkers() = %v, want %v,", errors, tt.errors)
			}
		})
	}
}

func TestValidateWorkerConfig(t *testing.T) {
	fldPath := field.NewPath("providerConfig")

	tests := []struct {
		name         string
		workerConfig *gdc.WorkerConfig
		wantErrors   field.ErrorList
	}{
		{
			name:         "nil config",
			workerConfig: nil,
			wantErrors:   field.ErrorList{},
		},
		{
			name: "valid config",
			workerConfig: &gdc.WorkerConfig{
				NodeTemplate: &extensionsv1alpha1.NodeTemplate{
					Capacity: corev1.ResourceList{
						"cpu":    resource.MustParse("8"),
						"memory": resource.MustParse("16Gi"),
					},
					VirtualCapacity: corev1.ResourceList{
						"gpu": resource.MustParse("2"),
					},
				},
			},
			wantErrors: field.ErrorList{},
		},
		{
			name: "negative capacity quantity",
			workerConfig: &gdc.WorkerConfig{
				NodeTemplate: &extensionsv1alpha1.NodeTemplate{
					Capacity: corev1.ResourceList{
						"cpu": resource.MustParse("-1"),
					},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(fldPath.Child("nodeTemplate").Child("capacity").Key("cpu"), "-1", "must be greater than or equal to 0"),
			},
		},
		{
			name: "valid config with fractional capacity but whole virtualCapacity",
			workerConfig: &gdc.WorkerConfig{
				NodeTemplate: &extensionsv1alpha1.NodeTemplate{
					Capacity: corev1.ResourceList{
						"cpu": resource.MustParse("1.5"),
					},
					VirtualCapacity: corev1.ResourceList{
						"gpu": resource.MustParse("2"),
					},
				},
			},
			wantErrors: field.ErrorList{},
		},
		{
			name: "invalid virtualCapacity fractional quantity",
			workerConfig: &gdc.WorkerConfig{
				NodeTemplate: &extensionsv1alpha1.NodeTemplate{
					VirtualCapacity: corev1.ResourceList{
						"gpu": resource.MustParse("1.5"),
					},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(fldPath.Child("nodeTemplate").Child("virtualCapacity").Key("gpu"), "1500m", "must be a whole number"),
			},
		},
		{
			name: "invalid virtualCapacity negative quantity",
			workerConfig: &gdc.WorkerConfig{
				NodeTemplate: &extensionsv1alpha1.NodeTemplate{
					VirtualCapacity: corev1.ResourceList{
						"gpu": resource.MustParse("-1"),
					},
				},
			},
			wantErrors: field.ErrorList{
				field.Invalid(fldPath.Child("nodeTemplate").Child("virtualCapacity").Key("gpu"), "-1", "must be greater than or equal to 0"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := ValidateWorkerConfig(tt.workerConfig, fldPath)
			if !cmp.Equal(errors, tt.wantErrors) {
				t.Fatalf("ValidateWorkerConfig() = %v, want %v", errors, tt.wantErrors)
			}
		})
	}
}
