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
	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"

	"k8s.io/utils/ptr"
)

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
		Zones: []string{"some-zone"},
		Volume: &core.Volume{
			Name:       ptr.To("test-volume-name-1"),
			Type:       ptr.To("test-volume-type-1"),
			VolumeSize: "test-volume-size-1",
		},
	}

	infraConfig = &gdc.InfrastructureConfig{
		Networks: gdc.NetworkConfig{
			NodeCIDR:     "10.1.0.0/24",
			ParentSubnet: "parentSubnet",
			Zones: []gdc.Zone{
				{Name: "some-zone", CIDR: "10.1.0.0/24"},
			},
		},
	}
	allowedZonesTwoZones = map[string]bool{
		"some-zone":  true,
		"some-zone2": true,
	}
)

func Test_ValidateNetworkingError(t *testing.T) {
	fldPath := &field.Path{}
	invalidCIDR := "invalid"
	tests := []struct {
		name       string
		networking *core.Networking
	}{
		{
			name:       "nil Nodes",
			networking: &core.Networking{},
		},
		{
			name: "invalid Nodes",
			networking: &core.Networking{
				Nodes: &invalidCIDR,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateNetworking(tt.networking, fldPath)
			if len(errs) == 0 {
				t.Fatalf("ValidateNetworking() want error, got nil")
			}
		})
	}
}

func Test_ValidateNetworking(t *testing.T) {
	fldPath := &field.Path{}
	validCIDR := "10.0.0.0/32"
	tests := []struct {
		name       string
		networking *core.Networking
	}{
		{
			name: "valid CIDR",
			networking: &core.Networking{
				Nodes: &validCIDR,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if errs := ValidateNetworking(tt.networking, fldPath); len(errs) > 0 {
				t.Fatalf("ValidateNetworking()  want error, got nil")
			}
		})
	}
}

func TestValidateWorkersErrors(t *testing.T) {
	specPath := field.NewPath("spec")
	providerPath := specPath.Child("provider")
	workersPath := providerPath.Child("workers")
	workerTwoZones := *validWorkerLancer.DeepCopy()
	workerTwoZones.Zones = []string{"some-zone", "some-zone2"}
	workerNoZones := *validWorkerLancer.DeepCopy()
	workerNoZones.Zones = []string{}
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
			name: "worker zone in cloudprofile but not in infrastructure config",
			args: args{
				workers: []core.Worker{workerTwoZones},
				fldPath: workersPath,
			},
			errors: field.ErrorList{
				field.Invalid(workersPath.Index(0).Child("zones").Index(1), "some-zone2", "zone must be defined in the cloudprofile and Infrastructure Config"),
			},
		},
		{
			name: "worker has no zones",
			args: args{
				workers: []core.Worker{
					workerNoZones,
				},
				fldPath: workersPath,
			},
			errors: field.ErrorList{
				field.Required(workersPath.Index(0).Child("zones"), "must specify at least one zone"),
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
