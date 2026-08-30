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

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

var allowedZones = map[string]bool{
	"valid-zone":   true,
	"valid-zone-2": true,
}

// TestValidateInfrastructureConfigLancer tests valid configurations for Lancer mode.
func TestValidateInfrastructureConfig(t *testing.T) {
	fldPath := &field.Path{}
	validClusterNodeCIDR := "10.0.0.0/8"
	infraNodeCIDR := "10.0.0.0/16"
	subnetName := "valid-parent-subnet"
	tests := []struct {
		name                      string
		infra                     *gdc.InfrastructureConfig
		clusterNetworkingNodeCIDR *string
	}{
		{
			name: "Valid Lancer configuration with single zone",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					NodeCIDR:     infraNodeCIDR,
					ParentSubnet: subnetName,
					Zones: []gdc.Zone{
						{Name: "valid-zone", CIDR: "10.0.1.0/24"}, // Subset of infraNodeCIDR
					},
				},
			},
			clusterNetworkingNodeCIDR: &validClusterNodeCIDR, // infraNodeCIDR is subset of this
		},
		{
			name: "Valid Lancer configuration where NodeCIDR is same as cluster NodeCIDR",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					NodeCIDR:     validClusterNodeCIDR, // NodeCIDR is same as cluster
					ParentSubnet: subnetName,
					Zones: []gdc.Zone{
						{Name: "valid-zone", CIDR: "10.1.0.0/24"}, // Subset of validClusterNodeCIDR
					},
				},
			},
			clusterNetworkingNodeCIDR: &validClusterNodeCIDR,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Testing valid Lancer configurations here
			if errs := ValidateInfrastructureConfig(tt.infra, allowedZones, tt.clusterNetworkingNodeCIDR, fldPath); len(errs) > 0 {
				t.Fatalf("ValidateInfrastructureConfig() returned unexpected errors: %v", errs.ToAggregate().Error())
			}
		})
	}
}

func TestValidateInfrastructureConfigError(t *testing.T) {
	fldPath := field.NewPath("spec")
	validNodeCIDR := "10.0.0.0/8"
	subnetName := "subnet-apc"
	tests := []struct {
		name                      string
		infra                     *gdc.InfrastructureConfig
		clusterNetworkingNodeCIDR *string
		wantErrors                []string
	}{
		{
			name: "Infrastructure.Networks.Zone is nil",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: subnetName,
					NodeCIDR:     validNodeCIDR,
				},
			},
			clusterNetworkingNodeCIDR: &validNodeCIDR,
			wantErrors:                []string{"spec.networks.zones: Required value: must specify the zones"},
		},
		{
			name: "Infrastructure.Networks.NodeCIDR is nil",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: subnetName,
					Zones: []gdc.Zone{
						{Name: "valid-zone", CIDR: "10.0.0.0/24"},
					},
				},
			},
			clusterNetworkingNodeCIDR: &validNodeCIDR,
			wantErrors:                []string{"spec.networks.nodecidr: Required value: must specify the network range for the worker nodecidr"},
		},
		{
			name: "Infrastructure.Networks.NodeCIDR is invalid",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					NodeCIDR:     "invalid",
					ParentSubnet: subnetName,
					Zones: []gdc.Zone{
						{Name: "valid-zone", CIDR: "10.0.0.0/24"},
					},
				},
			},
			clusterNetworkingNodeCIDR: &validNodeCIDR,
			wantErrors:                []string{"spec.networks.nodecidr: Invalid value: \"invalid\": invalid CIDR address: invalid"},
		},
		{
			name: "Infrastrucutre.Networks.NodeCIDR is a superset of Cluster.Networking.NodeCIDR",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					NodeCIDR:     validNodeCIDR,
					ParentSubnet: subnetName,
					Zones: []gdc.Zone{
						{Name: "valid-zone", CIDR: "10.0.0.0/6"},
					},
				},
			},
			clusterNetworkingNodeCIDR: &validNodeCIDR,
			wantErrors: []string{"spec.networks.zones[0].CIDR: Invalid value: \"10.0.0.0/6\": must be valid canonical CIDR",
				"spec.networks.zones[0].CIDR: Invalid value: \"10.0.0.0/6\": must be a subset of \"spec.networks.nodecidr\" (\"10.0.0.0/8\")",
			},
		},
		{
			name: "Infrastructure.Networks.ParentSubnet and Infrastructure.Networks.ParentReference are both nil",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					NodeCIDR: validNodeCIDR,
					Zones: []gdc.Zone{
						{Name: "valid-zone", CIDR: "10.0.0.0/24"},
					},
				},
			},
			clusterNetworkingNodeCIDR: &validNodeCIDR,
			wantErrors:                []string{"spec.networks.parentReference: Invalid value: null: either parentSubnet or parentReference must be specified"},
		},
		{
			name: "Infrastructure.Networks.Zone.Name is nil",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					NodeCIDR:     validNodeCIDR,
					ParentSubnet: subnetName,
					Zones: []gdc.Zone{
						{CIDR: "10.0.0.0/24"},
					},
				},
			},
			clusterNetworkingNodeCIDR: &validNodeCIDR,
			wantErrors:                []string{"spec.networks.zones[0].name: Required value: must specify the zone name"},
		},
		{
			name: "Infrastructure.Networks.Zone.Name must be a supported zone",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					NodeCIDR:     validNodeCIDR,
					ParentSubnet: subnetName,
					Zones: []gdc.Zone{
						{Name: "invalid-zone", CIDR: "10.0.0.0/24"},
					},
				},
			},
			clusterNetworkingNodeCIDR: &validNodeCIDR,
			wantErrors:                []string{"spec.networks.zones[0].name: Unsupported value: \"invalid-zone\": supported values: \"valid-zone\", \"valid-zone-2\""},
		},
		{
			name: "Infrastructure.Networks.Zone has overlapping CIDRs",
			infra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					NodeCIDR:     validNodeCIDR,
					ParentSubnet: subnetName,
					Zones: []gdc.Zone{
						{Name: "valid-zone", CIDR: "10.0.0.0/28"},
						{Name: "valid-zone-2", CIDR: "10.0.0.0/28"},
					},
				},
			},
			wantErrors: []string{
				"spec.networks.zones[1].CIDR: Invalid value: \"10.0.0.0/28\": zone CIDR must not overlap with other zone CIDRs",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateInfrastructureConfig(tt.infra, allowedZones, tt.clusterNetworkingNodeCIDR, fldPath)
			var gotErrors []string
			for _, err := range errs {
				gotErrors = append(gotErrors, err.Error())
			}
			if diff := cmp.Diff(tt.wantErrors, gotErrors); diff != "" {
				t.Errorf("ValidateInfrastructureConfig() mismatch want: %s\n, got: %s\n diff: %s", tt.wantErrors, gotErrors, diff)
			}
		})
	}
}

func TestValidateInfrastructureConfigUpdate(t *testing.T) {
	fldPath := field.NewPath("spec")
	oldProject := "old-project"
	newProject := "new-project"

	tests := []struct {
		name        string
		oldInfra    *gdc.InfrastructureConfig
		newInfra    *gdc.InfrastructureConfig
		wantDetails []string
	}{
		{
			name: "No change",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			wantDetails: nil,
		},
		{
			name: "Change parentSubnet fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "new-subnet",
				},
			},
			wantDetails: []string{"spec.networks.parentSubnet: parentSubnet is immutable"},
		},
		{
			name: "Change parentSubnetProject fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet:        "some-subnet",
					ParentSubnetProject: "old-project",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet:        "some-subnet",
					ParentSubnetProject: "new-project",
				},
			},
			wantDetails: []string{"spec.networks.parentSubnetProject: parentSubnetProject is immutable"},
		},
		{
			name: "Change parentSubnet with parentReference fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "new-subnet",
					ParentReference: &gdc.SubnetReference{
						Name: "some-reference",
					},
				},
			},
			wantDetails: []string{
				"spec.networks.parentSubnet: parentSubnet is immutable",
				"spec.networks.parentReference: cannot add parentReference without migration",
			},
		},
		{
			name: "Remove parentSubnet without parentReference fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "",
				},
			},
			wantDetails: []string{"spec.networks.parentSubnet: cannot remove parentSubnet without providing parentReference"},
		},
		{
			name: "Migrate to parentReference succeeds",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "",
					ParentReference: &gdc.SubnetReference{
						Name: "old-subnet",
					},
				},
			},
			wantDetails: nil,
		},
		{
			name: "Migrate to parentReference with matching project succeeds",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet:        "old-subnet",
					ParentSubnetProject: "old-project",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "",
					ParentReference: &gdc.SubnetReference{
						Name:      "old-subnet",
						Namespace: &oldProject,
					},
				},
			},
			wantDetails: nil,
		},
		{
			name: "Migrate to parentReference with non-matching name fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "",
					ParentReference: &gdc.SubnetReference{
						Name: "new-subnet",
					},
				},
			},
			wantDetails: []string{"spec.networks.parentReference.name: parentReference name must match the old parentSubnet during migration"},
		},
		{
			name: "Migrate to parentReference with non-matching project fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet:        "old-subnet",
					ParentSubnetProject: "old-project",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "",
					ParentReference: &gdc.SubnetReference{
						Name:      "old-subnet",
						Namespace: &newProject,
					},
				},
			},
			wantDetails: []string{"spec.networks.parentReference.namespace: parentReference namespace must match the old parentSubnetProject during migration"},
		},
		{
			name: "Migrate to parentReference with non-matching type fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "",
					ParentReference: &gdc.SubnetReference{
						Name: "old-subnet",
						Type: gdc.SubnetGroup,
					},
				},
			},
			wantDetails: []string{"spec.networks.parentReference.type: parentReference type must be SingleSubnet during migration from parentSubnet"},
		},
		{
			name: "Change parentReference name fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentReference: &gdc.SubnetReference{
						Name: "old-ref",
					},
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentReference: &gdc.SubnetReference{
						Name: "new-ref",
					},
				},
			},
			wantDetails: []string{"spec.networks.parentReference.name: parentReference name is immutable"},
		},
		{
			name: "Remove parentReference fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentReference: &gdc.SubnetReference{
						Name: "old-ref",
					},
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{},
			},
			wantDetails: []string{"spec.networks.parentReference: cannot remove parentReference"},
		},
		{
			name: "Add parentReference without migration fails",
			oldInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
				},
			},
			newInfra: &gdc.InfrastructureConfig{
				Networks: gdc.NetworkConfig{
					ParentSubnet: "old-subnet",
					ParentReference: &gdc.SubnetReference{
						Name: "some-ref",
					},
				},
			},
			wantDetails: []string{"spec.networks.parentReference: cannot add parentReference without migration"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateInfrastructureConfigUpdate(tt.oldInfra, tt.newInfra, fldPath)
			var gotDetails []string
			for _, err := range errs {
				gotDetails = append(gotDetails, err.Field+": "+err.Detail)
			}
			if diff := cmp.Diff(tt.wantDetails, gotDetails); diff != "" {
				t.Errorf("ValidateInfrastructureConfigUpdate() mismatch want: %s\n, got: %s\n diff: %s", tt.wantDetails, gotDetails, diff)
			}
		})
	}
}
