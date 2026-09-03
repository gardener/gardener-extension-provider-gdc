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
	"k8s.io/utils/ptr"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

var (
	allowedRegions = map[string]bool{
		"test-region-1-zone-1": true,
		"test-region-1-zone-2": true,
		"test-region-2-zone-1": true,
		"test-region-2-zone-2": true,
	}
)

func TestValidateCloudProfileConfig_Success(t *testing.T) {
	var nilPath *field.Path

	tests := []struct {
		name               string
		cloudProfileConfig *apisgdc.CloudProfileConfig
		machineImages      []core.MachineImage
		allowedRegions     map[string]bool
	}{
		{
			name: "valid configuration - Lancer Architecture",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{
						Name: "machine-img-1",
						Versions: []apisgdc.MachineImageVersion{
							{
								Version:      "1.20.0",
								Image:        "path/to/image-v1.20.0",
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
			},
			machineImages: []core.MachineImage{
				{
					Name: "machine-img-1",
					Versions: []core.MachineImageVersion{
						{
							ExpirableVersion: core.ExpirableVersion{Version: "1.20.0"},
						},
					},
				},
			},
			allowedRegions: allowedRegions,
		},
		{
			name: "caData set, registryURL missing (valid)",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"}},
					RegistryURL:         "",
					CAData:              "test-ca-data",
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
		},
		{
			name: "caData, Registry - both missing (valid)",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"}},
					RegistryURL:         "",
					CAData:              "",
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
		},
		{
			name: "orgConfig - Registry - both set (valid)",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"}},
					RegistryURL:         "test-registry-url",
					CAData:              "test-ca-data",
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorList := ValidateCloudProfileConfig(tt.cloudProfileConfig, tt.machineImages, nilPath, tt.allowedRegions)

			if len(errorList) > 0 {
				t.Errorf("ValidateCloudProfileConfig() returned errors for a valid configuration: %v", errorList)
			}
		})
	}
}

func TestValidateCloudProfileConfig_Errors(t *testing.T) {
	var nilPath *field.Path

	tests := []struct {
		name               string
		cloudProfileConfig *apisgdc.CloudProfileConfig
		machineImages      []core.MachineImage
		allowedRegions     map[string]bool
		wantErrors         []string
	}{
		{
			name: "machine images - missing mapping for image",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{
						Name: "machine-img-1",
						Versions: []apisgdc.MachineImageVersion{
							{Version: "v1", Image: "img-v1", Architecture: ptr.To("amd64")},
						},
					},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"}},
				},
			},
			machineImages: []core.MachineImage{
				{
					Name: "machine-img-1",
					Versions: []core.MachineImageVersion{
						{ExpirableVersion: core.ExpirableVersion{Version: "v1"}},
					},
				},
				{
					Name: "machine-img-2",
					Versions: []core.MachineImageVersion{
						{ExpirableVersion: core.ExpirableVersion{Version: "v2"}},
					},
				},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"machineImages"},
		},
		{
			name: "machine images - missing version mapping for Gardener image version",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{
						Name: "machine-img-1",
						Versions: []apisgdc.MachineImageVersion{
							{Version: "v1", Image: "img-v1", Architecture: ptr.To("amd64")},
						},
					},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"}},
				},
			},
			machineImages: []core.MachineImage{
				{
					Name: "machine-img-1",
					Versions: []core.MachineImageVersion{
						{ExpirableVersion: core.ExpirableVersion{Version: "v1"}},
						{ExpirableVersion: core.ExpirableVersion{Version: "v2"}},
						{ExpirableVersion: core.ExpirableVersion{Version: "v3"}},
					},
				},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"machineImages[0].versions", "machineImages[0].versions"},
		},
		{
			name: "machine images - empty image path in version mapping",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{
						Name: "machine-img-1",
						Versions: []apisgdc.MachineImageVersion{
							{Version: "v1", Image: "", Architecture: ptr.To("amd64")},
							{Version: "v2", Image: "img-v2", Architecture: ptr.To("amd64")},
						},
					},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"}},
				},
			},
			machineImages: []core.MachineImage{
				{
					Name: "machine-img-1",
					Versions: []core.MachineImageVersion{
						{ExpirableVersion: core.ExpirableVersion{Version: "v1"}},
						{ExpirableVersion: core.ExpirableVersion{Version: "v2"}},
					},
				},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"machineImages[0].versions[0].image"},
		},
		{
			name: "machine images - invalid architecture in version mapping",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{
						Name: "machine-img-1",
						Versions: []apisgdc.MachineImageVersion{
							{Version: "v1", Image: "img-v1", Architecture: ptr.To("invalid-arch")},
						},
					},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"}},
				},
			},
			machineImages: []core.MachineImage{
				{
					Name: "machine-img-1",
					Versions: []core.MachineImageVersion{
						{ExpirableVersion: core.ExpirableVersion{Version: "v1"}},
					},
				},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"machineImages[0].versions[0].architecture"},
		},
		{
			name: "orgConfig - missing",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: nil,
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"orgConfig"},
		},
		{
			name: "orgConfig - missing zones",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               nil,
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"orgConfig.zones"},
		},
		{
			name: "orgConfig - empty zones slice",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{},
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"orgConfig.zones"},
		},
		{
			name: "orgConfig - invalid zone name",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones: []*apisgdc.ZoneEndpoints{
						{
							Name:              "test-zone-1a",
							ManagementAPI:     "m",
							InfrastructureAPI: "test-infra-api",
						},
					},
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"orgConfig.zones[0].name"},
		},
		{
			name: "orgConfig - missing zone name",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones: []*apisgdc.ZoneEndpoints{
						{
							Name:              "",
							ManagementAPI:     "m",
							InfrastructureAPI: "test-infra-api",
						},
					},
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"orgConfig.zones[0].name"},
		},
		{
			name: "orgConfig - missing zone managementAPI",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones: []*apisgdc.ZoneEndpoints{
						{
							Name:              "test-region-1-zone-1",
							ManagementAPI:     "",
							InfrastructureAPI: "test-infra-api",
						},
					},
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"orgConfig.zones[0].managementAPI"},
		},
		{
			name: "orgConfig - missing zone infrastructureAPI",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones: []*apisgdc.ZoneEndpoints{
						{
							Name:              "test-region-1-zone-1",
							ManagementAPI:     "m",
							InfrastructureAPI: "",
						},
					},
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"orgConfig.zones[0].infrastructureAPI"},
		},
		{
			name: "orgConfig - Registry - registryURL set, caData missing",
			cloudProfileConfig: &apisgdc.CloudProfileConfig{
				MachineImages: []apisgdc.MachineImages{
					{Name: "img", Versions: []apisgdc.MachineImageVersion{{Version: "1.0", Image: "img", Architecture: ptr.To("amd64")}}},
				},
				OrgConfig: &apisgdc.OrgConfig{
					GlobalManagementAPI: "test-global-api",
					Zones:               []*apisgdc.ZoneEndpoints{{Name: "test-region-1-zone-1", ManagementAPI: "test-management-api", InfrastructureAPI: "test-infra-api"}},
					RegistryURL:         "test-registry-url",
					CAData:              "",
				},
			},
			machineImages: []core.MachineImage{
				{Name: "img", Versions: []core.MachineImageVersion{{ExpirableVersion: core.ExpirableVersion{Version: "1.0"}}}},
			},
			allowedRegions: allowedRegions,
			wantErrors:     []string{"orgConfig.caData"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorList := ValidateCloudProfileConfig(tt.cloudProfileConfig, tt.machineImages, nilPath, tt.allowedRegions)

			if len(errorList) == 0 {
				t.Fatalf("ValidateCloudProfileConfig() returned no errors for an invalid configuration, expected errors on fields: %v", tt.wantErrors)
			}
			errorFields := []string{}
			for _, e := range errorList {
				errorFields = append(errorFields, e.Field)
			}

			// Compare the actual error fields with the expected ones
			if diff := cmp.Diff(tt.wantErrors, errorFields); diff != "" {
				t.Errorf("ValidateCloudProfileConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
