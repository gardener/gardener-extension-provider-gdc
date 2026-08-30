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
	"fmt"

	"slices"

	"github.com/gardener/gardener/pkg/api/core/helper"
	"github.com/gardener/gardener/pkg/apis/core"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	"k8s.io/apimachinery/pkg/util/validation/field"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

// ValidateCloudProfileConfig validates a CloudProfileConfig object.
func ValidateCloudProfileConfig(cpConfig *apisgdc.CloudProfileConfig, machineImages []core.MachineImage, fldPath *field.Path, allowedRegions map[string]bool) field.ErrorList {
	allErrs := field.ErrorList{}
	machineImagesPath := fldPath.Child("machineImages")
	orgConfigPath := fldPath.Child("orgConfig")

	allErrs = append(allErrs, validateMachineImages(cpConfig, machineImages, machineImagesPath)...)
	allErrs = append(allErrs, ValidateOrgConfig(cpConfig, orgConfigPath, allowedRegions)...)

	return allErrs
}

func validateVersions(versionsConfig []apisgdc.MachineImageVersion, versions []core.ExpirableVersion, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for _, version := range versions {
		var processed bool
		for j, versionConfig := range versionsConfig {
			jdxPath := fldPath.Index(j)
			if version.Version == versionConfig.Version {
				if len(versionConfig.Image) == 0 {
					allErrs = append(allErrs, field.Required(jdxPath.Child("image"), "must provide an image"))
				}
				if !slices.Contains(v1beta1constants.ValidArchitectures, *versionConfig.Architecture) {
					allErrs = append(allErrs, field.NotSupported(jdxPath.Child("architecture"), *versionConfig.Architecture, v1beta1constants.ValidArchitectures))
				}
				processed = true
				break
			}
		}
		if !processed {
			allErrs = append(allErrs, field.Required(fldPath, fmt.Sprintf("must provide an image mapping for version %q", version.Version)))
		}
	}

	return allErrs
}

func validateMachineImages(cpConfig *apisgdc.CloudProfileConfig, machineImages []core.MachineImage, machineImagesPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for _, image := range machineImages {
		var processed bool
		for i, imageConfig := range cpConfig.MachineImages {
			if image.Name == imageConfig.Name {
				allErrs = append(allErrs, validateVersions(imageConfig.Versions, helper.ToExpirableVersions(image.Versions), machineImagesPath.Index(i).Child("versions"))...)
				processed = true
				break
			}
		}
		if !processed && len(image.Versions) > 0 {
			allErrs = append(allErrs, field.Required(machineImagesPath, fmt.Sprintf("must provide an image mapping for image %q", image.Name)))
		}
	}
	return allErrs
}

func ValidateOrgConfig(cpConfig *apisgdc.CloudProfileConfig, orgFieldsPath *field.Path, allowedRegions map[string]bool) field.ErrorList {
	allErrs := field.ErrorList{}

	if cpConfig.OrgConfig == nil {
		allErrs = append(allErrs, field.Required(orgFieldsPath, "must provide an orgConfig"))
		return allErrs
	}

	zonesPath := orgFieldsPath.Child("zones")
	if len(cpConfig.OrgConfig.Zones) == 0 {
		allErrs = append(allErrs, field.Required(zonesPath, "At least one zone must be specified"))
	} else {
		for i, zone := range cpConfig.OrgConfig.Zones {
			zonePath := zonesPath.Index(i)
			allErrs = append(allErrs, validateZoneEndpoints(zone, allowedRegions, zonePath)...)
		}
	}

	allErrs = append(allErrs, validateRegistryConfig(cpConfig.OrgConfig, orgFieldsPath)...)

	return allErrs
}

// validateZoneEndpoints validates a single ZoneEndpoints entry.
func validateZoneEndpoints(zone *apisgdc.ZoneEndpoints, allowedRegions map[string]bool, zonePath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if zone.Name == "" {
		allErrs = append(allErrs, field.Required(zonePath.Child("name"), "name must be specified for this zone entry"))
		return allErrs
	}

	if !allowedRegions[zone.Name] {
		allErrs = append(allErrs, field.Invalid(
			zonePath.Child("name"),
			zone.Name,
			fmt.Sprintf("zone %v is not a supported zone, must be defined in the CloudProfile regions", zone.Name),
		))
	}
	if zone.ManagementAPI == "" {
		allErrs = append(allErrs, field.Required(
			zonePath.Child("managementAPI"),
			"managementAPI must be specified for this zone entry",
		))
	}
	if zone.InfrastructureAPI == "" {
		allErrs = append(allErrs, field.Required(
			zonePath.Child("infrastructureAPI"),
			"infrastructureAPI must be specified for this zone entry",
		))
	}

	return allErrs
}

// validateRegistryConfig validates the CAData/RegistryURL dependency.
func validateRegistryConfig(orgConfig *apisgdc.OrgConfig, orgFieldsPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if orgConfig.RegistryURL != "" && orgConfig.CAData == "" {
		allErrs = append(allErrs, field.Required(orgFieldsPath.Child("caData"), "caData must be specified when registryURL is set"))
	}
	return allErrs
}
