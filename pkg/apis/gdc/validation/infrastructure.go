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
	"errors"
	"sort"

	"github.com/gardener/gardener/pkg/utils/validation/cidr"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

// ValidateInfrastructureConfigUpdate validates a InfrastructureConfig object.
func ValidateInfrastructureConfigUpdate(oldConfig, newConfig *gdc.InfrastructureConfig, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if oldConfig == nil {
		allErrs = append(allErrs, field.InternalError(fldPath, errors.New("old infrastructure config is nil")))
		return allErrs
	}
	if newConfig == nil {
		allErrs = append(allErrs, field.InternalError(fldPath, errors.New("new infrastructure config is nil")))
		return allErrs
	}

	networksPath := fldPath.Child("networks")

	oldNodeCIDRVal := oldConfig.Networks.NodeCIDR
	newNodeCIDRVal := newConfig.Networks.NodeCIDR
	nodeCidrFieldPath := networksPath.Child("nodeCIDR")

	if oldNodeCIDRVal != newNodeCIDRVal {
		allErrs = append(allErrs, field.Invalid(nodeCidrFieldPath, newNodeCIDRVal, "nodeCIDR is immutable"))
	}

	// TODO(b/500429651): delete the validation of updating parentSubnet to parentReference after SAP has fully migrated to use parentReference
	oldParentPool := oldConfig.Networks.ParentSubnet
	newParentPool := newConfig.Networks.ParentSubnet
	parentFieldPath := networksPath.Child("parentSubnet")

	oldParentProject := oldConfig.Networks.ParentSubnetProject
	newParentProject := newConfig.Networks.ParentSubnetProject
	projectFieldPath := networksPath.Child("parentSubnetProject")

	// A migration is identified when the old parentSubnet was set, the new parentSubnet is removed (set to ""),
	// and a new parentReference is provided.
	isMigration := oldParentPool != newParentPool && newParentPool == "" && newConfig.Networks.ParentReference != nil

	if isMigration {
		allErrs = append(allErrs, validateParentSubnetMigration(oldConfig, newConfig, networksPath)...)
	} else {
		// If it is not a valid migration, we enforce immutability of the legacy fields.

		// Fail if parentSubnet changed arbitrarily or was removed without providing parentReference.
		if oldParentPool != newParentPool {
			msg := "parentSubnet is immutable"
			if newParentPool == "" {
				msg = "cannot remove parentSubnet without providing parentReference"
			}
			allErrs = append(allErrs, field.Invalid(parentFieldPath, newParentPool, msg))
		}

		// Fail if parentSubnetProject changed arbitrarily.
		if oldParentProject != newParentProject {
			allErrs = append(allErrs, field.Invalid(projectFieldPath, newParentProject, "parentSubnetProject is immutable"))
		}
	}

	allErrs = append(allErrs, validateParentReferenceUpdate(oldConfig.Networks.ParentReference, newConfig.Networks.ParentReference, isMigration, networksPath)...)

	// TODO(b/423042265): update validation webhook to allow infraConfig CIDR change when IPAM supports subnet change
	return allErrs
}

// validateParentSubnetMigration enforces that parentReference targets the same subnet as the old fields during migration.
func validateParentSubnetMigration(oldConfig, newConfig *gdc.InfrastructureConfig, networksPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	ref := newConfig.Networks.ParentReference
	oldParentProject := oldConfig.Networks.ParentSubnetProject

	// To ensure data integrity and prevent breaking existing VMs, the new parentReference
	// must target the exact same subnet as the old fields.

	// Validate that the subnet name matches.
	if ref.Name != oldConfig.Networks.ParentSubnet {
		allErrs = append(allErrs, field.Invalid(networksPath.Child("parentReference").Child("name"), ref.Name, "parentReference name must match the old parentSubnet during migration"))
	}

	// Validate that the subnet namespace (project) matches.
	newNS := ""
	if ref.Namespace != nil {
		newNS = *ref.Namespace
	}
	if newNS != oldParentProject {
		allErrs = append(allErrs, field.Invalid(networksPath.Child("parentReference").Child("namespace"), newNS, "parentReference namespace must match the old parentSubnetProject during migration"))
	}

	// Validate that the reference type is SingleSubnet, as the old fields referenced a single subnet.
	refType := ref.Type
	if refType == "" {
		refType = gdc.SingleSubnet
	}
	if refType != gdc.SingleSubnet {
		allErrs = append(allErrs, field.Invalid(networksPath.Child("parentReference").Child("type"), refType, "parentReference type must be SingleSubnet during migration from parentSubnet"))
	}

	return allErrs
}

// validateParentReferenceUpdate enforces that parentReference is immutable when not migrating.
func validateParentReferenceUpdate(oldRef, newRef *gdc.SubnetReference, isMigration bool, networksPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if oldRef != nil && newRef != nil {
		// Enforce immutability if both are specified
		if oldRef.Name != newRef.Name {
			allErrs = append(allErrs, field.Invalid(networksPath.Child("parentReference").Child("name"), newRef.Name, "parentReference name is immutable"))
		}

		oldNS := ""
		if oldRef.Namespace != nil {
			oldNS = *oldRef.Namespace
		}
		newNS := ""
		if newRef.Namespace != nil {
			newNS = *newRef.Namespace
		}
		if oldNS != newNS {
			allErrs = append(allErrs, field.Invalid(networksPath.Child("parentReference").Child("namespace"), newNS, "parentReference namespace is immutable"))
		}

		if oldRef.Type != newRef.Type {
			allErrs = append(allErrs, field.Invalid(networksPath.Child("parentReference").Child("type"), newRef.Type, "parentReference type is immutable"))
		}
	} else if oldRef != nil && newRef == nil {
		// Prevent removal of parentReference
		allErrs = append(allErrs, field.Invalid(networksPath.Child("parentReference"), nil, "cannot remove parentReference"))
	} else if oldRef == nil && newRef != nil && !isMigration {
		// Prevent adding parentReference unless it's a valid migration
		allErrs = append(allErrs, field.Invalid(networksPath.Child("parentReference"), newRef, "cannot add parentReference without migration"))
	}

	return allErrs
}

// ValidateInfrastructureConfig validates a InfrastructureConfig object.
func ValidateInfrastructureConfig(infra *gdc.InfrastructureConfig, allowedZones map[string]bool, clusterNetworkingNodeCIDRstr *string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// retrieve networking.nodecidr field.
	var clusterNetworkingNodes cidr.CIDR
	networkingPath := field.NewPath("networking")
	if clusterNetworkingNodeCIDRstr != nil {
		clusterNetworkingNodes = cidr.NewCIDR(*clusterNetworkingNodeCIDRstr, networkingPath.Child("nodes"))
	}

	// retrieve NodeCIDR from infrastructure.provider.infrastructure.networks.nodecidr field.
	infraNetworksPath := fldPath.Child("networks")
	if len(infra.Networks.NodeCIDR) == 0 {
		allErrs = append(allErrs, field.Required(infraNetworksPath.Child("nodecidr"), "must specify the network range for the worker nodecidr"))
		return allErrs
	}

	infraNetworkNodeCIDR := cidr.NewCIDR(infra.Networks.NodeCIDR, infraNetworksPath.Child("nodecidr"))

	parseErrs := cidr.ValidateCIDRParse(infraNetworkNodeCIDR)
	if len(parseErrs) > 0 {
		allErrs = append(allErrs, parseErrs...)
		return allErrs
	}

	allErrs = append(allErrs, cidr.ValidateCIDRIsCanonical(infraNetworksPath.Child("nodecidr"), infra.Networks.NodeCIDR)...)

	if clusterNetworkingNodes != nil {
		allErrs = append(allErrs, clusterNetworkingNodes.ValidateSubset(infraNetworkNodeCIDR)...)
	}

	allErrs = append(allErrs, validate(infra.Networks, allowedZones, infraNetworksPath, infraNetworkNodeCIDR)...)

	return allErrs
}

func validate(infraNetwork gdc.NetworkConfig, allowedZones map[string]bool, fldPath *field.Path, infraNetworkNodeCIDR cidr.CIDR) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(infraNetwork.ParentSubnet) == 0 && (infraNetwork.ParentReference == nil || len(infraNetwork.ParentReference.Name) == 0) {
		allErrs = append(allErrs, field.Invalid(
			fldPath.Child("parentReference"),
			nil,
			"either parentSubnet or parentReference must be specified",
		))
	}

	// validate infraConfig.Networks.Zones
	zones := infraNetwork.Zones
	if len(zones) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("zones"), "must specify the zones"))
		return allErrs
	}
	validZonesList := make([]string, 0, len(allowedZones))
	for zone := range allowedZones {
		validZonesList = append(validZonesList, zone)
	}
	sort.Strings(validZonesList)
	validatedZoneCIDRs := make([]cidr.CIDR, 0, len(zones))
	for index, zone := range zones {
		// zone.name must be a valid zone specified in cloudprofile
		if len(zone.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("zones").Index(index).Child("name"), "must specify the zone name"))
		} else if !allowedZones[zone.Name] {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("zones").Index(index).Child("name"), zone.Name, validZonesList))
		}
		// validate if zone.CIDR is valid
		zoneCIDRPath := fldPath.Child("zones").Index(index).Child("CIDR")
		if len(zone.CIDR) == 0 {
			allErrs = append(allErrs, field.Required(zoneCIDRPath, "must specify the zone CIDR"))
			continue
		}
		zoneCIDR := cidr.NewCIDR(zone.CIDR, zoneCIDRPath)
		if parseErrs := cidr.ValidateCIDRParse(zoneCIDR); len(parseErrs) > 0 {
			allErrs = append(allErrs, parseErrs...)
			// If we can't parse it, we can't check overlaps/subsets, so we skip the rest for this zone
			continue
		}
		allErrs = append(allErrs, cidr.ValidateCIDRIsCanonical(zoneCIDRPath, zone.CIDR)...)

		isOverlapping := false
		for _, existingCIDR := range validatedZoneCIDRs {
			if overlapErrs := zoneCIDR.ValidateNotOverlap(existingCIDR); len(overlapErrs) > 0 {
				allErrs = append(allErrs, field.Invalid(zoneCIDRPath, zone.CIDR, "zone CIDR must not overlap with other zone CIDRs"))
				isOverlapping = true
				break
			}
		}
		allErrs = append(allErrs, infraNetworkNodeCIDR.ValidateSubset(zoneCIDR)...)

		if !isOverlapping {
			validatedZoneCIDRs = append(validatedZoneCIDRs, zoneCIDR)
		}
	}

	return allErrs
}
