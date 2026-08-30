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

package gdc

import (
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:generate=true
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// CloudProfileConfig contains provider-specific configuration that is embedded into Gardener's `CloudProfile`
// resource.
type CloudProfileConfig struct {
	metav1.TypeMeta
	// MachineImages is the list of machine images that are understood by the controller. It maps
	// logical names and versions to provider-specific identifiers.
	MachineImages []MachineImages `json:"machineImages"`

	// OrgConfig contains org wide endpoints information
	OrgConfig *OrgConfig `json:"orgConfig"`
}

// +kubebuilder:object:generate=true
// MachineImages is a mapping from logical names and versions to provider-specific identifiers.
type MachineImages struct {
	// Name is the logical name of the machine image.
	Name string `json:"name"`
	// Project specifies the namespace which the image comes from.
	// If not defined, the image is from `vm-system` namespace.
	Project string `json:"project,omitempty"`
	// Versions contains versions and a provider-specific identifier.
	Versions []MachineImageVersion `json:"versions"`
}

// +kubebuilder:object:generate=true
// MachineImageVersion contains a version and a provider-specific identifier.
type MachineImageVersion struct {
	// Version is the version of the image.
	Version string `json:"version"`
	// Image is the path to the image.
	Image string `json:"image"`
	// Architecture is the CPU architecture of the machine image.
	// +optional
	// Deprecated: Use CapabilityFlavors instead.
	Architecture *string `json:"architecture,omitempty"`
	// CapabilityFlavors is a collection of all images for that version with capabilities.
	// +optional
	CapabilityFlavors []MachineImageFlavor `json:"capabilityFlavors,omitempty"`
}

// +kubebuilder:object:generate=true
// MachineImageFlavor is a flavor of the machine image version that supports a specific set of capabilities.
type MachineImageFlavor struct {
	// Capabilities is the set of capabilities that are supported by the image in this flavor.
	Capabilities gardencorev1beta1.Capabilities `json:"capabilities"`
	// Image is the path to the image.
	Image string `json:"image"`
}

// +kubebuilder:object:generate=true
// OrgConfig contains org wide endpoints information
type OrgConfig struct {
	// OrgName is the name of org
	OrgName string `json:"orgName"`
	// GlobalManagementAPI is the URL endpoint for global management API
	GlobalManagementAPI string `json:"globalManagementAPI"`
	// RegistryURL is harbor registry endpoint that store images
	RegistryURL string `json:"registryURL"`
	// CAData is Base64 encoded certificateAuthorityData used to verify the server
	CAData string `json:"caData"`
	// Zones contains zonal API endpoints information
	Zones []*ZoneEndpoints `json:"zones,omitempty"`
}

// +kubebuilder:object:generate=true
// ZoneEndpoints contains zonal API endpoints information
type ZoneEndpoints struct {
	// Name is the name of a zone
	Name string `json:"name"`
	// ManagementAPI is the management API url of the zone.
	ManagementAPI string `json:"managementAPI"`
	// InfrastructureAPI is the URL endpoint for infrastructure cluster
	InfrastructureAPI string `json:"infrastructureAPI"`
}
