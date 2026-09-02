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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=SingleSubnet;SubnetGroup
type ReferenceType string

const (
	// SingleSubnet represents a reference to a single Subnet resource.
	SingleSubnet ReferenceType = "SingleSubnet"
	// SubnetGroup represents a reference to a SubnetGroup resource.
	SubnetGroup ReferenceType = "SubnetGroup"
)

// SubnetReference contains the information used to reference a Subnet or SubnetGroup.
type SubnetReference struct {
	// Name is the name of the referenced resource.
	Name string `json:"name"`

	// Namespace is the namespace of the referenced resource.
	// +optional
	Namespace *string `json:"namespace,omitempty"`

	// Type is the type of the reference (SingleSubnet or SubnetGroup).
	// +kubebuilder:default:="SingleSubnet"
	// +optional
	Type ReferenceType `json:"type,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:generate=true
// InfrastructureConfig contains information about creating an infrastructure resource.
type InfrastructureConfig struct {
	metav1.TypeMeta

	// Networks is the network configuration.
	Networks NetworkConfig `json:"networks"`
	// EnableEgress controls whether to enable egress for shoot Vms.
	// When it is true, the egress for shoot VMs will be enabled.
	// When it is false, the egress for shoot VMs will be disabled if it was previosly enabled otherwise ignored.
	// When it is not defined then legacy behavior is applied by MCM based on label on vm.
	// +optional
	EnableEgress *bool `json:"enableEgress,omitempty"`
}

// +kubebuilder:object:generate=true
// NetworkConfig holds information about the Kubernetes and infrastructure networks.
type NetworkConfig struct {
	// ParentAddressPoolClaim is the parent IP pool for all Gardener's VM nodes.
	ParentAddressPoolClaim string `json:"parentAddressPoolClaim"`

	// NodeCIDR is the CIDR range for the VM Nodes (e.g. xxx.xxx.xxx.x/xx)).
	NodeCIDR string `json:"nodeCIDR"`

	// ParentSubnet is the name of the Parent Subnet for all Gardener's VMa.
	// +optional
	// +deprecated: use parentReference instead. This field will be removed around Aug 2026.
	ParentSubnet string `json:"parentSubnet,omitempty"`

	// ParentSubnetProject is the namespace for parent subnet
	// +optional
	// +deprecated: use parentReference instead. This field will be removed around Aug 2026.
	ParentSubnetProject string `json:"parentSubnetProject,omitempty"`

	// ParentReference is the subnet reference of the parent.
	// If this is set, it takes precedence over ParentSubnet and ParentSubnetProject.
	// +optional
	ParentReference *SubnetReference `json:"parentReference,omitempty"`

	// Zones has info about each zone's root subnet
	Zones []Zone `json:"zones"`
}

type Zone struct {
	// Name is the name for the zone
	Name string `json:"name"`

	// CIDR is the CIDR for the zone, derived from its parent subnet's CIDR
	CIDR string `json:"CIDR"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:generate=true
// InfrastructureStatus contains information about created infrastructure resources.
type InfrastructureStatus struct {
	metav1.TypeMeta
	// EnableEgress controls whether to enable egress for shoot Vms.
	// +optional
	EnableEgress *bool `json:"enableEgress,omitempty"`

	// Networks is the status of the networks of the infrastructure.
	Networks NetworkStatus `json:"networks"`
}

// +kubebuilder:object:generate=true
// NetworkStatus is the current status of the infrastructure networks.
type NetworkStatus struct {
	// NodeCIDR are the CIDR range for the nodes.
	NodeCIDR string `json:"nodeCIDR"`

	// NodeSubnet is the Subnet for a given shoot's VM nodes
	// TODO(b/408281481): remove this after fully adapt lancer
	// +optional
	NodeSubnet string `json:"nodeSubnet,omitempty"`

	// Zones is a slice of {zone's name and zone's workload subnet name}
	Zones []Zones `json:"zones"`
}

type Zones struct {
	// Name is the name for the zone
	Name string `json:"name"`

	// Subnet is the name of zonal subnet apply to workload
	Subnet string `json:"subnet"`
}
