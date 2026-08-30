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
	"github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
)

type machineClass struct {
	// The name of the machine class.
	Name string `json:"name"`

	// Labels used for resource selection.
	ResourceLabels map[string]string `json:"resourceLabels"`

	// The project where this machine belongs.
	Project string `json:"project"`

	// Additional metadata associated with the machine.
	Annotations map[string]string `json:"annotations"`

	// Additional labels associated with the machine.
	Labels map[string]string `json:"labels"`

	// A list of disks attached to the machine.
	Disks []*disk `json:"disks"`

	// Configuration data stored as a secret.
	Secret *secret `json:"secret"`

	// Reference to a secret containing credentials.
	CredentialsSecretRef *credentialsSecretRef `json:"credentialsSecretRef"`

	NodeTemplate *v1alpha1.NodeTemplate `json:"nodeTemplate"`

	// AddressPoolName is the name of the IP address pool assigned to VMs.
	// This field will be deprecated after Lancer Evo Migration
	AddressPoolName string `json:"addressPoolName"`

	// SubnetName is the subnet assigned to the created VMs.
	SubnetName string `json:"subnetName"`

	// MachineType contains information about the machine type that should be used for this worker pool.
	MachineType string `json:"machineType"`

	// CAData is Base64 encoded certificateAuthorityData used to verify the server
	CAData string `json:"caData"`

	// OrgClusterURL is the API server for GDCH
	// This should be zonal managementURL for lancer or orgAdminURL for legacy
	OrgClusterURL string `json:"orgClusterURL"`

	// RegistryURL is harbor registry endpoint that store images

	RegistryURL string `json:"registryURL"`

	// EnableEgress controls whether to enable egress for shoot Vms using CloudNAT.
	EnableEgress *bool `json:"enableEgress,omitempty"`
}

type disk struct {
	// Indicates if the disk should be deleted with the machine.
	AutoDelete bool `json:"autoDelete"`

	// Indicates if this is the bootable disk.
	Boot bool `json:"boot"`

	// The size of the disk in gigabytes.
	SizeGB int `json:"sizeGb"`

	// Additional labels associated with the attached disk.
	Labels map[string]string `json:"labels"`

	// Image specifies the disk image
	Image string `json:"image"`

	// Project specifies the namespace which the image comes from
	Project string `json:"project"`

	// Type specifies the disk type for VM.
	Type string `json:"type"`
}

type secret struct {
	// CloudConfig contains cloud-specific configuration data.
	CloudConfig string `json:"cloudConfig"`
}

type credentialsSecretRef struct {
	// The name of the secret resource.
	Name string `json:"name"`

	// The namespace where the secret is located.
	Namespace string `json:"namespace"`
}
