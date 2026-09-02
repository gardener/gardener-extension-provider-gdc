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

package bastion

import "fmt"

const (
	bastionSetupScriptNameSuffix          = "bastion-setup-script"
	bastionProjectNetworkPolicyNameSuffix = "allow-ssh"
	bastionVirtualMachineDiskNameSuffix   = "bastion-vm-disk"
	bastionVirtualMachineNameSuffix       = "bastion-vm"
)

// VMName returns the name of the Bastion Virtual Machine.
func VMName(bastionName string) string {
	return fmt.Sprintf("%s-%s", bastionName, bastionVirtualMachineNameSuffix)
}

// DiskName returns the name of the Bastion Virtual Machine Disk.
func DiskName(bastionName string) string {
	return fmt.Sprintf("%s-%s", bastionName, bastionVirtualMachineDiskNameSuffix)
}

// SetupScriptSecretName returns the name of the Secret containing the setup script.
func SetupScriptSecretName(bastionName string) string {
	return fmt.Sprintf("%s-%s", bastionName, bastionSetupScriptNameSuffix)
}

// ProjectNetworkPolicyName returns the name of the ProjectNetworkPolicy.
func ProjectNetworkPolicyName(bastionName string) string {
	return fmt.Sprintf("%s-%s", bastionName, bastionProjectNetworkPolicyNameSuffix)
}
