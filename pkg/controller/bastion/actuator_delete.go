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

import (
	"context"
	"fmt"

	"github.com/gardener/gardener/extensions/pkg/controller"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	gdchnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"
	vmv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/virtualmachine/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/cloudprofile"
)

// Delete deletes all of the objects created for a bastion VM.
func (a *actuator) Delete(ctx context.Context, log logr.Logger, bastion *extensionsv1alpha1.Bastion, cluster *controller.Cluster) error {
	secretReference := corev1.SecretReference{
		Namespace: cluster.ObjectMeta.Name,
		Name:      v1beta1constants.SecretNameCloudProvider,
	}

	cp, err := cloudprofile.GetFromCluster(cluster, a.decoder)
	if err != nil {
		return fmt.Errorf("error getting cloud profile cluster: %w", err)
	}
	providerStatus, err := getProviderStatus(bastion)
	if err != nil {
		return err
	}
	zone, err := getBastionZone(cluster, providerStatus)
	if err != nil {
		return fmt.Errorf("error getting bastion zone: %w", err)
	}
	orgClusterCfg, err := getOrgConfig(cp, zone)
	if err != nil {
		return fmt.Errorf("error getting org cluster config: %w", err)
	}
	kubeClient, project, err := a.getClientAndProject(ctx, a.client, orgClusterCfg, secretReference, a.client.Scheme())
	if err != nil {
		return fmt.Errorf("error creating kube client: %w", err)
	}

	err = deleteProjectNetworkPolicy(ctx, kubeClient, project, bastion)
	if err != nil {
		return fmt.Errorf("error deleting project network policy: %w", err)
	}

	err = deleteVirtualMachineExternalAccess(ctx, kubeClient, project, bastion)
	if err != nil {
		return fmt.Errorf("error deleting virtual machine external access: %w", err)
	}

	err = deleteVirtualMachine(ctx, kubeClient, project, bastion)
	if err != nil {
		return fmt.Errorf("error deleting virtual machine: %w", err)
	}

	err = deleteVirtualMachineDisk(ctx, kubeClient, project, bastion)
	if err != nil {
		return fmt.Errorf("error deleting virtual machine disk: %w", err)
	}

	err = deleteSetupScriptSecret(ctx, kubeClient, project, bastion)
	if err != nil {
		return fmt.Errorf("error deleting setup script secret: %w", err)
	}

	return nil
}

func (a *actuator) ForceDelete(_ context.Context, _ logr.Logger, _ *extensionsv1alpha1.Bastion, _ *controller.Cluster) error {
	return nil
}

// deleteProjectNetworkPolicy deletes the ProjectNetworkPolicy CR for the bastion.
func deleteProjectNetworkPolicy(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion) error {
	pnp := &gdchnetworkingv1.ProjectNetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionProjectNetworkPolicyNameSuffix),
			Namespace: project,
		},
	}

	err := kubeClient.Delete(ctx, pnp)

	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error deleting project network policy %q: %w", pnp.Name, err)
	}
	return nil
}

// deleteVirtualMachineExternalAccess deletes the VirtualMachineExternalAccess CR for the bastion.
func deleteVirtualMachineExternalAccess(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion) error {
	vmExternalAccess := &vmv1.VirtualMachineExternalAccess{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineNameSuffix),
			Namespace: project,
		},
	}

	err := kubeClient.Delete(ctx, vmExternalAccess)

	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error deleting virtual machine external access %q: %w", vmExternalAccess.Name, err)
	}
	return nil
}

// deleteVirtualMachine deletes the VirtualMachine CR for the bastion.
func deleteVirtualMachine(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion) error {
	vm := &vmv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineNameSuffix),
			Namespace: project,
		},
	}

	err := kubeClient.Delete(ctx, vm)

	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error deleting virtual machine %q: %w", vm.Name, err)
	}
	return nil
}

// deleteVirtualMachineDisk deletes the VirtualMachineDisk CR for the bastion.
func deleteVirtualMachineDisk(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion) error {
	vmDisk := &vmv1.VirtualMachineDisk{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionVirtualMachineDiskNameSuffix),
			Namespace: project,
		},
	}

	err := kubeClient.Delete(ctx, vmDisk)

	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error deleting virtual machine disk %q: %w", vmDisk.Name, err)
	}
	return nil
}

// deleteSetupSecret deletes the setup Secret CR for the bastion.
func deleteSetupScriptSecret(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", bastion.Name, bastionSetupScriptNameSuffix),
			Namespace: project,
		},
	}

	err := kubeClient.Delete(ctx, secret)

	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error deleting setup script secret %q: %w", secret.Name, err)
	}
	return nil
}
