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
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/gardener/gardener/extensions/pkg/controller"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	reconcilerutils "github.com/gardener/gardener/pkg/controllerutils/reconciler"
	"github.com/go-logr/logr"
	gdchnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"
	vmv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/virtualmachine/v1"
	corev1 "k8s.io/api/core/v1"
	k8snetworkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/cloudprofile"
)

const (
	disksize = "16Gi"
)

// Reconcile reconciles a bastion CR to create a bastion VM.
func (a *actuator) Reconcile(ctx context.Context, log logr.Logger, bastion *extensionsv1alpha1.Bastion, cluster *controller.Cluster) error {
	log.V(1).Info("Starting reconciliation of Bastion", "bastion", bastion.Name, "namespace", bastion.Namespace)
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

	if providerStatus == nil {
		bytes, err := json.Marshal(&providerStatusRaw{Zone: zone})
		if err != nil {
			return fmt.Errorf("error marshalling provider status: %w", err)
		}
		patch := client.MergeFrom(bastion.DeepCopy())
		bastion.Status.ProviderStatus = &runtime.RawExtension{Raw: bytes}
		if err := a.client.Status().Patch(ctx, bastion, patch); err != nil {
			return fmt.Errorf("failed to store status.providerStatus for zone: %s :%w", zone, err)
		}
	}
	orgClusterCfg, err := getOrgConfig(cp, zone)

	if err != nil {
		return fmt.Errorf("error getting org cluster config: %w", err)
	}
	kubeClient, project, err := a.getClientAndProject(ctx, a.client, orgClusterCfg, secretReference, a.client.Scheme())
	if err != nil {
		return fmt.Errorf("error creating kube client: %w", err)
	}

	_, err = createOrUpdateSetupScriptSecret(ctx, kubeClient, project, bastion)
	if err != nil {
		return fmt.Errorf("error creating setup secret: %w", err)
	}

	machineType, machineArch, err := findMostSuitableMachineType(cluster.CloudProfile)
	if err != nil {
		return fmt.Errorf("error getting machine type: %w", err)
	}

	image, version, err := getGDCHMachineImage(cluster.CloudProfile.Spec.MachineImages, machineArch, cp.MachineImages)
	if err != nil {
		return fmt.Errorf("error getting machine type: %w", err)
	}

	_, err = createOrUpdateVirtualMachine(ctx, kubeClient, project, bastion, version.Image, image.Project, machineType)
	if err != nil {
		return fmt.Errorf("error creating virtual machine: %w", err)
	}

	bastionVMExternalAccess, err := createOrUpdateVirtualMachineExternalAccess(ctx, kubeClient, project, bastion)
	if err != nil {
		return fmt.Errorf("error creating virtual machine external access: %w", err)
	}

	err = createOrUpdateProjectNetworkPolicy(ctx, kubeClient, project, bastion)
	if err != nil {
		return fmt.Errorf("error creating project network policy: %w", err)
	}

	patch := client.MergeFrom(bastion.DeepCopy())
	bastion.Status.Ingress = &corev1.LoadBalancerIngress{
		IP: bastionVMExternalAccess.Status.IngressIP,
	}
	log.V(1).Info("Finished reconciliation of Bastion", "bastion", bastion.Name, "namespace", bastion.Namespace)

	if err := a.client.Status().Patch(ctx, bastion, patch); err != nil {
		return fmt.Errorf("error patching bastion object %q/%q: %w", bastion.Name, bastion.Namespace, err)
	}
	return nil
}

// findMostSuitableMachineType searches for the machine type that satisfies certain criteria
// currently we try to find the machine with the lowest amount of cpus
func findMostSuitableMachineType(profile *gardencorev1beta1.CloudProfile) (machineName string, machineArch string, err error) {
	var minCpu *int64

	for _, machine := range profile.Spec.MachineTypes {
		if machine.Architecture == nil {
			continue
		}

		arch := *machine.Architecture
		if minCpu == nil || machine.CPU.Value() < *minCpu {
			minCpu = ptr.To(machine.CPU.Value())
			machineName = machine.Name
			machineArch = arch
		}
	}

	if minCpu == nil {
		return "", "", fmt.Errorf("no suitable machine found")
	}

	return
}

// createOrUpdateProjectNetworkPolicy creates or updates the ProjectNetworkPolicy to allow connections from the given CIDR to the project of the bastion VM.
func createOrUpdateProjectNetworkPolicy(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion) error {
	from := []gdchnetworkingv1.ProjectNetworkPolicyPeer{}
	for _, ingress := range bastion.Spec.Ingress {
		from = append(from, gdchnetworkingv1.ProjectNetworkPolicyPeer{
			IPBlock: &k8snetworkingv1.IPBlock{
				CIDR: ingress.IPBlock.CIDR,
			},
		})
	}

	pnp := &gdchnetworkingv1.ProjectNetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ProjectNetworkPolicyName(bastion.Name),
			Namespace: project,
		},
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, kubeClient, pnp, func() error {
		pnp.Spec = gdchnetworkingv1.ProjectNetworkPolicySpec{
			Subject: gdchnetworkingv1.ProjectNetworkPolicySubject{
				SubjectType: "UserWorkload",
			},
			PolicyType: gdchnetworkingv1.PolicyTypeIngress,
			Ingress: []gdchnetworkingv1.ProjectNetworkPolicyIngressRule{
				{
					From: from,
				},
			},
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to create project network policy %q in namespace: %q, %w", bastion.Name, project, err)
	}

	return nil
}

// createOrUpdateSetupScriptSecret creates or updates the Secret CR containing the setup script for the bastion VM.
func createOrUpdateSetupScriptSecret(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion) (*corev1.Secret, error) {
	name := SetupScriptSecretName(bastion.Name)
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: project,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, kubeClient, s, func() error {
		s.Data = map[string][]byte{
			"script": bastion.Spec.UserData,
		}
		return nil
	}); err != nil {
		return s, fmt.Errorf("failed to create setup secret for %q in namespace: %q, %w", bastion.Name, project, err)
	}

	return s, nil
}

// createOrUpdateVirtualMachineDisk creates or updates the VirtualMachineDisk CR used for the bastion VM it returns the created DiskAttachment to be used in the VirtualMachine.
func createOrUpdateVirtualMachineDisk(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion, machineImage, machineImageProject string) (*vmv1.DiskAttachment, error) {
	diskName := DiskName(bastion.Name)
	diskSize, err := resource.ParseQuantity(disksize)
	if err != nil {
		return nil, fmt.Errorf("invalid disk size, %w", err)
	}
	disk := &vmv1.VirtualMachineDisk{
		ObjectMeta: metav1.ObjectMeta{
			Name:      diskName,
			Namespace: project,
		},
	}
	if _, err = controllerutil.CreateOrUpdate(ctx, kubeClient, disk, func() error {
		disk.Spec = vmv1.VirtualMachineDiskSpec{
			Source: &vmv1.DiskSource{
				Image: &vmv1.ImageDiskSource{
					Name:      machineImage,
					Namespace: machineImageProject,
				},
			},
			Size: diskSize,
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to create VirtualMachineDisk for %q in namespace: %q, %w", diskName, project, err)
	}

	_, err = waitForVirtualMachineDiskReady(ctx, kubeClient, disk)
	if err != nil {
		return nil, fmt.Errorf("error waiting for VirtualMachineDisk to be ready: %w", err)
	}

	return &vmv1.DiskAttachment{
		Boot:       ptr.To(true),
		AutoDelete: ptr.To(true),
		VirtualMachineDiskRef: corev1.LocalObjectReference{
			Name: diskName,
		},
	}, nil
}

// createOrUpdateVirtualMachine creates or updates the VirtualMachine CR for the bastion VM.
func createOrUpdateVirtualMachine(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion, machineImage, machineImageProject, machineType string) (*vmv1.VirtualMachine, error) {
	diskAttachment, err := createOrUpdateVirtualMachineDisk(ctx, kubeClient, project, bastion, machineImage, machineImageProject)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk: %w", err)
	}

	vm := &vmv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      VMName(bastion.Name),
			Namespace: project,
		},
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, kubeClient, vm, func() error {
		vm.Spec = vmv1.VirtualMachineSpec{
			Compute: vmv1.Compute{
				VirtualMachineType: machineType,
			},
			Disks: []vmv1.DiskAttachment{
				*diskAttachment,
			},
			StartupScripts: []vmv1.StartupScript{
				{
					Name: "bastion-setup-script",
					ScriptSecretRef: &corev1.LocalObjectReference{
						Name: SetupScriptSecretName(bastion.Name),
					},
				},
			},
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to create VirtualMachine for %q in namespace: %q, %w", bastion.Name, project, err)
	}

	return waitForVirtualMachineReady(ctx, kubeClient, vm)
}

// createOrUpdateVirtualMachineExternalAccess creates or updates a VirtualMachineExternalAccess CR to allow access to the bastion VM.
func createOrUpdateVirtualMachineExternalAccess(ctx context.Context, kubeClient client.Client, project string, bastion *extensionsv1alpha1.Bastion) (*vmv1.VirtualMachineExternalAccess, error) {
	bastionExternalAccess := &vmv1.VirtualMachineExternalAccess{
		ObjectMeta: metav1.ObjectMeta{
			Name:      VMName(bastion.Name),
			Namespace: project,
		},
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, kubeClient, bastionExternalAccess, func() error {
		bastionExternalAccess.Spec = vmv1.VirtualMachineExternalAccessSpec{
			Enabled: true,
			Ports: []vmv1.ServicePort{
				{
					Name:     "ssh",
					Port:     22,
					Protocol: corev1.ProtocolTCP,
				},
			},
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to create VirtualMachineExternalAccess for %q in namespace: %q, %w", bastion.Name, project, err)
	}

	return waitForVirtualMachineExternalAccessReady(ctx, kubeClient, bastionExternalAccess)
}

// waitForVirtualMachineReady returns the created VirtualMachine once it is ready.
func waitForVirtualMachineReady(ctx context.Context, kubeClient client.Client, vm *vmv1.VirtualMachine) (*vmv1.VirtualMachine, error) {
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(vm), vm); err != nil || vm.Status.State != vmv1.VirtualMachineStateRunning {
		if err != nil {
			return vm, &reconcilerutils.RequeueAfterError{
				Cause:        fmt.Errorf("error getting VirtualMachine: %w", err),
				RequeueAfter: 30 * time.Second,
			}
		}

		return vm, &reconcilerutils.RequeueAfterError{
			Cause:        fmt.Errorf("waiting for VirtualMachine state to be %q but got %q", vmv1.VirtualMachineStateRunning, vm.Status.State),
			RequeueAfter: 30 * time.Second,
		}
	}

	return vm, nil
}

// waitForVirtualMachineExternalAccessReady returns the created VirtualMachineExternalAccess once it is ready.
func waitForVirtualMachineExternalAccessReady(ctx context.Context, kubeClient client.Client, vmea *vmv1.VirtualMachineExternalAccess) (*vmv1.VirtualMachineExternalAccess, error) {
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(vmea), vmea); err != nil || vmea.Status.IngressIP == "" {
		if err != nil {
			return vmea, &reconcilerutils.RequeueAfterError{
				Cause:        fmt.Errorf("error getting VirtualMachineExternalAccess: %w", err),
				RequeueAfter: 30 * time.Second,
			}
		}

		return vmea, &reconcilerutils.RequeueAfterError{
			Cause:        fmt.Errorf("waiting for VirtualMachineExternalAccess %q to have an IngressIP", vmea.Name),
			RequeueAfter: 30 * time.Second,
		}
	}

	return vmea, nil
}

// waitForVirtualMachineDiskReady returns the created VirtualMachineDisk once it is ready.
func waitForVirtualMachineDiskReady(ctx context.Context, kubeClient client.Client, vmdisk *vmv1.VirtualMachineDisk) (*vmv1.VirtualMachineDisk, error) {
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(vmdisk), vmdisk); err != nil || vmdisk.Status.Phase != vmv1.DiskPhaseSucceeded {
		if err != nil {
			return vmdisk, &reconcilerutils.RequeueAfterError{
				Cause:        fmt.Errorf("error getting VirtualMachineDisk: %w", err),
				RequeueAfter: 30 * time.Second,
			}
		}

		return vmdisk, &reconcilerutils.RequeueAfterError{
			Cause:        fmt.Errorf("waiting for VirtualMachineDisk phase to be %q but got %q", vmv1.DiskPhaseSucceeded, vmdisk.Status.Phase),
			RequeueAfter: 30 * time.Second,
		}
	}

	return vmdisk, nil
}

// getGDCHMachineImage returns the gdch api image for the bastion.
func getGDCHMachineImage(images []gardencorev1beta1.MachineImage, arch string, gdchAPIImages []apisgdc.MachineImages) (*apisgdc.MachineImages, *apisgdc.MachineImageVersion, error) {
	imagesMap := make(map[string]apisgdc.MachineImages)
	for _, image := range gdchAPIImages {
		// TODO(b/402748114): update the project to with the long term solution for global project
		if image.Project == "" {
			image.Project = "vm-system"
		}
		imagesMap[image.Name] = image
	}

	// take the first image from cloud profile that is supported and arch compatible
	for _, image := range images {
		for _, version := range image.Versions {
			if version.Classification == nil || *version.Classification != gardencorev1beta1.ClassificationSupported {
				continue
			}
			if !slices.Contains(version.Architectures, arch) {
				continue
			}
			gdchAPIImage, found := imagesMap[image.Name]
			if !found {
				continue
			}

			for _, gdchAPIImageVersion := range gdchAPIImage.Versions {
				if gdchAPIImageVersion.Version != version.Version {
					continue
				}

				return ptr.To(gdchAPIImage), ptr.To(gdchAPIImageVersion), nil
			}
		}
	}
	return nil, nil, fmt.Errorf("could not find any supported bastion image for arch %s", arch)
}

// getBastionZone returns the zone in the providerStatus or the first zone in infrastructure config
func getBastionZone(cluster *controller.Cluster, providerStatus *providerStatusRaw) (string, error) {
	if providerStatus != nil {
		return providerStatus.Zone, nil
	}

	infrastructureConfig := &apisgdc.InfrastructureConfig{}
	if cluster.Shoot == nil || cluster.Shoot.Spec.Provider.InfrastructureConfig == nil {
		return "", fmt.Errorf("InfrastructureConfig is nil")
	}

	err := json.Unmarshal(cluster.Shoot.Spec.Provider.InfrastructureConfig.Raw, infrastructureConfig)
	if err != nil {
		return "", fmt.Errorf("error unmarshalling provider status: %w", err)
	}

	if len(infrastructureConfig.Networks.Zones) > 0 {
		return infrastructureConfig.Networks.Zones[0].Name, nil
	}
	return "", nil
}

func getOrgConfig(cp *apisgdc.CloudProfileConfig, bastionZone string) (*gdcclient.OrgClusterConfig, error) {
	config := &gdcclient.OrgClusterConfig{
		CAData: cp.OrgConfig.CAData,
	}

	if len(cp.OrgConfig.Zones) == 0 {
		return nil, fmt.Errorf("could not get zones from cloud profile")
	}
	for _, zone := range cp.OrgConfig.Zones {
		if zone.Name == bastionZone {
			config.OrgClusterURL = zone.ManagementAPI
			break
		}
	}
	if config.OrgClusterURL == "" {
		return nil, fmt.Errorf("OrgClusterURL is empty for target zone %q", bastionZone)
	}

	return config, nil
}
