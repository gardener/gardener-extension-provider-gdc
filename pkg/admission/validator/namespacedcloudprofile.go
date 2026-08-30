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

package validator

import (
	"context"
	"fmt"
	"slices"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/gardener/gardener/pkg/apis/core"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"

	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/validation"
)

type namespacedCloudProfile struct {
	client  client.Client
	decoder runtime.Decoder
}

// NewNamespacedCloudProfileValidator returns a new instance of a NamespacedCloudProfile validator.
func NewNamespacedCloudProfileValidator(mgr manager.Manager) extensionswebhook.Validator {
	return &namespacedCloudProfile{
		client:  mgr.GetClient(),
		decoder: serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder(),
	}
}

var (
	ncpSpecPath           = field.NewPath("spec")
	ncpProviderConfigPath = ncpSpecPath.Child("providerConfig")
	ncpParentPath         = ncpSpecPath.Child("parent")
)

// Validate validates the given NamespacedCloudProfile objects.
func (ncp *namespacedCloudProfile) Validate(ctx context.Context, newObj, oldObj client.Object) error {
	newProfile, ok := newObj.(*core.NamespacedCloudProfile)
	if !ok {
		return fmt.Errorf("wrong object type %T", newObj)
	}

	if newProfile.DeletionTimestamp != nil {
		return ValidateNamespacedCloudProfileDeletion(ctx, ncp.client, newProfile)
	}

	if err := ncp.validateCreate(ctx, newProfile); err != nil {
		return fmt.Errorf("invalid new namespacedCloudProfile: %w", err)
	}

	if oldObj != nil {
		// Validate Update
		oldProfile, ok := oldObj.(*core.NamespacedCloudProfile)
		if !ok {
			return fmt.Errorf("wrong object type %T for old object", oldObj)
		}
		return ncp.validateUpdate(ctx, oldProfile, newProfile)
	}
	return nil
}

func (ncp *namespacedCloudProfile) validateUpdate(ctx context.Context, oldProfile, newProfile *core.NamespacedCloudProfile) error {
	deletedKubernetesVersions, deletedMachineImageVersions := getDeletedVersions(oldProfile, newProfile)
	if len(deletedKubernetesVersions) == 0 && len(deletedMachineImageVersions) == 0 {
		return nil
	}

	shootList := &gardencorev1beta1.ShootList{}
	if err := ncp.client.List(ctx, shootList,
		client.InNamespace(newProfile.Namespace),
		client.MatchingFields{".spec.cloudProfile.name": newProfile.Name},
	); err != nil {
		return err
	}

	allErrors := field.ErrorList{}
	for _, shoot := range shootList.Items {
		// Check if the Shoot's Kubernetes version is one of the versions being removed.
		if deletedKubernetesVersions.Has(shoot.Spec.Kubernetes.Version) {
			path := ncpSpecPath.Child("kubernetes", "versions")
			msg := fmt.Sprintf("kubernetes version %q is being removed but is still in use by Shoot %q", shoot.Spec.Kubernetes.Version, shoot.Name)
			allErrors = append(allErrors, field.Forbidden(path, msg))
		}

		// Check all worker pools in the Shoot for any machine images that are being removed.
		for _, worker := range shoot.Spec.Provider.Workers {
			if worker.Machine.Image == nil {
				continue
			}
			// Create the unique key for the machine image version (e.g., "gardenlinux/v1").
			machineImageVersionKey := machineImageVersionKey(worker.Machine.Image.Name, *worker.Machine.Image.Version)
			if deletedMachineImageVersions.Has(machineImageVersionKey) {
				path := ncpSpecPath.Child("machineImages")
				msg := fmt.Sprintf("machine image %q version %q is being removed but is still in use by Shoot %q", worker.Machine.Image.Name, *worker.Machine.Image.Version, shoot.Name)
				allErrors = append(allErrors, field.Forbidden(path, msg))
			}
		}
	}

	return allErrors.ToAggregate()
}

func (ncp *namespacedCloudProfile) validateCreate(ctx context.Context, profile *core.NamespacedCloudProfile) error {
	v1beta1ParentProfile, err := ncp.getParentProfiles(ctx, profile.Spec.Parent, ncpParentPath)
	if err != nil {
		return err
	}

	allErrs := field.ErrorList{}
	allowedRegions, err := getAllowedRegions(v1beta1ParentProfile)
	if err != nil {
		return err
	}
	if profile.Spec.ProviderConfig != nil {
		providerConfig, err := DecodeCloudProfileConfig(ncp.decoder, profile.Spec.ProviderConfig)
		if err != nil {
			return err
		}
		allErrs = append(allErrs, validation.ValidateOrgConfig(providerConfig, ncpProviderConfigPath.Child("orgConfig"), allowedRegions)...)
		allErrs = append(allErrs, validateMachineImages(providerConfig, profile.Spec.MachineImages, v1beta1ParentProfile, ncpProviderConfigPath)...)
	}

	return allErrs.ToAggregate()
}

func validateMachineImages(providerConfig *gdc.CloudProfileConfig, specMachineImages []core.MachineImage, parentProfile *gardencorev1beta1.CloudProfile, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// Build a map of the machine images defined in the providerConfig for fast lookup.
	// e.g. map{gardenlinux:{v1, v2}}
	providerMachineImages := make(map[string]sets.Set[string])
	if providerConfig != nil {
		for _, img := range providerConfig.MachineImages {
			if _, ok := providerMachineImages[img.Name]; !ok {
				providerMachineImages[img.Name] = sets.New[string]()
			}
			for _, v := range img.Versions {
				providerMachineImages[img.Name].Insert(v.Version)
			}
		}
	}
	// Build a map of machine images defined in the parent CloudProfile for fast lookup.
	parentSpecImages := make(map[string]sets.Set[string])
	for _, img := range parentProfile.Spec.MachineImages {
		if _, ok := parentSpecImages[img.Name]; !ok {
			parentSpecImages[img.Name] = sets.New[string]()
		}
		for _, v := range img.Versions {
			parentSpecImages[img.Name].Insert(v.Version)
		}
	}

	// Build a map of machine images defined in the NamespacedCloudprofile spec for fast lookup.
	profileSpecImages := make(map[string]sets.Set[string])
	for _, img := range specMachineImages {
		if _, ok := profileSpecImages[img.Name]; !ok {
			profileSpecImages[img.Name] = sets.New[string]()
		}
		for _, v := range img.Versions {
			profileSpecImages[img.Name].Insert(v.Version)
		}
	}

	// Loop over all logical machine images defined in the NamespacedCloudprofile spec.
	for i, img := range specMachineImages {
		imgFldPath := fldPath.Index(i)
		versionsFldPath := imgFldPath.Child("versions")

		providerVersions := providerMachineImages[img.Name]
		parentVersions := parentSpecImages[img.Name]

		for j, version := range img.Versions {
			versionFldPath := versionsFldPath.Index(j)

			// Check that the image is also defined in the providerConfig or parentSpec.
			if providerVersions == nil && parentVersions == nil {
				allErrs = append(allErrs, field.Required(imgFldPath, fmt.Sprintf("machine image %s is not defined in the NamespacedCloudProfile providerConfig and parent CloudProfile", img.Name)))
				continue
			}
			isInProvider := (providerVersions != nil && providerVersions.Has(version.Version))
			isInParent := (parentVersions != nil && parentVersions.Has(version.Version))
			if !isInProvider && !isInParent {
				allErrs = append(allErrs, field.Required(versionFldPath, fmt.Sprintf("machine image version %s@%s is not defined in either the parent CloudProfile or the NamespacedCloudProfile providerConfig", img.Name, version.Version)))
			}
		}
	}

	if providerConfig != nil {
		for imageIdx, machineImage := range providerConfig.MachineImages {
			imageFldPath := fldPath.Index(imageIdx)
			// Check that the providerConfig machine image version is not already defined in the parent CloudProfile.
			if parentVersions, exists := parentSpecImages[machineImage.Name]; exists {
				for versionIdx, version := range machineImage.Versions {
					if parentVersions.Has(version.Version) {
						allErrs = append(allErrs, field.Forbidden(
							imageFldPath.Child("versions").Index(versionIdx),
							fmt.Sprintf("machine image version %s@%s is already defined in the parent CloudProfile and cannot be overridden", machineImage.Name, version.Version),
						))
					}
				}
			}

			// Check that the providerConfig machine image is declared in the NamespacedCloudprofile's .spec.machineImages.
			profileVersions, existsInProfile := profileSpecImages[machineImage.Name]
			if !existsInProfile {
				allErrs = append(allErrs, field.Required(
					imageFldPath,
					fmt.Sprintf("machine image %s is defined in providerConfig but is not declared in .spec.machineImages", machineImage.Name),
				))
				continue
			}

			for versionIdx, version := range machineImage.Versions {
				versionFldPath := imageFldPath.Child("versions").Index(versionIdx)

				// Check that the specific version is declared in the NamespacedCloudprofile's .spec.machineImages.
				if !profileVersions.Has(version.Version) {
					allErrs = append(allErrs, field.Invalid(
						versionFldPath,
						fmt.Sprintf("%s@%s", machineImage.Name, version.Version),
						"machine image version is defined in providerConfig but is not declared in the corresponding entry in .spec.machineImages",
					))
				}

				// Validate architecture
				if version.Architecture == nil || !slices.Contains(v1beta1constants.ValidArchitectures, *version.Architecture) {
					allErrs = append(allErrs, field.NotSupported(versionFldPath.Child("architecture"), version.Architecture, v1beta1constants.ValidArchitectures))
				}
			}
		}
	}

	return allErrs
}

// getDeletedVersions calculates which Kubernetes and machine image versions were removed between an old and new profile.
func getDeletedVersions(oldProfile, newProfile *core.NamespacedCloudProfile) (sets.Set[string], sets.Set[string]) {
	// Get Kubernetes versions from both profiles as sets.
	oldK8sVersions := buildKubernetesVersionSet(oldProfile)
	newK8sVersions := buildKubernetesVersionSet(newProfile)

	// Get MachineImage versions from both profiles as sets.
	oldImageKeys := buildMachineImageKeySet(oldProfile)
	newImageKeys := buildMachineImageKeySet(newProfile)

	deletedKubernetesVersions := oldK8sVersions.Difference(newK8sVersions)
	deletedMachineImageVersions := oldImageKeys.Difference(newImageKeys)

	return deletedKubernetesVersions, deletedMachineImageVersions
}

// buildKubernetesVersionSet creates a set of all Kubernetes versions in a profile.
func buildKubernetesVersionSet(profile *core.NamespacedCloudProfile) sets.Set[string] {
	versions := sets.New[string]()
	if profile.Spec.Kubernetes != nil {
		for _, v := range profile.Spec.Kubernetes.Versions {
			versions.Insert(v.Version)
		}
	}
	return versions
}

// buildMachineImageKeySet creates a set of all unique machine image keys (e.g., "gardenlinux/v1") in a profile.
func buildMachineImageKeySet(profile *core.NamespacedCloudProfile) sets.Set[string] {
	keySet := sets.New[string]()
	for _, image := range profile.Spec.MachineImages {
		for _, v := range image.Versions {
			keySet.Insert(machineImageVersionKey(image.Name, v.Version))
		}
	}
	return keySet
}

func ValidateNamespacedCloudProfileDeletion(ctx context.Context, c client.Client, profile *core.NamespacedCloudProfile) error {
	shootList := &gardencorev1beta1.ShootList{}
	if err := c.List(ctx, shootList,
		client.InNamespace(profile.Namespace),
		client.MatchingFields{".spec.cloudProfile.name": profile.Name},
		client.Limit(1),
	); err != nil {
		return err
	}

	if len(shootList.Items) > 0 {
		return fmt.Errorf("cannot delete namespaced cloud profile %q because it is still referenced by shoot %q", profile.Name, shootList.Items[0].Name)
	}

	return nil
}

func machineImageVersionKey(name, version string) string {
	return fmt.Sprintf("%s/%s", name, version)
}

func (ncp *namespacedCloudProfile) getParentProfiles(ctx context.Context, parentRef core.CloudProfileReference, parentPath *field.Path) (*gardencorev1beta1.CloudProfile, error) {
	// Currently a NCP cannot refer another NCP as parent
	if parentRef.Kind != v1beta1constants.CloudProfileReferenceKindCloudProfile {
		return nil, field.Invalid(parentPath.Child("kind"), parentRef.Kind, "parent reference must be of kind CloudProfile")
	}

	// corev1 api group does not have ListOptions for CloudProfile
	v1beta1ParentProfile := &gardencorev1beta1.CloudProfile{}
	if err := ncp.client.Get(ctx, client.ObjectKey{Name: parentRef.Name}, v1beta1ParentProfile); err != nil {
		return nil, err
	}

	return v1beta1ParentProfile, nil
}
