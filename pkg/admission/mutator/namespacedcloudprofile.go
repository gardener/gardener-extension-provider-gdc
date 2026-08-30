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

package mutator

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	"github.com/gardener/gardener/pkg/utils"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
)

// NewNamespacedCloudProfileMutator returns a new instance of a NamespacedCloudProfile mutator.
func NewNamespacedCloudProfileMutator(mgr manager.Manager) extensionswebhook.Mutator {
	return &namespacedCloudProfile{
		decoder: serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder(),
	}
}

type namespacedCloudProfile struct {
	decoder runtime.Decoder
}

// Mutate mutates the given NamespacedCloudProfile object.
func (p *namespacedCloudProfile) Mutate(_ context.Context, newObj, _ client.Object) error {
	profile, ok := newObj.(*gardencorev1beta1.NamespacedCloudProfile)
	if !ok {
		return fmt.Errorf("wrong object type %T", newObj)
	}
	// Ignore NamespacedCloudProfiles being deleted and wait for core mutator to patch the status.
	if profile.DeletionTimestamp != nil || profile.Spec.ProviderConfig == nil ||
		profile.Status.CloudProfileSpec.ProviderConfig == nil {
		return nil
	}

	specConfig := &v1alpha1.CloudProfileConfig{}
	if _, _, err := p.decoder.Decode(profile.Spec.ProviderConfig.Raw, nil, specConfig); err != nil {
		return fmt.Errorf("could not decode providerConfig of namespacedCloudProfile spec for '%s': %w", profile.Name, err)
	}
	statusConfig := &v1alpha1.CloudProfileConfig{}
	if _, _, err := p.decoder.Decode(profile.Status.CloudProfileSpec.ProviderConfig.Raw, nil, statusConfig); err != nil {
		return fmt.Errorf("could not decode providerConfig of namespacedCloudProfile status for '%s': %w", profile.Name, err)
	}

	statusConfig.MachineImages = mergeMachineImages(specConfig.MachineImages, statusConfig.MachineImages)

	modifiedStatusConfig, err := json.Marshal(statusConfig)
	if err != nil {
		return err
	}
	profile.Status.CloudProfileSpec.ProviderConfig.Raw = modifiedStatusConfig

	return nil
}

func mergeMachineImages(specMachineImages, statusMachineImages []v1alpha1.MachineImages) []v1alpha1.MachineImages {
	specImages := utils.CreateMapFromSlice(specMachineImages, func(mi v1alpha1.MachineImages) string { return mi.Name })
	statusImages := utils.CreateMapFromSlice(statusMachineImages, func(mi v1alpha1.MachineImages) string { return mi.Name })
	for _, specMachineImage := range specImages {
		if _, exists := statusImages[specMachineImage.Name]; !exists {
			statusImages[specMachineImage.Name] = specMachineImage
		} else {
			// since multiple version entries can exist for the same version string
			mergedVersions := make([]v1alpha1.MachineImageVersion, 0, len(statusImages[specMachineImage.Name].Versions)+len(specImages[specMachineImage.Name].Versions))

			// Add all existing status versions
			mergedVersions = append(mergedVersions, statusImages[specMachineImage.Name].Versions...)

			// Add all spec versions
			mergedVersions = append(mergedVersions, specImages[specMachineImage.Name].Versions...)

			statusImages[specMachineImage.Name] = v1alpha1.MachineImages{
				Name:     specMachineImage.Name,
				Versions: mergedVersions,
			}
		}
	}
	values := make([]v1alpha1.MachineImages, 0, len(statusImages))
	// Manually iterate and append values
	for _, value := range statusImages {
		values = append(values, value)
	}
	slices.SortFunc(values, func(a, b v1alpha1.MachineImages) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return values
}
