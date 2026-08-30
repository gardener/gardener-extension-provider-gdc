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
	"context"
	"fmt"

	"github.com/gardener/gardener/extensions/pkg/controller/worker"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

// UpdateMachineImagesStatus updates the machine image status
// with the used machine images for the `Worker` resource.
func (w *workerDelegate) UpdateMachineImagesStatus(ctx context.Context) error {
	if w.machineImages == nil {
		if err := w.generateMachineImages(); err != nil {
			return fmt.Errorf("unable to generate the machine images: %w", err)
		}
	}

	// Decode the current worker provider status.
	workerStatus, err := w.decodeWorkerProviderStatus()
	if err != nil {
		return fmt.Errorf("unable to decode the worker provider status: %w", err)
	}

	workerStatus.MachineImages = w.machineImages
	if err := w.updateWorkerProviderStatus(ctx, workerStatus); err != nil {
		return fmt.Errorf("unable to update worker provider status: %w", err)
	}
	return nil
}

func (w *workerDelegate) generateMachineImages() error {
	var machineImages = []apisgdc.MachineImage{}

	for _, pool := range w.worker.Spec.Pools {
		arch := ptr.Deref(pool.Architecture, v1beta1constants.ArchitectureAMD64)
		machineImage, machineImageProject, err := w.findMachineImage(pool.MachineImage.Name, pool.MachineImage.Version, &arch)
		if err != nil {
			return fmt.Errorf("could not find machine image with name %q, version %q, and arch %q: %w",
				pool.MachineImage.Name, pool.MachineImage.Version, arch, err)
		}

		machineImages = appendMachineImage(machineImages, apisgdc.MachineImage{
			Name:         pool.MachineImage.Name,
			Project:      machineImageProject,
			Version:      pool.MachineImage.Version,
			Image:        machineImage,
			Architecture: &arch,
		})
	}

	w.machineImages = machineImages
	return nil
}

func (w *workerDelegate) findMachineImage(name, version string, architecture *string) (string, string, error) {
	machineImage, machineImageProject, err := w.findImageFromCloudProfile(name, version, architecture)
	if err == nil {
		return machineImage, machineImageProject, nil
	}

	// Try to look up machine image in worker provider status as it was not found in componentconfig.
	if providerStatus := w.worker.Status.ProviderStatus; providerStatus != nil {
		workerStatus := &apisgdc.WorkerStatus{}
		if _, _, err := w.decoder.Decode(providerStatus.Raw, nil, workerStatus); err != nil {
			return "", "", fmt.Errorf("could not decode worker status of worker '%s': %w", gdc.ObjectName(w.worker), err)
		}

		machineImage, err := findMachineImage(workerStatus.MachineImages, name, version, architecture)
		if err != nil {
			return "", "", worker.ErrorMachineImageNotFound(name, version, *architecture)
		}

		return machineImage.Image, machineImage.Project, nil
	}

	return "", "", worker.ErrorMachineImageNotFound(name, version, *architecture)
}

func appendMachineImage(machineImages []apisgdc.MachineImage, machineImage apisgdc.MachineImage) []apisgdc.MachineImage {
	if _, err := findMachineImage(machineImages, machineImage.Name, machineImage.Version, machineImage.Architecture); err != nil {
		// Error indicates the image is not yet present in the 'machineImages' slice.
		// We can safely append it to ensure uniqueness.
		return append(machineImages, machineImage)
	}
	// Image already exists; return the original slice.
	return machineImages
}

func (w *workerDelegate) decodeWorkerProviderStatus() (*apisgdc.WorkerStatus, error) {
	workerStatus := &apisgdc.WorkerStatus{}

	if w.worker.Status.ProviderStatus == nil {
		return workerStatus, nil
	}

	if _, _, err := w.decoder.Decode(w.worker.Status.ProviderStatus.Raw, nil, workerStatus); err != nil {
		return nil, fmt.Errorf("could not decode WorkerStatus %q: %w", gdc.ObjectName(w.worker), err)
	}

	return workerStatus, nil
}

func (w *workerDelegate) updateWorkerProviderStatus(ctx context.Context, workerStatus *apisgdc.WorkerStatus) error {
	workerStatusV1alpha1 := &v1alpha1.WorkerStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "WorkerStatus",
		},
	}

	if err := w.scheme.Convert(workerStatus, workerStatusV1alpha1, nil); err != nil {
		return err
	}

	patch := client.MergeFrom(w.worker.DeepCopy())
	w.worker.Status.ProviderStatus = &runtime.RawExtension{Object: workerStatusV1alpha1}
	return w.client.Status().Patch(ctx, w.worker, patch)
}

// findMachineImage takes a list of machine images and tries to find the first
// entry whose name, version, and architecture matches with the given name, and
// version. If no such entry is found then an error will be returned.
func findMachineImage(machineImages []apisgdc.MachineImage, name, version string, architecture *string) (*apisgdc.MachineImage, error) {
	for _, machineImage := range machineImages {
		if machineImage.Architecture == nil {
			machineImage.Architecture = ptr.To[string](v1beta1constants.ArchitectureAMD64)
		}
		if machineImage.Name == name && machineImage.Version == version && ptr.Equal[string](architecture, machineImage.Architecture) {
			return &machineImage, nil
		}
	}
	return nil, fmt.Errorf("no machine image found with name %q, architecture %q and version %q", name, *architecture, version)
}

// findImageFromCloudProfile takes a list of machine images, and the desired
// image name and version. It tries to find the image with the given name,
// architecture and version in the desired cloud profile. If it cannot be found
// then an error is returned.
func (w *workerDelegate) findImageFromCloudProfile(imageName, imageVersion string, architecture *string) (string, string, error) {
	for _, machineImage := range w.cloudProfileConfig.MachineImages {
		if machineImage.Name != imageName {
			continue
		}
		for _, version := range machineImage.Versions {
			if imageVersion == version.Version && ptr.Equal[string](architecture, version.Architecture) {
				// TODO(b/402748114): update the project to with the long term solution for global project
				machineImageProject := "vm-system"
				if machineImage.Project != "" {
					machineImageProject = machineImage.Project
				}
				return version.Image, machineImageProject, nil
			}
		}
	}

	return "", "", fmt.Errorf("could not find an image for name %q and architecture %q in version %q", imageName, *architecture, imageVersion)
}
