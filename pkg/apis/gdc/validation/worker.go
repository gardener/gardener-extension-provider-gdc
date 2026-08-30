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
	"github.com/gardener/gardener/pkg/apis/core"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

func validateWorker(worker core.Worker, cloudProfileZones map[string]bool, infraZones map[string]bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	zonesPath := fldPath.Child("zones")
	allErrs = append(allErrs, validateZones(worker, zonesPath, cloudProfileZones, infraZones)...)

	volumePath := fldPath.Child("volume")
	if worker.Volume == nil {
		allErrs = append(allErrs, field.Required(volumePath, "must specify volume configuration"))
	} else {
		if worker.Volume.Type == nil {
			allErrs = append(allErrs, field.Required(volumePath.Child("type"), "must specify volume type"))
		}
		if worker.Volume.VolumeSize == "" {
			allErrs = append(allErrs, field.Required(volumePath.Child("size"), "must specify volume size"))
		}
	}

	return allErrs
}

func ValidateWorkers(workers []core.Worker, cloudProfileZones map[string]bool, infraConfig *gdc.InfrastructureConfig, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// Each entry must have a unique workers[*].name
	names := make(map[string]struct{}, len(workers))
	for i, worker := range workers {
		workerPath := fldPath.Index(i)
		if worker.Name == "" {
			allErrs = append(allErrs, field.Required(workerPath.Child("name"), "must specify a name for the worker pool"))
		} else if _, exists := names[worker.Name]; exists {
			allErrs = append(allErrs, field.Duplicate(workerPath.Child("name"), worker.Name))
		}
		names[worker.Name] = struct{}{}
	}

	for i, worker := range workers {
		workerPath := fldPath.Index(i)
		// validate each worker
		infraZones := getInfraZones(infraConfig)
		allErrs = append(allErrs, validateWorker(worker, cloudProfileZones, infraZones, workerPath)...)
	}

	return allErrs
}

func validateZones(worker core.Worker, zonesPath *field.Path, cloudProfileZones map[string]bool, infraZones map[string]bool) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(worker.Zones) == 0 {
		allErrs = append(allErrs, field.Required(zonesPath, "must specify at least one zone"))
	} else {
		for i, zone := range worker.Zones {
			if !cloudProfileZones[zone] || !infraZones[zone] {
				allErrs = append(allErrs, field.Invalid(zonesPath.Index(i), zone, "zone must be defined in the cloudprofile and Infrastructure Config"))
			}
		}
	}
	return allErrs
}

func getInfraZones(infraConfig *gdc.InfrastructureConfig) map[string]bool {
	infraZones := map[string]bool{}
	for _, zone := range infraConfig.Networks.Zones {
		infraZones[zone.Name] = true
	}
	return infraZones
}

// ValidateWorkerConfig validates a WorkerConfig resource.
func ValidateWorkerConfig(workerConfig *gdc.WorkerConfig, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if workerConfig == nil {
		return allErrs
	}

	if workerConfig.NodeTemplate != nil {
		nodeTemplatePath := fldPath.Child("nodeTemplate")
		allErrs = append(allErrs, validateResourceList(workerConfig.NodeTemplate.Capacity, nodeTemplatePath.Child("capacity"))...)
		allErrs = append(allErrs, validateVirtualResourceList(workerConfig.NodeTemplate.VirtualCapacity, nodeTemplatePath.Child("virtualCapacity"))...)
	}

	return allErrs
}

func validateResourceList(resources corev1.ResourceList, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for name, quantity := range resources {
		if err := validateNonNegative(quantity, fldPath.Key(string(name))); err != nil {
			allErrs = append(allErrs, err)
		}
	}
	return allErrs
}

func validateVirtualResourceList(resources corev1.ResourceList, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for name, quantity := range resources {
		path := fldPath.Key(string(name))
		if err := validateNonNegative(quantity, path); err != nil {
			allErrs = append(allErrs, err)
			continue
		}
		if err := validateWholeNumber(quantity, path); err != nil {
			allErrs = append(allErrs, err)
		}
	}
	return allErrs
}

func validateNonNegative(quantity resource.Quantity, fldPath *field.Path) *field.Error {
	if quantity.Sign() < 0 {
		return field.Invalid(fldPath, quantity.String(), "must be greater than or equal to 0")
	}
	return nil
}

func validateWholeNumber(quantity resource.Quantity, fldPath *field.Path) *field.Error {
	if quantity.MilliValue()%1000 != 0 {
		return field.Invalid(fldPath, quantity.String(), "must be a whole number")
	}
	return nil
}
