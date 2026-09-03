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
	"regexp"
	"strings"

	"github.com/gardener/gardener/extensions/pkg/controller/worker"
	"github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"k8s.io/utils/ptr"

	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"
)

var labelRegex = regexp.MustCompile(`[^a-z0-9_-]`)

const (
	maxLabelCharactersSize = 63
)

func getGDCHPoolLabels(worker *v1alpha1.Worker, pool v1alpha1.WorkerPool, clusterName string) map[string]string {
	gdchInstanceLabels := map[string]string{
		"name": sanitizeLabelOrValue(worker.Name),
		// These labels are required to allow node to be garbage collected by the safety controller
		"kubernetes-io-cluster-" + worker.Namespace: "true",
		"kubernetes-io-role-node":                   "true",
		gdcconstants.WorkloadLabelSelectorKey:       clusterName,
	}
	for k, v := range pool.Labels {
		if label := sanitizeLabelOrValue(k); label != "" {
			gdchInstanceLabels[label] = sanitizeLabelOrValue(v)
		}
	}
	return gdchInstanceLabels
}

// sanitizeLabelOrValue will sanitize the label/value base on the k8s label Restrictions
func sanitizeLabelOrValue(label string) string {
	v := labelRegex.ReplaceAllString(strings.ToLower(label), "_")
	if len(v) > maxLabelCharactersSize {
		return v[0:maxLabelCharactersSize]
	}
	return v
}

func createDiskSpecForVolume(volume v1alpha1.Volume, machineImage, project string, boot bool, labels map[string]string) (*disk, error) {
	return createDiskSpec(volume.Size, boot, &machineImage, &project, volume.Type, labels)
}

func createDiskSpecForDataVolume(volume v1alpha1.DataVolume, boot bool, labels map[string]string) (*disk, error) {
	return createDiskSpec(volume.Size, boot, nil, nil, nil, labels)
}

func createDiskSpec(size string, boot bool, machineImage, project, diskType *string, labels map[string]string) (*disk, error) {
	volumeSize, err := worker.DiskSize(size)
	if err != nil {
		return nil, err
	}

	disk := &disk{
		AutoDelete: true,
		Boot:       boot,
		SizeGB:     volumeSize,
		Type:       ptr.Deref(diskType, ""),
	}

	// TODO(b/458401774): Remove this logic when SAP has fully migrated away from pd-standard
	if disk.Type == "pd-standard" {
		disk.Type = "Standard"
	}

	if len(labels) != 0 {
		disk.Labels = labels
	}

	if machineImage != nil {
		disk.Image = *machineImage
	}

	if project != nil {
		disk.Project = *project
	}

	return disk, nil
}
