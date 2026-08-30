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
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
)

const (
	// Name is the name of the GDCH provider.
	ProviderName = "provider-gdch"

	// AccessKeyIDField is the field in a secret where the access key ID is stored.
	AccessKeyIDField = "accessKeyID"

	// SecretAccessKeyField is the field in a secret where the secret access key is stored.
	SecretAccessKeyField = "secretAccessKey"

	// CloudControllerManagerName is a constant for the name of the CloudController deployed by the worker controller.
	CloudControllerManagerName = "cloud-controller-manager"

	// CSIControllerName is a constant for the name of the CSI controller deployment in the seed.
	CSIControllerName = "csi-driver-controller"

	// CSIProvisionerName is a constant for the name of the csi-provisioner component.
	CSIProvisionerName = "csi-provisioner"

	// CSIAttacherName is a constant for the name of the csi-attacher component.
	CSIAttacherName = "csi-attacher"

	// CSISnapshotterName is a constant for the name of the csi-snapshotter component.
	CSISnapshotterName = "csi-snapshotter"

	// CSIResizerName is a constant for the name of the csi-resizer component.
	CSIResizerName = "csi-resizer"

	// CSISnapshotControllerName is a constant for the name of the csi-snapshot-controller component.
	CSISnapshotControllerName = "csi-snapshot-controller"

	// CSISnapshotValidationName is the constant for the name of the csi-snapshot-validation-webhook component.
	CSISnapshotValidationName = "csi-snapshot-validation"

	// CSIDriverName is a constant for the name of the csi-driver component.
	CSIDriverName = "csi-driver"

	// CSINodeName is a constant for the name of the CSI node deployment in the shoot.
	CSINodeName = "csi-driver-node"

	// MCMProviderGDCImageName is the name of the Machine Controller manager provider GDC image.
	MCMProviderGDCImageName = "machine-controller-manager-provider-gdch"

	// CloudControllerManagerImageName is the name of the cloud-controller-manager image.
	CloudControllerManagerImageName = "cloud-controller-manager"

	// CSIDriverImageName is the name of the csi-driver image.
	CSIDriverImageName = "csi-driver"

	// CSIProvisionerImageName is the name of the csi-provisioner image.
	CSIProvisionerImageName = "csi-provisioner"

	// CSIAttacherImageName is the name of the csi-attacher image.
	CSIAttacherImageName = "csi-attacher"

	// CSISnapshotterImageName is the name of the csi-provisioner image.
	CSISnapshotterImageName = "csi-snapshotter"

	// CSISnapshotterImageName is the name of the csi-provisioner image.
	CSIResizerImageName = "csi-resizer"

	// CSISnapshotControllerImageName is the name of the csi-attacher image.
	CSISnapshotControllerImageName = "csi-snapshot-controller"

	// CSILivenessProbeImageName is the name of the csi-liveness-probe image.
	CSILivenessProbeImageName = "csi-liveness-probe"

	// CSINodeDriverRegistrarImageName is the name of the csi-node-driver-registrar image.
	CSINodeDriverRegistrarImageName = "csi-node-driver-registrar"

	// CSIControllerConfigName is the name of the CSI controller config in the seed.
	CSIControllerConfigName = "kvcsi-driver-config"

	// InfraClusterKubeconfigName is the name of the config for the kubeconfig to infra cluster.
	InfraClusterKubeconfigName = "infra-cluster-kubeconfigs"

	// FullyQualifiedBucketNameAnnotationKey is the key on annotation set on BackupBucket CR.
	FullyQualifiedBucketNameAnnotationKey = "object.gdc.goog/fully-qualified-bucket-name"
)

var (
	// UsernamePrefix is a constant for the username prefix of components deployed by GDCH.
	UsernamePrefix = extensionsv1alpha1.SchemeGroupVersion.Group + ":" + ProviderName + ":"
)
