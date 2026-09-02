// Copyright 2026 Google LLC
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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:generate=true
// ControlPlaneConfig contains configuration settings for the control plane.
type ControlPlaneConfig struct {
	metav1.TypeMeta `json:",inline"`

	// Zone is the GDCH zone.
	// TODO(b/498116396): Remove this field once shoots are cleaned up.
	// +optional
	Zone string `json:"zone,omitempty"`

	// Storage contains configuration for the storage in the cluster.
	// +optional
	Storage *Storage `json:"storage,omitempty"`
}

// +kubebuilder:object:generate=true
// Storage contains settings for the default StorageClass and VolumeSnapshotClass
type Storage struct {
	// ManagedDefaultStorageClass controls if the 'default' StorageClass would be marked as default. Set to false to
	// suppress marking the 'default' StorageClass as default, allowing another StorageClass not managed by Gardener
	// to be set as default by the user.
	// Defaults to true.
	// +optional
	ManagedDefaultStorageClass *bool `json:"managedDefaultStorageClass,omitempty"`
	// ManagedDefaultVolumeSnapshotClass controls if the 'default' VolumeSnapshotClass would be marked as default.
	// Set to false to suppress marking the 'default' VolumeSnapshotClass as default, allowing another VolumeSnapshotClass
	// not managed by Gardener to be set as default by the user.
	// Defaults to true.
	// +optional
	ManagedDefaultVolumeSnapshotClass *bool `json:"managedDefaultVolumeSnapshotClass,omitempty"`
}
