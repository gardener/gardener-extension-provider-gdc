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

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	// ChecksumWhenSupported indicates checksum will be calculated or validated
	// if the operation supports it.
	ChecksumWhenSupported = "WHEN_SUPPORTED"
	// ChecksumWhenRequired indicates checksum will be calculated or validated
	// if required by the operation.
	ChecksumWhenRequired = "WHEN_REQUIRED"
)

// BackupBucketConfig contains backup bucket specific configuration that is embedded into Gardener's `BackupBucket`
// +genclient
// +kubebuilder:object:generate=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type BackupBucketConfig struct {
	metav1.TypeMeta

	// DualZoneBucketLocation is created by IO(Infra Operator) for DualBucket.
	// If not specified, the backupbucket controller falls back to creating a zonal backup bucket.
	// +optional
	DualZoneBucketLocation string `json:"dualZoneBucketLocation,omitempty"`
	// RequestChecksumCalculation is used to configure request checksum calculation.
	// Supported values are: "WHEN_REQUIRED", "WHEN_SUPPORTED".
	// +optional
	RequestChecksumCalculation string `json:"requestChecksumCalculation,omitempty"`

	// ResponseChecksumValidation is used to configure response checksum validation.
	// Supported values are: "WHEN_REQUIRED", "WHEN_SUPPORTED".
	// +optional
	ResponseChecksumValidation string `json:"responseChecksumValidation,omitempty"`
}
