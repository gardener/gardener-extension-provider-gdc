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
	"reflect"
	"testing"

	"github.com/gardener/gardener/pkg/apis/core"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/install"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
)

func TestDecodeBackupBucketConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add core to scheme: %v", err)
	}
	install.Install(scheme)

	decoder := serializer.NewCodecFactory(scheme).UniversalDecoder()

	tests := []struct {
		name           string
		config         *runtime.RawExtension
		expectErr      bool
		expectedConfig *gdc.BackupBucketConfig
	}{
		{
			name: "should correctly decode a valid gdch config",
			config: &runtime.RawExtension{
				Raw: encode(&gdc.BackupBucketConfig{
					TypeMeta: metav1.TypeMeta{
						APIVersion: v1alpha1.SchemeGroupVersion.String(),
						Kind:       "BackupBucketConfig",
					},
					DualZoneBucketLocation: "us-central1",
				})},
			expectErr: false,
			expectedConfig: &gdc.BackupBucketConfig{
				DualZoneBucketLocation: "us-central1",
			},
		},
		{
			name: "should correctly decode a valid v1alpha1 config",
			config: &runtime.RawExtension{
				Raw: encode(&v1alpha1.BackupBucketConfig{
					TypeMeta: metav1.TypeMeta{
						APIVersion: v1alpha1.SchemeGroupVersion.String(),
						Kind:       "BackupBucketConfig",
					},
					DualZoneBucketLocation: "us-central1",
				})},
			expectErr: false,
			expectedConfig: &gdc.BackupBucketConfig{
				DualZoneBucketLocation: "us-central1",
			},
		},
		{
			name: "should return an error for a nil BackupBucketConfig",
			config: &runtime.RawExtension{
				Raw: nil,
			},
			expectErr:      true,
			expectedConfig: nil,
		},
		{
			name: "should return an error for malformed JSON",
			config: &runtime.RawExtension{
				Raw: []byte("{invalid-json]"),
			},
			expectErr:      true,
			expectedConfig: nil,
		},
	}

	// Iterate over the test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decodedConfig, err := DecodeBackupBucketConfig(decoder, tt.config)

			if (err != nil) != tt.expectErr {
				t.Fatalf("DecodeBackupBucketConfig() error = %v, expectErr %v", err, tt.expectErr)
			}

			if !reflect.DeepEqual(decodedConfig, tt.expectedConfig) {
				t.Errorf("DecodeBackupBucketConfig() got = %#v, want %#v", decodedConfig, tt.expectedConfig)
			}
		})
	}
}
