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
	"testing"

	"github.com/gardener/gardener/pkg/apis/core"
	"github.com/gardener/gardener/pkg/apis/core/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	fakemanager "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

func TestBackupBucketValidator(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)

	backupBucketConfig1 := &apisgdc.BackupBucketConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apisgdc.SchemeGroupVersion.String(),
			Kind:       "BackupBucketConfig",
		},
		DualZoneBucketLocation: "syncz1z2",
	}

	backupBucketConfig2 := &apisgdc.BackupBucketConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apisgdc.SchemeGroupVersion.String(),
			Kind:       "BackupBucketConfig",
		},
		DualZoneBucketLocation: "syncz3z4",
	}

	tests := []struct {
		name    string
		newObj  client.Object
		oldObj  client.Object
		wantErr bool
	}{
		{
			name:   "Create new seed with no backup config",
			newObj: &core.Seed{},
			oldObj: nil,
		},
		{
			name: "Create new seed with backup config",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
			oldObj: nil,
		},
		{
			name: "Update seed with no change in backup config",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
		},
		{
			name: "Update seed with change in backup config without annotation",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig2)},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Update seed with change in backup config with annotation",
			newObj: &core.Seed{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{overrideAnnotation: "true"},
				},
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig2)},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
		},
		{
			name: "Update seed with change in backup config with wrong annotation value",
			newObj: &core.Seed{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{overrideAnnotation: "override"},
				},
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig2)},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Update seed from nil to non-nil backup config without annotation",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: nil,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Update seed from non-nil to nil backup config without annotation",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: nil,
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Update seed from nil to non-nil backup config with annotation",
			newObj: &core.Seed{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{overrideAnnotation: "true"},
				},
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: nil,
					},
				},
			},
		},
		{
			name: "Update seed from non-nil to nil backup config with annotation",
			newObj: &core.Seed{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{overrideAnnotation: "true"},
				},
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: nil,
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
		},
		{
			name: "Create new seed with valid checksum settings",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(&apisgdc.BackupBucketConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: apisgdc.SchemeGroupVersion.String(),
								Kind:       "BackupBucketConfig",
							},
							RequestChecksumCalculation: "WHEN_SUPPORTED",
							ResponseChecksumValidation: "WHEN_REQUIRED",
						})},
					},
				},
			},
			oldObj: nil,
		},
		{
			name: "Create new seed with invalid request checksum",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(&apisgdc.BackupBucketConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: apisgdc.SchemeGroupVersion.String(),
								Kind:       "BackupBucketConfig",
							},
							RequestChecksumCalculation: "INVALID",
						})},
					},
				},
			},
			oldObj:  nil,
			wantErr: true,
		},
		{
			name: "Create new seed with invalid response checksum",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(&apisgdc.BackupBucketConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: apisgdc.SchemeGroupVersion.String(),
								Kind:       "BackupBucketConfig",
							},
							ResponseChecksumValidation: "INVALID",
						})},
					},
				},
			},
			oldObj:  nil,
			wantErr: true,
		},
		{
			name: "Update seed with invalid checksum settings",
			newObj: &core.Seed{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{overrideAnnotation: "true"},
				},
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(&apisgdc.BackupBucketConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: apisgdc.SchemeGroupVersion.String(),
								Kind:       "BackupBucketConfig",
							},
							RequestChecksumCalculation: "INVALID",
						})},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(backupBucketConfig1)},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Scenario 1: Update seed from nil to non-nil backup config with invalid checksum",
			newObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(&apisgdc.BackupBucketConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: apisgdc.SchemeGroupVersion.String(),
								Kind:       "BackupBucketConfig",
							},
							RequestChecksumCalculation: "INVALID",
						})},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: nil,
				},
			},
			wantErr: true,
		},
		{
			name: "Scenario 2: Update seed from nil to non-nil providerConfig with annotation and invalid checksum",
			newObj: &core.Seed{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{overrideAnnotation: "true"},
				},
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: &runtime.RawExtension{Raw: encode(&apisgdc.BackupBucketConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: apisgdc.SchemeGroupVersion.String(),
								Kind:       "BackupBucketConfig",
							},
							RequestChecksumCalculation: "INVALID",
						})},
					},
				},
			},
			oldObj: &core.Seed{
				Spec: core.SeedSpec{
					Backup: &core.Backup{
						ProviderConfig: nil,
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			validator := NewBackupBucketValidator(fakemanager.NewManager(c))
			err := validator.Validate(context.Background(), tt.newObj, tt.oldObj)

			if tt.wantErr && err == nil {
				t.Fatal("validator.Validate() expected error, but got none")
			}

			if !tt.wantErr && err != nil {
				t.Fatalf("validator.Validate() unexpected error = %v", err)
			}
		})
	}
}
