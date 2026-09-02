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
	"reflect"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/gardener/gardener/pkg/apis/core"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

const (
	// When set to true, it allows override of backup bucket immutability check.
	overrideAnnotation = "backupbucket.webook.gdc.goog/override"
)

type backupBucket struct {
	decoder runtime.Decoder
}

// NewBackupBucketValidator returns a new instance of a backup bucket validator.
func NewBackupBucketValidator(mgr manager.Manager) extensionswebhook.Validator {
	return &backupBucket{
		decoder: serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder(),
	}
}

// Validate validates the given seed objects.
func (b *backupBucket) Validate(_ context.Context, newObj, oldObj client.Object) error {
	newSeed, ok := newObj.(*core.Seed)
	if !ok {
		return fmt.Errorf("wrong object type %T", newObj)
	}

	var newBackupBucketConfig *apisgdc.BackupBucketConfig
	if newSeed.Spec.Backup != nil && newSeed.Spec.Backup.ProviderConfig != nil {
		var err error
		newBackupBucketConfig, err = DecodeBackupBucketConfig(b.decoder, newSeed.Spec.Backup.ProviderConfig)
		if err != nil {
			return err
		}
		if err := validateChecksumSettings(newBackupBucketConfig); err != nil {
			return err
		}
	}

	if oldObj == nil {
		// Seed is being created.
		return nil
	}

	oldSeed, ok := oldObj.(*core.Seed)
	if !ok {
		return fmt.Errorf("wrong object type %T for old object", oldObj)
	}

	if newSeed.Spec.Backup == nil || oldSeed.Spec.Backup == nil {
		// If either new or old backup spec is nil, there is nothing to compare.
		return nil
	}

	newProviderConfig := newSeed.Spec.Backup.ProviderConfig
	oldProviderConfig := oldSeed.Spec.Backup.ProviderConfig

	if newProviderConfig == nil && oldProviderConfig == nil {
		return nil
	}

	if (newProviderConfig == nil && oldProviderConfig != nil) || (newProviderConfig != nil && oldProviderConfig == nil) {
		if val, ok := newSeed.Annotations[overrideAnnotation]; ok && val == "true" {
			return nil
		}
		return fmt.Errorf("cannot change backup bucket flow from dual-zone to zonal or vice-versa without '%s=true' annotation", overrideAnnotation)
	}

	oldBackupBucketConfig, err := DecodeBackupBucketConfig(b.decoder, oldProviderConfig)
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(newBackupBucketConfig, oldBackupBucketConfig) {
		if val, ok := newSeed.Annotations[overrideAnnotation]; ok && val == "true" {
			return nil
		}
		return fmt.Errorf("seed backup providerConfig is immutable. To override, use annotation '%s=true'", overrideAnnotation)
	}

	return nil
}

func validateChecksumSettings(config *apisgdc.BackupBucketConfig) error {
	if config.RequestChecksumCalculation != "" {
		if err := validateChecksumValue(config.RequestChecksumCalculation); err != nil {
			return fmt.Errorf("invalid RequestChecksumCalculation: %w", err)
		}
	}
	if config.ResponseChecksumValidation != "" {
		if err := validateChecksumValue(config.ResponseChecksumValidation); err != nil {
			return fmt.Errorf("invalid ResponseChecksumValidation: %w", err)
		}
	}
	return nil
}

func validateChecksumValue(val string) error {
	switch val {
	case apisgdc.ChecksumWhenSupported,
		apisgdc.ChecksumWhenRequired:
		return nil
	default:
		return fmt.Errorf("value %q must be one of: %s, %s", val,
			apisgdc.ChecksumWhenSupported,
			apisgdc.ChecksumWhenRequired)
	}
}
