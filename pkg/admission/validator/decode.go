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
	"github.com/gardener/gardener/extensions/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

// DecodeInfrastructureConfig decodes the `InfrastructureConfig` from the given `RawExtension`.
func DecodeInfrastructureConfig(decoder runtime.Decoder, infra *runtime.RawExtension) (*gdc.InfrastructureConfig, error) {
	infraConfig := &gdc.InfrastructureConfig{}
	if err := util.Decode(decoder, infra.Raw, infraConfig); err != nil {
		return nil, err
	}

	return infraConfig, nil
}

// DecodeCloudProfileConfig decodes the `CloudProfileConfig` from the given `RawExtension`.
func DecodeCloudProfileConfig(decoder runtime.Decoder, config *runtime.RawExtension) (*gdc.CloudProfileConfig, error) {
	cloudProfileConfig := &gdc.CloudProfileConfig{}
	if err := util.Decode(decoder, config.Raw, cloudProfileConfig); err != nil {
		return nil, err
	}

	return cloudProfileConfig, nil
}

// DecodeBackupBucketConfig decodes the `BackupBucketConfig` from the given `RawExtension`.
func DecodeBackupBucketConfig(decoder runtime.Decoder, config *runtime.RawExtension) (*gdc.BackupBucketConfig, error) {
	backupBucketConfig := &gdc.BackupBucketConfig{}
	if err := util.Decode(decoder, config.Raw, backupBucketConfig); err != nil {
		return nil, err
	}

	return backupBucketConfig, nil
}

// DecodeWorkerConfig decodes the `WorkerConfig` from the given `RawExtension`.
func DecodeWorkerConfig(decoder runtime.Decoder, config *runtime.RawExtension) (*gdc.WorkerConfig, error) {
	workerConfig := &gdc.WorkerConfig{}
	if err := util.Decode(decoder, config.Raw, workerConfig); err != nil {
		return nil, err
	}

	return workerConfig, nil
}
