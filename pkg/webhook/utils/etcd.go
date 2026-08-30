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

package utils

import (
	"context"
	"fmt"
	"strings"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

// EnsureETCDBackup ensures that the etcd backup is configured correctly.
// It sets the backup bucket container and provider for the target etcd resource.
func EnsureETCDBackup(ctx context.Context, c client.Client, logger logr.Logger, etcd *druidv1alpha1.Etcd) error {
	if etcd.Spec.Backup.Store != nil {
		backupBucketList := &extensionsv1alpha1.BackupBucketList{}
		if err := c.List(ctx, backupBucketList); err != nil {
			return fmt.Errorf("failed to list BackupBucket resources: %w", err)
		}

		if len(backupBucketList.Items) == 0 {
			return fmt.Errorf("no backup bucket found for etcd %s/%s", etcd.Namespace, etcd.Name)
		}

		var backupBucket *extensionsv1alpha1.BackupBucket
		etcdContainerName := *etcd.Spec.Backup.Store.Container
		for i := range backupBucketList.Items {
			bucket := &backupBucketList.Items[i]
			if eitherContains(bucket.Name, etcdContainerName) || bucket.Annotations[gdc.FullyQualifiedBucketNameAnnotationKey] == etcdContainerName {
				backupBucket = bucket
				logger.Info("Found matching backup bucket for etcd", "etcd", client.ObjectKeyFromObject(etcd), "bucketName", backupBucket.Name)
				break
			}
		}
		if backupBucket == nil {
			return fmt.Errorf("no matching backup bucket found for etcd %s/%s with container name %s", etcd.Namespace, etcd.Name, etcdContainerName)
		}

		fqBucketName, exists := backupBucket.Annotations[gdc.FullyQualifiedBucketNameAnnotationKey]
		if !exists || fqBucketName == "" {
			return fmt.Errorf("fqBucketName annotation not found in BackupBucket %s", backupBucket.Name)
		}

		// The `container` field must contain the fully qualified bucket name
		etcd.Spec.Backup.Store.Container = &fqBucketName

		// Update the storage provider to use AWS (S3 Snapstore)
		var backupProvider druidv1alpha1.StorageProvider = "aws"
		etcd.Spec.Backup.Store.Provider = &backupProvider
	}
	return nil
}

// eitherContains checks if one string is a substring of the other.
func eitherContains(s1, s2 string) bool {
	if len(s1) < len(s2) {
		s1, s2 = s2, s1
	}
	return strings.Contains(s1, s2)
}
