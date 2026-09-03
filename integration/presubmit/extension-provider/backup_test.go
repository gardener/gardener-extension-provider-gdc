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

package main

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	globalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/object/v1"
	objectv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/object/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/kubernetes"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
)

const (
	dualZoneBucketLocation = "async-grover-gonzo"
	dualZoneBucketTimeout  = 30 * time.Minute
	zonalBucketTimeout     = 10 * time.Minute
	pollingInterval        = 15 * time.Second
)

type backupTestFixture struct {
	*commonTestFixture
}

func (f *backupTestFixture) test(t *testing.T) {
	ctx := context.Background()

	// Create a dedicated, isolated vcluster client for this subtest
	f.vucClient = f.NewVClusterClient(t)

	// Create secret with service account and gdch-config
	sa, err := os.ReadFile(*safile)
	if err != nil {
		t.Fatalf("cannot read service account file %v", err)
	}
	zonalGDCHConfig := &gdcclient.OrgClusterConfig{
		OrgClusterURL: "https://management-kube.apiserver." + *org + "." + *zone + "." + *labURL,
		CAData:        f.gdchConfig.CAData,
	}
	zonalSecret, err := f.createBackupSecret(ctx, t, sa, "sa-backup-", zonalGDCHConfig)
	if err != nil {
		t.Fatalf("unable to create Secret for backup test %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, zonalSecret); err != nil {
			t.Fatalf("unable to delete Secret for backup test %v", err)
		}
	})

	t.Run("BackupBucketCreation", func(t *testing.T) {
		f.testBackupBucketCreation(t, ctx, corev1.SecretReference{
			Name:      zonalSecret.Name,
			Namespace: zonalSecret.Namespace,
		})
	})

	t.Run("BackupEntryDeletionWithNonExistingBucket", func(t *testing.T) {
		f.testBackupEntryCreation(t, ctx, "backupentry-non-existing-"+*commitHash, "non-existing-bucket-"+*commitHash, corev1.SecretReference{
			Name:      zonalSecret.Name,
			Namespace: zonalSecret.Namespace,
		})
	})

	t.Run("BackupEntryWithPreExistingBucket", func(t *testing.T) {
		f.testBackupEntryWithPreExistingBucket(t, ctx, corev1.SecretReference{
			Name:      zonalSecret.Name,
			Namespace: zonalSecret.Namespace,
		})
	})

	t.Run("DualzoneBackupBucketCreation", func(t *testing.T) {
		globalSecret, err := f.createBackupSecret(ctx, t, sa, "sa-backup-global-", f.gdchConfig)
		if err != nil {
			t.Fatalf("unable to create Secret for backup test %v", err)
		}
		t.Cleanup(func() {
			if err := f.vucClient.Delete(ctx, globalSecret); err != nil {
				t.Fatalf("unable to delete Secret for backup test %v", err)
			}
		})

		f.testDualZoneBackupBucketCreation(t, ctx, corev1.SecretReference{
			Name:      globalSecret.Name,
			Namespace: globalSecret.Namespace,
		})
	})
}

func (f *backupTestFixture) testBackupBucketCreation(t *testing.T, ctx context.Context, secretRef corev1.SecretReference) {
	// Action: create zonal BackupBucket
	backupBucketName := "backupbucket-" + *commitHash
	backupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: backupBucketName,
		},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
			},
			Region:    *region,
			SecretRef: secretRef,
		},
	}
	if err := f.vucClient.Create(ctx, backupBucket); err != nil {
		t.Fatalf("failed to create BackupBucket %q, %v", backupBucket.Name, err)
	}
	t.Cleanup(func() {
		t.Logf("cleaning up BackupBucket object %q", backupBucket.Name)
		if err := f.vucClient.Delete(ctx, backupBucket); err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			t.Fatalf("cannot delete BackupBucket object %v", err)
		}
		if err := wait.PollUntilContextTimeout(ctx, pollingInterval, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(backupBucket), backupBucket); err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		}); err != nil {
			t.Fatalf("error waiting for BackupBucket object to be deleted: %v", err)
		}
	})

	// Assert: BackupBucket is in Succeeded state
	backupBucketList := &extensionsv1alpha1.BackupBucketList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": backupBucketName},
	}
	if err := kubernetes.WaitForCondition[*extensionsv1alpha1.BackupBucket](ctx, zonalBucketTimeout, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, backupBucketList, listOptions...)
	}, func(obj *extensionsv1alpha1.BackupBucket) bool {
		lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
		t.Logf("Waiting for %q, State: %q, LastError: %q",
			obj.Name,
			lastOptState,
			ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
		return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
	}); err != nil {
		t.Fatalf("BackupBucket is not Succeeded in %d minutes %v", int(zonalBucketTimeout.Minutes()), err)
	}

	// Assert GDC Resource: Bucket
	bucketList := &objectv1.BucketList{}
	listOptions = []client.ListOption{
		client.MatchingFields{"metadata.name": backupBucketName},
		client.InNamespace(f.project),
	}
	if err := kubernetes.WaitForCondition[*objectv1.Bucket](ctx, zonalBucketTimeout, func() (watch.Interface, error) {
		return f.mgmtClient.Watch(ctx, bucketList, listOptions...)
	}, func(obj *objectv1.Bucket) bool {
		for _, condition := range obj.Status.Conditions {
			if condition.Type == objectv1.BucketReady && condition.Status == metav1.ConditionTrue {
				return true
			}
		}
		return false
	}); err != nil {
		t.Errorf("GDC Bucket %s is not Ready in %d minutes: %v", backupBucketName, int(zonalBucketTimeout.Minutes()), err)
	}
}

func (f *backupTestFixture) testDualZoneBackupBucketCreation(t *testing.T, ctx context.Context, secretRef corev1.SecretReference) {

	// Action: create DualZone BackupBucket
	backupBucketName := "dualzone-backupbucket-" + *commitHash
	backupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: backupBucketName,
		},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(t, &v1alpha1.BackupBucketConfig{
						// Assume an existing BackupLocation
						// See: go/extension-provider-integration-test
						TypeMeta: metav1.TypeMeta{
							Kind:       "BackupBucketConfig",
							APIVersion: "gdch.provider.extensions.gardener.gdc.goog/v1alpha1",
						},
						DualZoneBucketLocation: dualZoneBucketLocation,
					}),
				},
			},
			Region:    *region,
			SecretRef: secretRef,
		},
	}
	if err := f.vucClient.Create(ctx, backupBucket); err != nil {
		t.Fatalf("failed to create BackupBucket %q, %v", backupBucket.Name, err)
	}
	t.Cleanup(func() {
		t.Logf("cleaning up BackupBucket object %q", backupBucket.Name)
		if err := f.vucClient.Delete(ctx, backupBucket); err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			t.Fatalf("cannot delete BackupBucket object %v", err)
		}
		if err := wait.PollUntilContextTimeout(ctx, pollingInterval, dualZoneBucketTimeout, true, func(ctx context.Context) (bool, error) {
			if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(backupBucket), backupBucket); err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		}); err != nil {
			t.Fatalf("error waiting for BackupBucket object to be deleted in %d minutes: %v", int(dualZoneBucketTimeout.Minutes()), err)
		}
	})

	// Assert: BackupBucket is in Succeeded state
	backupBucketList := &extensionsv1alpha1.BackupBucketList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": backupBucketName},
	}
	if err := kubernetes.WaitForCondition[*extensionsv1alpha1.BackupBucket](ctx, dualZoneBucketTimeout, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, backupBucketList, listOptions...)
	}, func(obj *extensionsv1alpha1.BackupBucket) bool {
		t.Logf("Waiting for %q, LastError: %q",
			obj.Name,
			ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
		lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
		return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
	}); err != nil {
		t.Fatalf("BackupBucket is not Succeeded in %d minutes %v", int(dualZoneBucketTimeout.Minutes()), err)
	}

	// Assert GDC Resource: DualZone Bucket
	globalBucketList := &globalv1.BucketList{}
	globalListOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": backupBucketName},
		client.InNamespace(f.project),
	}
	if err := kubernetes.WaitForCondition[*globalv1.Bucket](ctx, dualZoneBucketTimeout, func() (watch.Interface, error) {
		return f.globalClient.Watch(ctx, globalBucketList, globalListOptions...)
	}, func(obj *globalv1.Bucket) bool {
		for _, condition := range obj.Status.Conditions {
			if condition.Type == objectv1.BucketReady && condition.Status == metav1.ConditionTrue {
				return true
			}
		}
		return false
	}); err != nil {
		t.Errorf("GDC DualZone Bucket %s is not Ready in %d minutes: %v", backupBucketName, int(dualZoneBucketTimeout.Minutes()), err)
	}
}

func (f *backupTestFixture) testBackupEntryCreation(t *testing.T, ctx context.Context, name, bucketName string, secretRef corev1.SecretReference) {
	entry := &extensionsv1alpha1.BackupEntry{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: extensionsv1alpha1.BackupEntrySpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
			},
			Region:     *region,
			BucketName: bucketName,
			SecretRef:  secretRef,
		},
	}
	if err := f.vucClient.Create(ctx, entry); err != nil {
		t.Fatalf("failed to create backupentry %q, %v", entry.Name, err)
	}
	t.Cleanup(func() {
		t.Logf("cleaning up BackupEntry %q", entry.Name)
		if err := f.vucClient.Delete(ctx, entry); err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			t.Fatalf("cannot delete BackupEntry object %v", err)
		}
		if err := wait.PollUntilContextTimeout(ctx, pollingInterval, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(entry), entry); err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		}); err != nil {
			t.Fatalf("error waiting for BackupEntry object to be deleted: %v", err)
		}
	})

	backupEntryList := &extensionsv1alpha1.BackupEntryList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": entry.Name},
	}
	if err := kubernetes.WaitForCondition[*extensionsv1alpha1.BackupEntry](ctx, 5*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, backupEntryList, listOptions...)
	}, func(obj *extensionsv1alpha1.BackupEntry) bool {
		lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
		t.Logf("Waiting for %q, State: %q, LastError: %q",
			obj.Name,
			lastOptState,
			ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
		return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
	}); err != nil {
		t.Fatalf("BackupEntry is not Succeeded in 5 minutes %v", err)
	}
}

func (f *backupTestFixture) testBackupEntryWithPreExistingBucket(t *testing.T, ctx context.Context, secretRef corev1.SecretReference) {
	// Action: Create Bucket
	bucketName := "pre-existing-bucket-" + *commitHash
	bucket := &objectv1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucketName,
			Namespace: f.project,
		},
		Spec: objectv1.BucketSpec{
			Description:  "pre-existing bucket for testing",
			StorageClass: objectv1.Standard,
		},
	}
	if err := f.mgmtClient.Create(ctx, bucket); err != nil {
		t.Fatalf("failed to create Bucket %q, %v", bucket.Name, err)
	}
	t.Cleanup(func() {
		t.Logf("cleaning up Bucket object %q in namespace %q", bucket.Name, bucket.Namespace)
		if err := f.mgmtClient.Delete(ctx, bucket); err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			t.Fatalf("cannot delete Bucket object %v", err)
		}
	})

	// Wait for Bucket to be ready
	bucketList := &objectv1.BucketList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": bucketName},
		client.InNamespace(f.project),
	}
	if err := kubernetes.WaitForCondition[*objectv1.Bucket](ctx, zonalBucketTimeout, func() (watch.Interface, error) {
		return f.mgmtClient.Watch(ctx, bucketList, listOptions...)
	}, func(obj *objectv1.Bucket) bool {
		for _, condition := range obj.Status.Conditions {
			if condition.Type == objectv1.BucketReady && condition.Status == metav1.ConditionTrue {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatalf("Bucket is not Ready in %d minutes %v", int(zonalBucketTimeout.Minutes()), err)
	}

	// Action: Create BackupEntry pointing to existing bucket
	f.testBackupEntryCreation(t, ctx, "backupentry-pre-existing-"+*commitHash, bucketName, secretRef)
}

func (f *backupTestFixture) createBackupSecret(ctx context.Context, t *testing.T, sa []byte, namePrefix string, config interface{}) (*corev1.Secret, error) {
	rawConfig, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("cannot marshal gdch-config %v", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namePrefix + *commitHash,
			Namespace: f.namespace,
		},
		Data: map[string][]byte{
			"serviceaccount.json": sa,
			"gdch-config":         rawConfig,
		},
	}
	if err := f.vucClient.Create(ctx, secret); err != nil {
		return nil, err
	}
	return secret, nil
}
