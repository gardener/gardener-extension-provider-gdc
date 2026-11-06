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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
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

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	gdcs3 "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/s3"
	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/kubernetes"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/storage"
)

const (
	dualZoneBucketLocation               = "async-grover-gonzo"
	dualZoneBucketTimeout                = 30 * time.Minute
	zonalBucketTimeout                   = 10 * time.Minute
	pollingInterval                      = 15 * time.Second
	fastPollingInterval                  = 1 * time.Second
	perfObjectCount                      = 10005
	deleteMarkerSweepKeyMarkerAnnotation = "gdch.provider.extensions.gardener.cloud/delete-marker-sweep-key-marker"
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
	t.Cleanup(func() {
		dumpPodLogsOnFailure(t, ctx, f.vclusterKubeconfig, f.namespace)
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
		}, sa, zonalGDCHConfig)
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
		f.deleteBackupBucketAndWait(t, ctx, backupBucket.Name, zonalBucketTimeout)
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
		f.deleteBackupEntryAndWait(t, ctx, entry.Name)
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

func (f *backupTestFixture) deleteBackupEntryAndWait(t *testing.T, ctx context.Context, entryName string) time.Duration {
	t.Helper()
	entry := &extensionsv1alpha1.BackupEntry{
		ObjectMeta: metav1.ObjectMeta{
			Name: entryName,
		},
	}
	start := time.Now()
	notFound := false
	if err := wait.PollUntilContextTimeout(ctx, fastPollingInterval, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := f.vucClient.Delete(ctx, entry); err != nil {
			if apierrors.IsNotFound(err) {
				notFound = true
				return true, nil
			}
			t.Logf("transient error issuing Delete for BackupEntry %q (will retry): %v", entry.Name, err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("cannot delete BackupEntry object %q: %v", entry.Name, err)
	}
	if notFound {
		return 0
	}
	t.Logf("cleaning up BackupEntry %q", entry.Name)
	var prevState, prevErr string
	if err := wait.PollUntilContextTimeout(ctx, fastPollingInterval, zonalBucketTimeout, true, func(ctx context.Context) (bool, error) {
		current := &extensionsv1alpha1.BackupEntry{}
		if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(entry), current); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			t.Logf("transient error polling BackupEntry %q deletion (will retry): %v", entry.Name, err)
			return false, nil
		}
		state := string(ptr.Deref(current.Status.LastOperation, gardencorev1beta1.LastOperation{}).State)
		lastErr := ptr.Deref(current.Status.LastError, gardencorev1beta1.LastError{}).Description
		if state != prevState || lastErr != prevErr {
			t.Logf("[PERF] BackupEntry.Delete (%q) at +%v: state=%q lastError=%q",
				entry.Name, time.Since(start).Round(time.Millisecond), state, lastErr)
			prevState, prevErr = state, lastErr
		}
		return false, nil
	}); err != nil {
		t.Fatalf("error waiting for BackupEntry object %q to be deleted: %v", entry.Name, err)
	}
	return time.Since(start)
}

func (f *backupTestFixture) deleteBackupBucketAndWait(t *testing.T, ctx context.Context, bucketName string, timeout time.Duration) (time.Duration, int) {
	t.Helper()
	backupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: bucketName,
		},
	}
	start := time.Now()
	notFound := false
	if err := wait.PollUntilContextTimeout(ctx, fastPollingInterval, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := f.vucClient.Delete(ctx, backupBucket); err != nil {
			if apierrors.IsNotFound(err) {
				notFound = true
				return true, nil
			}
			t.Logf("transient error issuing Delete for BackupBucket %q (will retry): %v", backupBucket.Name, err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("cannot delete BackupBucket object %q: %v", backupBucket.Name, err)
	}
	if notFound {
		return 0, 0
	}
	t.Logf("cleaning up BackupBucket object %q", backupBucket.Name)
	var prevState, prevErr, prevKeyMarker string
	checkpoints := 0
	if err := wait.PollUntilContextTimeout(ctx, fastPollingInterval, timeout, true, func(ctx context.Context) (bool, error) {
		current := &extensionsv1alpha1.BackupBucket{}
		if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(backupBucket), current); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			t.Logf("transient error polling BackupBucket %q deletion (will retry): %v", backupBucket.Name, err)
			return false, nil
		}
		state := string(ptr.Deref(current.Status.LastOperation, gardencorev1beta1.LastOperation{}).State)
		lastErr := ptr.Deref(current.Status.LastError, gardencorev1beta1.LastError{}).Description
		keyMarker := current.Annotations[deleteMarkerSweepKeyMarkerAnnotation]
		if keyMarker != "" && keyMarker != prevKeyMarker {
			checkpoints++
		}
		if state != prevState || lastErr != prevErr || keyMarker != prevKeyMarker {
			t.Logf("[PERF] BackupBucket.Delete (%q) at +%v: state=%q sweepKeyMarker=%q lastError=%q",
				backupBucket.Name, time.Since(start).Round(time.Millisecond), state, keyMarker, lastErr)
			prevState, prevErr, prevKeyMarker = state, lastErr, keyMarker
		}
		return false, nil
	}); err != nil {
		t.Fatalf("error waiting for BackupBucket object %q to be deleted: %v", backupBucket.Name, err)
	}
	return time.Since(start), checkpoints
}

func legacySweepLifecyclePolicy() *objectv1.LifecyclePolicy {
	return &objectv1.LifecyclePolicy{
		Enable: true,
		LifecycleRules: []*objectv1.LifecycleRule{
			{
				ID:     ptr.To("expire-noncurrent-backup-versions"),
				Status: ptr.To(objectv1.Enabled),
				NoncurrentExpiration: &objectv1.LifecycleNoncurrentExpiration{
					NoncurrentDays: ptr.To(int64(1)),
				},
			},
		},
	}
}

func (f *backupTestFixture) testBackupEntryWithPreExistingBucket(t *testing.T, ctx context.Context, secretRef corev1.SecretReference, sa []byte, gdchConfig *gdcclient.OrgClusterConfig) {
	// Action: Create Bucket configured with a legacy LifecyclePolicy (ExpiredObjectDeleteMarker=nil)
	// so storage.NeedsExpiredDeleteMarkerSweep is true while DefaultObjectRetentionDays is unset
	// (preventing 24h Object Lock retention on uploaded test objects).
	bucketName := "pre-existing-bucket-" + *commitHash
	bucket := &objectv1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucketName,
			Namespace: f.project,
		},
		Spec: objectv1.BucketSpec{
			Description:  "pre-existing bucket for testing",
			StorageClass: objectv1.Standard,
			BucketPolicy: &objectv1.BucketPolicy{
				LockingPolicy:   &objectv1.LockingPolicy{},
				LifecyclePolicy: legacySweepLifecyclePolicy(),
			},
		},
	}
	if err := f.mgmtClient.Create(ctx, bucket); err != nil {
		t.Logf("[PERF] Creating Bucket %q with empty LockingPolicy returned %v; retrying with LifecyclePolicy only", bucket.Name, err)
		bucket.Spec.BucketPolicy.LockingPolicy = nil
		if retryErr := f.mgmtClient.Create(ctx, bucket); retryErr != nil {
			t.Fatalf("failed to create Bucket %q, %v", bucket.Name, retryErr)
		}
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

	readyBucket, err := storage.GetAndValidateZonalBucket(ctx, f.mgmtClient, client.ObjectKeyFromObject(bucket))
	if err != nil {
		t.Fatalf("failed to get ready zonal bucket %q: %v", bucketName, err)
	}
	s3Client, rawS3 := f.newZonalBucketS3Clients(t, ctx, readyBucket, sa, gdchConfig)
	bucketFQN := readyBucket.Status.FullyQualifiedName

	// Registered after bucket creation so LIFO cleanup purges all retained versions
	// and delete markers before deleting the GDC Bucket CR.
	t.Cleanup(func() {
		purgeBucketVersionsAndMarkers(t, ctx, rawS3, bucketFQN)
	})

	// Enable S3 versioning directly if not already enabled by GDC Bucket controller.
	verOut, err := rawS3.GetBucketVersioningWithContext(ctx, &awss3.GetBucketVersioningInput{
		Bucket: aws.String(bucketFQN),
	})
	if err != nil {
		t.Fatalf("failed to get bucket versioning for %q: %v", bucketFQN, err)
	}
	if aws.StringValue(verOut.Status) != awss3.BucketVersioningStatusEnabled {
		if _, putVerErr := rawS3.PutBucketVersioningWithContext(ctx, &awss3.PutBucketVersioningInput{
			Bucket: aws.String(bucketFQN),
			VersioningConfiguration: &awss3.VersioningConfiguration{
				Status: aws.String(awss3.BucketVersioningStatusEnabled),
			},
		}); putVerErr != nil {
			t.Logf("[PERF] PutBucketVersioning on %q returned: %v", bucketFQN, putVerErr)
		}
		verOut, err = rawS3.GetBucketVersioningWithContext(ctx, &awss3.GetBucketVersioningInput{
			Bucket: aws.String(bucketFQN),
		})
		if err != nil {
			t.Fatalf("failed to re-check bucket versioning for %q: %v", bucketFQN, err)
		}
	}
	versioned := aws.StringValue(verOut.Status) == awss3.BucketVersioningStatusEnabled
	t.Logf("[PERF] Bucket %q versioning enabled=%v (status=%q)", bucketFQN, versioned, aws.StringValue(verOut.Status))

	// Action: Create two BackupEntries pointing to the same existing bucket to verify
	// prefix-scoped deletion and sibling prefix isolation on a populated bucket.
	entryName := "backupentry-pre-existing-" + *commitHash
	siblingEntryName := "backupentry-pre-existing-sibling-" + *commitHash
	f.testBackupEntryCreation(t, ctx, entryName, bucketName, secretRef)
	f.testBackupEntryCreation(t, ctx, siblingEntryName, bucketName, secretRef)

	entryKeys := []string{
		entryName + "/etcd-main/v2/Full-00000000-00000001.gz",
		entryName + "/etcd-main/v2/Incr-00000001-00000002.gz",
	}
	siblingKey := siblingEntryName + "/etcd-main/v2/Full-00000000-00000001.gz"

	for _, key := range slices.Concat(entryKeys, []string{siblingKey}) {
		if _, err := s3Client.UploadObject(gdcs3.UploadObjectInput{
			BucketFqn: bucketFQN,
			Key:       key,
			Reader:    bytes.NewReader([]byte("test-backup-payload-" + key)),
		}); err != nil {
			t.Fatalf("failed to upload S3 object %q to bucket %q: %v", key, bucketFQN, err)
		}
		f.assertS3ObjectExists(t, s3Client, bucketFQN, key)
	}

	// Seed 10,005 live objects (>10 pages of 1,000 keys) to measure BackupEntry.Delete
	// and DeleteExpiredDeleteMarkers() performance at 10k-object scale.
	const perfPrefix = "perf-seed/"
	uploadObjectsConcurrently(t, ctx, rawS3, bucketFQN, perfPrefix, perfObjectCount)

	// Delete the primary BackupEntry and verify its objects are soft-deleted while the sibling prefix
	// and the 10,005 seeded objects remain intact.
	primaryDeleteElapsed := f.deleteBackupEntryAndWait(t, ctx, entryName)
	t.Logf("[PERF] BackupEntry.Delete (%q) with %d live objects in bucket took %v",
		entryName, perfObjectCount, primaryDeleteElapsed)

	for _, key := range entryKeys {
		f.assertS3ObjectDeleted(t, s3Client, bucketFQN, key)
	}
	f.assertS3ObjectExists(t, s3Client, bucketFQN, siblingKey)
	assertVersionedSoftDelete(t, ctx, rawS3, bucketFQN, entryName+"/", len(entryKeys), siblingEntryName+"/", 1)

	if versioned {
		// Convert the 10,005 live objects into 10,005 expired delete markers (simulating noncurrentExpiration
		// expiring the noncurrent versions after soft-deletion) and measure BackupEntry.Delete,
		// BackupBucket.Reconcile, and BackupBucket.Delete against 10,005 expired delete markers (11 pages).
		convertLiveObjectsToExpiredDeleteMarkers(t, ctx, rawS3, bucketFQN, perfPrefix, perfObjectCount)

		siblingDeleteElapsed := f.deleteBackupEntryAndWait(t, ctx, siblingEntryName)
		f.assertS3ObjectDeleted(t, s3Client, bucketFQN, siblingKey)
		versionsAfterSibling, markersAfterSibling, pagesAfterSibling := countVersionsAndDeleteMarkers(t, ctx, rawS3, bucketFQN, perfPrefix)
		t.Logf("[PERF] BackupEntry.Delete (%q) with %d expired delete markers in bucket took %v (remaining under %q: versions=%d deleteMarkers=%d pages=%d)",
			siblingEntryName, perfObjectCount, siblingDeleteElapsed, perfPrefix, versionsAfterSibling, markersAfterSibling, pagesAfterSibling)

		// Purge the soft-deleted entryName/ and siblingEntryName/ noncurrent versions so only expired delete markers remain.
		purgePrefixVersionsAndMarkers(t, ctx, rawS3, bucketFQN, entryName+"/")
		purgePrefixVersionsAndMarkers(t, ctx, rawS3, bucketFQN, siblingEntryName+"/")

		if markersAfterSibling < perfObjectCount {
			t.Logf("[PERF] Re-seeding %d expired delete markers under %q after BackupEntry.Delete swept them", perfObjectCount, perfPrefix)
			seedExpiredDeleteMarkers(t, ctx, rawS3, bucketFQN, perfPrefix, perfObjectCount)
		}

		f.measureBackupBucketSweepPerformance(t, ctx, bucketName, secretRef, rawS3, bucketFQN, perfPrefix)
		return
	}

	// Delete the sibling BackupEntry and verify its object is also soft-deleted.
	siblingDeleteElapsed := f.deleteBackupEntryAndWait(t, ctx, siblingEntryName)
	t.Logf("[PERF] BackupEntry.Delete (%q) with %d live objects in unversioned bucket took %v",
		siblingEntryName, perfObjectCount, siblingDeleteElapsed)
	f.assertS3ObjectDeleted(t, s3Client, bucketFQN, siblingKey)
	assertVersionedSoftDelete(t, ctx, rawS3, bucketFQN, siblingEntryName+"/", 1, "", 0)

	// Since pre-existing-bucket was unversioned, run the 10,005 expired-delete-marker sweep benchmark
	// on the versioned BackupBucket created by BackupBucketCreation.
	f.measureExpiredDeleteMarkerSweepOnVersionedBackupBucket(t, ctx, secretRef, sa, gdchConfig)
}

func (f *backupTestFixture) measureBackupBucketSweepPerformance(t *testing.T, ctx context.Context, bucketName string, secretRef corev1.SecretReference, rawS3 *awss3.S3, bucketFQN, markerPrefix string) {
	t.Helper()
	backupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: bucketName,
		},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(t, &v1alpha1.BackupBucketConfig{
						TypeMeta: metav1.TypeMeta{
							Kind:       "BackupBucketConfig",
							APIVersion: "gdch.provider.extensions.gardener.gdc.goog/v1alpha1",
						},
						DefaultObjectRetentionDays: ptr.To(int32(0)),
					}),
				},
			},
			Region:    *region,
			SecretRef: secretRef,
		},
	}
	reconcileStart := time.Now()
	if err := f.vucClient.Create(ctx, backupBucket); err != nil {
		t.Fatalf("failed to create BackupBucket %q for sweep benchmark: %v", backupBucket.Name, err)
	}
	t.Cleanup(func() {
		f.deleteBackupBucketAndWait(t, ctx, backupBucket.Name, zonalBucketTimeout)
	})

	backupBucketList := &extensionsv1alpha1.BackupBucketList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": bucketName},
	}
	if err := kubernetes.WaitForCondition[*extensionsv1alpha1.BackupBucket](ctx, zonalBucketTimeout, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, backupBucketList, listOptions...)
	}, func(obj *extensionsv1alpha1.BackupBucket) bool {
		lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
		return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
	}); err != nil {
		t.Fatalf("BackupBucket %q is not Succeeded in %d minutes: %v", bucketName, int(zonalBucketTimeout.Minutes()), err)
	}
	reconcileElapsed := time.Since(reconcileStart)
	versionsAfterReconcile, markersAfterReconcile, pagesAfterReconcile := countVersionsAndDeleteMarkers(t, ctx, rawS3, bucketFQN, markerPrefix)
	t.Logf("[PERF] BackupBucket.Reconcile (%q) with %d expired delete markers took %v (remaining under %q: versions=%d deleteMarkers=%d pages=%d)",
		bucketName, perfObjectCount, reconcileElapsed, markerPrefix, versionsAfterReconcile, markersAfterReconcile, pagesAfterReconcile)

	if markersAfterReconcile < perfObjectCount {
		t.Logf("[PERF] Re-seeding %d expired delete markers under %q before BackupBucket.Delete", perfObjectCount, markerPrefix)
		seedExpiredDeleteMarkers(t, ctx, rawS3, bucketFQN, markerPrefix, perfObjectCount)
	}

	deleteElapsed, checkpoints := f.deleteBackupBucketAndWait(t, ctx, backupBucket.Name, zonalBucketTimeout)
	t.Logf("[PERF] BackupBucket.Delete (%q) with %d expired delete markers took %v (observedSweepCheckpoints=%d)",
		backupBucket.Name, perfObjectCount, deleteElapsed, checkpoints)
}

func (f *backupTestFixture) measureExpiredDeleteMarkerSweepOnVersionedBackupBucket(t *testing.T, ctx context.Context, secretRef corev1.SecretReference, sa []byte, gdchConfig *gdcclient.OrgClusterConfig) {
	t.Helper()
	backupBucketName := "backupbucket-sweep-" + *commitHash
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
		t.Fatalf("failed to create BackupBucket %q: %v", backupBucket.Name, err)
	}
	t.Cleanup(func() {
		f.deleteBackupBucketAndWait(t, ctx, backupBucket.Name, zonalBucketTimeout)
	})

	backupBucketList := &extensionsv1alpha1.BackupBucketList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": backupBucketName},
	}
	if err := kubernetes.WaitForCondition[*extensionsv1alpha1.BackupBucket](ctx, zonalBucketTimeout, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, backupBucketList, listOptions...)
	}, func(obj *extensionsv1alpha1.BackupBucket) bool {
		lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
		return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
	}); err != nil {
		t.Fatalf("BackupBucket %q is not Succeeded in %d minutes: %v", backupBucketName, int(zonalBucketTimeout.Minutes()), err)
	}

	readyBucket, err := storage.GetAndValidateZonalBucket(ctx, f.mgmtClient, client.ObjectKey{Name: backupBucketName, Namespace: f.project})
	if err != nil {
		t.Fatalf("failed to get ready zonal bucket %q: %v", backupBucketName, err)
	}
	_, rawS3 := f.newZonalBucketS3Clients(t, ctx, readyBucket, sa, gdchConfig)
	bucketFQN := readyBucket.Status.FullyQualifiedName
	t.Cleanup(func() {
		purgeBucketVersionsAndMarkers(t, ctx, rawS3, bucketFQN)
	})

	f.ensureLegacySweepLifecyclePolicy(t, ctx, backupBucketName)

	const markerPrefix = "perf-marker/"
	seedExpiredDeleteMarkersWithoutPut(t, ctx, rawS3, bucketFQN, markerPrefix, perfObjectCount)

	perfEntryName := "backupentry-perf-sweep-" + *commitHash
	f.testBackupEntryCreation(t, ctx, perfEntryName, backupBucketName, secretRef)
	entryDeleteElapsed := f.deleteBackupEntryAndWait(t, ctx, perfEntryName)
	versionsAfterEntryDelete, markersAfterEntryDelete, pagesAfterEntryDelete := countVersionsAndDeleteMarkers(t, ctx, rawS3, bucketFQN, markerPrefix)
	t.Logf("[PERF] BackupEntry.Delete (%q) on versioned bucket with %d expired delete markers took %v (remaining under %q: versions=%d deleteMarkers=%d pages=%d)",
		perfEntryName, perfObjectCount, entryDeleteElapsed, markerPrefix, versionsAfterEntryDelete, markersAfterEntryDelete, pagesAfterEntryDelete)

	if markersAfterEntryDelete < perfObjectCount {
		t.Logf("[PERF] Re-seeding %d expired delete markers before BackupBucket.Delete", perfObjectCount)
		seedExpiredDeleteMarkersWithoutPut(t, ctx, rawS3, bucketFQN, markerPrefix, perfObjectCount)
	}

	bucketDeleteElapsed, checkpoints := f.deleteBackupBucketAndWait(t, ctx, backupBucket.Name, zonalBucketTimeout)
	t.Logf("[PERF] BackupBucket.Delete (%q) with %d expired delete markers took %v (observedSweepCheckpoints=%d)",
		backupBucket.Name, perfObjectCount, bucketDeleteElapsed, checkpoints)
}

func (f *backupTestFixture) ensureLegacySweepLifecyclePolicy(t *testing.T, ctx context.Context, bucketName string) {
	t.Helper()
	b := &objectv1.Bucket{}
	if err := f.mgmtClient.Get(ctx, client.ObjectKey{Name: bucketName, Namespace: f.project}, b); err != nil {
		t.Fatalf("failed to fetch GDC Bucket %q: %v", bucketName, err)
	}
	if b.Spec.BucketPolicy == nil {
		b.Spec.BucketPolicy = &objectv1.BucketPolicy{}
	}
	b.Spec.BucketPolicy.LifecyclePolicy = legacySweepLifecyclePolicy()
	if err := f.mgmtClient.Update(ctx, b); err != nil {
		t.Fatalf("failed to update GDC Bucket %q with legacy LifecyclePolicy: %v", bucketName, err)
	}
}

func uploadObjectsConcurrently(t *testing.T, ctx context.Context, rawS3 *awss3.S3, bucketFQN, prefix string, count int) {
	t.Helper()
	start := time.Now()
	const workers = 64
	jobs := make(chan int, count)
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	payload := []byte("perf-test-object")
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				key := fmt.Sprintf("%sobj-%05d", prefix, idx)
				var lastErr error
				for attempt := 0; attempt < 3; attempt++ {
					_, lastErr = rawS3.PutObjectWithContext(ctx, &awss3.PutObjectInput{
						Bucket: aws.String(bucketFQN),
						Key:    aws.String(key),
						Body:   bytes.NewReader(payload),
					})
					if lastErr == nil {
						break
					}
					time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				}
				if lastErr != nil {
					select {
					case errCh <- fmt.Errorf("failed to upload %q: %w", key, lastErr):
					default:
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		t.Fatalf("concurrent object upload failed in bucket %q: %v", bucketFQN, err)
	}
	t.Logf("[PERF] Uploaded %d live objects under prefix %q in bucket %q in %v", count, prefix, bucketFQN, time.Since(start))
}

func convertLiveObjectsToExpiredDeleteMarkers(t *testing.T, ctx context.Context, rawS3 *awss3.S3, bucketFQN, prefix string, count int) {
	t.Helper()
	start := time.Now()
	// 1. Soft-delete all keys (without VersionId) so S3 creates a current DeleteMarker for each key.
	const batchSize = 1000
	for startIdx := 0; startIdx < count; startIdx += batchSize {
		endIdx := min(startIdx+batchSize, count)
		objs := make([]*awss3.ObjectIdentifier, 0, endIdx-startIdx)
		for i := startIdx; i < endIdx; i++ {
			objs = append(objs, &awss3.ObjectIdentifier{
				Key: aws.String(fmt.Sprintf("%sobj-%05d", prefix, i)),
			})
		}
		out, err := rawS3.DeleteObjectsWithContext(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(bucketFQN),
			Delete: &awss3.Delete{Objects: objs, Quiet: aws.Bool(true)},
		})
		if err != nil {
			t.Fatalf("failed to soft-delete batch [%d,%d) in bucket %q: %v", startIdx, endIdx, bucketFQN, err)
		}
		if out != nil && len(out.Errors) > 0 {
			firstErr := out.Errors[0]
			t.Fatalf("failed to soft-delete %d objects in bucket %q; first %q: %s: %s",
				len(out.Errors), bucketFQN, aws.StringValue(firstErr.Key), aws.StringValue(firstErr.Code), aws.StringValue(firstErr.Message))
		}
	}

	// 2. Delete all noncurrent object versions (with VersionId) under prefix, leaving only the DeleteMarkers.
	var versionIDs []*awss3.ObjectIdentifier
	if err := rawS3.ListObjectVersionsPagesWithContext(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String(bucketFQN),
		Prefix: aws.String(prefix),
	}, func(page *awss3.ListObjectVersionsOutput, _ bool) bool {
		for _, v := range page.Versions {
			if v != nil && v.Key != nil && v.VersionId != nil {
				versionIDs = append(versionIDs, &awss3.ObjectIdentifier{
					Key:       v.Key,
					VersionId: v.VersionId,
				})
			}
		}
		return true
	}); err != nil {
		t.Fatalf("failed to list noncurrent versions under %q in bucket %q: %v", prefix, bucketFQN, err)
	}

	for startIdx := 0; startIdx < len(versionIDs); startIdx += batchSize {
		endIdx := min(startIdx+batchSize, len(versionIDs))
		out, err := rawS3.DeleteObjectsWithContext(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(bucketFQN),
			Delete: &awss3.Delete{Objects: versionIDs[startIdx:endIdx], Quiet: aws.Bool(true)},
		})
		if err != nil {
			t.Fatalf("failed to delete noncurrent versions [%d,%d) in bucket %q: %v", startIdx, endIdx, bucketFQN, err)
		}
		if out != nil && len(out.Errors) > 0 {
			firstErr := out.Errors[0]
			t.Fatalf("failed to delete %d noncurrent versions in bucket %q; first %q (%q): %s: %s",
				len(out.Errors), bucketFQN, aws.StringValue(firstErr.Key), aws.StringValue(firstErr.VersionId), aws.StringValue(firstErr.Code), aws.StringValue(firstErr.Message))
		}
	}

	vCount, mCount, pages := countVersionsAndDeleteMarkers(t, ctx, rawS3, bucketFQN, prefix)
	if vCount != 0 || mCount != count {
		t.Fatalf("expected 0 versions and %d expired delete markers under %q in bucket %q, got versions=%d deleteMarkers=%d",
			count, prefix, bucketFQN, vCount, mCount)
	}
	t.Logf("[PERF] Converted %d live objects into %d expired delete markers across %d pages in %v",
		count, mCount, pages, time.Since(start))
}

func seedExpiredDeleteMarkers(t *testing.T, ctx context.Context, rawS3 *awss3.S3, bucketFQN, prefix string, count int) {
	t.Helper()
	purgePrefixVersionsAndMarkers(t, ctx, rawS3, bucketFQN, prefix)

	// Probe whether batch DeleteObjects (without VersionId) creates delete markers directly without PutObject.
	probeKey := fmt.Sprintf("%sobj-00000", prefix)
	_, err := rawS3.DeleteObjectsWithContext(ctx, &awss3.DeleteObjectsInput{
		Bucket: aws.String(bucketFQN),
		Delete: &awss3.Delete{
			Objects: []*awss3.ObjectIdentifier{{Key: aws.String(probeKey)}},
			Quiet:   aws.Bool(true),
		},
	})
	if err == nil {
		if v, m, _ := countVersionsAndDeleteMarkers(t, ctx, rawS3, bucketFQN, probeKey); v == 0 && m == 1 {
			purgePrefixVersionsAndMarkers(t, ctx, rawS3, bucketFQN, probeKey)
			seedExpiredDeleteMarkersWithoutPut(t, ctx, rawS3, bucketFQN, prefix, count)
			return
		}
	}

	uploadObjectsConcurrently(t, ctx, rawS3, bucketFQN, prefix, count)
	convertLiveObjectsToExpiredDeleteMarkers(t, ctx, rawS3, bucketFQN, prefix, count)
}

func seedExpiredDeleteMarkersWithoutPut(t *testing.T, ctx context.Context, rawS3 *awss3.S3, bucketFQN, prefix string, count int) {
	t.Helper()
	start := time.Now()
	purgePrefixVersionsAndMarkers(t, ctx, rawS3, bucketFQN, prefix)

	probeKey := fmt.Sprintf("%sobj-00000", prefix)
	_, _ = rawS3.DeleteObjectsWithContext(ctx, &awss3.DeleteObjectsInput{
		Bucket: aws.String(bucketFQN),
		Delete: &awss3.Delete{
			Objects: []*awss3.ObjectIdentifier{{Key: aws.String(probeKey)}},
			Quiet:   aws.Bool(true),
		},
	})
	_, probeMarkers, _ := countVersionsAndDeleteMarkers(t, ctx, rawS3, bucketFQN, probeKey)
	if probeMarkers == 1 {
		const batchSize = 1000
		for startIdx := 1; startIdx < count; startIdx += batchSize {
			endIdx := min(startIdx+batchSize, count)
			objs := make([]*awss3.ObjectIdentifier, 0, endIdx-startIdx)
			for i := startIdx; i < endIdx; i++ {
				objs = append(objs, &awss3.ObjectIdentifier{
					Key: aws.String(fmt.Sprintf("%sobj-%05d", prefix, i)),
				})
			}
			out, err := rawS3.DeleteObjectsWithContext(ctx, &awss3.DeleteObjectsInput{
				Bucket: aws.String(bucketFQN),
				Delete: &awss3.Delete{Objects: objs, Quiet: aws.Bool(true)},
			})
			if err != nil {
				t.Fatalf("failed to create delete markers batch [%d,%d) in bucket %q: %v", startIdx, endIdx, bucketFQN, err)
			}
			if out != nil && len(out.Errors) > 0 {
				firstErr := out.Errors[0]
				t.Fatalf("failed to create %d delete markers in bucket %q; first %q: %s: %s",
					len(out.Errors), bucketFQN, aws.StringValue(firstErr.Key), aws.StringValue(firstErr.Code), aws.StringValue(firstErr.Message))
			}
		}
	} else {
		const workers = 64
		jobs := make(chan int, count)
		for i := 0; i < count; i++ {
			jobs <- i
		}
		close(jobs)
		errCh := make(chan error, workers)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range jobs {
					key := fmt.Sprintf("%sobj-%05d", prefix, idx)
					if _, err := rawS3.DeleteObjectWithContext(ctx, &awss3.DeleteObjectInput{
						Bucket: aws.String(bucketFQN),
						Key:    aws.String(key),
					}); err != nil {
						select {
						case errCh <- fmt.Errorf("DeleteObject %q: %w", key, err):
						default:
						}
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errCh)
		if err := <-errCh; err != nil {
			t.Fatalf("failed to create delete markers via DeleteObject in bucket %q: %v", bucketFQN, err)
		}
	}

	vCount, mCount, pages := countVersionsAndDeleteMarkers(t, ctx, rawS3, bucketFQN, prefix)
	if vCount != 0 || mCount != count {
		t.Fatalf("expected 0 versions and %d delete markers under %q in bucket %q, got versions=%d deleteMarkers=%d",
			count, prefix, bucketFQN, vCount, mCount)
	}
	t.Logf("[PERF] Seeded %d expired delete markers across %d pages under %q in bucket %q in %v",
		mCount, pages, prefix, bucketFQN, time.Since(start))
}

func countVersionsAndDeleteMarkers(t *testing.T, ctx context.Context, rawS3 *awss3.S3, bucketFQN, prefix string) (int, int, int) {
	t.Helper()
	versions, markers, pages := 0, 0, 0
	if err := rawS3.ListObjectVersionsPagesWithContext(ctx, &awss3.ListObjectVersionsInput{
		Bucket:  aws.String(bucketFQN),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000),
	}, func(page *awss3.ListObjectVersionsOutput, _ bool) bool {
		pages++
		versions += len(page.Versions)
		markers += len(page.DeleteMarkers)
		return true
	}); err != nil {
		t.Fatalf("failed to count versions and delete markers under %q in bucket %q: %v", prefix, bucketFQN, err)
	}
	return versions, markers, pages
}

func purgePrefixVersionsAndMarkers(t *testing.T, ctx context.Context, rawS3 *awss3.S3, bucketFQN, prefix string) {
	t.Helper()
	_ = rawS3.ListObjectVersionsPagesWithContext(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String(bucketFQN),
		Prefix: aws.String(prefix),
	}, func(page *awss3.ListObjectVersionsOutput, _ bool) bool {
		var ids []*awss3.ObjectIdentifier
		for _, v := range page.Versions {
			ids = append(ids, &awss3.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range page.DeleteMarkers {
			ids = append(ids, &awss3.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(ids) == 0 {
			return true
		}
		_, _ = rawS3.DeleteObjectsWithContext(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(bucketFQN),
			Delete: &awss3.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		return true
	})
}

func (f *backupTestFixture) newZonalBucketS3Clients(t *testing.T, ctx context.Context, readyBucket *objectv1.Bucket, sa []byte, gdchConfig *gdcclient.OrgClusterConfig) (gdcs3.Client, *awss3.S3) {
	t.Helper()
	serviceAccount := &auth.ServiceAccount{}
	if err := json.Unmarshal(sa, serviceAccount); err != nil {
		t.Fatalf("failed to parse service account JSON: %v", err)
	}
	accessKeys, err := storage.GetAccessKeyAndKeyID(ctx, f.mgmtClient, serviceAccount, gdchConfig.OrgClusterURL)
	if err != nil {
		t.Fatalf("failed to get bucket access keys: %v", err)
	}
	caDataBytes, err := base64.StdEncoding.DecodeString(gdchConfig.CAData)
	if err != nil {
		t.Fatalf("failed to decode CA data from GDC config: %v", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caDataBytes) {
		t.Fatal("failed to parse CA PEM data from GDC config")
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    caCertPool,
				MinVersion: tls.VersionTLS12,
			},
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 60 * time.Second,
	}

	s3Client, err := gdcs3.NewGDCHS3Client(&gdcs3.Config{
		AccessKeyId:      string(accessKeys.AccessKeyID),
		SecretAccessKey:  string(accessKeys.AccessKey),
		S3Certificate:    caDataBytes,
		EndpointUrl:      readyBucket.Status.Endpoint,
		Region:           readyBucket.Status.Region,
		CustomHttpClient: httpClient,
	})
	if err != nil {
		t.Fatalf("failed to create GDC S3 client: %v", err)
	}

	sess, err := session.NewSession(&aws.Config{
		Region:               aws.String(readyBucket.Status.Region),
		Endpoint:             aws.String(readyBucket.Status.Endpoint),
		HTTPClient:           httpClient,
		Credentials:          credentials.NewStaticCredentials(string(accessKeys.AccessKeyID), string(accessKeys.AccessKey), ""),
		S3ForcePathStyle:     aws.Bool(true),
		S3Disable100Continue: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("failed to create AWS S3 session: %v", err)
	}

	return s3Client, awss3.New(sess)
}

func (f *backupTestFixture) assertS3ObjectExists(t *testing.T, s3Client gdcs3.Client, bucketFQN, key string) {
	t.Helper()
	out, err := s3Client.GetObject(gdcs3.GetObjectInput{
		BucketFqn: bucketFQN,
		ObjectKey: key,
	})
	if err != nil {
		t.Fatalf("expected S3 object %q in bucket %q to exist, got error: %v", key, bucketFQN, err)
	}
	if out != nil && out.Body != nil {
		_ = out.Body.Close()
	}
}

func (f *backupTestFixture) assertS3ObjectDeleted(t *testing.T, s3Client gdcs3.Client, bucketFQN, key string) {
	t.Helper()
	out, err := s3Client.GetObject(gdcs3.GetObjectInput{
		BucketFqn: bucketFQN,
		ObjectKey: key,
	})
	if out != nil && out.Body != nil {
		_ = out.Body.Close()
	}
	if !errors.Is(err, gdcs3.ErrorS3NoSuchKey) {
		t.Fatalf("expected ErrorS3NoSuchKey for deleted S3 object %q in bucket %q, got: %v", key, bucketFQN, err)
	}
}

func assertVersionedSoftDelete(t *testing.T, ctx context.Context, rawS3 *awss3.S3, bucketFQN, deletedPrefix string, wantDeletedCount int, siblingPrefix string, wantSiblingCount int) {
	t.Helper()
	verOut, err := rawS3.GetBucketVersioningWithContext(ctx, &awss3.GetBucketVersioningInput{
		Bucket: aws.String(bucketFQN),
	})
	if err != nil {
		t.Fatalf("failed to get bucket versioning for %q: %v", bucketFQN, err)
	}
	versioned := aws.StringValue(verOut.Status) == awss3.BucketVersioningStatusEnabled

	deletedPrefixVersions, deletedPrefixMarkers := 0, 0
	siblingPrefixCurrentVersions, siblingPrefixMarkers := 0, 0
	if err := rawS3.ListObjectVersionsPagesWithContext(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String(bucketFQN),
	}, func(page *awss3.ListObjectVersionsOutput, _ bool) bool {
		for _, v := range page.Versions {
			key := aws.StringValue(v.Key)
			switch {
			case strings.HasPrefix(key, deletedPrefix):
				deletedPrefixVersions++
				if aws.BoolValue(v.IsLatest) {
					t.Errorf("expected version %q of deleted key %q to be noncurrent (IsLatest=false)", aws.StringValue(v.VersionId), key)
				}
			case siblingPrefix != "" && strings.HasPrefix(key, siblingPrefix):
				if aws.BoolValue(v.IsLatest) {
					siblingPrefixCurrentVersions++
				}
			}
		}
		for _, m := range page.DeleteMarkers {
			key := aws.StringValue(m.Key)
			switch {
			case strings.HasPrefix(key, deletedPrefix):
				if aws.BoolValue(m.IsLatest) {
					deletedPrefixMarkers++
				}
			case siblingPrefix != "" && strings.HasPrefix(key, siblingPrefix):
				siblingPrefixMarkers++
			}
		}
		return true
	}); err != nil {
		t.Fatalf("failed to list object versions in bucket %q: %v", bucketFQN, err)
	}

	wantVersions, wantMarkers := 0, 0
	if versioned {
		wantVersions, wantMarkers = wantDeletedCount, wantDeletedCount
	}
	if deletedPrefixVersions != wantVersions || deletedPrefixMarkers != wantMarkers {
		t.Fatalf("prefix %q (versioned=%v): got versions=%d deleteMarkers=%d, want %d and %d",
			deletedPrefix, versioned, deletedPrefixVersions, deletedPrefixMarkers, wantVersions, wantMarkers)
	}
	if siblingPrefixCurrentVersions != wantSiblingCount || siblingPrefixMarkers != 0 {
		t.Fatalf("sibling prefix %q: got currentVersions=%d deleteMarkers=%d, want %d and 0",
			siblingPrefix, siblingPrefixCurrentVersions, siblingPrefixMarkers, wantSiblingCount)
	}
}

func purgeBucketVersionsAndMarkers(t *testing.T, ctx context.Context, rawS3 *awss3.S3, bucketFQN string) {
	t.Helper()
	err := rawS3.ListObjectVersionsPagesWithContext(ctx, &awss3.ListObjectVersionsInput{
		Bucket: aws.String(bucketFQN),
	}, func(page *awss3.ListObjectVersionsOutput, _ bool) bool {
		var ids []*awss3.ObjectIdentifier
		for _, v := range page.Versions {
			ids = append(ids, &awss3.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range page.DeleteMarkers {
			ids = append(ids, &awss3.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(ids) == 0 {
			return true
		}
		output, err := rawS3.DeleteObjectsWithContext(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(bucketFQN),
			Delete: &awss3.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		if err != nil {
			t.Logf("warning: failed to purge object versions/markers in bucket %q: %v", bucketFQN, err)
			return false
		}
		if output != nil && len(output.Errors) > 0 {
			firstErr := output.Errors[0]
			t.Logf("warning: %d object versions/markers failed to purge in bucket %q; first: %q (version %q): %s: %s",
				len(output.Errors), bucketFQN, aws.StringValue(firstErr.Key), aws.StringValue(firstErr.VersionId), aws.StringValue(firstErr.Code), aws.StringValue(firstErr.Message))
			return false
		}
		return true
	})
	if err != nil {
		t.Logf("warning: failed to list object versions in bucket %q during cleanup: %v", bucketFQN, err)
	}
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
