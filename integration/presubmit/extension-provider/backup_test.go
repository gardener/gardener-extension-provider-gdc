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
	"net/http"
	"os"
	"slices"
	"strings"
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

func (f *backupTestFixture) deleteBackupEntryAndWait(t *testing.T, ctx context.Context, entryName string) {
	t.Helper()
	entry := &extensionsv1alpha1.BackupEntry{
		ObjectMeta: metav1.ObjectMeta{
			Name: entryName,
		},
	}
	if err := f.vucClient.Delete(ctx, entry); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Fatalf("cannot delete BackupEntry object %v", err)
	}
	t.Logf("cleaning up BackupEntry %q", entry.Name)
	if err := wait.PollUntilContextTimeout(ctx, pollingInterval, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(entry), entry); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	}); err != nil {
		t.Fatalf("error waiting for BackupEntry object %q to be deleted: %v", entry.Name, err)
	}
}

func (f *backupTestFixture) testBackupEntryWithPreExistingBucket(t *testing.T, ctx context.Context, secretRef corev1.SecretReference, sa []byte, gdchConfig *gdcclient.OrgClusterConfig) {
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

	// Delete the primary BackupEntry and verify its objects are soft-deleted while the sibling prefix remains intact.
	f.deleteBackupEntryAndWait(t, ctx, entryName)

	for _, key := range entryKeys {
		f.assertS3ObjectDeleted(t, s3Client, bucketFQN, key)
	}
	f.assertS3ObjectExists(t, s3Client, bucketFQN, siblingKey)
	assertVersionedSoftDelete(t, ctx, rawS3, bucketFQN, entryName+"/", len(entryKeys), siblingEntryName+"/", 1)

	// Delete the sibling BackupEntry and verify its object is also soft-deleted.
	f.deleteBackupEntryAndWait(t, ctx, siblingEntryName)
	f.assertS3ObjectDeleted(t, s3Client, bucketFQN, siblingKey)
	assertVersionedSoftDelete(t, ctx, rawS3, bucketFQN, siblingEntryName+"/", 1, "", 0)
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
