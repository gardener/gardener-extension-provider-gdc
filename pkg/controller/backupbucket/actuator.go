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

// Package backupbucket contains the cloud provider specific implementations for
// managing backup buckets
package backupbucket

import (
	"context"
	"encoding/base64"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/gardener/gardener/extensions/pkg/controller/backupbucket"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/gardener/gardener/pkg/controllerutils/reconciler"
	"github.com/go-logr/logr"
	globalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/object/v1"
	objectv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/object/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/s3"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/admission/validator"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/errors"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/storage"
)

const (
	generatedSecretNamespace = "garden"
	bucketDescription        = "storage for etcd backups"
	backupLifecycleRuleID    = "expire-noncurrent-backup-versions"

	// deleteMarkerSweepKeyMarkerAnnotation stores the ListObjectVersions key marker at which a partial expired
	// delete marker sweep stopped, so that the next BackupBucket deletion attempt resumes instead of restarting.
	deleteMarkerSweepKeyMarkerAnnotation = "gdch.provider.extensions.gardener.cloud/delete-marker-sweep-key-marker"
	// maxDeleteMarkerSweepPagesPerDeletion bounds the number of ListObjectVersions pages (1000 entries each)
	// scanned per BackupBucket deletion attempt. Large buckets converge over several retries instead of
	// blocking a single reconciliation for a long time.
	maxDeleteMarkerSweepPagesPerDeletion = 10
	// deleteMarkerSweepReserve is the part of the reconciliation deadline kept free for persisting the sweep
	// progress and for the remaining deletion steps. The sweep itself is stopped this long before the deadline.
	deleteMarkerSweepReserve = 30 * time.Second
	// deleteMarkerSweepMaxDuration caps a single sweep when the reconciliation context carries no deadline.
	deleteMarkerSweepMaxDuration = 2 * time.Minute
	// deleteMarkerSweepRequeueInterval is how soon a BackupBucket deletion is retried after a partial sweep.
	deleteMarkerSweepRequeueInterval = 15 * time.Second
)

func getGeneratedSecretName(bucketName string) string {
	return fmt.Sprintf("generated-gdch-backup-secret-%s", bucketName)
}

// clientFactory defines an interface for creating external clients.
// This allows for easy mocking in unit tests.
type clientFactory interface {
	GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error)
	NewS3Client(config *s3.Config) (s3.Client, error)
}

// defaultClientFactory is the standard implementation of clientFactory for production code.
type defaultClientFactory struct{}

// GetOrgClient creates a new org client.
func (f *defaultClientFactory) GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error) {
	orgClient, err := gdcclient.Get(gdchConfig, serviceAccount, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to create org client: %w", err)
	}
	return orgClient, nil
}

// NewS3Client creates a client for the GDC object storage S3 endpoint.
func (f *defaultClientFactory) NewS3Client(config *s3.Config) (s3.Client, error) {
	return s3.NewGDCHS3Client(config)
}

type actuator struct {
	backupbucket.Actuator
	client        client.Client
	decoder       runtime.Decoder
	clientFactory clientFactory
}

type backupBucketClientConfig struct {
	orgClient      client.Client
	serviceAccount *auth.ServiceAccount
	gdchConfig     *gdcclient.OrgClusterConfig
}

func newActuator(mgr manager.Manager) backupbucket.Actuator {
	a := &actuator{
		client:        mgr.GetClient(),
		decoder:       serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder(),
		clientFactory: &defaultClientFactory{}, // Use default client factory for production implementation
	}
	return a
}

func (a *actuator) Reconcile(ctx context.Context, _ logr.Logger, backupBucket *extensionsv1alpha1.BackupBucket) error {
	bucketClient, err := a.getClientAndConfig(ctx, backupBucket)
	if err != nil {
		return errors.DetermineError(fmt.Errorf("failed to get client and config while reconciling backup bucket %s: %w", backupBucket.Name, err))
	}
	klog.Infof("Reconciling BackupBucket %s on OrgClusterURL: %s", backupBucket.Name, bucketClient.gdchConfig.OrgClusterURL)
	isDualZone, err := storage.IsDualZoneBucketFlow(a.decoder, backupBucket.Name, bucketClient.gdchConfig, backupBucket.Spec.ProviderConfig)
	if err != nil {
		return errors.DetermineError(fmt.Errorf("failed to determine backup bucket type %s: %w while reconciling", backupBucket.Name, err))
	}

	if isDualZone {
		return errors.DetermineError(a.reconcileDualZoneBucket(ctx, backupBucket, bucketClient))
	}

	return errors.DetermineError(a.reconcileZonalBucket(ctx, backupBucket, bucketClient))
}

func (a *actuator) reconcileZonalBucket(ctx context.Context, backupBucket *extensionsv1alpha1.BackupBucket, bucketClient *backupBucketClientConfig) error {
	if err := createZonalBucketIfNotExists(ctx, a.decoder, backupBucket.Name, backupBucket.Spec.ProviderConfig, bucketClient.orgClient, bucketClient.serviceAccount); err != nil {
		return err
	}

	bucket, err := storage.GetAndValidateZonalBucket(ctx, bucketClient.orgClient, client.ObjectKey{Name: backupBucket.Name, Namespace: bucketClient.serviceAccount.Project})
	if err != nil {
		return fmt.Errorf("RetryableError: failed to get bucket with bucket name %v: %w", backupBucket.Name, err)
	}

	if err := a.updateBackupBucketAnnotations(ctx, backupBucket, bucket.Status.FullyQualifiedName); err != nil {
		return fmt.Errorf("RetryableError: failed to update BackupBucket annotations: %w", err)
	}

	return a.updateBackupBucketStatus(ctx, backupBucket, bucket.Status.Endpoint, bucket.Status.Region, bucketClient)
}

func (a *actuator) reconcileDualZoneBucket(ctx context.Context, backupBucket *extensionsv1alpha1.BackupBucket, bucketClient *backupBucketClientConfig) error {
	if err := createDualZoneBucketIfNotExists(ctx, a.decoder, backupBucket.Name, backupBucket.Spec.ProviderConfig, bucketClient.orgClient, bucketClient.serviceAccount); err != nil {
		return err
	}

	bucket, err := storage.GetAndValidateDualZoneBucket(ctx, bucketClient.orgClient, client.ObjectKey{Name: backupBucket.Name, Namespace: bucketClient.serviceAccount.Project})
	if err != nil {
		return fmt.Errorf("RetryableError: failed to get bucket with bucket name %v: %w", backupBucket.Name, err)
	}

	if err := a.updateBackupBucketAnnotations(ctx, backupBucket, bucket.Status.FullyQualifiedName); err != nil {
		return fmt.Errorf("RetryableError: failed to update BackupBucket annotations: %w", err)
	}

	return a.updateBackupBucketStatus(ctx, backupBucket, bucket.Status.GlobalEndpoint, bucket.Status.Region, bucketClient)
}

func (a *actuator) Delete(ctx context.Context, _ logr.Logger, backupBucket *extensionsv1alpha1.BackupBucket) error {
	bucketClient, err := a.getClientAndConfig(ctx, backupBucket)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the configs are not found, we can't delete the bucket anyway.
			// We will treat this as a success to avoid the deletion getting stuck.
			return nil
		}
		return fmt.Errorf("failed to get client and config while deleting backup bucket %s: %w", backupBucket.Name, err)
	}

	isDualZone, err := storage.IsDualZoneBucketFlow(a.decoder, backupBucket.Name, bucketClient.gdchConfig, backupBucket.Spec.ProviderConfig)
	if err != nil {
		return errors.DetermineError(fmt.Errorf("failed to determine backup bucket type %s: %w while deleting", backupBucket.Name, err))
	}

	var bucketToDelete client.Object
	if isDualZone {
		bucketToDelete = &globalv1.Bucket{}
	} else {
		bucketToDelete = &objectv1.Bucket{}
	}
	// Set the metadata for the object to be deleted.
	bucketToDelete.SetName(backupBucket.Name)
	bucketToDelete.SetNamespace(bucketClient.serviceAccount.Project)

	if err := a.sweepExpiredDeleteMarkersBeforeBucketDelete(ctx, backupBucket, bucketToDelete, bucketClient); err != nil {
		return errors.DetermineError(err)
	}

	if err := bucketClient.orgClient.Delete(ctx, bucketToDelete); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete bucket %q: %w", backupBucket.Name, err)
		}
	}

	secretName := getGeneratedSecretName(backupBucket.Name)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: generatedSecretNamespace,
		},
	}
	if err := a.client.Delete(ctx, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete generated secret %q: %w", secretName, err)
		}
	}

	return nil
}

// sweepExpiredDeleteMarkersBeforeBucketDelete removes orphaned delete markers (keys without any remaining
// object version) from the GDC bucket before its Bucket resource is deleted. GDC refuses to delete a bucket that
// still holds delete markers, and buckets created before native ExpiredObjectDeleteMarker support never get rid
// of them via lifecycle.
//
// The sweep is bounded to maxDeleteMarkerSweepPagesPerDeletion listing pages per call. When the budget is
// exhausted, the resume key is persisted in an annotation on the BackupBucket and a retryable error is returned
// so that Gardener requeues the deletion; the following attempt continues from the stored key marker.
// Any other problem (missing credentials, S3 errors) is logged and the sweep is skipped, keeping bucket deletion
// best effort as before.
func (a *actuator) sweepExpiredDeleteMarkersBeforeBucketDelete(ctx context.Context, backupBucket *extensionsv1alpha1.BackupBucket, bucketObject client.Object, bucketClient *backupBucketClientConfig) error {
	if a.clientFactory == nil {
		return nil
	}
	fetched := bucketObject.DeepCopyObject().(client.Object)
	if err := bucketClient.orgClient.Get(ctx, client.ObjectKeyFromObject(bucketObject), fetched); err != nil {
		// Bucket already gone or not readable: nothing to sweep; bucket deletion below handles NotFound.
		klog.V(2).Infof("Skipping expired delete marker sweep for bucket %q: %v", bucketObject.GetName(), err)
		return nil //nolint:nilerr // the sweep is best effort
	}

	var endpoint, region, fqn string
	var lockingPolicy *objectv1.LockingPolicy
	var lifecyclePolicy *objectv1.LifecyclePolicy
	switch b := fetched.(type) {
	case *objectv1.Bucket:
		endpoint = b.Status.Endpoint
		region = b.Status.Region
		fqn = b.Status.FullyQualifiedName
		if b.Spec.BucketPolicy != nil {
			lockingPolicy = b.Spec.BucketPolicy.LockingPolicy
			lifecyclePolicy = b.Spec.BucketPolicy.LifecyclePolicy
		}
	case *globalv1.Bucket:
		endpoint = b.Status.GlobalEndpoint
		region = b.Status.Region
		fqn = b.Status.FullyQualifiedName
		if b.Spec.BucketPolicy != nil {
			lockingPolicy = b.Spec.BucketPolicy.LockingPolicy
			lifecyclePolicy = b.Spec.BucketPolicy.LifecyclePolicy
		}
	default:
		return nil
	}

	if !storage.NeedsExpiredDeleteMarkerSweep(lockingPolicy, lifecyclePolicy) || endpoint == "" || region == "" || fqn == "" {
		return nil
	}
	accessKeys, err := storage.GetAccessKeyAndKeyID(ctx, bucketClient.orgClient, bucketClient.serviceAccount, bucketClient.gdchConfig.OrgClusterURL)
	if err != nil || accessKeys == nil {
		klog.Warningf("Skipping expired delete marker sweep before deleting bucket %q: failed to retrieve access keys: %v", bucketObject.GetName(), err)
		return nil
	}
	caDataBytes, err := base64.StdEncoding.DecodeString(bucketClient.gdchConfig.CAData)
	if err != nil {
		klog.Warningf("Skipping expired delete marker sweep before deleting bucket %q: failed to decode CA data: %v", bucketObject.GetName(), err)
		return nil
	}
	s3Client, err := a.clientFactory.NewS3Client(&s3.Config{
		AccessKeyId:     string(accessKeys.AccessKeyID),
		SecretAccessKey: string(accessKeys.AccessKey),
		EndpointUrl:     endpoint,
		Region:          region,
		S3Certificate:   caDataBytes,
	})
	if err != nil || s3Client == nil {
		klog.Warningf("Skipping expired delete marker sweep for bucket %q (%s): failed to initialize S3 client: %v", bucketObject.GetName(), fqn, err)
		return nil
	}

	keyMarker := backupBucket.Annotations[deleteMarkerSweepKeyMarkerAnnotation]
	// Stop the sweep well before the reconciliation deadline (3m by default) so that the progress made can
	// still be persisted and the deletion requeued instead of the whole attempt timing out without a trace.
	sweepCtx, cancel := deleteMarkerSweepContext(ctx)
	result, err := s3Client.DeleteExpiredDeleteMarkers(sweepCtx, fqn, s3.DeleteExpiredDeleteMarkersOptions{
		KeyMarker: keyMarker,
		MaxPages:  maxDeleteMarkerSweepPagesPerDeletion,
	})
	cancel()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		hasProgress := (result != nil && result.NextKeyMarker != "") || keyMarker != ""
		if stderrors.Is(err, s3.ErrorS3NoSuchBucket) || (sweepCtx.Err() == nil && !hasProgress) {
			klog.Warningf("Failed to sweep expired delete markers for bucket %q (%s): %v", bucketObject.GetName(), fqn, err)
			return nil
		}
		// The sweep hit its per-reconcile time budget or was interrupted after making progress: persist any
		// newly completed key and requeue.
		klog.Infof("Expired delete marker sweep for bucket %q (%s) interrupted, will resume: %v", bucketObject.GetName(), fqn, err)
	}
	if result == nil {
		result = &s3.DeleteExpiredDeleteMarkersResult{}
	}
	if result.Done() {
		if keyMarker != "" {
			if err := a.patchSweepKeyMarker(ctx, backupBucket, ""); err != nil {
				return fmt.Errorf("RetryableError: failed to clear delete marker sweep progress for bucket %q: %w", backupBucket.Name, err)
			}
		}
		klog.Infof("Expired delete marker sweep for bucket %q (%s) complete: removed %d markers in %d page(s)", bucketObject.GetName(), fqn, result.DeletedMarkers, result.ScannedPages)
		return nil
	}

	resumeAfter := result.NextKeyMarker
	if resumeAfter != "" {
		if err := a.patchSweepKeyMarker(ctx, backupBucket, resumeAfter); err != nil {
			return fmt.Errorf("RetryableError: failed to persist delete marker sweep progress for bucket %q: %w", backupBucket.Name, err)
		}
	} else {
		resumeAfter = keyMarker
	}
	klog.Infof("Expired delete marker sweep for bucket %q (%s) in progress: removed %d markers in %d page(s), resuming after key %q", bucketObject.GetName(), fqn, result.DeletedMarkers, result.ScannedPages, resumeAfter)
	// Requeue promptly: the remaining work is known and bounded, so exponential backoff would only delay it.
	return &reconciler.RequeueAfterError{
		RequeueAfter: deleteMarkerSweepRequeueInterval,
		Cause: fmt.Errorf("RetryableError: expired delete marker sweep for bucket %q (%s) in progress: removed %d markers in %d page(s), resuming after key %q",
			backupBucket.Name, fqn, result.DeletedMarkers, result.ScannedPages, resumeAfter),
	}
}

// deleteMarkerSweepContext derives the context for one sweep pass: it ends deleteMarkerSweepReserve before the
// parent deadline (or after deleteMarkerSweepMaxDuration when the parent has none).
func deleteMarkerSweepContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := deleteMarkerSweepMaxDuration
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline) - deleteMarkerSweepReserve; remaining < timeout {
			timeout = max(remaining, 0)
		}
	}
	return context.WithTimeout(ctx, timeout)
}

// patchSweepKeyMarker sets (or removes, when empty) the sweep resume annotation on the BackupBucket.
func (a *actuator) patchSweepKeyMarker(ctx context.Context, backupBucket *extensionsv1alpha1.BackupBucket, keyMarker string) error {
	original := backupBucket.DeepCopy()
	if keyMarker == "" {
		delete(backupBucket.Annotations, deleteMarkerSweepKeyMarkerAnnotation)
	} else {
		if backupBucket.Annotations == nil {
			backupBucket.Annotations = map[string]string{}
		}
		backupBucket.Annotations[deleteMarkerSweepKeyMarkerAnnotation] = keyMarker
	}
	return a.client.Patch(ctx, backupBucket, client.MergeFrom(original))
}

// getClientAndConfig is a helper function to initialize all necessary clients and configurations
func (a *actuator) getClientAndConfig(ctx context.Context, backupBucket *extensionsv1alpha1.BackupBucket) (*backupBucketClientConfig, error) {
	serviceAccount, err := gdc.GetServiceAccountFromSecretReference(ctx, a.client, backupBucket.Spec.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get service account for backup bucket %s: %w", backupBucket.Name, err)
	}

	gdchConfig, err := gdc.GetGDCHConfigFromSecretReference(ctx, a.client, backupBucket.Spec.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get GDCH config for backup bucket %s: %w", backupBucket.Name, err)
	}

	orgClient, err := a.clientFactory.GetOrgClient(gdchConfig, serviceAccount, a.client.Scheme())
	if err != nil {
		return nil, fmt.Errorf("failed to get org client for backup bucket %s: %w", backupBucket.Name, err)
	}

	return &backupBucketClientConfig{
		orgClient:      orgClient,
		serviceAccount: serviceAccount,
		gdchConfig:     gdchConfig,
	}, nil
}

// updateBackupBucketAnnotations updates the annotations of the BackupBucket resource
func (a *actuator) updateBackupBucketAnnotations(ctx context.Context, backupBucket *extensionsv1alpha1.BackupBucket, fqBucketName string) error {
	originalBackupBucket := backupBucket.DeepCopy()
	if backupBucket.Annotations == nil {
		backupBucket.Annotations = make(map[string]string)
	}
	backupBucket.Annotations[gdc.FullyQualifiedBucketNameAnnotationKey] = fqBucketName

	if err := a.client.Patch(ctx, backupBucket, client.MergeFrom(originalBackupBucket)); err != nil {
		return fmt.Errorf("failed to patch BackupBucket annotations: %w", err)
	}
	return nil
}

func (a *actuator) updateBackupBucketStatus(ctx context.Context, backupBucket *extensionsv1alpha1.BackupBucket, bucketEndpoint string, bucketRegion string, bucketClient *backupBucketClientConfig) error {
	accessKeys, err := storage.GetAccessKeyAndKeyID(ctx, bucketClient.orgClient, bucketClient.serviceAccount, bucketClient.gdchConfig.OrgClusterURL)
	if err != nil {
		return fmt.Errorf("failed to get bucket access keys: %w", err)
	}
	accessKeyID := accessKeys.AccessKeyID
	secretAccessKey := accessKeys.AccessKey
	secretName := getGeneratedSecretName(backupBucket.Name)
	backupSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: generatedSecretNamespace,
		},
	}

	gdchConfigJSONData, err := json.Marshal(bucketClient.gdchConfig)
	if err != nil {
		return fmt.Errorf("failed to enode a gdch-config json: %w", err)
	}

	serviceAccountJSONData, err := json.Marshal(bucketClient.serviceAccount) // #nosec G117 -- intentional serialization of service account credentials for backup secret
	if err != nil {
		return fmt.Errorf("failed to enode a service account json: %w", err)
	}

	caDataBytes, err := base64.StdEncoding.DecodeString(bucketClient.gdchConfig.CAData)
	if err != nil {
		return fmt.Errorf("failed to decode CA data from gdch config: %w", err)
	}
	trustedCaCert := string(caDataBytes)

	requestChecksumCalculation := apisgdc.ChecksumWhenRequired
	responseChecksumValidation := apisgdc.ChecksumWhenRequired

	if backupBucket.Spec.ProviderConfig != nil {
		backupBucketConfig, err := validator.DecodeBackupBucketConfig(a.decoder, backupBucket.Spec.ProviderConfig)
		if err != nil {
			return fmt.Errorf("failed to decode BackupBucketConfig: %w", err)
		}
		if backupBucketConfig.RequestChecksumCalculation != "" {
			if err := validateChecksumValue(backupBucketConfig.RequestChecksumCalculation); err != nil {
				return fmt.Errorf("invalid RequestChecksumCalculation: %w", err)
			}
			requestChecksumCalculation = backupBucketConfig.RequestChecksumCalculation
		}
		if backupBucketConfig.ResponseChecksumValidation != "" {
			if err := validateChecksumValue(backupBucketConfig.ResponseChecksumValidation); err != nil {
				return fmt.Errorf("invalid ResponseChecksumValidation: %w", err)
			}
			responseChecksumValidation = backupBucketConfig.ResponseChecksumValidation
		}
	}

	backupSecretJSON := map[string]interface{}{
		"accessKeyID":                string(accessKeyID),
		"secretAccessKey":            string(secretAccessKey),
		"endpoint":                   bucketEndpoint,
		"region":                     bucketRegion,
		"s3ForcePathStyle":           true,
		"insecureSkipVerify":         false,
		"requestChecksumCalculation": requestChecksumCalculation,
		"responseChecksumValidation": responseChecksumValidation,
		"trustedCaCert":              trustedCaCert,
		"gdch-config":                gdchConfigJSONData,
		"serviceaccount.json":        serviceAccountJSONData,
	}

	backupSecretJSONData, err := json.Marshal(backupSecretJSON)
	if err != nil {
		return fmt.Errorf("failed to enode a backup json: %w", err)
	}

	// For backward compatibility: package the credentials in two formats.
	// - `aaa_backup_secret.json`: used by newer backup-restore versions (v0.29.0+).
	// - Flat key-value pairs: retained for compatibility with older versions (e.g., v0.28.0).
	// This ensures support across clusters running different backup-restore versions.
	// TODO(hkibona) Remove Flat key-value pairs format once it is not longer needed
	backupSecretData := map[string][]byte{
		"backup_secret.json":         backupSecretJSONData,
		"accessKeyID":                accessKeyID,
		"secretAccessKey":            secretAccessKey,
		"endpoint":                   []byte(bucketEndpoint),
		"region":                     []byte(bucketRegion),
		"requestChecksumCalculation": []byte(requestChecksumCalculation),
		"responseChecksumValidation": []byte(responseChecksumValidation),
		"trustedCaCert":              []byte(trustedCaCert),
		"insecureSkipVerify":         []byte("false"),
		"s3ForcePathStyle":           []byte("true"),
		"gdch-config":                gdchConfigJSONData,
		"serviceaccount.json":        serviceAccountJSONData,
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, a.client, backupSecret, func() error {
		backupSecret.Data = backupSecretData
		return nil
	}); err != nil {
		return fmt.Errorf("failed to create a backup secret: %w", err)
	}

	generatedSecretReference := &corev1.SecretReference{
		Name:      secretName,
		Namespace: generatedSecretNamespace,
	}
	originalBackupBucket := backupBucket.DeepCopy()
	backupBucket.Status.GeneratedSecretRef = generatedSecretReference

	if err := a.client.Status().Patch(ctx, backupBucket, client.MergeFrom(originalBackupBucket)); err != nil {
		return fmt.Errorf("failed to patch BackupBucket Status: %w", err)
	}

	return nil
}

// getLockingPolicy computes the initial object LockingPolicy to be set when creating a bucket.
// Note that DefaultObjectRetentionDays is immutable in Gardener (enforced by the Seed admission validator),
// and createBucketIfNotExists only sets the policy at bucket creation time without overwriting external updates.
func getLockingPolicy(config *apisgdc.BackupBucketConfig) (*objectv1.LockingPolicy, error) {
	retentionDays := apisgdc.DefaultObjectRetentionDays
	if config != nil && config.DefaultObjectRetentionDays != nil {
		if *config.DefaultObjectRetentionDays < 0 || *config.DefaultObjectRetentionDays > apisgdc.MaxObjectRetentionDays {
			return nil, fmt.Errorf("defaultObjectRetentionDays must be between 0 and %d, got %d", apisgdc.MaxObjectRetentionDays, *config.DefaultObjectRetentionDays)
		}
		retentionDays = *config.DefaultObjectRetentionDays
	}
	if retentionDays == 0 {
		return nil, nil
	}
	return &objectv1.LockingPolicy{
		DefaultObjectRetentionDays: ptr.To(retentionDays),
	}, nil
}

// getLifecyclePolicy computes the LifecyclePolicy to pair with a LockingPolicy so that soft-deleted
// noncurrent backup versions and expired delete markers are automatically purged after the retention period.
func getLifecyclePolicy(lockingPolicy *objectv1.LockingPolicy) *objectv1.LifecyclePolicy {
	return getLifecyclePolicyWithOptions(lockingPolicy, true)
}

func getLifecyclePolicyWithOptions(lockingPolicy *objectv1.LockingPolicy, includeExpiredObjectDeleteMarker bool) *objectv1.LifecyclePolicy {
	if lockingPolicy == nil || lockingPolicy.DefaultObjectRetentionDays == nil || *lockingPolicy.DefaultObjectRetentionDays <= 0 {
		return nil
	}
	rule := &objectv1.LifecycleRule{
		ID:     ptr.To(backupLifecycleRuleID),
		Status: ptr.To(objectv1.Enabled),
		NoncurrentExpiration: &objectv1.LifecycleNoncurrentExpiration{
			NoncurrentDays: ptr.To(int64(*lockingPolicy.DefaultObjectRetentionDays)),
		},
	}
	if includeExpiredObjectDeleteMarker {
		rule.ExpiredObjectDeleteMarker = ptr.To(true)
	}
	return &objectv1.LifecyclePolicy{
		Enable:         true,
		LifecycleRules: []*objectv1.LifecycleRule{rule},
	}
}

func isUnsupportedExpiredObjectDeleteMarkerError(err error) bool {
	if err == nil || !strings.Contains(err.Error(), "expiredObjectDeleteMarker") {
		return false
	}
	return apierrors.IsInvalid(err) ||
		apierrors.IsBadRequest(err) ||
		strings.Contains(err.Error(), "strict decoding error") ||
		strings.Contains(err.Error(), "unknown field")
}

// createZonalBucketIfNotExists creates the zonal Bucket if it does not exist, or backfills a missing
// LifecyclePolicy on pre-existing locked buckets while falling back gracefully on GDC 1.16.2 clusters
// whose Bucket CRD predates expiredObjectDeleteMarker.
func createZonalBucketIfNotExists(ctx context.Context, decoder runtime.Decoder, bucketName string, providerConfig *runtime.RawExtension, orgClient client.Client, serviceAccount *auth.ServiceAccount) error {
	newObj := func() client.Object {
		return &objectv1.Bucket{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bucketName,
				Namespace: serviceAccount.Project,
			},
			Spec: objectv1.BucketSpec{
				Description:  bucketDescription,
				StorageClass: objectv1.Standard,
			},
		}
	}
	return createBucketIfNotExists(ctx, decoder, providerConfig, orgClient, newObj)
}

// createDualZoneBucketIfNotExists creates the dual-zone Bucket if it does not exist, or backfills a missing
// LifecyclePolicy on pre-existing locked buckets while falling back gracefully on GDC 1.16.2 clusters
// whose Bucket CRD predates expiredObjectDeleteMarker.
func createDualZoneBucketIfNotExists(ctx context.Context, decoder runtime.Decoder, bucketName string, providerConfig *runtime.RawExtension, orgClient client.Client, serviceAccount *auth.ServiceAccount) error {
	newObj := func() client.Object {
		return &globalv1.Bucket{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bucketName,
				Namespace: serviceAccount.Project,
			},
			Spec: globalv1.BucketSpec{
				Description:  bucketDescription,
				StorageClass: objectv1.Standard,
			},
		}
	}
	return createBucketIfNotExists(ctx, decoder, providerConfig, orgClient, newObj)
}

func applyBucketPolicies(bucketObject client.Object, backupBucketConfig *apisgdc.BackupBucketConfig, lockingPolicy *objectv1.LockingPolicy, includeExpiredObjectDeleteMarker bool) error {
	lifecyclePolicy := getLifecyclePolicyWithOptions(lockingPolicy, includeExpiredObjectDeleteMarker)
	switch b := bucketObject.(type) {
	case *objectv1.Bucket:
		if lockingPolicy != nil {
			b.Spec.BucketPolicy = &objectv1.BucketPolicy{
				LockingPolicy:   lockingPolicy,
				LifecyclePolicy: lifecyclePolicy,
			}
		}
	case *globalv1.Bucket:
		if backupBucketConfig == nil {
			return fmt.Errorf("backupBucketConfig is required for dual-zone bucket %s", bucketObject.GetName())
		}
		b.Spec.Location = backupBucketConfig.DualZoneBucketLocation
		if lockingPolicy != nil {
			b.Spec.BucketPolicy = &objectv1.GlobalBucketPolicy{
				LockingPolicy:   lockingPolicy,
				LifecyclePolicy: lifecyclePolicy,
			}
		}
	}
	return nil
}

// createBucketIfNotExists ensures a zonal or dual-zone Bucket exists and has a compatible LifecyclePolicy.
func createBucketIfNotExists(ctx context.Context, decoder runtime.Decoder, providerConfig *runtime.RawExtension, orgClient client.Client, newBucketObj func() client.Object) error {
	probe := newBucketObj()
	var backupBucketConfig *apisgdc.BackupBucketConfig
	if providerConfig != nil {
		var err error
		backupBucketConfig, err = validator.DecodeBackupBucketConfig(decoder, providerConfig)
		if err != nil {
			return fmt.Errorf("failed to decode BackupBucketConfig for backup bucket %s: %w", probe.GetName(), err)
		}
	}

	lockingPolicy, err := getLockingPolicy(backupBucketConfig)
	if err != nil {
		return err
	}

	// Validate dual-zone config before making API calls.
	if err := applyBucketPolicies(probe, backupBucketConfig, lockingPolicy, true); err != nil {
		return err
	}

	existing := newBucketObj()
	getErr := orgClient.Get(ctx, client.ObjectKeyFromObject(probe), existing)
	if getErr == nil {
		return backfillExistingBucketLifecycleIfMissing(ctx, orgClient, existing, newBucketObj)
	}
	if !apierrors.IsNotFound(getErr) {
		return fmt.Errorf("failed to check existing bucket %s in namespace %s: %w", probe.GetName(), probe.GetNamespace(), getErr)
	}

	if err := orgClient.Create(ctx, probe); err != nil {
		if apierrors.IsAlreadyExists(err) {
			klog.Infof("bucket %s of type %T already exists in namespace %s.", probe.GetName(), probe, probe.GetNamespace())
			return nil
		}
		if lockingPolicy != nil && isUnsupportedExpiredObjectDeleteMarkerError(err) {
			klog.Infof("Bucket CRD on target GDC cluster does not support expiredObjectDeleteMarker for bucket %s/%s; retrying creation with noncurrentExpiration only", probe.GetNamespace(), probe.GetName())
			fallback := newBucketObj()
			if policyErr := applyBucketPolicies(fallback, backupBucketConfig, lockingPolicy, false); policyErr != nil {
				return policyErr
			}
			if fallbackErr := orgClient.Create(ctx, fallback); fallbackErr != nil {
				if apierrors.IsAlreadyExists(fallbackErr) {
					return nil
				}
				return fmt.Errorf("failed to create bucketName %s in namespace %s without expiredObjectDeleteMarker: %w", fallback.GetName(), fallback.GetNamespace(), fallbackErr)
			}
			return nil
		}
		return fmt.Errorf("failed to create bucketName %s in namespace %s: %w", probe.GetName(), probe.GetNamespace(), err)
	}

	return nil
}

func backfillExistingBucketLifecycleIfMissing(ctx context.Context, orgClient client.Client, existing client.Object, newBucketObj func() client.Object) error {
	if existing.GetDeletionTimestamp() != nil {
		return nil
	}

	extractPolicies := func(obj client.Object) (*objectv1.LockingPolicy, *objectv1.LifecyclePolicy) {
		switch b := obj.(type) {
		case *objectv1.Bucket:
			if b.Spec.BucketPolicy != nil {
				return b.Spec.BucketPolicy.LockingPolicy, b.Spec.BucketPolicy.LifecyclePolicy
			}
		case *globalv1.Bucket:
			if b.Spec.BucketPolicy != nil {
				return b.Spec.BucketPolicy.LockingPolicy, b.Spec.BucketPolicy.LifecyclePolicy
			}
		}
		return nil, nil
	}

	setLifecyclePolicy := func(obj client.Object, lc *objectv1.LifecyclePolicy) {
		switch b := obj.(type) {
		case *objectv1.Bucket:
			if b.Spec.BucketPolicy != nil {
				b.Spec.BucketPolicy.LifecyclePolicy = lc
			}
		case *globalv1.Bucket:
			if b.Spec.BucketPolicy != nil {
				b.Spec.BucketPolicy.LifecyclePolicy = lc
			}
		}
	}

	existingLocking, existingLifecycle := extractPolicies(existing)
	if existingLocking == nil || existingLocking.DefaultObjectRetentionDays == nil || *existingLocking.DefaultObjectRetentionDays <= 0 || existingLifecycle != nil {
		return nil
	}

	setLifecyclePolicy(existing, getLifecyclePolicyWithOptions(existingLocking, true))
	if err := orgClient.Update(ctx, existing); err != nil {
		if !isUnsupportedExpiredObjectDeleteMarkerError(err) {
			return fmt.Errorf("failed to backfill lifecyclePolicy on existing bucket %s/%s: %w", existing.GetNamespace(), existing.GetName(), err)
		}
		klog.Infof("Bucket CRD on target GDC cluster does not support expiredObjectDeleteMarker for existing bucket %s/%s; backfilling noncurrentExpiration only", existing.GetNamespace(), existing.GetName())
		refetched := newBucketObj()
		if getErr := orgClient.Get(ctx, client.ObjectKeyFromObject(existing), refetched); getErr != nil {
			return fmt.Errorf("failed to re-fetch existing bucket %s/%s for lifecycle fallback: %w", existing.GetNamespace(), existing.GetName(), getErr)
		}
		refetchedLocking, refetchedLifecycle := extractPolicies(refetched)
		if refetchedLocking == nil || refetchedLifecycle != nil {
			return nil
		}
		setLifecyclePolicy(refetched, getLifecyclePolicyWithOptions(refetchedLocking, false))
		if fallbackErr := orgClient.Update(ctx, refetched); fallbackErr != nil {
			return fmt.Errorf("failed to backfill fallback lifecyclePolicy on existing bucket %s/%s: %w", refetched.GetNamespace(), refetched.GetName(), fallbackErr)
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
