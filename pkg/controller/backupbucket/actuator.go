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
	"fmt"

	"github.com/gardener/gardener/extensions/pkg/controller/backupbucket"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
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
	"github.com/gardener/gardener-extension-provider-gdc/pkg/admission/validator"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/errors"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/storage"
)

const (
	generatedSecretNamespace = "garden"
	bucketDescription        = "storage for etcd backups"
	defaultRetentionDays     = int32(1)
)

func getGeneratedSecretName(bucketName string) string {
	return fmt.Sprintf("generated-gdch-backup-secret-%s", bucketName)
}

// clientFactory defines an interface for creating external clients.
// This allows for easy mocking in unit tests.
type clientFactory interface {
	GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error)
}

// defaultClientFactory is the standard implementation of clientFactory for production code.
type defaultClientFactory struct{}

// GetOrgClient creates a new org client.
func (f *defaultClientFactory) GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error) {
	client, err := gdcclient.Get(gdchConfig, serviceAccount, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to create org client: %w", err)
	}
	return client, nil
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
	if err := createZonalBucketIfNotExists(ctx, backupBucket.Name, bucketClient.orgClient, bucketClient.serviceAccount); err != nil {
		return err
	}

	bucket, err := storage.GetAndValidateZonalBucket(ctx, bucketClient.orgClient, client.ObjectKey{Name: backupBucket.Name, Namespace: bucketClient.serviceAccount.Project})
	if err != nil {
		return fmt.Errorf("RetryableError: failed to get bucket with bucket name %v: %w", backupBucket.Name, err)
	}

	if err := a.updateBackupBucketAnnotations(ctx, backupBucket, bucket.Status.FullyQualifiedName); err != nil {
		return fmt.Errorf("RetryableError: failed to update BackupBucket annotations: %w", err)
	}

	if err := a.updateBackupBucketStatus(ctx, backupBucket, bucket.Status.Endpoint, bucket.Status.Region, bucketClient); err != nil {
		return err
	}

	return err
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

	if err := a.updateBackupBucketStatus(ctx, backupBucket, bucket.Status.GlobalEndpoint, bucket.Status.Region, bucketClient); err != nil {
		return err
	}

	return err
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

	serviceAccountJSONData, err := json.Marshal(bucketClient.serviceAccount)
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

// createZonalBucketIfNotExists ignore if bucket already exist otherwise create it.
func createZonalBucketIfNotExists(ctx context.Context, bucketName string, orgClient client.Client, serviceAccount *auth.ServiceAccount) error {
	bucket := &objectv1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucketName,
			Namespace: serviceAccount.Project,
		},
		Spec: objectv1.BucketSpec{
			Description:  bucketDescription,
			StorageClass: objectv1.Standard,
			BucketPolicy: &objectv1.BucketPolicy{
				LockingPolicy: &objectv1.LockingPolicy{
					DefaultObjectRetentionDays: ptr.To(defaultRetentionDays),
				},
			},
		},
	}
	return createBucketIfNotExists(ctx, orgClient, bucket)
}

// createDualZoneBucketIfNotExists ignore if bucket already exist otherwise create it.
func createDualZoneBucketIfNotExists(ctx context.Context, decoder runtime.Decoder, bucketName string, providerConfig *runtime.RawExtension, orgClient client.Client, serviceAccount *auth.ServiceAccount) error {
	backupBucketConfig, err := validator.DecodeBackupBucketConfig(decoder, providerConfig)
	if err != nil {
		return fmt.Errorf("failed to decode BackupBucketConfig for backup bucket %s: %w", bucketName, err)
	}
	bucket := &globalv1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucketName,
			Namespace: serviceAccount.Project,
		},
		Spec: globalv1.BucketSpec{
			Description:  bucketDescription,
			Location:     backupBucketConfig.DualZoneBucketLocation,
			StorageClass: objectv1.Standard,
			BucketPolicy: &objectv1.GlobalBucketPolicy{
				LockingPolicy: &objectv1.LockingPolicy{
					DefaultObjectRetentionDays: ptr.To(defaultRetentionDays),
				},
			},
		},
	}
	return createBucketIfNotExists(ctx, orgClient, bucket)
}

// createBucketIfNotExists is a generic helper that creates a bucket if it does not already exist.
func createBucketIfNotExists(ctx context.Context, orgClient client.Client, bucketObject client.Object) error {
	if err := orgClient.Create(ctx, bucketObject); err != nil {
		if apierrors.IsAlreadyExists(err) {
			klog.Infof("bucket %s of type %T already exists in namespace %s. ", bucketObject.GetName(), bucketObject, bucketObject.GetNamespace())
			return nil
		}
		return fmt.Errorf("failed to create bucketName %s in namespace %s: %w", bucketObject.GetName(), bucketObject.GetNamespace(), err)
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
