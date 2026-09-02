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

package backupentry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gardener/gardener/extensions/pkg/controller/backupentry/genericactuator"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	klog "k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/s3"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/errors"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
	storage "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/storage"
)

const (
	s3Timeout = 100 * time.Second
)

type bucketMetadata struct {
	Endpoint           string
	Region             string
	FullyQualifiedName string
}

type storageClient struct {
	// http client used to call GDCH object storage's S3 compatible rest APIs
	s3Client s3.Client
	// object storage bucket object
	bucket *bucketMetadata
}

// clientFactory defines an interface for creating external clients.
// This allows for easy mocking in unit tests.
type clientFactory interface {
	NewS3Client(config *s3.Config) (s3.Client, error)
	GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error)
}

// defaultClientFactory is the standard implementation of clientFactory for production code.
type defaultClientFactory struct{}

// NewS3Client creates a new S3 client.
func (f *defaultClientFactory) NewS3Client(config *s3.Config) (s3.Client, error) {
	return s3.NewGDCHS3Client(config)
}

// GetOrgClient creates a new org client.
func (f *defaultClientFactory) GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error) {
	orgClient, err := gdcclient.Get(gdchConfig, serviceAccount, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to create org client: %w", err)
	}
	return orgClient, nil
}

type actuator struct {
	client        client.Client
	decoder       runtime.Decoder
	clientFactory clientFactory
}

type backupEntryClientConfig struct {
	orgClient      client.Client
	serviceAccount *auth.ServiceAccount
	gdchConfig     *gdcclient.OrgClusterConfig
}

func newActuator(mgr manager.Manager) genericactuator.BackupEntryDelegate {
	return &actuator{
		client:        mgr.GetClient(),
		decoder:       serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder(),
		clientFactory: &defaultClientFactory{}, // Use default client factory for production implementation
	}
}

func (a *actuator) GetETCDSecretData(_ context.Context, _ logr.Logger, _ *extensionsv1alpha1.BackupEntry, backupSecretData map[string][]byte) (map[string][]byte, error) {
	return backupSecretData, nil
}

func (a *actuator) Delete(ctx context.Context, _ logr.Logger, backupEntry *extensionsv1alpha1.BackupEntry) error {
	bucketClient, err := a.getClientAndConfig(ctx, backupEntry)
	if err != nil {
		return errors.DetermineError(fmt.Errorf("failed to get client and config while reconciling backup bucket %s: %w", backupEntry.Name, err))
	}
	entryName := strings.TrimPrefix(backupEntry.Name, v1beta1constants.BackupSourcePrefix+"-")

	// TODO(b/334957997): find out the GDCH specific error code and validate the error conversion
	return errors.DetermineError(a.deleteObjectsWithPrefix(ctx, backupEntry.Spec.BucketName, backupEntry.Spec.ProviderConfig, fmt.Sprintf("%s/", entryName), bucketClient))
}

// getClientAndConfig is a helper function to initialize all necessary clients and configurations
func (a *actuator) getClientAndConfig(ctx context.Context, backupEntry *extensionsv1alpha1.BackupEntry) (*backupEntryClientConfig, error) {
	serviceAccount, err := gdc.GetServiceAccountFromSecretReference(ctx, a.client, backupEntry.Spec.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get service account for backup entry %s: %w", backupEntry.Name, err)
	}

	gdchConfig, err := gdc.GetGDCHConfigFromSecretReference(ctx, a.client, backupEntry.Spec.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get GDCH config for backup entry %s: %w", backupEntry.Name, err)
	}

	orgClient, err := a.clientFactory.GetOrgClient(gdchConfig, serviceAccount, a.client.Scheme())
	if err != nil {
		return nil, fmt.Errorf("failed to get org client for backup entry %s: %w", backupEntry.Name, err)
	}

	return &backupEntryClientConfig{
		orgClient:      orgClient,
		serviceAccount: serviceAccount,
		gdchConfig:     gdchConfig,
	}, nil
}

// deleteObjectsWithPrefix deletes all objects within the specified bucket that have a key starting with the given prefix.
func (a *actuator) deleteObjectsWithPrefix(ctx context.Context, bucketName string, providerConfig *runtime.RawExtension, prefix string, bucketClient *backupEntryClientConfig) error {
	bucketStorageClient, err := a.createS3Client(ctx, bucketName, providerConfig, bucketClient)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Infof("Bucket %s not found, skipping object deletion", bucketName)
			return nil
		}
		return fmt.Errorf("failed to create s3Client, %w", err)
	}
	bucketFQN := bucketStorageClient.bucket.FullyQualifiedName
	objects, err := bucketStorageClient.s3Client.ListObjectsV2Pages(bucketFQN)
	if err != nil {
		return fmt.Errorf("failed to list objects in bucket %s, %w", bucketName, err)
	}
	for _, object := range objects {
		if strings.HasPrefix(object, prefix) {
			_, err := bucketStorageClient.s3Client.DeleteObject(s3.DeleteObjectInput{BucketFqn: bucketFQN, ObjectKey: object})
			if err != nil {
				klog.Errorf("failed to delete object %s from bucket %s, %v", object, bucketFQN, err)
				return fmt.Errorf("failed to delete object %s, %w", object, err)
			}
			klog.Infof("Deleted object %s from bucket %s", object, bucketFQN)
		}
	}
	return nil
}

func (a *actuator) createS3Client(ctx context.Context, bucketName string, providerConfig *runtime.RawExtension, bucketClient *backupEntryClientConfig) (*storageClient, error) {
	bucketMetadata, err := a.getBucketMetadata(ctx, client.ObjectKey{Name: bucketName, Namespace: bucketClient.serviceAccount.Project}, providerConfig, bucketClient.orgClient, bucketClient.gdchConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get object storage bucket %q: %w", bucketName, err)
	}
	accessKeys, err := storage.GetAccessKeyAndKeyID(ctx, bucketClient.orgClient, bucketClient.serviceAccount, bucketClient.gdchConfig.OrgClusterURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket access keys: %w", err)
	}
	caDataBytes, err := base64.StdEncoding.DecodeString(bucketClient.gdchConfig.CAData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode CA data from GDC config while creating S3 client for bucket %q: %w", bucketName, err)
	}

	s3Config := &s3.Config{
		AccessKeyId:     string(accessKeys.AccessKeyID),
		SecretAccessKey: string(accessKeys.AccessKey),
		S3Certificate:   caDataBytes,
		EndpointUrl:     bucketMetadata.Endpoint,
		Region:          bucketMetadata.Region,
	}

	tr := &http.Transport{}
	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(s3Config.S3Certificate)
	if !ok {
		return nil, fmt.Errorf("failed to parse CA PEM data from GDC config: check format")
	}
	tr.TLSClientConfig = &tls.Config{RootCAs: caCertPool}
	s3Config.CustomHttpClient = &http.Client{
		Transport: tr,
		Timeout:   s3Timeout,
	}

	s3Client, err := a.clientFactory.NewS3Client(s3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize S3 client for bucket %q: %w",
			bucketName, err)
	}

	return &storageClient{
		s3Client: s3Client,
		bucket:   bucketMetadata,
	}, nil
}

func (a *actuator) getBucketMetadata(ctx context.Context, bucketObjectKey client.ObjectKey, providerConfig *runtime.RawExtension, orgClient client.Client, gdchConfig *gdcclient.OrgClusterConfig) (*bucketMetadata, error) {
	isDualZone, err := storage.IsDualZoneBucketFlow(a.decoder, bucketObjectKey.Name, gdchConfig, providerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to determine backup entry type %s with naemspace %s: %w while reconciling", bucketObjectKey.Name, bucketObjectKey.Namespace, err)
	}

	if isDualZone {
		dualZoneBucket, err := storage.GetAndValidateDualZoneBucket(ctx, orgClient, bucketObjectKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get object storage for dual zone bucket %s with namespace %s: %w", bucketObjectKey.Name, bucketObjectKey.Namespace, err)
		}
		return &bucketMetadata{
			Endpoint:           dualZoneBucket.Status.GlobalEndpoint,
			Region:             dualZoneBucket.Status.Region,
			FullyQualifiedName: dualZoneBucket.Status.FullyQualifiedName}, nil
	}

	// Zonal bucket use case
	zonalBucket, err := storage.GetAndValidateZonalBucket(ctx, orgClient, bucketObjectKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get object storage for zonal bucket %s with namespace %s: %w", bucketObjectKey.Name, bucketObjectKey.Namespace, err)
	}
	return &bucketMetadata{
		Endpoint:           zonalBucket.Status.Endpoint,
		Region:             zonalBucket.Status.Region,
		FullyQualifiedName: zonalBucket.Status.FullyQualifiedName}, nil
}
