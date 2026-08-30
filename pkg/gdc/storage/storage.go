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

package storage

import (
	"context"
	"fmt"
	"slices"
	"strings"

	globalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/object/v1"
	objectv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/object/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/admission/validator"
)

const (
	accessKeyIDKey                  = "access-key-id"
	accessKeyKey                    = "secret-access-key"
	ObjectStorageAccessKeyNamespace = "object-storage-access-keys"
	zonalEndpointPrefix             = "management-kube"
	globalEndpointPrefix            = "global-api"
)

type AccessKeys struct {
	AccessKeyID []byte
	AccessKey   []byte
}

// GetAndValidateZonalBucket read bucket and throw error if required fields are missing.
func GetAndValidateZonalBucket(ctx context.Context, orgClient client.Client, bucketObjectKey client.ObjectKey) (*objectv1.Bucket, error) {
	bucketObject := objectv1.Bucket{}
	if err := orgClient.Get(ctx, bucketObjectKey, &bucketObject); err != nil {
		return nil, fmt.Errorf("failed to get zonal bucket object %v: %w", bucketObjectKey, err)
	}

	err := validateBucketStatus(
		bucketObject.Status.Conditions,
		bucketObject.Status.Endpoint,
		bucketObject.Status.FullyQualifiedName,
		bucketObject.Status.Region,
	)
	if err != nil {
		return nil, err
	}
	return &bucketObject, nil
}

// GetAndValidateDualZoneBucket read dual zone bucket and throw error if required fields are missing.
func GetAndValidateDualZoneBucket(ctx context.Context, orgClient client.Client, bucketObjectKey client.ObjectKey) (*globalv1.Bucket, error) {
	bucketObject := globalv1.Bucket{}
	if err := orgClient.Get(ctx, bucketObjectKey, &bucketObject); err != nil {
		return nil, fmt.Errorf("failed to get dual zone bucket object %v: %w", bucketObjectKey, err)
	}

	err := validateBucketStatus(
		bucketObject.Status.Conditions,
		bucketObject.Status.GlobalEndpoint,
		bucketObject.Status.FullyQualifiedName,
		bucketObject.Status.Region,
	)
	if err != nil {
		return nil, err
	}
	return &bucketObject, nil
}

// validateBucketStatus contains the common validation logic for any bucket type.
// It checks for readiness and the presence of essential status fields.
func validateBucketStatus(conditions []metav1.Condition, endpoint string, fqn string, region string) error {
	if !meta.IsStatusConditionTrue(conditions, objectv1.BucketReady) {
		return fmt.Errorf("bucket object is not ready")
	}
	if endpoint == "" {
		return fmt.Errorf("bucket Endpoint is empty")
	}
	if fqn == "" {
		return fmt.Errorf("bucket fully qualified name is empty")
	}
	if region == "" {
		return fmt.Errorf("bucket region is empty")
	}
	return nil
}

// getAccessKeysFromProjectNamespace retrieves object storage access key pairs from the project namespace.
func getAccessKeysFromProjectNamespace(ctx context.Context, orgClient client.Client, serviceAccount *auth.ServiceAccount) (*AccessKeys, error) {
	secretList := &corev1.SecretList{}
	selector := client.MatchingLabels{
		objectv1.SubjectTypeLabel: rbacv1.ServiceAccountKind,
	}
	if err := orgClient.List(ctx, secretList, client.InNamespace(serviceAccount.Project), selector); err != nil {
		return nil, fmt.Errorf("failed to list secrets in namespace %q: %w", serviceAccount.Project, err)
	}

	serviceAccountName := serviceAccount.Name

	for _, secret := range secretList.Items {
		if !strings.HasPrefix(secret.Name, objectv1.AccessKeySecretNamePrefix) {
			continue
		}

		annotation, ok := secret.Annotations[objectv1.SubjectAnnotation]
		if !ok {
			continue
		}

		// Project namespace uses short service account name format.
		if annotation != serviceAccountName {
			continue
		}

		accessKeyID, ok := secret.Data[accessKeyIDKey]
		if !ok {
			return nil, fmt.Errorf("failed to get data from (secret/key)=(%q/%q)", secret.Name, accessKeyIDKey)
		}
		accessKey, ok := secret.Data[accessKeyKey]
		if !ok {
			return nil, fmt.Errorf("failed to get data from (secret/key)=(%q/%q)", secret.Name, accessKeyKey)
		}

		klog.Infof("Successfully matched secret %q and retrieved access keys from project namespace %q for service account %q", secret.Name, serviceAccount.Project, serviceAccount.Name)
		return &AccessKeys{
			AccessKeyID: accessKeyID,
			AccessKey:   accessKey,
		}, nil
	}
	return nil, nil
}

// getAccessibleSecretNamesFromAccessKeysNamespace retrieves a list of secret names from the object-storage-access-keys namespace that are accessible by the service account.
// This function is designed for both zonal and global Kubernetes clusters if service account is created as kind: user. It uses SelfSubjectRulesReview to fetch the secret name.
func getAccessibleSecretNamesFromAccessKeysNamespace(ctx context.Context, orgClient client.Client, serviceAccount *auth.ServiceAccount) ([]string, error) {
	ssr := &authorizationv1.SelfSubjectRulesReview{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s", serviceAccount.Project, serviceAccount.Name),
		},
		Spec: authorizationv1.SelfSubjectRulesReviewSpec{
			Namespace: ObjectStorageAccessKeyNamespace,
		},
	}

	if err := orgClient.Create(ctx, ssr); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to perform self subject rules review: %w", err)
		}
	}

	var accessibleSecretNames []string
	// Iterate through the resource rules from the SelfSubjectRulesReview (SSR) to determine
	// which specific Secret resources the service account has `get` access to.
	for _, rule := range ssr.Status.ResourceRules {
		isCoreAPIGroup := slices.Contains(rule.APIGroups, "") || slices.Contains(rule.APIGroups, "*")
		isSecretsResource := slices.Contains(rule.Resources, "secrets") || slices.Contains(rule.Resources, "*")
		canGet := slices.Contains(rule.Verbs, "get") || slices.Contains(rule.Verbs, "*")

		if isCoreAPIGroup && isSecretsResource && canGet {
			accessibleSecretNames = append(accessibleSecretNames, rule.ResourceNames...)
		}
	}
	return accessibleSecretNames, nil
}

// GetAccessKeyAndKeyID retrieves object storage access key pairs.
func GetAccessKeyAndKeyID(ctx context.Context, orgClient client.Client, serviceAccount *auth.ServiceAccount, orgClusterURL string) (*AccessKeys, error) {
	// First, try to discover accessible secrets from the project namespace (for service account created based on kind:ServiceAccount for both zonal and global clusters).
	keys, err := getAccessKeysFromProjectNamespace(ctx, orgClient, serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to get access keys from project namespace: %w", err)
	}
	if keys != nil {
		return keys, nil
	}

	klog.Infof("Access keys not found in project namespace %q for service account %q on OrgCluster %q. Falling back to find the secret in shared namespace %q that matches canonical service account", serviceAccount.Project, serviceAccount.Name, orgClusterURL, ObjectStorageAccessKeyNamespace)
	return getAccessKeysFromSharedNamespace(ctx, orgClient, serviceAccount)
}

// getAccessKeysFromSharedNamespace retrieves object storage access key pairs from the shared namespace.
func getAccessKeysFromSharedNamespace(ctx context.Context, orgClient client.Client, serviceAccount *auth.ServiceAccount) (*AccessKeys, error) {
	accessibleSecretNames, err := getAccessibleSecretNamesFromAccessKeysNamespace(ctx, orgClient, serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to perform self subject rules review: %w", err)
	}

	if len(accessibleSecretNames) == 0 {
		return nil, fmt.Errorf("service account %q has no 'get' permissions on any secrets in namespace %q", serviceAccount.Name, ObjectStorageAccessKeyNamespace)
	}

	expectedServiceAccount := fmt.Sprintf("system:serviceaccount:%s:%s", serviceAccount.Project, serviceAccount.Name)
	for _, secretName := range accessibleSecretNames {
		if !strings.HasPrefix(secretName, objectv1.AccessKeySecretNamePrefix) {
			continue
		}

		objectStorageKey := &corev1.Secret{}
		secretObject := client.ObjectKey{Namespace: ObjectStorageAccessKeyNamespace, Name: secretName}
		if err := orgClient.Get(ctx, secretObject, objectStorageKey); err != nil {
			return nil, fmt.Errorf("failed to get secret object %v: %w", secretObject, err)
		}

		labelValue, ok := objectStorageKey.Labels[objectv1.SubjectTypeLabel]
		if !ok || labelValue != rbacv1.UserKind {
			continue
		}

		svcAccount, ok := objectStorageKey.Annotations[objectv1.SubjectAnnotation]
		if !ok {
			continue
		}
		if svcAccount != expectedServiceAccount {
			continue
		}

		accessKeyID, ok := objectStorageKey.Data[accessKeyIDKey]
		if !ok {
			return nil, fmt.Errorf("failed to get data from (secret/key)=(%q/%q)", objectStorageKey.Name, accessKeyIDKey)
		}
		accessKey, ok := objectStorageKey.Data[accessKeyKey]
		if !ok {
			return nil, fmt.Errorf("failed to get data from (secret/key)=(%q/%q)", objectStorageKey.Name, accessKeyKey)
		}

		klog.Infof("Successfully matched secret %q and retrieved access keys for service account %q in namespace %q", secretName, expectedServiceAccount, ObjectStorageAccessKeyNamespace)
		return &AccessKeys{
			AccessKeyID: accessKeyID,
			AccessKey:   accessKey,
		}, nil
	}

	return nil, fmt.Errorf("failed to find matching secret to read bucket access keys for service account %q in namespace %q", expectedServiceAccount, ObjectStorageAccessKeyNamespace)
}

// IsDualZoneBucketFlow determines if the configuration corresponds to a dual-zone backup bucket flow.
// It also validates the OrgClusterURL based on whether the flow is zonal or dual-zone.
func IsDualZoneBucketFlow(decoder runtime.Decoder, bucketName string, gdchConfig *gdcclient.OrgClusterConfig, providerConfig *runtime.RawExtension) (bool, error) {
	var isDualZone bool // Default is false (zonal)

	if providerConfig != nil {
		// Decode the provider configuration to check for the dual-zone location.
		backupBucketConfig, err := validator.DecodeBackupBucketConfig(decoder, providerConfig)
		if err != nil {
			return false, fmt.Errorf("failed to decode BackupBucketConfig for backup bucket %s: %w", bucketName, err)
		}

		// The presence of a DualZoneBucketLocation confirms a dual-zone flow.
		if backupBucketConfig != nil && backupBucketConfig.DualZoneBucketLocation != "" {
			isDualZone = true
		}
	}

	// Perform the URL validation once, based on the final determined flow type.
	if err := validateOrgClusterURL(gdchConfig, bucketName, isDualZone); err != nil {
		return false, err
	}

	return isDualZone, nil
}

// validateOrgClusterURL validation of the gdchConfig.OrgClusterURL.
func validateOrgClusterURL(gdchConfig *gdcclient.OrgClusterConfig, bucketName string, isDualZone bool) error {
	if isDualZone {
		// Dual-zone buckets must use the 'global-api' endpoint.
		if !strings.Contains(gdchConfig.OrgClusterURL, globalEndpointPrefix) {
			return fmt.Errorf("invalid gdch-config for dual-zone bucket %s: orgClusterURL=%s must use '%s'", bucketName, gdchConfig.OrgClusterURL, globalEndpointPrefix)
		}
	} else {
		// Zonal buckets must use the 'management-kube' endpoint.
		if !strings.Contains(gdchConfig.OrgClusterURL, zonalEndpointPrefix) {
			return fmt.Errorf("invalid gdch-config for zonal bucket %s: orgClusterURL=%s must use '%s'", bucketName, gdchConfig.OrgClusterURL, zonalEndpointPrefix)
		}
	}
	return nil
}
