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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	globalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/object/v1"
	objectv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/object/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
)

const (
	bucketName               = "bucketName"
	testProject              = "my-namespace"
	endpoint                 = "https://objectstorage.gdc1.us-west6-c.staging.gpcdemolabs.com"
	fullyQualifiedBucketName = "b1jr3qr-test-zone-bucket"
	resourceVersion          = "999"
	bucketRegion             = "us-west6-c"
	serviceAccountName       = "sa"
	testAccessKeyID          = "testAccessKeyID"
	testSecretAccessKey      = "testSecretAccessKey"
)

func TestGetAndValidateZonalBucket(t *testing.T) {
	// Define common variables for tests
	bucketName := bucketName
	namespace := testProject
	bucketKey := types.NamespacedName{Name: bucketName, Namespace: namespace}

	// Test cases
	testCases := []struct {
		name           string
		bucket         *objectv1.Bucket
		expectErr      bool
		expectedBucket *objectv1.Bucket
	}{
		{
			name: "should return bucket when it is ready and valid",
			bucket: &objectv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace, ResourceVersion: resourceVersion},
				Status: objectv1.BucketStatus{
					Endpoint:           endpoint,
					FullyQualifiedName: fullyQualifiedBucketName,
					Region:             bucketRegion,
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
			expectErr: false,
			expectedBucket: &objectv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace, ResourceVersion: resourceVersion}, // Fake client adds ResourceVersion
				Status: objectv1.BucketStatus{
					Endpoint:           endpoint,
					FullyQualifiedName: fullyQualifiedBucketName,
					Region:             bucketRegion,
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
		},
		{
			name:      "should return error when bucket is not found",
			bucket:    nil, // No bucket object provided to the fake client
			expectErr: true,
		},
		{
			name: "should return error when bucket is not ready",
			bucket: &objectv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace},
				Status: objectv1.BucketStatus{
					Conditions: []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionFalse}},
				},
			},
			expectErr: true,
		},
		{
			name: "should return error when bucket endpoint is empty",
			bucket: &objectv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace},
				Status: objectv1.BucketStatus{
					Endpoint:           "", // Missing endpoint
					FullyQualifiedName: fullyQualifiedBucketName,
					Region:             bucketRegion,
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
			expectErr: true,
		},
		{
			name: "should return error when bucket FQN is empty",
			bucket: &objectv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace},
				Status: objectv1.BucketStatus{
					Endpoint:           endpoint,
					FullyQualifiedName: "", // Missing FQN
					Region:             bucketRegion,
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
			expectErr: true,
		},
		{
			name: "should return error when bucket region is empty",
			bucket: &objectv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace},
				Status: objectv1.BucketStatus{
					Endpoint:           endpoint,
					FullyQualifiedName: fullyQualifiedBucketName,
					Region:             "", // Missing region
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var fakeClient client.Client
			if tc.bucket != nil {
				fakeClient, _ = newFakeClient(nil, tc.bucket)
			} else {
				fakeClient, _ = newFakeClient(nil)
			}

			bucket, err := GetAndValidateZonalBucket(context.Background(), fakeClient, bucketKey)

			if tc.expectErr && err == nil {
				t.Errorf("GetAndValidateBucket() expected error, but got none")
			}
			if !tc.expectErr && err != nil {
				t.Errorf("GetAndValidateBucket() returned unexpected error: %v", err)
			}
			if !tc.expectErr {
				if diff := cmp.Diff(tc.expectedBucket, bucket); diff != "" {
					t.Errorf("GetAndValidateBucket() returned diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestGetAndValidateDualZoneBucket(t *testing.T) {
	// Define common variables for tests
	bucketName := bucketName
	namespace := testProject
	bucketKey := types.NamespacedName{Name: bucketName, Namespace: namespace}

	// Test cases
	testCases := []struct {
		name           string
		bucket         *globalv1.Bucket
		expectErr      bool
		expectedBucket *globalv1.Bucket
	}{
		{
			name: "should return bucket when it is ready and valid",
			bucket: &globalv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace, ResourceVersion: resourceVersion},
				Status: globalv1.BucketStatus{
					GlobalEndpoint:     endpoint,
					FullyQualifiedName: fullyQualifiedBucketName,
					Region:             bucketRegion,
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
			expectErr: false,
			expectedBucket: &globalv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace, ResourceVersion: resourceVersion}, // Fake client adds ResourceVersion
				Status: globalv1.BucketStatus{
					GlobalEndpoint:     endpoint,
					FullyQualifiedName: fullyQualifiedBucketName,
					Region:             bucketRegion,
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
		},
		{
			name:      "should return error when bucket is not found",
			bucket:    nil, // No bucket object provided to the fake client
			expectErr: true,
		},
		{
			name: "should return error when bucket is not ready",
			bucket: &globalv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace},
				Status: globalv1.BucketStatus{
					Conditions: []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionFalse}},
				},
			},
			expectErr: true,
		},
		{
			name: "should return error when bucket endpoint is empty",
			bucket: &globalv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace},
				Status: globalv1.BucketStatus{
					GlobalEndpoint:     "", // Missing endpoint
					FullyQualifiedName: fullyQualifiedBucketName,
					Region:             bucketRegion,
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
			expectErr: true,
		},
		{
			name: "should return error when bucket FQN is empty",
			bucket: &globalv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace},
				Status: globalv1.BucketStatus{
					GlobalEndpoint:     endpoint,
					FullyQualifiedName: "", // Missing FQN
					Region:             bucketRegion,
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
			expectErr: true,
		},
		{
			name: "should return error when bucket region is empty",
			bucket: &globalv1.Bucket{
				ObjectMeta: metav1.ObjectMeta{Name: bucketName, Namespace: namespace},
				Status: globalv1.BucketStatus{
					GlobalEndpoint:     endpoint,
					FullyQualifiedName: fullyQualifiedBucketName,
					Region:             "", // Missing region
					Conditions:         []metav1.Condition{{Type: objectv1.BucketReady, Status: metav1.ConditionTrue}},
				},
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var fakeClient client.Client
			if tc.bucket != nil {
				fakeClient, _ = newFakeClient(nil, tc.bucket)
			} else {
				fakeClient, _ = newFakeClient(nil)
			}

			bucket, err := GetAndValidateDualZoneBucket(context.Background(), fakeClient, bucketKey)

			if tc.expectErr && err == nil {
				t.Errorf("GetAndValidateBucket() expected error, but got none")
			}
			if !tc.expectErr && err != nil {
				t.Errorf("GetAndValidateBucket() returned unexpected error: %v", err)
			}
			if !tc.expectErr {
				if diff := cmp.Diff(tc.expectedBucket, bucket); diff != "" {
					t.Errorf("GetAndValidateBucket() returned diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestGetAccessKeyAndKeyID(t *testing.T) {
	serviceAccount := &auth.ServiceAccount{
		Name:    serviceAccountName,
		Project: testProject,
	}
	serviceAccountNameStr := fmt.Sprintf("system:serviceaccount:%s:%s", serviceAccount.Project, serviceAccount.Name)

	// Secret in project namespace for the global cluster scenario (service account created as kind:ServiceAccount)
	projectNamespaceSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectv1.AccessKeySecretNamePrefix + "global-123",
			Namespace: testProject, // Project namespace
			Labels:    map[string]string{objectv1.SubjectTypeLabel: rbacv1.ServiceAccountKind},
			Annotations: map[string]string{
				objectv1.SubjectAnnotation: serviceAccountName,
			},
		},
		Data: map[string][]byte{
			accessKeyIDKey: []byte("globalAccessKeyID"),
			accessKeyKey:   []byte("globalSecretAccessKey"),
		},
	}

	// Secret in object-storage-access-keys namespace for the zonal cluster scenario (service account created as kind:User)
	objectStoreNamespaceSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectv1.AccessKeySecretNamePrefix + "zonal-456",
			Namespace: ObjectStorageAccessKeyNamespace, // Zonal namespace
			Labels:    map[string]string{objectv1.SubjectTypeLabel: rbacv1.UserKind},
			Annotations: map[string]string{
				objectv1.SubjectAnnotation: serviceAccountNameStr,
			},
		},
		Data: map[string][]byte{
			accessKeyIDKey: []byte(testAccessKeyID),
			accessKeyKey:   []byte(testSecretAccessKey),
		},
	}

	otherSASecret := objectStoreNamespaceSecret.DeepCopy()
	otherSASecret.Name = objectv1.AccessKeySecretNamePrefix + "other-sa"
	otherSASecret.Annotations[objectv1.SubjectAnnotation] = "another-sa"

	testCases := []struct {
		name                string
		initialObjects      []client.Object
		accessibleSecrets   []string // Secrets that the SelfSubjectRulesReview will indicate are accessible
		expectedKeys        *AccessKeys
		expectErr           bool
		expectedErrContains string
	}{
		{
			name:           "should find keys in project namespace (global cluster scenario)",
			initialObjects: []client.Object{projectNamespaceSecret, objectStoreNamespaceSecret},
			// No SelfSubjectRulesReview secrets needed, should be found in project namespace first
			accessibleSecrets: []string{},
			expectedKeys: &AccessKeys{
				AccessKeyID: []byte("globalAccessKeyID"),
				AccessKey:   []byte("globalSecretAccessKey"),
			},
			expectErr: false,
		},
		{
			name: "should find keys in project namespace with short service account name",
			initialObjects: []client.Object{
				func() *corev1.Secret {
					s := projectNamespaceSecret.DeepCopy()
					s.Annotations[objectv1.SubjectAnnotation] = serviceAccountName
					return s
				}(),
			},
			accessibleSecrets: []string{},
			expectedKeys: &AccessKeys{
				AccessKeyID: []byte("globalAccessKeyID"),
				AccessKey:   []byte("globalSecretAccessKey"),
			},
			expectErr: false,
		},
		{
			name:           "should find keys using SelfSubjectRulesReview (zonal cluster fallback)",
			initialObjects: []client.Object{objectStoreNamespaceSecret, otherSASecret},
			// No secrets in project namespace, SelfSubjectRulesReview will point to the zonal secret
			accessibleSecrets: []string{objectStoreNamespaceSecret.Name, otherSASecret.Name},
			expectedKeys: &AccessKeys{
				AccessKeyID: []byte(testAccessKeyID),
				AccessKey:   []byte(testSecretAccessKey),
			},
			expectErr: false,
		},
		{
			name: "throw error for key using SelfSubjectRulesReview with short service account name",
			initialObjects: []client.Object{
				func() *corev1.Secret {
					s := objectStoreNamespaceSecret.DeepCopy()
					s.Annotations[objectv1.SubjectAnnotation] = serviceAccountName
					return s
				}(),
				otherSASecret,
			},
			accessibleSecrets:   []string{objectStoreNamespaceSecret.Name, otherSASecret.Name},
			expectErr:           true,
			expectedErrContains: "failed to find matching secret to read bucket access keys for service account",
		},
		{
			name:                "should return error if no secrets are found",
			initialObjects:      []client.Object{otherSASecret},
			accessibleSecrets:   []string{otherSASecret.Name}, // SelfSubjectRulesReview only finds other SA's secret
			expectErr:           true,
			expectedErrContains: "failed to find matching secret to read bucket access keys for service account",
		},
		{
			name:                "should return error if SelfSubjectRulesReview finds nothing and project namespace is empty",
			initialObjects:      []client.Object{},
			accessibleSecrets:   []string{},
			expectErr:           true,
			expectedErrContains: "service account \"sa\" has no 'get' permissions on any secrets in namespace \"object-storage-access-keys\"",
		},
		{
			name: "should return error if secret is missing access key id",
			initialObjects: []client.Object{
				func() *corev1.Secret {
					s := objectStoreNamespaceSecret.DeepCopy()
					delete(s.Data, accessKeyIDKey)
					return s
				}(),
			},
			accessibleSecrets:   []string{objectStoreNamespaceSecret.Name},
			expectErr:           true,
			expectedErrContains: "failed to get data from (secret/key)=(\"object-storage-key-zonal-456\"/\"access-key-id\")",
		},
		{
			name: "should return error if secret is missing secret access key",
			initialObjects: []client.Object{
				func() *corev1.Secret {
					s := objectStoreNamespaceSecret.DeepCopy()
					delete(s.Data, accessKeyKey)
					return s
				}(),
			},
			accessibleSecrets:   []string{objectStoreNamespaceSecret.Name},
			expectErr:           true,
			expectedErrContains: "failed to get data from (secret/key)=(\"object-storage-key-zonal-456\"/\"secret-access-key\")",
		},
		{
			name: "should ignore secrets without the correct prefix",
			initialObjects: []client.Object{
				func() *corev1.Secret {
					s := objectStoreNamespaceSecret.DeepCopy()
					s.Name = "not-the-right-prefix"
					return s
				}(),
			},
			accessibleSecrets:   []string{"not-the-right-prefix"},
			expectErr:           true,
			expectedErrContains: "failed to find matching secret to read bucket access keys for service account",
		},
		{
			name: "should ignore secrets without the correct label",
			initialObjects: []client.Object{
				func() *corev1.Secret {
					s := objectStoreNamespaceSecret.DeepCopy()
					delete(s.Labels, objectv1.SubjectTypeLabel)
					return s
				}(),
			},
			accessibleSecrets:   []string{objectStoreNamespaceSecret.Name},
			expectErr:           true,
			expectedErrContains: "failed to find matching secret to read bucket access keys for service account",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient, _ := newFakeClient(tc.accessibleSecrets, tc.initialObjects...)

			keys, err := GetAccessKeyAndKeyID(context.Background(), fakeClient, serviceAccount, "https://dummy.url")

			if tc.expectErr {
				if err == nil {
					t.Errorf("GetAccessKeyAndKeyID() expected error, but got none")
				}
				if !strings.Contains(err.Error(), tc.expectedErrContains) {
					t.Errorf("Expected the error message to contain\n \"%s\" \n\nbut got \"%s\"", tc.expectedErrContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("GetAccessKeyAndKeyID() returned unexpected error: %v", err)
				}
				if diff := cmp.Diff(tc.expectedKeys, keys); diff != "" {
					t.Errorf("GetAccessKeyAndKeyID() returned diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestIsDualZoneBucketFlow(t *testing.T) {
	// Setup a decoder that understands the BackupBucketConfig type
	scheme := getRuntimeScheme()
	decoder := serializer.NewCodecFactory(scheme).UniversalDecoder()

	testCases := []struct {
		name               string
		gdchConfig         *gdcclient.OrgClusterConfig
		providerConfig     *runtime.RawExtension
		expectedIsDualZone bool
		expectErr          bool
		expectErrContains  string
	}{
		{
			name: "should NOT be dual-zone with nil providerConfig",
			gdchConfig: &gdcclient.OrgClusterConfig{
				OrgClusterURL: "https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com",
			},
			providerConfig:     nil,
			expectedIsDualZone: false,
			expectErr:          false,
		},
		{
			name: "should NOT be dual-zone with nil BackupBucket config",
			gdchConfig: &gdcclient.OrgClusterConfig{
				OrgClusterURL: "https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com",
			},
			providerConfig:     newProviderConfig(t, nil),
			expectedIsDualZone: false,
			expectErr:          false,
		},
		{
			name: "should NOT be dual-zone with empty DualZoneBucketLocation",
			gdchConfig: &gdcclient.OrgClusterConfig{
				OrgClusterURL: "https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com",
			},
			providerConfig: newProviderConfig(t, &apisgdc.BackupBucketConfig{
				DualZoneBucketLocation: "", // Location is empty
			}),
			expectedIsDualZone: false,
			expectErr:          false,
		},
		{
			name: "should return error for zonal with non-glomanagement-kubebal orgClusterURL",
			gdchConfig: &gdcclient.OrgClusterConfig{
				OrgClusterURL: "https://api.some-region.example.com", // Not management-kube
			},
			expectedIsDualZone: false,
			expectErr:          true,
			expectErrContains:  "must use 'management-kube'",
		},
		{
			name: "should return error for dual-zone with non-global orgClusterURL",
			gdchConfig: &gdcclient.OrgClusterConfig{
				OrgClusterURL: "https://api.some-region.example.com", // Not global
			},
			providerConfig: newProviderConfig(t, &apisgdc.BackupBucketConfig{
				DualZoneBucketLocation: "syncz1z2",
			}),
			expectedIsDualZone: false,
			expectErr:          true,
			expectErrContains:  "must use 'global-api'",
		},
		{
			name: "should be dual-zone with global orgClusterURL and location",
			gdchConfig: &gdcclient.OrgClusterConfig{
				OrgClusterURL: "https://global-api.gdc1.us-west6-a.staging.gpcdemolabs.com", // Contains "global-api"
			},
			providerConfig: newProviderConfig(t, &apisgdc.BackupBucketConfig{
				DualZoneBucketLocation: "syncz1z2",
			}),
			expectedIsDualZone: true,
			expectErr:          false,
		},
		{
			name:       "should return error for malformed providerConfig",
			gdchConfig: &gdcclient.OrgClusterConfig{},
			providerConfig: &runtime.RawExtension{
				Raw: []byte(`{"apiVersion": "v1", "kind": "Pod"}`), // Incorrect type
			},
			expectedIsDualZone: false,
			expectErr:          true,
			expectErrContains:  "failed to decode BackupBucketConfig",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isDualZone, err := IsDualZoneBucketFlow(decoder, "test-bucket", tc.gdchConfig, tc.providerConfig)

			if tc.expectErr {
				if err == nil {
					t.Fatal("IsDualZoneBucketFlow() expected an error, but got none")
				}
				if !strings.Contains(err.Error(), tc.expectErrContains) {
					t.Errorf("IsDualZoneBucketFlow() got error = %q, want error containing %q", err.Error(), tc.expectErrContains)
				}
			} else {
				if err != nil {
					t.Fatalf("IsDualZoneBucketFlow() returned an unexpected error: %v", err)
				}
				if isDualZone != tc.expectedIsDualZone {
					t.Errorf("IsDualZoneBucketFlow() got isDualZone = %t, want %t", isDualZone, tc.expectedIsDualZone)
				}
			}
		})
	}
}

// Helper function to create a new fake client with a given scheme and initial objects.
func newFakeClient(accessibleSecretNames []string, initObjs ...client.Object) (client.Client, *runtime.Scheme) {
	scheme := getRuntimeScheme()
	ssrModifier := func(ssr *authorizationv1.SelfSubjectRulesReview) {
		ssr.Status.ResourceRules = []authorizationv1.ResourceRule{
			{
				Verbs:         []string{"get"},
				APIGroups:     []string{""},
				Resources:     []string{"secrets"},
				ResourceNames: accessibleSecretNames,
			},
		}
	}
	interceptors := interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if ssr, ok := obj.(*authorizationv1.SelfSubjectRulesReview); ok {
				ssrModifier(ssr)
			}
			return cl.Create(ctx, obj, opts...)
		},
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initObjs...).
		WithInterceptorFuncs(interceptors).
		Build(), scheme
}

func getRuntimeScheme() *runtime.Scheme {
	// Use a real scheme to encode/decode the object correctly
	scheme := runtime.NewScheme()
	_ = apisgdc.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	_ = objectv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = rbacv1.AddToScheme(scheme)
	_ = authorizationv1.AddToScheme(scheme)
	_ = globalv1.AddToScheme(scheme)
	return scheme
}

// newProviderConfig is a helper to correctly encode a BackupBucketConfig into a RawExtension.
func newProviderConfig(t *testing.T, config *apisgdc.BackupBucketConfig) *runtime.RawExtension {
	t.Helper()
	if config == nil {
		return nil
	}

	// Use a real scheme to encode the object correctly
	scheme := getRuntimeScheme()
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(v1alpha1.SchemeGroupVersion)

	// Set the GVK (GroupVersionKind) on the object before encoding.
	config.SetGroupVersionKind(v1alpha1.SchemeGroupVersion.WithKind("BackupBucketConfig"))

	encoded, err := runtime.Encode(codec, config)
	if err != nil {
		t.Fatalf("failed to encode provider config: %v", err)
	}
	return &runtime.RawExtension{Raw: encoded}
}
