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

package backupbucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"

	v1beta1helper "github.com/gardener/gardener/pkg/api/core/v1beta1/helper"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/gardener/gardener/pkg/controllerutils/reconciler"
	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	globalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/object/v1"
	objectv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/object/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/s3"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/storage"
)

const (
	backupBucketName         = "test-backup-bucket"
	testProject              = "my-namespace"
	fullyQualifiedBucketName = "test-bucket-fully-qualified-name"
	testAccessKeyID          = "testAccessKeyID"
	testSecretAccessKey      = "testSecretAccessKey"
	serviceAccountName       = "sa"
	credentialSecretName     = "object-storage-key-my-secret"
)

// mockClientFactory is a mock implementation of the clientFactory interface for testing.
type mockClientFactory struct {
	// Fields to hold the mock functions
	mockGetOrgClientFn func(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error)
	mockNewS3ClientFn  func(config *s3.Config) (s3.Client, error)
}

// GetOrgClient calls the mock function.
func (m *mockClientFactory) GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error) {
	return m.mockGetOrgClientFn(gdchConfig, serviceAccount, scheme)
}

// NewS3Client calls the mock function or returns an empty mock S3 client.
func (m *mockClientFactory) NewS3Client(config *s3.Config) (s3.Client, error) {
	if m.mockNewS3ClientFn != nil {
		return m.mockNewS3ClientFn(config)
	}
	return s3.CreateMockS3Client(s3.MockS3ClientConfig{
		Buckets: map[string]*s3.MockBucket{
			fullyQualifiedBucketName: {Objects: map[string]*s3.MockObject{}},
		},
	}), nil
}

func TestActuator_ZonalBucket_Reconcile(t *testing.T) {
	// Define common objects used across test cases
	baseBackupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: backupBucketName,
		},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			Region: "us-west1",
			SecretRef: corev1.SecretReference{
				Name:      credentialSecretName,
				Namespace: testProject,
			},
		},
	}

	credentialSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      credentialSecretName,
			Namespace: testProject,
		},
		Data: map[string][]byte{
			"serviceaccount.json": []byte(fmt.Sprintf(`{ "Name": "%s", "project":"%s"}`, serviceAccountName, testProject)),
			"gdch-config":         []byte(`{"caData": "ZmFrZS1jYWRhdGE=", "orgClusterURL": "https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com"}`),
		},
	}

	accessKeySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      credentialSecretName,
			Namespace: storage.ObjectStorageAccessKeyNamespace,
			Labels: map[string]string{
				objectv1.SubjectTypeLabel: "User",
			},
			Annotations: map[string]string{
				objectv1.SubjectAnnotation: fmt.Sprintf("system:serviceaccount:%s:%s", testProject, serviceAccountName),
			},
		},
		Data: map[string][]byte{
			"access-key-id":     []byte(testAccessKeyID),
			"secret-access-key": []byte(testSecretAccessKey),
		},
	}
	testNamespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: testProject,
		},
	}
	testCases := []struct {
		name                               string
		initialGardenObjects               []client.Object
		initialMgmtObjects                 []client.Object
		newBucket                          bool
		expectErr                          bool
		expectErrContains                  string
		expectedRequestChecksumCalculation string
		expectedResponseChecksumValidation string
	}{
		{
			name: "should succeed and create bucket if it does not exist",
			initialGardenObjects: []client.Object{
				baseBackupBucket.DeepCopy(),
				credentialSecret.DeepCopy(),
			},
			initialMgmtObjects: []client.Object{
				// No initial bucket, it will be created by the Reconcile function
				testNamespace.DeepCopy(),
				accessKeySecret.DeepCopy(),
			},
			newBucket: true,
			expectErr: false,
		},
		{
			name: "should succeed if bucket already exists",
			initialGardenObjects: []client.Object{
				baseBackupBucket.DeepCopy(),
				credentialSecret.DeepCopy(),
			},
			initialMgmtObjects: []client.Object{
				// Pre-populate the org client with the bucket
				testNamespace.DeepCopy(),
				newBucket(backupBucketName, true), // isReady = true
				accessKeySecret.DeepCopy(),
			},
			newBucket: false,
			expectErr: false,
		},
		{
			name: "should return retryable error if bucket is not ready",
			initialGardenObjects: []client.Object{
				baseBackupBucket.DeepCopy(),
				credentialSecret.DeepCopy(),
			},
			initialMgmtObjects: []client.Object{
				testNamespace.DeepCopy(),
				newBucket(backupBucketName, false), // isReady = false
				accessKeySecret.DeepCopy(),
			},
			newBucket:         false,
			expectErr:         true,
			expectErrContains: "RetryableError: failed to get bucket",
		},
		{
			name: "should fail if credential secret is missing",
			initialGardenObjects: []client.Object{
				baseBackupBucket.DeepCopy(),
				// credentialSecret is missing
			},
			initialMgmtObjects: []client.Object{
				testNamespace.DeepCopy(),
				accessKeySecret.DeepCopy(),
			},
			newBucket:         false,
			expectErr:         true,
			expectErrContains: "failed to get service account",
		},

		{
			name: "should succeed and use custom checksum values if provided",
			initialGardenObjects: []client.Object{
				func() *extensionsv1alpha1.BackupBucket {
					bb := baseBackupBucket.DeepCopy()
					bb.Spec.ProviderConfig = &runtime.RawExtension{
						Raw: encode(&gdc.BackupBucketConfig{
							TypeMeta: metav1.TypeMeta{
								Kind:       "BackupBucketConfig",
								APIVersion: gdc.SchemeGroupVersion.String(),
							},
							RequestChecksumCalculation: gdc.ChecksumWhenSupported,
							ResponseChecksumValidation: gdc.ChecksumWhenSupported,
						}),
					}
					return bb
				}(),
				credentialSecret.DeepCopy(),
			},
			initialMgmtObjects: []client.Object{
				testNamespace.DeepCopy(),
				accessKeySecret.DeepCopy(),
			},
			newBucket:                          true,
			expectErr:                          false,
			expectedRequestChecksumCalculation: gdc.ChecksumWhenSupported,
			expectedResponseChecksumValidation: gdc.ChecksumWhenSupported,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create fake kubernetes client for controller
			testClient := newTestClient(tc.initialGardenObjects...)
			// Create fake kuberentes client for org api of gdch
			orgClient := newTestClient(tc.initialMgmtObjects...)

			// Create an instance of our mock factory
			mockFactory := &mockClientFactory{
				// Implement the mock functions for this test setup
				mockGetOrgClientFn: func(_ *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.Client, error) {
					// If the bucket does NOT exist in the initial setup, it means this test case
					// is testing the creation path. In this scenario, we return a client that
					// is pre-programmed with the final, "Ready" state of the bucket.
					if tc.newBucket {
						// We add the final, expected object to the initial list.
						// This simulates that the bucket was created AND became ready.
						finalMgmtObjects := append(tc.initialMgmtObjects, newBucket(backupBucketName, true))
						return newTestClient(finalMgmtObjects...), nil
					}
					// The mock function returns the fake org client we prepared.
					return orgClient, nil
				},
			}

			a := &actuator{
				client:        testClient,
				decoder:       serializer.NewCodecFactory(getRuntimeScheme(), serializer.EnableStrict).UniversalDecoder(),
				clientFactory: mockFactory,
			}

			// Call the Reconcile function
			err := a.Reconcile(context.Background(), logr.Logger{}, baseBackupBucket.DeepCopy())

			// Check for errors
			if tc.expectErr {
				if err == nil {
					t.Fatal("Reconcile() expected error, but got none")
				}
				if !strings.Contains(err.Error(), tc.expectErrContains) {
					t.Errorf("Reconcile() got error = %q, want error containing %q", err.Error(), tc.expectErrContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("Reconcile() expected no error but got %v", err)
			}

			ctx := context.Background()
			updatedBackupBucket := &extensionsv1alpha1.BackupBucket{}
			if err := testClient.Get(ctx, client.ObjectKey{Name: backupBucketName}, updatedBackupBucket); err != nil {
				t.Fatalf("failed to fetch updated BackupBucket: %v", err)
			}

			t.Run("should update BackupBucket annotation", func(t *testing.T) {
				gotFQN, ok := updatedBackupBucket.Annotations[gdcconstants.FullyQualifiedBucketNameAnnotationKey]
				if !ok {
					t.Errorf("expected annotation %q to be set", gdcconstants.FullyQualifiedBucketNameAnnotationKey)
				}
				if gotFQN != fullyQualifiedBucketName {
					t.Errorf("expected annotation value %q, got %q", fullyQualifiedBucketName, gotFQN)
				}
			})

			t.Run("should update BackupBucket status", func(t *testing.T) {
				expectedStatus := extensionsv1alpha1.BackupBucketStatus{
					GeneratedSecretRef: &corev1.SecretReference{
						Name:      getGeneratedSecretName(backupBucketName),
						Namespace: generatedSecretNamespace,
					},
				}
				// Note: Fake client doesn't automatically populate providerStatus, so we only check the secret ref.
				if diff := cmp.Diff(expectedStatus.GeneratedSecretRef, updatedBackupBucket.Status.GeneratedSecretRef); diff != "" {
					t.Errorf("unexpected status.generatedSecretRef (-want +got):\n%s", diff)
				}
			})

			t.Run("should create the generated secret with correct data", func(t *testing.T) {
				secretName := getGeneratedSecretName(backupBucketName)
				secret := &corev1.Secret{}
				if err := testClient.Get(ctx, client.ObjectKey{Name: secretName, Namespace: generatedSecretNamespace}, secret); err != nil {
					t.Fatalf("failed to fetch generated Secret: %v", err)
				}

				expectedBucketStatus := newBucket(backupBucketName, true).Status
				serviceaccountData := credentialSecret.Data["serviceaccount.json"]
				gdchConfigData := credentialSecret.Data["gdch-config"]

				reqChecksum := tc.expectedRequestChecksumCalculation
				if reqChecksum == "" {
					reqChecksum = gdc.ChecksumWhenRequired
				}
				respChecksum := tc.expectedResponseChecksumValidation
				if respChecksum == "" {
					respChecksum = gdc.ChecksumWhenRequired
				}

				expectedSecretData := map[string][]byte{
					"backup_secret.json": mustMarshalJSON(t, map[string]interface{}{
						"accessKeyID":                testAccessKeyID,
						"secretAccessKey":            testSecretAccessKey,
						"endpoint":                   expectedBucketStatus.Endpoint,
						"region":                     expectedBucketStatus.Region,
						"s3ForcePathStyle":           true,
						"insecureSkipVerify":         false,
						"requestChecksumCalculation": reqChecksum,
						"responseChecksumValidation": respChecksum,
						"trustedCaCert":              "fake-cadata",
					}),
					"accessKeyID":                []byte(testAccessKeyID),
					"secretAccessKey":            []byte(testSecretAccessKey),
					"endpoint":                   []byte(expectedBucketStatus.Endpoint),
					"region":                     []byte(expectedBucketStatus.Region),
					"requestChecksumCalculation": []byte(reqChecksum),
					"responseChecksumValidation": []byte(respChecksum),
					"trustedCaCert":              []byte("fake-cadata"),
					"insecureSkipVerify":         []byte("false"),
					"s3ForcePathStyle":           []byte("true"),
					"serviceaccount.json":        serviceaccountData,
					"gdch-config":                gdchConfigData,
				}
				// Compare the flat keys from the actual secret against our expectations
				for key, expectedVal := range expectedSecretData {
					gotVal, ok := secret.Data[key]
					if !ok {
						t.Errorf("secret.Data is missing flat key %q", key)
						continue
					}

					// Use a helper to compare JSON strings if they are JSON, otherwise compare as strings
					if json.Valid(expectedVal) && json.Valid(gotVal) {
						var expectedJSON, gotJSON interface{}
						if diff := cmp.Diff(expectedJSON, gotJSON); diff != "" {
							t.Errorf("unexpected data for flat JSON key %q (-want +got):\n%s", key, diff)
						}
					} else {
						if diff := cmp.Diff(string(expectedVal), string(gotVal)); diff != "" {
							t.Errorf("unexpected data for flat key %q (-want +got):\n%s", key, diff)
						}
					}
				}
				// Also check that there are no extra keys in the secret
				if len(secret.Data) != len(expectedSecretData) { // +1 for backup_secret.json
					t.Errorf("secret.Data has an unexpected number of keys. got %d, want %d", len(secret.Data), len(expectedSecretData))
				}
			})
		})
	}
}

func TestActuator_DualZoneBucket_Reconcile(t *testing.T) {
	// Define common objects used across test cases
	baseBackupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: backupBucketName,
		},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			Region: "us-west1",
			SecretRef: corev1.SecretReference{
				Name:      credentialSecretName,
				Namespace: testProject,
			},
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(&gdc.BackupBucketConfig{
						TypeMeta: metav1.TypeMeta{
							Kind:       "BackupBucketConfig",
							APIVersion: gdc.SchemeGroupVersion.String(),
						},
						DualZoneBucketLocation: "syncz1z2",
					}),
				},
			},
		},
	}

	credentialSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      credentialSecretName,
			Namespace: testProject,
		},
		Data: map[string][]byte{
			"serviceaccount.json": []byte(fmt.Sprintf(`{ "Name": "%s", "project":"%s"}`, serviceAccountName, testProject)),
			"gdch-config":         []byte(`{"caData": "ZmFrZS1jYWRhdGE=", "isLancer": true, "orgClusterURL": "https://global-api.gdc1.us-west6-a.staging.gpcdemolabs.com"}`),
		},
	}

	accessKeySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      credentialSecretName,
			Namespace: storage.ObjectStorageAccessKeyNamespace,
			Labels: map[string]string{
				objectv1.SubjectTypeLabel: "User",
			},
			Annotations: map[string]string{
				objectv1.SubjectAnnotation: fmt.Sprintf("system:serviceaccount:%s:%s", testProject, serviceAccountName),
			},
		},
		Data: map[string][]byte{
			"access-key-id":     []byte(testAccessKeyID),
			"secret-access-key": []byte(testSecretAccessKey),
		},
	}
	testNamespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: testProject,
		},
	}
	testCases := []struct {
		name                               string
		initialGardenObjects               []client.Object
		initialMgmtObjects                 []client.Object
		newBucket                          bool
		expectErr                          bool
		expectErrContains                  string
		expectedRequestChecksumCalculation string
		expectedResponseChecksumValidation string
	}{
		{
			name: "should succeed and create bucket if it does not exist",
			initialGardenObjects: []client.Object{
				baseBackupBucket.DeepCopy(),
				credentialSecret.DeepCopy(),
			},
			initialMgmtObjects: []client.Object{
				// No initial bucket, it will be created by the Reconcile function
				testNamespace.DeepCopy(),
				accessKeySecret.DeepCopy(),
			},
			newBucket: true,
			expectErr: false,
		},
		{
			name: "should succeed if bucket already exists",
			initialGardenObjects: []client.Object{
				baseBackupBucket.DeepCopy(),
				credentialSecret.DeepCopy(),
			},
			initialMgmtObjects: []client.Object{
				// Pre-populate the org client with the bucket
				testNamespace.DeepCopy(),
				newDualZoneBucket(backupBucketName, true), // isReady = true
				accessKeySecret.DeepCopy(),
			},
			newBucket: false,
			expectErr: false,
		},
		{
			name: "should return retryable error if bucket is not ready",
			initialGardenObjects: []client.Object{
				baseBackupBucket.DeepCopy(),
				credentialSecret.DeepCopy(),
			},
			initialMgmtObjects: []client.Object{
				testNamespace.DeepCopy(),
				newDualZoneBucket(backupBucketName, false), // isReady = false
				accessKeySecret.DeepCopy(),
			},
			newBucket:         false,
			expectErr:         true,
			expectErrContains: "RetryableError: failed to get bucket",
		},
		{
			name: "should fail if credential secret is missing",
			initialGardenObjects: []client.Object{
				baseBackupBucket.DeepCopy(),
				// credentialSecret is missing
			},
			initialMgmtObjects: []client.Object{
				testNamespace.DeepCopy(),
				accessKeySecret.DeepCopy(),
			},
			newBucket:         false,
			expectErr:         true,
			expectErrContains: "failed to get service account",
		},

		{
			name: "should succeed and use custom checksum values if provided",
			initialGardenObjects: []client.Object{
				func() *extensionsv1alpha1.BackupBucket {
					bb := baseBackupBucket.DeepCopy()
					bb.Spec.ProviderConfig = &runtime.RawExtension{
						Raw: encode(&gdc.BackupBucketConfig{
							TypeMeta: metav1.TypeMeta{
								Kind:       "BackupBucketConfig",
								APIVersion: gdc.SchemeGroupVersion.String(),
							},
							DualZoneBucketLocation:     "syncz1z2",
							RequestChecksumCalculation: gdc.ChecksumWhenSupported,
							ResponseChecksumValidation: gdc.ChecksumWhenSupported,
						}),
					}
					return bb
				}(),
				credentialSecret.DeepCopy(),
			},
			initialMgmtObjects: []client.Object{
				testNamespace.DeepCopy(),
				accessKeySecret.DeepCopy(),
			},
			newBucket:                          true,
			expectErr:                          false,
			expectedRequestChecksumCalculation: gdc.ChecksumWhenSupported,
			expectedResponseChecksumValidation: gdc.ChecksumWhenSupported,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create fake kubernetes client for controller
			testClient := newTestClient(tc.initialGardenObjects...)
			// Create fake kuberentes client for org api of gdch
			orgClient := newTestClient(tc.initialMgmtObjects...)

			// Create an instance of our mock factory
			mockFactory := &mockClientFactory{
				// Implement the mock functions for this test setup
				mockGetOrgClientFn: func(_ *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.Client, error) {
					// If the bucket does NOT exist in the initial setup, it means this test case
					// is testing the creation path. In this scenario, we return a client that
					// is pre-programmed with the final, "Ready" state of the bucket.
					if tc.newBucket {
						// We add the final, expected object to the initial list.
						// This simulates that the bucket was created AND became ready.
						finalMgmtObjects := append(tc.initialMgmtObjects, newDualZoneBucket(backupBucketName, true))
						return newTestClient(finalMgmtObjects...), nil
					}
					// The mock function returns the fake org client we prepared.
					return orgClient, nil
				},
			}

			a := &actuator{
				client:        testClient,
				decoder:       serializer.NewCodecFactory(getRuntimeScheme(), serializer.EnableStrict).UniversalDecoder(),
				clientFactory: mockFactory,
			}

			// Call the Reconcile function
			err := a.Reconcile(context.Background(), logr.Logger{}, baseBackupBucket.DeepCopy())

			// Check for errors
			if tc.expectErr {
				if err == nil {
					t.Fatal("Reconcile() expected error, but got none")
				}
				if !strings.Contains(err.Error(), tc.expectErrContains) {
					t.Errorf("Reconcile() got error = %q, want error containing %q", err.Error(), tc.expectErrContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("Reconcile() expected no error but got %v", err)
			}

			ctx := context.Background()
			updatedBackupBucket := &extensionsv1alpha1.BackupBucket{}
			if err := testClient.Get(ctx, client.ObjectKey{Name: backupBucketName}, updatedBackupBucket); err != nil {
				t.Fatalf("failed to fetch updated BackupBucket: %v", err)
			}

			t.Run("should update BackupBucket annotation", func(t *testing.T) {
				gotFQN, ok := updatedBackupBucket.Annotations[gdcconstants.FullyQualifiedBucketNameAnnotationKey]
				if !ok {
					t.Errorf("expected annotation %q to be set", gdcconstants.FullyQualifiedBucketNameAnnotationKey)
				}
				if gotFQN != fullyQualifiedBucketName {
					t.Errorf("expected annotation value %q, got %q", fullyQualifiedBucketName, gotFQN)
				}
			})

			t.Run("should update BackupBucket status", func(t *testing.T) {
				expectedStatus := extensionsv1alpha1.BackupBucketStatus{
					GeneratedSecretRef: &corev1.SecretReference{
						Name:      getGeneratedSecretName(backupBucketName),
						Namespace: generatedSecretNamespace,
					},
				}
				// Note: Fake client doesn't automatically populate providerStatus, so we only check the secret ref.
				if diff := cmp.Diff(expectedStatus.GeneratedSecretRef, updatedBackupBucket.Status.GeneratedSecretRef); diff != "" {
					t.Errorf("unexpected status.generatedSecretRef (-want +got):\n%s", diff)
				}
			})

			t.Run("should create the generated secret with correct data", func(t *testing.T) {
				secretName := getGeneratedSecretName(backupBucketName)
				secret := &corev1.Secret{}
				if err := testClient.Get(ctx, client.ObjectKey{Name: secretName, Namespace: generatedSecretNamespace}, secret); err != nil {
					t.Fatalf("failed to fetch generated Secret: %v", err)
				}

				expectedBucketStatus := newBucket(backupBucketName, true).Status
				serviceaccountData := credentialSecret.Data["serviceaccount.json"]
				gdchConfigData := credentialSecret.Data["gdch-config"]

				reqChecksum := tc.expectedRequestChecksumCalculation
				if reqChecksum == "" {
					reqChecksum = gdc.ChecksumWhenRequired
				}
				respChecksum := tc.expectedResponseChecksumValidation
				if respChecksum == "" {
					respChecksum = gdc.ChecksumWhenRequired
				}

				expectedSecretData := map[string][]byte{
					"backup_secret.json": mustMarshalJSON(t, map[string]interface{}{
						"accessKeyID":                testAccessKeyID,
						"secretAccessKey":            testSecretAccessKey,
						"endpoint":                   expectedBucketStatus.Endpoint,
						"region":                     expectedBucketStatus.Region,
						"s3ForcePathStyle":           true,
						"insecureSkipVerify":         false,
						"requestChecksumCalculation": reqChecksum,
						"responseChecksumValidation": respChecksum,
						"trustedCaCert":              "fake-cadata",
					}),
					"accessKeyID":                []byte(testAccessKeyID),
					"secretAccessKey":            []byte(testSecretAccessKey),
					"endpoint":                   []byte(expectedBucketStatus.Endpoint),
					"region":                     []byte(expectedBucketStatus.Region),
					"requestChecksumCalculation": []byte(reqChecksum),
					"responseChecksumValidation": []byte(respChecksum),
					"trustedCaCert":              []byte("fake-cadata"),
					"insecureSkipVerify":         []byte("false"),
					"s3ForcePathStyle":           []byte("true"),
					"serviceaccount.json":        serviceaccountData,
					"gdch-config":                gdchConfigData,
				}
				// Compare the flat keys from the actual secret against our expectations
				for key, expectedVal := range expectedSecretData {
					gotVal, ok := secret.Data[key]
					if !ok {
						t.Errorf("secret.Data is missing flat key %q", key)
						continue
					}

					// Use a helper to compare JSON strings if they are JSON, otherwise compare as strings
					if json.Valid(expectedVal) && json.Valid(gotVal) {
						var expectedJSON, gotJSON interface{}
						if diff := cmp.Diff(expectedJSON, gotJSON); diff != "" {
							t.Errorf("unexpected data for flat JSON key %q (-want +got):\n%s", key, diff)
						}
					} else {
						if diff := cmp.Diff(string(expectedVal), string(gotVal)); diff != "" {
							t.Errorf("unexpected data for flat key %q (-want +got):\n%s", key, diff)
						}
					}
				}
				// Also check that there are no extra keys in the secret
				if len(secret.Data) != len(expectedSecretData) { // +1 for backup_secret.json
					t.Errorf("secret.Data has an unexpected number of keys. got %d, want %d", len(secret.Data), len(expectedSecretData))
				}
			})
		})
	}
}

func TestActuator_ZonalBucket_Delete(t *testing.T) {
	bucket := newBucket(backupBucketName, true)
	credentialSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName, Namespace: testProject},
		Data: map[string][]byte{
			"serviceaccount.json": []byte(fmt.Sprintf(`{ "Name": "%s", "project":"%s"}`, serviceAccountName, testProject)),
			"gdch-config":         []byte(`{"caData": "ZmFrZS1jYWRhdGE=", "orgClusterURL": "https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com"}`),
		},
	}
	backupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{Name: backupBucketName},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			SecretRef: corev1.SecretReference{Name: credentialSecretName, Namespace: testProject},
		},
	}

	t.Run("should succeed when bucket and secrets exist", func(t *testing.T) {
		secretName := getGeneratedSecretName(backupBucketName)
		generatedSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: generatedSecretNamespace},
		}
		testClient := newTestClient(backupBucket, credentialSecret, generatedSecret)
		orgClient := newTestClient(bucket)

		// Create an instance of our mock factory
		mockFactory := &mockClientFactory{
			// Implement the mock functions for this test setup
			mockGetOrgClientFn: func(_ *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.Client, error) {
				return orgClient, nil
			},
		}
		a := &actuator{
			client:        testClient,
			clientFactory: mockFactory,
			decoder:       serializer.NewCodecFactory(getRuntimeScheme(), serializer.EnableStrict).UniversalDecoder(),
		}

		err := a.Delete(context.TODO(), logr.Logger{}, backupBucket)
		if err != nil {
			t.Errorf("Delete() expected no error but got = %v", err)
		}

		// Verify the bucket was deleted from the org client
		err = orgClient.Get(context.TODO(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, &objectv1.Bucket{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected bucket to be deleted from org cluster, but it was not. err: %v", err)
		}

		// Verify the secret was deleted from the seed cluster
		err = testClient.Get(context.TODO(), client.ObjectKey{Name: secretName, Namespace: generatedSecretNamespace}, &corev1.Secret{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected generated secret to be deleted from seed cluster, but it was not. err: %v", err)
		}
	})
}

func TestActuator_DualZoneBucket_Delete(t *testing.T) {
	bucket := newDualZoneBucket(backupBucketName, true)
	credentialSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName, Namespace: testProject},
		Data: map[string][]byte{
			"serviceaccount.json": []byte(fmt.Sprintf(`{ "Name": "%s", "project":"%s"}`, serviceAccountName, testProject)),
			"gdch-config":         []byte(`{"caData": "ZmFrZS1jYWRhdGE=", "isLancer": true, "orgClusterURL": "https://global-api.gdc1.us-west6-a.staging.gpcdemolabs.com"}`),
		},
	}
	backupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{Name: backupBucketName},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			SecretRef: corev1.SecretReference{Name: credentialSecretName, Namespace: testProject},
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(&gdc.BackupBucketConfig{
						TypeMeta: metav1.TypeMeta{
							Kind:       "BackupBucketConfig",
							APIVersion: gdc.SchemeGroupVersion.String(),
						},
						DualZoneBucketLocation: "syncz1z2",
					}),
				},
			},
		},
	}

	t.Run("should succeed when bucket and secrets exist", func(t *testing.T) {
		secretName := getGeneratedSecretName(backupBucketName)
		generatedSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: generatedSecretNamespace},
		}
		testClient := newTestClient(backupBucket, credentialSecret, generatedSecret)
		orgClient := newTestClient(bucket)

		// Create an instance of our mock factory
		mockFactory := &mockClientFactory{
			// Implement the mock functions for this test setup
			mockGetOrgClientFn: func(_ *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.Client, error) {
				return orgClient, nil
			},
		}
		a := &actuator{
			client:        testClient,
			clientFactory: mockFactory,
			decoder:       serializer.NewCodecFactory(getRuntimeScheme(), serializer.EnableStrict).UniversalDecoder(),
		}

		err := a.Delete(context.TODO(), logr.Logger{}, backupBucket)
		if err != nil {
			t.Errorf("Delete() expected no error but got = %v", err)
		}

		// Verify the bucket was deleted from the org client
		err = orgClient.Get(context.TODO(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, &globalv1.Bucket{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected bucket to be deleted from org cluster, but it was not. err: %v", err)
		}

		// Verify the secret was deleted from the seed cluster
		err = testClient.Get(context.TODO(), client.ObjectKey{Name: secretName, Namespace: generatedSecretNamespace}, &corev1.Secret{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected generated secret to be deleted from seed cluster, but it was not. err: %v", err)
		}
	})
}

func TestCreateBucket_RetentionDays(t *testing.T) {
	decoder := serializer.NewCodecFactory(getRuntimeScheme(), serializer.EnableStrict).UniversalDecoder()
	sa := &auth.ServiceAccount{Name: serviceAccountName, Project: testProject}

	testCases := []struct {
		name               string
		retentionDays      *int32
		expectedPolicyDays *int32
		expectNilPolicy    bool
		expectErr          bool
	}{
		{
			name:               "defaults to 1 day when unset",
			retentionDays:      nil,
			expectedPolicyDays: ptr.To(int32(1)),
		},
		{
			name:               "uses configured positive value",
			retentionDays:      ptr.To(int32(7)),
			expectedPolicyDays: ptr.To(int32(7)),
		},
		{
			name:            "omits locking policy when set to 0",
			retentionDays:   ptr.To(int32(0)),
			expectNilPolicy: true,
		},
		{
			name:          "returns error when negative",
			retentionDays: ptr.To(int32(-1)),
			expectErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run("zonal_"+tc.name, func(t *testing.T) {
			orgClient := newTestClient()
			var providerConfig *runtime.RawExtension
			if tc.retentionDays != nil {
				providerConfig = &runtime.RawExtension{
					Raw: encode(&gdc.BackupBucketConfig{
						TypeMeta: metav1.TypeMeta{
							Kind:       "BackupBucketConfig",
							APIVersion: gdc.SchemeGroupVersion.String(),
						},
						DefaultObjectRetentionDays: tc.retentionDays,
					}),
				}
			}

			err := createZonalBucketIfNotExists(context.Background(), decoder, backupBucketName, providerConfig, orgClient, sa)
			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := &objectv1.Bucket{}
			if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, got); err != nil {
				t.Fatalf("failed to get created zonal bucket: %v", err)
			}
			if tc.expectNilPolicy {
				if got.Spec.BucketPolicy != nil {
					t.Errorf("expected nil BucketPolicy, got %+v", got.Spec.BucketPolicy)
				}
			} else {
				if got.Spec.BucketPolicy == nil || got.Spec.BucketPolicy.LockingPolicy == nil {
					t.Fatalf("expected non-nil BucketPolicy.LockingPolicy, got %+v", got.Spec.BucketPolicy)
				}
				if diff := cmp.Diff(tc.expectedPolicyDays, got.Spec.BucketPolicy.LockingPolicy.DefaultObjectRetentionDays); diff != "" {
					t.Errorf("unexpected DefaultObjectRetentionDays (-want +got):\n%s", diff)
				}
				expectedLifecycle := &objectv1.LifecyclePolicy{
					Enable: true,
					LifecycleRules: []*objectv1.LifecycleRule{
						{
							ID:     ptr.To(backupLifecycleRuleID),
							Status: ptr.To(objectv1.Enabled),
							NoncurrentExpiration: &objectv1.LifecycleNoncurrentExpiration{
								NoncurrentDays: ptr.To(int64(*tc.expectedPolicyDays)),
							},
							ExpiredObjectDeleteMarker: ptr.To(true),
						},
					},
				}
				if diff := cmp.Diff(expectedLifecycle, got.Spec.BucketPolicy.LifecyclePolicy); diff != "" {
					t.Errorf("unexpected LifecyclePolicy (-want +got):\n%s", diff)
				}
			}
		})

		t.Run("dualzone_"+tc.name, func(t *testing.T) {
			orgClient := newTestClient()
			providerConfig := &runtime.RawExtension{
				Raw: encode(&gdc.BackupBucketConfig{
					TypeMeta: metav1.TypeMeta{
						Kind:       "BackupBucketConfig",
						APIVersion: gdc.SchemeGroupVersion.String(),
					},
					DualZoneBucketLocation:     "syncz1z2",
					DefaultObjectRetentionDays: tc.retentionDays,
				}),
			}

			err := createDualZoneBucketIfNotExists(context.Background(), decoder, backupBucketName, providerConfig, orgClient, sa)
			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := &globalv1.Bucket{}
			if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, got); err != nil {
				t.Fatalf("failed to get created dual-zone bucket: %v", err)
			}
			if tc.expectNilPolicy {
				if got.Spec.BucketPolicy != nil {
					t.Errorf("expected nil BucketPolicy, got %+v", got.Spec.BucketPolicy)
				}
			} else {
				if got.Spec.BucketPolicy == nil || got.Spec.BucketPolicy.LockingPolicy == nil {
					t.Fatalf("expected non-nil BucketPolicy.LockingPolicy, got %+v", got.Spec.BucketPolicy)
				}
				if diff := cmp.Diff(tc.expectedPolicyDays, got.Spec.BucketPolicy.LockingPolicy.DefaultObjectRetentionDays); diff != "" {
					t.Errorf("unexpected DefaultObjectRetentionDays (-want +got):\n%s", diff)
				}
				expectedLifecycle := &objectv1.LifecyclePolicy{
					Enable: true,
					LifecycleRules: []*objectv1.LifecycleRule{
						{
							ID:     ptr.To(backupLifecycleRuleID),
							Status: ptr.To(objectv1.Enabled),
							NoncurrentExpiration: &objectv1.LifecycleNoncurrentExpiration{
								NoncurrentDays: ptr.To(int64(*tc.expectedPolicyDays)),
							},
							ExpiredObjectDeleteMarker: ptr.To(true),
						},
					},
				}
				if diff := cmp.Diff(expectedLifecycle, got.Spec.BucketPolicy.LifecyclePolicy); diff != "" {
					t.Errorf("unexpected LifecyclePolicy (-want +got):\n%s", diff)
				}
			}
		})
	}

	t.Run("dualzone_returns_error_when_providerConfig_is_nil", func(t *testing.T) {
		orgClient := newTestClient()
		if err := createDualZoneBucketIfNotExists(context.Background(), decoder, backupBucketName, nil, orgClient, sa); err == nil {
			t.Fatal("expected error when providerConfig is nil for dual-zone bucket, got nil")
		}
	})
}

func mustMarshalJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal JSON: %v", err)
	}
	return data
}

func newBucket(name string, isReady bool) *objectv1.Bucket {
	status := metav1.ConditionFalse
	if isReady {
		status = metav1.ConditionTrue
	}
	return &objectv1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testProject,
		},
		Status: objectv1.BucketStatus{
			Endpoint:           "s3EndPoint",
			Region:             "zone1",
			FullyQualifiedName: fullyQualifiedBucketName,
			Conditions: []metav1.Condition{
				{
					Type:               objectv1.BucketReady,
					Status:             status,
					LastTransitionTime: metav1.Now(),
					Reason:             "Test",
				},
			},
		},
	}
}

func newDualZoneBucket(name string, isReady bool) *globalv1.Bucket {
	status := metav1.ConditionFalse
	if isReady {
		status = metav1.ConditionTrue
	}
	return &globalv1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testProject,
		},
		Status: globalv1.BucketStatus{
			GlobalEndpoint:     "s3EndPoint",
			Region:             "zone1",
			FullyQualifiedName: fullyQualifiedBucketName,
			Conditions: []metav1.Condition{
				{
					Type:               objectv1.BucketReady,
					Status:             status,
					LastTransitionTime: metav1.Now(),
					Reason:             "Test",
				},
			},
		},
	}
}

// newTestClient consolidates the creation of a fake client with the necessary schemes.
func newTestClient(initObjs ...client.Object) client.Client {
	scheme := getRuntimeScheme()
	ssrModifier := func(ssr *authorizationv1.SelfSubjectRulesReview) {
		ssr.Status.ResourceRules = []authorizationv1.ResourceRule{
			{
				Verbs:         []string{"get"},
				APIGroups:     []string{""},
				Resources:     []string{"secrets"},
				ResourceNames: []string{credentialSecretName},
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
		WithStatusSubresource(&extensionsv1alpha1.BackupBucket{}, &objectv1.Bucket{}).
		Build()
}

func getRuntimeScheme() *runtime.Scheme {
	// Use a real scheme to encode/decode the object correctly
	scheme := runtime.NewScheme()
	utilruntime.Must(gdc.AddToScheme(scheme))
	utilruntime.Must(extensionsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(objectv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(globalv1.AddToScheme(scheme))
	utilruntime.Must(authorizationv1.AddToScheme(scheme))
	return scheme
}

func encode(obj runtime.Object) []byte {
	data, _ := json.Marshal(obj)
	return data
}

func TestGetLockingPolicy(t *testing.T) {
	tests := []struct {
		name      string
		config    *gdc.BackupBucketConfig
		wantDays  *int32
		wantNil   bool
		wantError bool
	}{
		{
			name:     "nil config defaults to 1 day",
			config:   nil,
			wantDays: ptr.To(int32(1)),
		},
		{
			name:     "unset DefaultObjectRetentionDays defaults to 1 day",
			config:   &gdc.BackupBucketConfig{},
			wantDays: ptr.To(int32(1)),
		},
		{
			name:    "DefaultObjectRetentionDays set to 0 disables locking policy",
			config:  &gdc.BackupBucketConfig{DefaultObjectRetentionDays: ptr.To(int32(0))},
			wantNil: true,
		},
		{
			name:     "DefaultObjectRetentionDays set to 7",
			config:   &gdc.BackupBucketConfig{DefaultObjectRetentionDays: ptr.To(int32(7))},
			wantDays: ptr.To(int32(7)),
		},
		{
			name:     "DefaultObjectRetentionDays set to maximum 36500",
			config:   &gdc.BackupBucketConfig{DefaultObjectRetentionDays: ptr.To(int32(36500))},
			wantDays: ptr.To(int32(36500)),
		},
		{
			name:      "DefaultObjectRetentionDays negative returns error",
			config:    &gdc.BackupBucketConfig{DefaultObjectRetentionDays: ptr.To(int32(-1))},
			wantError: true,
		},
		{
			name:      "DefaultObjectRetentionDays exceeding 36500 returns error",
			config:    &gdc.BackupBucketConfig{DefaultObjectRetentionDays: ptr.To(int32(36501))},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getLockingPolicy(tt.config)
			if (err != nil) != tt.wantError {
				t.Fatalf("getLockingPolicy() error = %v, wantError %v", err, tt.wantError)
			}
			if tt.wantError {
				return
			}
			if tt.wantNil {
				if got != nil {
					t.Fatalf("getLockingPolicy() = %+v, want nil", got)
				}
				return
			}
			if got == nil || got.DefaultObjectRetentionDays == nil || *got.DefaultObjectRetentionDays != *tt.wantDays {
				t.Fatalf("getLockingPolicy() = %+v, want DefaultObjectRetentionDays=%d", got, *tt.wantDays)
			}
		})
	}
}

func TestGetLifecyclePolicy(t *testing.T) {
	tests := []struct {
		name          string
		lockingPolicy *objectv1.LockingPolicy
		want          *objectv1.LifecyclePolicy
	}{
		{
			name:          "nil locking policy returns nil",
			lockingPolicy: nil,
			want:          nil,
		},
		{
			name:          "nil DefaultObjectRetentionDays returns nil",
			lockingPolicy: &objectv1.LockingPolicy{},
			want:          nil,
		},
		{
			name:          "zero DefaultObjectRetentionDays returns nil",
			lockingPolicy: &objectv1.LockingPolicy{DefaultObjectRetentionDays: ptr.To(int32(0))},
			want:          nil,
		},
		{
			name:          "positive DefaultObjectRetentionDays configures noncurrent expiration and delete marker cleanup",
			lockingPolicy: &objectv1.LockingPolicy{DefaultObjectRetentionDays: ptr.To(int32(7))},
			want: &objectv1.LifecyclePolicy{
				Enable: true,
				LifecycleRules: []*objectv1.LifecycleRule{
					{
						ID:     ptr.To(backupLifecycleRuleID),
						Status: ptr.To(objectv1.Enabled),
						NoncurrentExpiration: &objectv1.LifecycleNoncurrentExpiration{
							NoncurrentDays: ptr.To(int64(7)),
						},
						ExpiredObjectDeleteMarker: ptr.To(true),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getLifecyclePolicy(tt.lockingPolicy)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("getLifecyclePolicy() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func newStrictDecodeInterceptor() interceptor.Funcs {
	rejectExpiredDeleteMarker := func(obj client.Object) error {
		var lc *objectv1.LifecyclePolicy
		switch b := obj.(type) {
		case *objectv1.Bucket:
			if b.Spec.BucketPolicy != nil {
				lc = b.Spec.BucketPolicy.LifecyclePolicy
			}
		case *globalv1.Bucket:
			if b.Spec.BucketPolicy != nil {
				lc = b.Spec.BucketPolicy.LifecyclePolicy
			}
		}
		if lc != nil {
			for _, r := range lc.LifecycleRules {
				if r != nil && r.ExpiredObjectDeleteMarker != nil {
					return apierrors.NewInvalid(
						objectv1.SchemeGroupVersion.WithKind("Bucket").GroupKind(),
						obj.GetName(),
						nil,
					)
				}
			}
		}
		return nil
	}
	wrapErr := func(err error) error {
		if err == nil {
			return nil
		}
		return fmt.Errorf(`%w: strict decoding error: unknown field "spec.bucketPolicy.lifecyclePolicy.lifecycleRules[0].expiredObjectDeleteMarker"`, err)
	}
	return interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if ssr, ok := obj.(*authorizationv1.SelfSubjectRulesReview); ok {
				ssr.Status.ResourceRules = []authorizationv1.ResourceRule{
					{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{credentialSecretName}},
				}
			}
			if err := rejectExpiredDeleteMarker(obj); err != nil {
				return wrapErr(err)
			}
			return cl.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := rejectExpiredDeleteMarker(obj); err != nil {
				return wrapErr(err)
			}
			return cl.Update(ctx, obj, opts...)
		},
	}
}

func TestLegacyGDC1162Compatibility_FallbackAndDeleteMarkerSweep(t *testing.T) {
	decoder := serializer.NewCodecFactory(getRuntimeScheme(), serializer.EnableStrict).UniversalDecoder()
	sa := &auth.ServiceAccount{Name: serviceAccountName, Project: testProject}

	t.Run("creates zonal and dual-zone buckets without expiredObjectDeleteMarker when GDC 1.16.2 CRD rejects field", func(t *testing.T) {
		orgClient := fake.NewClientBuilder().
			WithScheme(getRuntimeScheme()).
			WithInterceptorFuncs(newStrictDecodeInterceptor()).
			Build()

		if err := createZonalBucketIfNotExists(context.Background(), decoder, backupBucketName, nil, orgClient, sa); err != nil {
			t.Fatalf("createZonalBucketIfNotExists failed on legacy 1.16.2: %v", err)
		}
		zonalGot := &objectv1.Bucket{}
		if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, zonalGot); err != nil {
			t.Fatalf("failed to get zonal bucket: %v", err)
		}
		wantFallbackLC := &objectv1.LifecyclePolicy{
			Enable: true,
			LifecycleRules: []*objectv1.LifecycleRule{
				{
					ID:     ptr.To(backupLifecycleRuleID),
					Status: ptr.To(objectv1.Enabled),
					NoncurrentExpiration: &objectv1.LifecycleNoncurrentExpiration{
						NoncurrentDays: ptr.To(int64(1)),
					},
				},
			},
		}
		if diff := cmp.Diff(wantFallbackLC, zonalGot.Spec.BucketPolicy.LifecyclePolicy); diff != "" {
			t.Fatalf("zonal fallback LifecyclePolicy mismatch (-want +got):\n%s", diff)
		}

		dzConfig := &runtime.RawExtension{
			Raw: encode(&gdc.BackupBucketConfig{
				TypeMeta:               metav1.TypeMeta{Kind: "BackupBucketConfig", APIVersion: gdc.SchemeGroupVersion.String()},
				DualZoneBucketLocation: "syncz1z2",
			}),
		}
		if err := createDualZoneBucketIfNotExists(context.Background(), decoder, backupBucketName+"-dz", dzConfig, orgClient, sa); err != nil {
			t.Fatalf("createDualZoneBucketIfNotExists failed on legacy 1.16.2: %v", err)
		}
		dzGot := &globalv1.Bucket{}
		if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName + "-dz", Namespace: testProject}, dzGot); err != nil {
			t.Fatalf("failed to get dual-zone bucket: %v", err)
		}
		if diff := cmp.Diff(wantFallbackLC, dzGot.Spec.BucketPolicy.LifecyclePolicy); diff != "" {
			t.Fatalf("dual-zone fallback LifecyclePolicy mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("backfills missing lifecyclePolicy on pre-existing locked bucket without sweeping delete markers during reconcile", func(t *testing.T) {
		existingLockedBucket := newBucket(backupBucketName, true)
		existingLockedBucket.Spec.BucketPolicy = &objectv1.BucketPolicy{
			LockingPolicy: &objectv1.LockingPolicy{DefaultObjectRetentionDays: ptr.To(int32(1))},
		}
		accessKeySecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      credentialSecretName,
				Namespace: storage.ObjectStorageAccessKeyNamespace,
				Labels:    map[string]string{objectv1.SubjectTypeLabel: "User"},
				Annotations: map[string]string{
					objectv1.SubjectAnnotation: fmt.Sprintf("system:serviceaccount:%s:%s", testProject, serviceAccountName),
				},
			},
			Data: map[string][]byte{
				"access-key-id":     []byte(testAccessKeyID),
				"secret-access-key": []byte(testSecretAccessKey),
			},
		}
		credSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName, Namespace: testProject},
			Data: map[string][]byte{
				"serviceaccount.json": []byte(fmt.Sprintf(`{"Name":%q,"project":%q}`, serviceAccountName, testProject)),
				"gdch-config":         []byte(`{"caData":"ZmFrZS1jYWRhdGE=","orgClusterURL":"https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com"}`),
			},
		}
		bb := &extensionsv1alpha1.BackupBucket{
			ObjectMeta: metav1.ObjectMeta{Name: backupBucketName},
			Spec: extensionsv1alpha1.BackupBucketSpec{
				Region:    "us-west1",
				SecretRef: corev1.SecretReference{Name: credentialSecretName, Namespace: testProject},
			},
		}

		orgClient := fake.NewClientBuilder().
			WithScheme(getRuntimeScheme()).
			WithObjects(existingLockedBucket, accessKeySecret).
			WithInterceptorFuncs(newStrictDecodeInterceptor()).
			WithStatusSubresource(&objectv1.Bucket{}).
			Build()
		gardenClient := newTestClient(bb, credSecret)

		mockBucket := &s3.MockBucket{
			Objects: map[string]*s3.MockObject{
				"shoot-1/expired-snap": {
					Versions: []*s3.MockObjectVersion{
						{VersionID: ptr.To("dm-1"), IsDeleteMarker: true, IsLatest: true},
					},
				},
				"shoot-1/recoverable-snap": {
					Versions: []*s3.MockObjectVersion{
						{VersionID: ptr.To("dm-2"), IsDeleteMarker: true, IsLatest: true},
						{VersionID: ptr.To("v-1"), Data: []byte("backup"), IsLatest: false},
					},
				},
			},
		}

		sweepCalls := 0
		a := &actuator{
			client:  gardenClient,
			decoder: decoder,
			clientFactory: &mockClientFactory{
				mockGetOrgClientFn: func(_ *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.Client, error) {
					return orgClient, nil
				},
				mockNewS3ClientFn: func(_ *s3.Config) (s3.Client, error) {
					return s3.CreateMockS3Client(s3.MockS3ClientConfig{
						Buckets: map[string]*s3.MockBucket{fullyQualifiedBucketName: mockBucket},
						DeleteExpiredDeleteMarkersFunc: func(context.Context, string, s3.DeleteExpiredDeleteMarkersOptions) (*s3.DeleteExpiredDeleteMarkersResult, error) {
							sweepCalls++
							return &s3.DeleteExpiredDeleteMarkersResult{}, nil
						},
					}), nil
				},
			},
		}

		if err := a.Reconcile(context.Background(), logr.Logger{}, bb); err != nil {
			t.Fatalf("Reconcile() failed on legacy 1.16.2 existing bucket: %v", err)
		}

		updatedBucket := &objectv1.Bucket{}
		if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, updatedBucket); err != nil {
			t.Fatalf("failed to get updated bucket: %v", err)
		}
		if updatedBucket.Spec.BucketPolicy.LifecyclePolicy == nil || len(updatedBucket.Spec.BucketPolicy.LifecyclePolicy.LifecycleRules) != 1 {
			t.Fatalf("expected backfilled LifecyclePolicy on existing bucket, got %+v", updatedBucket.Spec.BucketPolicy.LifecyclePolicy)
		}
		if updatedBucket.Spec.BucketPolicy.LifecyclePolicy.LifecycleRules[0].ExpiredObjectDeleteMarker != nil {
			t.Fatalf("expected nil ExpiredObjectDeleteMarker on legacy 1.16.2, got %v", updatedBucket.Spec.BucketPolicy.LifecyclePolicy.LifecycleRules[0].ExpiredObjectDeleteMarker)
		}

		// Reconcile must not iterate over the bucket: orphaned delete markers are only swept on deletion.
		if sweepCalls != 0 {
			t.Errorf("expected no delete marker sweep during Reconcile, got %d call(s)", sweepCalls)
		}
		if _, exists := mockBucket.Objects["shoot-1/expired-snap"]; !exists {
			t.Errorf("expected delete marker key shoot-1/expired-snap to be left untouched during Reconcile")
		}
	})

	t.Run("sweeps expired delete markers in bounded, resumable passes before deleting the bucket", func(t *testing.T) {
		lockedBucket := newBucket(backupBucketName, true)
		lockedBucket.Spec.BucketPolicy = &objectv1.BucketPolicy{
			LockingPolicy: &objectv1.LockingPolicy{DefaultObjectRetentionDays: ptr.To(int32(1))},
			LifecyclePolicy: &objectv1.LifecyclePolicy{
				Enable: true,
				LifecycleRules: []*objectv1.LifecycleRule{{
					ID:                   ptr.To(backupLifecycleRuleID),
					Status:               ptr.To(objectv1.Enabled),
					NoncurrentExpiration: &objectv1.LifecycleNoncurrentExpiration{NoncurrentDays: ptr.To(int64(1))},
				}},
			},
		}
		accessKeySecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        credentialSecretName,
				Namespace:   storage.ObjectStorageAccessKeyNamespace,
				Labels:      map[string]string{objectv1.SubjectTypeLabel: "User"},
				Annotations: map[string]string{objectv1.SubjectAnnotation: fmt.Sprintf("system:serviceaccount:%s:%s", testProject, serviceAccountName)},
			},
			Data: map[string][]byte{"access-key-id": []byte(testAccessKeyID), "secret-access-key": []byte(testSecretAccessKey)},
		}
		credSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName, Namespace: testProject},
			Data: map[string][]byte{
				"serviceaccount.json": []byte(fmt.Sprintf(`{"Name":%q,"project":%q}`, serviceAccountName, testProject)),
				"gdch-config":         []byte(`{"caData":"ZmFrZS1jYWRhdGE=","orgClusterURL":"https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com"}`),
			},
		}
		bb := &extensionsv1alpha1.BackupBucket{
			ObjectMeta: metav1.ObjectMeta{Name: backupBucketName},
			Spec: extensionsv1alpha1.BackupBucketSpec{
				Region:    "us-west1",
				SecretRef: corev1.SecretReference{Name: credentialSecretName, Namespace: testProject},
			},
		}
		orgClient := newTestClient(lockedBucket, accessKeySecret)
		gardenClient := newTestClient(bb, credSecret)

		// 3 expired keys + 1 recoverable key; with one key per mock "page" and a budget of 2 pages per
		// deletion attempt, the sweep needs two attempts before the bucket may be deleted.
		mockBucket := &s3.MockBucket{Objects: map[string]*s3.MockObject{
			"shoot-1/expired-a":   {Versions: []*s3.MockObjectVersion{{VersionID: ptr.To("dm-a"), IsDeleteMarker: true, IsLatest: true}}},
			"shoot-1/expired-b":   {Versions: []*s3.MockObjectVersion{{VersionID: ptr.To("dm-b"), IsDeleteMarker: true, IsLatest: true}}},
			"shoot-1/recoverable": {Versions: []*s3.MockObjectVersion{{VersionID: ptr.To("dm-r"), IsDeleteMarker: true, IsLatest: true}, {VersionID: ptr.To("v-r"), Data: []byte("backup")}}},
			"shoot-2/expired-c":   {Versions: []*s3.MockObjectVersion{{VersionID: ptr.To("dm-c"), IsDeleteMarker: true, IsLatest: true}}},
		}}
		var sweepOpts []s3.DeleteExpiredDeleteMarkersOptions
		mock := s3.CreateMockS3Client(s3.MockS3ClientConfig{Buckets: map[string]*s3.MockBucket{fullyQualifiedBucketName: mockBucket}})
		a := &actuator{
			client:  gardenClient,
			decoder: decoder,
			clientFactory: &mockClientFactory{
				mockGetOrgClientFn: func(_ *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.Client, error) {
					return orgClient, nil
				},
				mockNewS3ClientFn: func(_ *s3.Config) (s3.Client, error) {
					return s3.CreateMockS3Client(s3.MockS3ClientConfig{
						DeleteExpiredDeleteMarkersFunc: func(ctx context.Context, fqn string, opts s3.DeleteExpiredDeleteMarkersOptions) (*s3.DeleteExpiredDeleteMarkersResult, error) {
							sweepOpts = append(sweepOpts, opts)
							opts.MaxPages = 2 // shrink the budget so the test exercises the resume path
							return mock.DeleteExpiredDeleteMarkers(ctx, fqn, opts)
						},
					}), nil
				},
			},
		}

		// First attempt: budget exhausted -> progress persisted, retryable error, bucket NOT deleted yet.
		err := a.Delete(context.Background(), logr.Logger{}, bb)
		if err == nil || !strings.Contains(err.Error(), "in progress") {
			t.Fatalf("expected in-progress error on first deletion attempt, got %v", err)
		}
		// Gardener's BackupBucket reconciler reports the cause of a RequeueAfterError in the status.
		if codes := v1beta1helper.ExtractErrorCodes(reconciler.ReconcileErrCauseOrErr(err)); !slices.Contains(codes, gardencorev1beta1.ErrorRetryableInfraDependencies) {
			t.Fatalf("expected ErrorRetryableInfraDependencies error code, got %v (%v)", codes, err)
		}
		var requeue *reconciler.RequeueAfterError
		if !errors.As(err, &requeue) || requeue.RequeueAfter != deleteMarkerSweepRequeueInterval {
			t.Fatalf("expected RequeueAfterError with %s, got %v", deleteMarkerSweepRequeueInterval, err)
		}
		if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, &objectv1.Bucket{}); err != nil {
			t.Fatalf("bucket must not be deleted while the sweep is in progress: %v", err)
		}
		if err := gardenClient.Get(context.Background(), client.ObjectKeyFromObject(bb), bb); err != nil {
			t.Fatal(err)
		}
		marker := bb.Annotations[deleteMarkerSweepKeyMarkerAnnotation]
		if marker == "" {
			t.Fatalf("expected resume key marker annotation after partial sweep, annotations=%v", bb.Annotations)
		}
		if _, exists := mockBucket.Objects["shoot-1/expired-a"]; exists {
			t.Errorf("expected shoot-1/expired-a to be swept in the first pass")
		}

		// Second attempt: resumes from the stored marker, completes, clears the annotation, deletes the bucket.
		if err := a.Delete(context.Background(), logr.Logger{}, bb); err != nil {
			t.Fatalf("second deletion attempt failed: %v", err)
		}
		if len(sweepOpts) != 2 || sweepOpts[0].KeyMarker != "" || sweepOpts[1].KeyMarker != marker || sweepOpts[1].MaxPages != maxDeleteMarkerSweepPagesPerDeletion {
			t.Fatalf("unexpected sweep options across attempts: %+v (marker %q)", sweepOpts, marker)
		}
		if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, &objectv1.Bucket{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected bucket to be deleted after the sweep completed, got %v", err)
		}
		if err := gardenClient.Get(context.Background(), client.ObjectKeyFromObject(bb), bb); err != nil {
			t.Fatal(err)
		}
		if _, ok := bb.Annotations[deleteMarkerSweepKeyMarkerAnnotation]; ok {
			t.Errorf("expected resume key marker annotation to be cleared after completion, annotations=%v", bb.Annotations)
		}
		for _, key := range []string{"shoot-1/expired-a", "shoot-1/expired-b", "shoot-2/expired-c"} {
			if _, exists := mockBucket.Objects[key]; exists {
				t.Errorf("expected expired delete marker key %s to be swept", key)
			}
		}
		if _, exists := mockBucket.Objects["shoot-1/recoverable"]; !exists {
			t.Errorf("expected recoverable key (with retained version) to be preserved")
		}
	})

	t.Run("persists partial sweep progress when the sweep hits its time budget", func(t *testing.T) {
		lockedBucket := newBucket(backupBucketName, true)
		lockedBucket.Spec.BucketPolicy = &objectv1.BucketPolicy{
			LockingPolicy: &objectv1.LockingPolicy{DefaultObjectRetentionDays: ptr.To(int32(1))},
		}
		accessKeySecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        credentialSecretName,
				Namespace:   storage.ObjectStorageAccessKeyNamespace,
				Labels:      map[string]string{objectv1.SubjectTypeLabel: "User"},
				Annotations: map[string]string{objectv1.SubjectAnnotation: fmt.Sprintf("system:serviceaccount:%s:%s", testProject, serviceAccountName)},
			},
			Data: map[string][]byte{"access-key-id": []byte(testAccessKeyID), "secret-access-key": []byte(testSecretAccessKey)},
		}
		credSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName, Namespace: testProject},
			Data: map[string][]byte{
				"serviceaccount.json": []byte(fmt.Sprintf(`{"Name":%q,"project":%q}`, serviceAccountName, testProject)),
				"gdch-config":         []byte(`{"caData":"ZmFrZS1jYWRhdGE=","orgClusterURL":"https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com"}`),
			},
		}
		bb := &extensionsv1alpha1.BackupBucket{
			ObjectMeta: metav1.ObjectMeta{Name: backupBucketName},
			Spec: extensionsv1alpha1.BackupBucketSpec{
				Region:    "us-west1",
				SecretRef: corev1.SecretReference{Name: credentialSecretName, Namespace: testProject},
			},
		}
		orgClient := newTestClient(lockedBucket, accessKeySecret)
		gardenClient := newTestClient(bb, credSecret)
		a := &actuator{
			client:  gardenClient,
			decoder: decoder,
			clientFactory: &mockClientFactory{
				mockGetOrgClientFn: func(_ *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.Client, error) {
					return orgClient, nil
				},
				mockNewS3ClientFn: func(_ *s3.Config) (s3.Client, error) {
					return s3.CreateMockS3Client(s3.MockS3ClientConfig{
						// Simulate a slow bucket: the sweep context expires after some progress was made.
						DeleteExpiredDeleteMarkersFunc: func(ctx context.Context, _ string, _ s3.DeleteExpiredDeleteMarkersOptions) (*s3.DeleteExpiredDeleteMarkersResult, error) {
							<-ctx.Done()
							return &s3.DeleteExpiredDeleteMarkersResult{DeletedMarkers: 7, ScannedPages: 3, NextKeyMarker: "shoot-9/last-flushed"}, ctx.Err()
						},
					}), nil
				},
			},
		}

		// Reconciliation deadline just above the reserve, so the sweep gets a tiny time budget.
		ctx, cancel := context.WithTimeout(context.Background(), deleteMarkerSweepReserve+200*time.Millisecond)
		defer cancel()
		err := a.Delete(ctx, logr.Logger{}, bb)
		var requeue *reconciler.RequeueAfterError
		if !errors.As(err, &requeue) {
			t.Fatalf("expected RequeueAfterError after a time-budgeted partial sweep, got %v", err)
		}
		if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, &objectv1.Bucket{}); err != nil {
			t.Fatalf("bucket must not be deleted while the sweep is in progress: %v", err)
		}
		if err := gardenClient.Get(context.Background(), client.ObjectKeyFromObject(bb), bb); err != nil {
			t.Fatal(err)
		}
		if got := bb.Annotations[deleteMarkerSweepKeyMarkerAnnotation]; got != "shoot-9/last-flushed" {
			t.Fatalf("expected persisted resume marker from the interrupted sweep, got %q", got)
		}

		// Follow-up attempt where the remaining deadline is already within the reserve (zero-progress timeout):
		// the existing resume annotation and the bucket must both be preserved and the deletion requeued.
		shortCtx, shortCancel := context.WithTimeout(context.Background(), deleteMarkerSweepReserve/2)
		defer shortCancel()
		err = a.Delete(shortCtx, logr.Logger{}, bb)
		if !errors.As(err, &requeue) {
			t.Fatalf("expected RequeueAfterError when time budget expires with zero new keys closed, got %v", err)
		}
		if err := orgClient.Get(context.Background(), client.ObjectKey{Name: backupBucketName, Namespace: testProject}, &objectv1.Bucket{}); err != nil {
			t.Fatalf("bucket must not be deleted on a zero-progress time-budget timeout: %v", err)
		}
		if err := gardenClient.Get(context.Background(), client.ObjectKeyFromObject(bb), bb); err != nil {
			t.Fatal(err)
		}
		if got := bb.Annotations[deleteMarkerSweepKeyMarkerAnnotation]; got != "shoot-9/last-flushed" {
			t.Fatalf("expected existing resume marker to be preserved across zero-progress timeout, got %q", got)
		}
	})
}
