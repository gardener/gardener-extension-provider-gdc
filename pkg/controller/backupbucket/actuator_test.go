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
	"fmt"
	"strings"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"

	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
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
}

// GetOrgClient calls the mock function.
func (m *mockClientFactory) GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error) {
	return m.mockGetOrgClientFn(gdchConfig, serviceAccount, scheme)
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
