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
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	objectv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/object/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/cert"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/s3"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/storage"
)

const (
	testProject          = "my-namespace"
	testBucketName       = "test-bucket"
	testBucketFQN        = "test-bucket-fqn"
	defaultS3Endpoint    = "gdch/s3"
	defaultRegion        = "org-1"
	serviceAccountName   = "sa"
	credentialSecretName = "object-storage-key-my-secret"
)

// mockClientFactory is a mock implementation of the clientFactory interface for testing.
type mockClientFactory struct {
	// Fields to hold the mock functions
	mockNewS3ClientFn  func(config *s3.Config) (s3.Client, error)
	mockGetOrgClientFn func(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error)
}

// NewS3Client calls the mock function.
func (m *mockClientFactory) NewS3Client(config *s3.Config) (s3.Client, error) {
	return m.mockNewS3ClientFn(config)
}

// GetOrgClient calls the mock function.
func (m *mockClientFactory) GetOrgClient(gdchConfig *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.Client, error) {
	return m.mockGetOrgClientFn(gdchConfig, serviceAccount, scheme)
}

func TestActuator_GetETCDSecretData(t *testing.T) {
	// The GetETCDSecretData function is a simple pass-through and doesn't use
	// any clients or mocks, so we don't need the complex setup from other tests.
	// We only need a simple actuator instance.
	a := &actuator{}

	// Define the input data we will pass to the function.
	backupSecretData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	// Call the function with dummy context/logger/backupentry as they are not used.
	gotbackupSecretData, err := a.GetETCDSecretData(context.Background(), logr.Logger{}, nil, backupSecretData)
	if err != nil {
		t.Fatalf("GetETCDSecretData() expected no error but got = %v", err)
	}
	if !reflect.DeepEqual(backupSecretData, gotbackupSecretData) {
		t.Errorf("Expected maps to be equal, but they are not. got = %v, want = %v", gotbackupSecretData, backupSecretData)
	}
}

func TestActuator_Delete(t *testing.T) {
	// Define common objects for all test cases
	baseBackupEntry := &extensionsv1alpha1.BackupEntry{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-shoot",
		},
		Spec: extensionsv1alpha1.BackupEntrySpec{
			BucketName: testBucketName,
			Region:     defaultRegion,
			SecretRef: corev1.SecretReference{
				Name:      credentialSecretName,
				Namespace: testProject,
			},
		},
	}

	fakeCACert := generateCACertPEM(t)
	credentialSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName, Namespace: testProject},
		Data: map[string][]byte{
			"serviceaccount.json": []byte(fmt.Sprintf(`{ "Name": "%s", "project":"%s"}`, serviceAccountName, testProject)),
			"gdch-config":         []byte(fmt.Sprintf(`{"orgClusterURL": "https://management-kube.apiserver.gdc1.us-west16-c.s.gpcdemolabs.com", "caData": %q}`, fakeCACert)),
		},
	}
	bucket := newBucket(testBucketName, defaultS3Endpoint, defaultRegion)
	accessKeySecret := newAccessKeySecret()

	testCases := []struct {
		name                  string
		backupEntry           *extensionsv1alpha1.BackupEntry
		initialGardenObjects  []client.Object
		initialMgmtObjects    []client.Object
		s3MockConfig          s3.MockS3ClientConfig
		expectErr             bool
		expectErrContains     string
		expectDeleteCallCount int32
	}{
		{
			name:        "should succeed and delete correct objects",
			backupEntry: baseBackupEntry,
			initialGardenObjects: []client.Object{
				credentialSecret,
			},
			initialMgmtObjects: []client.Object{
				bucket,
				accessKeySecret,
			},
			s3MockConfig: s3.MockS3ClientConfig{
				Buckets: map[string]*s3.MockBucket{
					testBucketFQN: {
						Objects: map[string]*s3.MockObject{
							// This object has the correct prefix and should be deleted
							"my-shoot/object1": {Versions: []*s3.MockObjectVersion{{Data: []byte("a")}}},
							// This one too
							"my-shoot/object2": {Versions: []*s3.MockObjectVersion{{Data: []byte("b")}}},
							// This one has a different prefix and should NOT be deleted
							"another-shoot/object3": {Versions: []*s3.MockObjectVersion{{Data: []byte("c")}}},
						},
					},
				},
			},
			expectErr:             false,
			expectDeleteCallCount: 2, // Expects exactly two calls to DeleteObject
		},
		{
			name:                 "should fail if S3 delete operation fails",
			backupEntry:          baseBackupEntry,
			initialGardenObjects: []client.Object{credentialSecret},
			initialMgmtObjects:   []client.Object{bucket, accessKeySecret},
			s3MockConfig: s3.MockS3ClientConfig{
				Buckets: map[string]*s3.MockBucket{
					testBucketFQN: {
						Objects: map[string]*s3.MockObject{"my-shoot/object1": {Versions: []*s3.MockObjectVersion{{Data: []byte("a")}}}},
					},
				},
				// Inject a function to simulate an error
				DeleteObjectFunc: func(input s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
					return nil, fmt.Errorf("mock S3 delete error")
				},
			},
			expectErr:             true,
			expectErrContains:     "mock S3 delete error",
			expectDeleteCallCount: 1,
		},
		{
			name:                 "should succeed if bucket is not found in org cluster",
			backupEntry:          baseBackupEntry,
			initialGardenObjects: []client.Object{credentialSecret},
			initialMgmtObjects: []client.Object{
				// Bucket is missing
				accessKeySecret,
			},
			expectErr: false,
		},
		{
			name:                 "should fail if credential secret is missing",
			backupEntry:          baseBackupEntry,
			initialGardenObjects: []client.Object{
				// Credential secret is missing
			},
			initialMgmtObjects: []client.Object{bucket, accessKeySecret},
			expectErr:          true,
			expectErrContains:  "failed to get service account",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var deleteCallCount int32
			s3MockConfig := tc.s3MockConfig
			originalDeleteFunc := s3MockConfig.DeleteObjectFunc

			// Now, verwrite the DeleteObjectFunc with a wrapper that
			// ALWAYS increments the counter and then calls the original function (if any).
			s3MockConfig.DeleteObjectFunc = func(input s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
				atomic.AddInt32(&deleteCallCount, 1)
				if originalDeleteFunc != nil {
					return originalDeleteFunc(input)
				}
				return &s3.DeleteObjectOutput{}, nil
			}

			testClient := newTestClient(tc.initialGardenObjects...)
			orgClient := newTestClient(tc.initialMgmtObjects...)

			mockFactory := &mockClientFactory{
				mockNewS3ClientFn: func(_ *s3.Config) (s3.Client, error) {
					return s3.CreateMockS3Client(s3MockConfig), nil
				},
				mockGetOrgClientFn: func(_ *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.Client, error) {
					return orgClient, nil
				},
			}

			a := &actuator{
				client:        testClient,
				decoder:       serializer.NewCodecFactory(runtime.NewScheme(), serializer.EnableStrict).UniversalDecoder(),
				clientFactory: mockFactory,
			}

			err := a.Delete(context.Background(), logr.Logger{}, tc.backupEntry)

			if tc.expectErr {
				if err == nil {
					t.Fatal("Delete() expected error but got none")
				}
				if !strings.Contains(err.Error(), tc.expectErrContains) {
					t.Errorf("Delete() got error = %q, want error containing %q", err.Error(), tc.expectErrContains)
				}
			} else {
				if err != nil {
					t.Fatalf("Delete() expected no error but got: %v", err)
				}
			}

			if atomic.LoadInt32(&deleteCallCount) != tc.expectDeleteCallCount {
				t.Errorf("expected S3 DeleteObject to be called %d time(s), but was called %d time(s)", tc.expectDeleteCallCount, deleteCallCount)
			}
		})
	}
}

// newTestClient creates a fake client with the necessary schemes.
func newTestClient(initObjs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(objectv1.AddToScheme(scheme))
	utilruntime.Must(extensionsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(authorizationv1.AddToScheme(scheme))

	var accessibleSecretNames []string
	for _, obj := range initObjs {
		if secret, ok := obj.(*corev1.Secret); ok {
			accessibleSecretNames = append(accessibleSecretNames, secret.Name)
		}
	}

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
		Build()
}

func newBucket(bucketName, s3EndPoint, region string) *objectv1.Bucket {
	return &objectv1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucketName,
			Namespace: testProject,
		},
		Status: objectv1.BucketStatus{
			Endpoint:           s3EndPoint,
			Region:             region,
			FullyQualifiedName: testBucketFQN,
			Conditions: []metav1.Condition{
				{
					Type:               objectv1.BucketReady,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					Reason:             "Test",
				},
			},
		},
	}
}

func newAccessKeySecret() *corev1.Secret {
	return &corev1.Secret{
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
			"access-key-id":     []byte("testAccessKeyID"),
			"secret-access-key": []byte("testSecretKeyAccess"),
		},
	}
}

// It returns only the PEM-encoded certificate string
func generateCACertPEM(t *testing.T) string {
	certPEM, _, err := cert.GenerateSelfSignedCertKey("My Test CA", nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}

	return base64.StdEncoding.EncodeToString(certPEM)
}
