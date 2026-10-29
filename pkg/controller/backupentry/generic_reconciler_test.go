// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package backupentry

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	gardenerbackupentry "github.com/gardener/gardener/extensions/pkg/controller/backupentry"
	"github.com/gardener/gardener/extensions/pkg/controller/backupentry/genericactuator"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/s3"
)

type testManager struct {
	manager.Manager
	client client.Client
}

func (m *testManager) GetClient() client.Client    { return m.client }
func (m *testManager) GetAPIReader() client.Reader { return m.client }

type testS3DeleteClient struct {
	s3.Client
	delete func(context.Context, string, string) error
}

func (c *testS3DeleteClient) DeleteObjectsWithPrefix(ctx context.Context, bucket, prefix string) error {
	return c.delete(ctx, bucket, prefix)
}

// Use the actual Gardener reconciler, generic actuator, and GDC delegate. A new
// reconciler after an API failure must retry an empty prefix and finish both
// finalizers, including when the previous Delete status was already Succeeded.
func TestGenericDeletionRetriesAfterSeedAPIFailure(t *testing.T) {
	for _, failure := range []string{"status", "secret finalizer", "entry finalizer", "context after S3"} {
		t.Run(failure, func(t *testing.T) {
			be := &extensionsv1alpha1.BackupEntry{
				ObjectMeta: metav1.ObjectMeta{Name: "shoot--test--foo--uid", UID: "entry-uid", Generation: 1,
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
					Finalizers:        []string{gardenerbackupentry.FinalizerName, "other-controller"}},
				Spec: extensionsv1alpha1.BackupEntrySpec{BucketName: testBucketName,
					SecretRef: corev1.SecretReference{Namespace: testProject, Name: credentialSecretName}},
			}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName, Namespace: testProject,
				Finalizers: []string{gardenerbackupentry.FinalizerName, "other-controller"}},
				Data: map[string][]byte{
					"serviceaccount.json": []byte(fmt.Sprintf("{\"Name\":%q,\"project\":%q}", serviceAccountName, testProject)),
					"gdch-config":         []byte(fmt.Sprintf("{\"orgClusterURL\":\"https://management-kube.org.example\",\"caData\":%q}", generateCACertPEM(t))),
				}}
			etcd := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: v1beta1constants.BackupSecretName, Namespace: "shoot--test--foo",
				Annotations: map[string]string{genericactuator.AnnotationKeyCreatedByBackupEntry: be.Name}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fail := true
			funcs := interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					if fail && ((failure == "secret finalizer" && obj.GetName() == secret.Name) ||
						(failure == "entry finalizer" && obj.GetName() == be.Name)) {
						return apierrors.NewConflict(schema.GroupResource{Resource: "objects"}, obj.GetName(), fmt.Errorf("injected conflict"))
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
				SubResourcePatch: func(ctx context.Context, c client.Client, name string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					entry := obj.(*extensionsv1alpha1.BackupEntry)
					if fail && failure == "status" && entry.Status.LastOperation.State == gardencorev1beta1.LastOperationStateSucceeded {
						return fmt.Errorf("client rate limiter Wait returned an error: context deadline exceeded")
					}
					return c.SubResource(name).Patch(ctx, obj, patch, opts...)
				},
			}
			c := fake.NewClientBuilder().WithScheme(newTestClient().Scheme()).WithObjects(be, secret, etcd).
				WithStatusSubresource(&extensionsv1alpha1.BackupEntry{}).WithInterceptorFuncs(funcs).Build()
			mgr := &testManager{client: c}
			bucket := &s3.MockBucket{Objects: map[string]*s3.MockObject{
				be.Name + "/object": {}, "other-shoot/object": {},
			}}
			storage := s3.CreateMockS3Client(s3.MockS3ClientConfig{Buckets: map[string]*s3.MockBucket{testBucketFQN: bucket}})
			deleteCalls, deletedObjects := 0, 0
			s3Client := &testS3DeleteClient{Client: storage, delete: func(ctx context.Context, bucketFQN, prefix string) error {
				deleteCalls++
				for key := range bucket.Objects {
					if strings.HasPrefix(key, prefix) {
						deletedObjects++
					}
				}
				if err := storage.DeleteObjectsWithPrefix(ctx, bucketFQN, prefix); err != nil {
					return err
				}
				if fail && failure == "context after S3" {
					cancel()
				}
				return nil
			}}
			orgClient := newTestClient(newBucket(testBucketName, defaultS3Endpoint, defaultRegion), newAccessKeySecret())
			delegate := &actuator{client: c, decoder: serializer.NewCodecFactory(runtime.NewScheme(), serializer.EnableStrict).UniversalDecoder(),
				clientFactory: &mockClientFactory{
					mockNewS3ClientFn: func(*s3.Config) (s3.Client, error) { return s3Client, nil },
					mockGetOrgClientFn: func(*gdcclient.OrgClusterConfig, *auth.ServiceAccount, *runtime.Scheme) (client.Client, error) {
						return orgClient, nil
					},
				}}
			request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(be)}
			r := gardenerbackupentry.NewReconciler(mgr, genericactuator.NewActuator(mgr, delegate))
			if _, err := r.Reconcile(ctx, request); err == nil {
				t.Fatal("expected injected API failure")
			}
			current := &extensionsv1alpha1.BackupEntry{}
			if err := c.Get(context.Background(), request.NamespacedName, current); err != nil {
				t.Fatal(err)
			}
			if !controllerutil.ContainsFinalizer(current, gardenerbackupentry.FinalizerName) {
				t.Fatal("entry finalizer removed despite API failure")
			}
			// Gardener's generic actuator deletes the etcd backup Secret before the
			// success status patch. Revisit this assertion if that ordering changes
			// when upgrading gardener/gardener.
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(etcd), &corev1.Secret{}); !apierrors.IsNotFound(err) {
				t.Fatalf("etcd Secret should already be deleted, got %v", err)
			}
			fail = false
			r = gardenerbackupentry.NewReconciler(mgr, genericactuator.NewActuator(mgr, delegate))
			if _, err := r.Reconcile(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if deleteCalls != 2 || deletedObjects != 1 || len(bucket.Objects) != 1 {
				t.Fatalf("storage calls=%d, deleted objects=%d, remaining=%d; want 2, 1, 1", deleteCalls, deletedObjects, len(bucket.Objects))
			}
			for _, obj := range []client.Object{&extensionsv1alpha1.BackupEntry{}, &corev1.Secret{}} {
				key := request.NamespacedName
				if _, ok := obj.(*corev1.Secret); ok {
					key = client.ObjectKeyFromObject(secret)
				}
				if err := c.Get(context.Background(), key, obj); err != nil {
					t.Fatal(err)
				}
				if controllerutil.ContainsFinalizer(obj, gardenerbackupentry.FinalizerName) || !controllerutil.ContainsFinalizer(obj, "other-controller") {
					t.Fatalf("unexpected finalizers: %v", obj.GetFinalizers())
				}
			}
		})
	}
}
