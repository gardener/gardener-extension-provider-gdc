// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backupprovider

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
)

func TestMutate_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = druidv1alpha1.AddToScheme(scheme)
	_ = extensionsv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	ctx := context.Background()

	tests := []struct {
		name          string
		newObj        *druidv1alpha1.Etcd
		oldObj        *druidv1alpha1.Etcd
		backupBuckets []client.Object
		expectedETCD  *druidv1alpha1.Etcd
	}{
		{
			name: "BackupBucket exists and bucket annotation is present; modify",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      virtualGardenEtcdMainName,
					Namespace: "garden",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: ptr.To("backup-bucket"),
							Provider:  pointerToProvider("gdch"),
						},
					},
				},
			},
			oldObj: nil,
			backupBuckets: []client.Object{
				&extensionsv1alpha1.BackupBucket{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "non-matching-backup-bucket",
						Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-backup-bucket-name"},
					},
				},
				&extensionsv1alpha1.BackupBucket{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "backup-bucket",
						Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-backup-bucket-name"},
					},
				},
			},
			expectedETCD: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      virtualGardenEtcdMainName,
					Namespace: "garden",
					Annotations: map[string]string{
						"backupprovider.gdc.gardener.cloud/mutated": "true",
					},
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: ptr.To("fq-backup-bucket-name"),
							Provider:  pointerToProvider("aws"),
						},
					},
				},
			},
		},
		{
			name: "Etcd is not virtual-garden-etcd-main; do not modify store",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "virtual-garden-etcd-events",
					Namespace: "garden",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: ptr.To("original-container"),
							Provider:  pointerToProvider("gcp"),
						},
					},
				},
			},
			oldObj:        nil,
			backupBuckets: nil,
			expectedETCD: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "virtual-garden-etcd-events",
					Namespace: "garden",
					Annotations: map[string]string{
						"backupprovider.gdc.gardener.cloud/mutated": "true",
					},
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: ptr.To("original-container"),
							Provider:  pointerToProvider("gcp"),
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ensurer, _, err := newTestMutator(tt.backupBuckets...)
			if err != nil {
				t.Fatalf("failed to create mutator: %v", err)
			}
			err = ensurer.Mutate(ctx, tt.newObj, tt.oldObj)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.expectedETCD, tt.newObj); diff != "" {
				t.Errorf("ETCD mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMutate_Fail(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = druidv1alpha1.AddToScheme(scheme)
	_ = extensionsv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	ctx := context.Background()

	tests := []struct {
		name          string
		newObj        *druidv1alpha1.Etcd
		oldObj        *druidv1alpha1.Etcd
		backupBucket  *extensionsv1alpha1.BackupBucket
		expectedError error
	}{
		{
			name: "BackupBucket annotation is missing",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      virtualGardenEtcdMainName,
					Namespace: "garden",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: ptr.To("backup-bucket"),
						},
					},
				},
			},
			oldObj: nil,
			backupBucket: &extensionsv1alpha1.BackupBucket{
				ObjectMeta: metav1.ObjectMeta{
					Name: "backup-bucket",
				},
			},
			expectedError: errors.New("fqBucketName annotation not found in BackupBucket backup-bucket"),
		},
		{
			name: "BackupBucket does not exist",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      virtualGardenEtcdMainName,
					Namespace: "garden",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: ptr.To("backup-bucket"),
							Provider:  pointerToProvider("gdch"),
						},
					},
				},
			},
			oldObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      virtualGardenEtcdMainName,
					Namespace: "garden",
				},
			},
			backupBucket:  nil,
			expectedError: errors.New("no backup bucket found for etcd garden/virtual-garden-etcd-main"),
		},
		{
			name: "No matching BackupBucket exist",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      virtualGardenEtcdMainName,
					Namespace: "garden",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: ptr.To("backup-bucket"),
							Provider:  pointerToProvider("gdch"),
						},
					},
				},
			},
			oldObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      virtualGardenEtcdMainName,
					Namespace: "garden",
				},
			},
			backupBucket: &extensionsv1alpha1.BackupBucket{
				ObjectMeta: metav1.ObjectMeta{
					Name: "non-matching-bucket",
				},
			},
			expectedError: errors.New("no matching backup bucket found for etcd garden/virtual-garden-etcd-main with container name backup-bucket"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []client.Object{}
			if tt.backupBucket != nil {
				objs = append(objs, tt.backupBucket)
			}

			ensurer, _, err := newTestMutator(objs...)
			if err != nil {
				t.Fatalf("failed to create mutator: %v", err)
			}
			err = ensurer.Mutate(ctx, tt.newObj, tt.oldObj)

			if err == nil || err.Error() != tt.expectedError.Error() {
				t.Fatalf("expected error: %v, got: %v", tt.expectedError, err)
			}
		})
	}
}

func pointerToProvider(p druidv1alpha1.StorageProvider) *druidv1alpha1.StorageProvider {
	return &p
}

// newTestMutator is a helper function to create a new mutator with a fake client
// and a pre-configured scheme for testing.
func newTestMutator(initObjs ...client.Object) (*mutator, client.Client, error) {
	// Create a new scheme
	s := runtime.NewScheme()

	// Add known types to the scheme
	if err := corev1.AddToScheme(s); err != nil {
		return nil, nil, err
	}
	if err := druidv1alpha1.AddToScheme(s); err != nil {
		return nil, nil, err
	}
	if err := extensionsv1alpha1.AddToScheme(s); err != nil {
		return nil, nil, err
	}

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(initObjs...).Build()
	m := &mutator{
		client: cl,
		logger: logr.Discard(), // Use a no-op logger for tests
	}
	return m, cl, nil
}
