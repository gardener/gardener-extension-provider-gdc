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

package utils

import (
	"context"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

func TestEitherContains(t *testing.T) {
	tests := []struct {
		name string
		s1   string
		s2   string
		want bool
	}{
		{
			name: "exact match",
			s1:   "test-bucket",
			s2:   "test-bucket",
			want: true,
		},
		{
			name: "s1 contains s2",
			s1:   "ahyq2qm-garden-eae90ba0",
			s2:   "garden-eae90ba0",
			want: true,
		},
		{
			name: "s2 contains s1",
			s1:   "garden-eae90ba0",
			s2:   "ahyq2qm-garden-eae90ba0",
			want: true,
		},
		{
			name: "no match",
			s1:   "bucket-a",
			s2:   "bucket-b",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eitherContains(tt.s1, tt.s2); got != tt.want {
				t.Errorf("eitherContains(%q, %q) = %v, want %v", tt.s1, tt.s2, got, tt.want)
			}
		})
	}
}

func TestEnsureETCDBackup_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = druidv1alpha1.AddToScheme(scheme)
	_ = extensionsv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	ctx := context.Background()

	backupBuckets := []runtime.Object{
		&extensionsv1alpha1.BackupBucket{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "garden-backup-bucket",
				Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-garden-bucket-name"},
			},
		},
		&extensionsv1alpha1.BackupBucket{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "shoot-backup-bucket",
				Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-shoot-backup-name"},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(backupBuckets...).Build()

	var provider druidv1alpha1.StorageProvider = "gdch"
	etcd := &druidv1alpha1.Etcd{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd-main",
			Namespace: "shoot--garden--test",
		},
		Spec: druidv1alpha1.EtcdSpec{
			Backup: druidv1alpha1.BackupSpec{
				Store: &druidv1alpha1.StoreSpec{
					Container: ptr.To("shoot-backup-bucket"),
					Provider:  &provider,
				},
			},
		},
	}

	err := EnsureETCDBackup(ctx, c, logr.Discard(), etcd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedContainer := "fq-shoot-backup-name"
	var expectedProvider druidv1alpha1.StorageProvider = "aws"
	if *etcd.Spec.Backup.Store.Container != expectedContainer {
		t.Errorf("container = %v, want %v", *etcd.Spec.Backup.Store.Container, expectedContainer)
	}
	if *etcd.Spec.Backup.Store.Provider != expectedProvider {
		t.Errorf("provider = %v, want %v", *etcd.Spec.Backup.Store.Provider, expectedProvider)
	}

	// Verify idempotency on update when Container is already mutated to the FQDN
	err = EnsureETCDBackup(ctx, c, logr.Discard(), etcd)
	if err != nil {
		t.Fatalf("unexpected error on update: %v", err)
	}
	if *etcd.Spec.Backup.Store.Container != expectedContainer {
		t.Errorf("container on update = %v, want %v", *etcd.Spec.Backup.Store.Container, expectedContainer)
	}
}
