// Copyright 2026 Google LLC
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

package validator

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gardener/gardener/pkg/apis/core"
	gardensecurity "github.com/gardener/gardener/pkg/apis/security"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"
	fakemanager "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

func TestBindingValidators(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	validSA, err := json.Marshal(map[string]string{
		"project":        "tenant-project",
		"name":           "account",
		"private_key_id": "key-id",
		"private_key":    "-----BEGIN PRIVATE KEY-----\nZmFrZQ==\n-----END PRIVATE KEY-----\n",
		"token_uri":      "https://sts.org.example.com/v1/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	invalidSA, err := json.Marshal(map[string]string{
		"project":        "tenant-project",
		"name":           "account",
		"private_key_id": "key-id",
		"private_key":    "-----BEGIN PRIVATE KEY-----\nZmFrZQ==\n-----END PRIVATE KEY-----\n",
		"token_uri":      "http://169.254.169.254/latest/meta-data",
	})
	if err != nil {
		t.Fatal(err)
	}

	validSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "valid-secret", Namespace: "garden-test"},
		Data:       map[string][]byte{gdcconstants.ServiceAccountJSONField: validSA},
	}
	invalidSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-secret", Namespace: "garden-test"},
		Data:       map[string][]byte{gdcconstants.ServiceAccountJSONField: invalidSA},
	}

	mgr := fakemanager.NewManager(fake.NewClientBuilder().WithScheme(scheme).WithObjects(validSecret, invalidSecret).Build())
	sbValidator := NewSecretBindingValidator(mgr)
	cbValidator := NewCredentialsBindingValidator(mgr)

	ctx := context.Background()

	t.Run("SecretBinding create validates unlabelled Secret", func(t *testing.T) {
		validSB := &core.SecretBinding{ //nolint:staticcheck // SA1019: SecretBinding remains supported alongside CredentialsBinding.
			SecretRef: corev1.SecretReference{Name: "valid-secret", Namespace: "garden-test"},
		}
		if err := sbValidator.Validate(ctx, validSB, nil); err != nil {
			t.Fatalf("expected valid SecretBinding to succeed, got: %v", err)
		}

		invalidSB := &core.SecretBinding{ //nolint:staticcheck // SA1019: SecretBinding remains supported alongside CredentialsBinding.
			SecretRef: corev1.SecretReference{Name: "invalid-secret", Namespace: "garden-test"},
		}
		if err := sbValidator.Validate(ctx, invalidSB, nil); err == nil {
			t.Fatal("expected invalid SecretBinding to be rejected")
		}

		// Update should not block existing bindings (e.g. finalizer removal).
		if err := sbValidator.Validate(ctx, invalidSB, invalidSB.DeepCopy()); err != nil {
			t.Fatalf("expected SecretBinding update to be allowed, got: %v", err)
		}
	})

	t.Run("CredentialsBinding create validates unlabelled Secret and skips non-Secret refs", func(t *testing.T) {
		validCB := &gardensecurity.CredentialsBinding{
			CredentialsRef: corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "Secret",
				Name:       "valid-secret",
				Namespace:  "garden-test",
			},
		}
		if err := cbValidator.Validate(ctx, validCB, nil); err != nil {
			t.Fatalf("expected valid CredentialsBinding to succeed, got: %v", err)
		}

		invalidCB := &gardensecurity.CredentialsBinding{
			CredentialsRef: corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "Secret",
				Name:       "invalid-secret",
				Namespace:  "garden-test",
			},
		}
		if err := cbValidator.Validate(ctx, invalidCB, nil); err == nil {
			t.Fatal("expected invalid CredentialsBinding to be rejected")
		}

		workloadIdentityCB := &gardensecurity.CredentialsBinding{
			CredentialsRef: corev1.ObjectReference{
				APIVersion: "security.gardener.cloud/v1alpha1",
				Kind:       "WorkloadIdentity",
				Name:       "wi-test",
				Namespace:  "garden-test",
			},
		}
		if err := cbValidator.Validate(ctx, workloadIdentityCB, nil); err != nil {
			t.Fatalf("expected WorkloadIdentity CredentialsBinding to pass, got: %v", err)
		}
	})
}
