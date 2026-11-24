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

package cmd

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gardener/gardener/pkg/apis/core/install"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/admission/validator"
	fakemanager "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

func TestSecretWebhookEnabledByDefault(t *testing.T) {
	scheme := runtime.NewScheme()
	install.Install(scheme)
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mgr := fakemanager.NewManager(fake.NewClientBuilder().WithScheme(scheme).Build())
	options := GardenWebhookSwitchOptions()
	if err := options.Complete(); err != nil {
		t.Fatal(err)
	}
	webhooks, err := options.Completed().WebhooksFactory(mgr)
	if err != nil {
		t.Fatal(err)
	}
	for _, webhook := range webhooks {
		if webhook.Name != validator.SecretsValidatorName {
			continue
		}
		if webhook.Path != "/webhooks/validate/secrets" || webhook.Target != "seed" || webhook.Action != "validating" {
			t.Fatalf("unexpected webhook configuration: %+v", webhook)
		}
		if webhook.ObjectSelector == nil || webhook.ObjectSelector.MatchLabels["provider.shoot.gardener.cloud/gdch"] != "true" {
			t.Fatalf("incorrect Secret selector: %v", webhook.ObjectSelector)
		}
		if len(webhook.Types) != 1 {
			t.Fatal("expected one Secret type")
		}
		if _, ok := webhook.Types[0].Obj.(*corev1.Secret); !ok {
			t.Fatal("expected core/v1 Secret")
		}
		// Verify that the registered handler rejects the payload, not just that the
		// factory advertises a Secret type.
		for _, project := range []string{"tenant-project", "project\n---\nkind: MachineClass\nmetadata:\n  name: injected"} {
			credentials, err := json.Marshal(map[string]string{"project": project, "name": "account", "private_key_id": "key-id", "private_key": "fake-key", "token_uri": "https://example.com/token"})
			if err != nil {
				t.Fatal(err)
			}
			obj := &corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, Data: map[string][]byte{"serviceaccount.json": credentials}}
			raw, err := json.Marshal(obj)
			if err != nil {
				t.Fatal(err)
			}
			response := webhook.Webhook.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Kind: metav1.GroupVersionKind{Version: "v1", Kind: "Secret"}, Operation: admissionv1.Create, Object: runtime.RawExtension{Raw: raw},
			}})
			if response.Allowed != (project == "tenant-project") {
				t.Fatalf("unexpected admission response for project %q: %+v", project, response)
			}
		}
		return
	}
	t.Fatal("Secret admission webhook is not enabled by default")
}
