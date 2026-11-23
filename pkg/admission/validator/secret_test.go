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

package validator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"
)

func TestSecretValidator(t *testing.T) {
	validFields := func() map[string]string {
		return map[string]string{
			"project": "tenant-project", "name": "account", "private_key_id": "key-id",
			"private_key": "-----BEGIN PRIVATE KEY-----\nZmFrZQ==\n-----END PRIVATE KEY-----\n",
			"token_uri":   "https://example.com/token",
		}
	}
	makeSecret := func(fields map[string]string) *corev1.Secret {
		data, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return &corev1.Secret{Data: map[string][]byte{gdcconstants.ServiceAccountJSONField: data}}
	}
	validator := NewSecretValidator()
	for _, operation := range []string{"create", "update", "unchanged update"} {
		t.Run(operation, func(t *testing.T) {
			run := func(credentials *corev1.Secret) error {
				var old client.Object
				if operation == "update" {
					old = makeSecret(validFields())
				}
				if operation == "unchanged update" {
					old = credentials.DeepCopy()
				}
				return validator.Validate(context.Background(), credentials, old)
			}
			if err := run(makeSecret(validFields())); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"project", "name", "private_key_id", "private_key", "token_uri"} {
				fields := validFields()
				delete(fields, name)
				if err := run(makeSecret(fields)); err == nil {
					t.Errorf("missing %s accepted", name)
				}
				for _, value := range []string{"a b", "a\nb", "a\tb", "a'b", "a\"b"} {
					fields = validFields()
					fields[name] = value
					if err := run(makeSecret(fields)); err == nil {
						t.Errorf("unsafe %s accepted", name)
					}
				}
			}
			for _, project := range []string{"Project", "a.b", "a_b", "-a", "a-", strings.Repeat("a", 64), "project\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: injected"} {
				fields := validFields()
				fields["project"] = project
				if err := run(makeSecret(fields)); err == nil {
					t.Error("invalid project accepted")
				}
			}
			for _, credentials := range []*corev1.Secret{
				{}, {Data: map[string][]byte{gdcconstants.ServiceAccountJSONField: nil}},
				{Data: map[string][]byte{gdcconstants.ServiceAccountJSONField: []byte("invalid JSON")}},
			} {
				if err := run(credentials); err == nil {
					t.Error("invalid Secret accepted")
				}
			}
		})
	}
	if err := validator.Validate(context.Background(), &corev1.ConfigMap{}, nil); err == nil {
		t.Error("wrong new object type accepted")
	}
	if err := validator.Validate(context.Background(), makeSecret(validFields()), &corev1.ConfigMap{}); err == nil {
		t.Error("wrong old object type accepted")
	}
	fields := validFields()
	fields["private_key"] = "credential-marker\ninjection"
	err := validator.Validate(context.Background(), makeSecret(fields), nil)
	if err == nil || strings.Contains(err.Error(), "credential-marker") {
		t.Fatalf("credential error must be redacted: %v", err)
	}
}
