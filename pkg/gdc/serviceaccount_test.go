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

package gdc

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"
)

func TestServiceAccount(t *testing.T) {
	project := "project"
	name := "name"
	namespace := "foo"
	private_key_id := "123"
	private_key := "fake-private-key"
	token_uri := "https://some.url.com"
	serviceAccountData := []byte(fmt.Sprintf(`{"project": %q, "name": %q, "private_key_id": %q, "private_key": %q, "token_uri": %q}`,
		project, name, private_key_id, private_key, token_uri))
	serviceAccount := &auth.ServiceAccount{
		Project:      project,
		Name:         name,
		PrivateKeyID: private_key_id,
		PrivateKey:   private_key,
		TokenURI:     token_uri,
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			gdcconstants.ServiceAccountJSONField: serviceAccountData,
		},
	}

	t.Run("ExtractServiceAccountFields", func(t *testing.T) {
		sa, err := getServiceAccountFromJSON(serviceAccountData)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if sa.Project != project {
			t.Errorf("expected project %s, got %s", project, sa.Project)
		}

		_, err = getServiceAccountFromJSON([]byte(`{"project": ""`))
		if err == nil {
			t.Errorf("expected an error for empty project, got none")
		}

		if sa.Name != name {
			t.Errorf("expected name %s, got %s", name, sa.Name)
		}

		_, err = getServiceAccountFromJSON([]byte(`{"name": ""`))
		if err == nil {
			t.Errorf("expected an error for empty name, got none")
		}

		if sa.PrivateKeyID != private_key_id {
			t.Errorf("expected private_key_id %s, got %s", private_key_id, sa.PrivateKeyID)
		}

		_, err = getServiceAccountFromJSON([]byte(`{"private_key_id": ""`))
		if err == nil {
			t.Errorf("expected an error for empty private_key_id, got none")
		}

		if sa.PrivateKey != private_key {
			t.Errorf("expected private_key %s, got %s", private_key, sa.PrivateKey)
		}

		_, err = getServiceAccountFromJSON([]byte(`{"private_key": ""`))
		if err == nil {
			t.Errorf("expected an error for empty private_key, got none")
		}

		_, err = getServiceAccountFromJSON([]byte(`{"format_version": `))
		if err == nil {
			t.Errorf("expected an error for empty format_version, got none")
		}

		if sa.TokenURI != token_uri {
			t.Errorf("expected token_uri %s, got %s", token_uri, sa.TokenURI)
		}

		_, err = getServiceAccountFromJSON([]byte(`{"token_uri": ""`))
		if err == nil {
			t.Errorf("expected an error for empty token_uri, got none")
		}
	})

	t.Run("GetServiceAccount", func(t *testing.T) {
		ctx := context.Background()
		secretRef := corev1.SecretReference{
			Namespace: namespace,
			Name:      name,
		}
		c := fake.NewClientBuilder().WithObjects(secret).Build()

		got, err := GetServiceAccountFromSecretReference(ctx, c, secretRef)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if diff := cmp.Diff(got, serviceAccount); diff != "" {
			t.Errorf("expected service account %+v, got %+v", serviceAccount, got)
		}
	})
}

func TestGetGDCHConfigFromSecretReference(t *testing.T) {
	type args struct {
		ctx       context.Context
		c         client.Client
		secretRef corev1.SecretReference
	}
	tests := []struct {
		name string
		args args
		want *gdcclient.OrgClusterConfig
	}{
		{
			name: "decode GDCH Config from Secret Reference",
			args: args{
				ctx: nil,
				c: fake.NewClientBuilder().WithObjects(&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "secret-name",
						Namespace: "secret-namespace",
					},
					Data: map[string][]byte{
						"gdch-config": []byte(`{"orgName": "sap", "orgClusterURL": "sap-url.com", "caData": "fake-ca-data"}`),
					},
				}).Build(),
				secretRef: corev1.SecretReference{
					Name:      "secret-name",
					Namespace: "secret-namespace",
				},
			},
			want: &gdcclient.OrgClusterConfig{
				OrgClusterURL: "sap-url.com",
				CAData:        "fake-ca-data",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetGDCHConfigFromSecretReference(tt.args.ctx, tt.args.c, tt.args.secretRef)
			if err != nil {
				t.Errorf("GetGDCHConfigFromSecretReference() returns with error %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetGDCHConfigFromSecretReference() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetGDCHConfigFromSecretReferenceErrors(t *testing.T) {
	type args struct {
		ctx       context.Context
		c         client.Client
		secretRef corev1.SecretReference
	}
	tests := []struct {
		name         string
		args         args
		wantErrorMsg string
	}{
		{
			name: "secret is not found",
			args: args{
				ctx: nil,
				c:   fake.NewClientBuilder().Build(),
				secretRef: corev1.SecretReference{
					Name:      "secret-name",
					Namespace: "secret-namespace",
				},
			},
			wantErrorMsg: `secrets "secret-name" not found`,
		},
		{
			name: "secret is missing gdch-config key",
			args: args{
				ctx: nil,
				c: fake.NewClientBuilder().WithObjects(&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "secret-name",
						Namespace: "secret-namespace",
					},
					Data: map[string][]byte{
						"wrong-key": []byte(`{"orgName": "sap", "orgAdminURL": "sap-url.com", "caData": "fake-ca-data"}`),
					},
				}).Build(),
				secretRef: corev1.SecretReference{
					Name:      "secret-name",
					Namespace: "secret-namespace",
				},
			},
			wantErrorMsg: "doesn't have a gdch-config",
		},
		{
			name: "gdch-config does not contain a json",
			args: args{
				ctx: nil,
				c: fake.NewClientBuilder().WithObjects(&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "secret-name",
						Namespace: "secret-namespace",
					},
					Data: map[string][]byte{
						"gdch-config": []byte(`invalid-json-object`),
					},
				}).Build(),
				secretRef: corev1.SecretReference{
					Name:      "secret-name",
					Namespace: "secret-namespace",
				},
			},
			wantErrorMsg: "invalid character",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := GetGDCHConfigFromSecretReference(tt.args.ctx, tt.args.c, tt.args.secretRef)
			if err == nil {
				t.Fatal("GetGDCHConfigFromSecretReference() expected to return with an error")
			}
			if !strings.Contains(err.Error(), tt.wantErrorMsg) {
				t.Fatalf("GetGDCHConfigFromSecretReference() error = %v, wantErrMsg %v", err.Error(), tt.wantErrorMsg)
			}
		})
	}
}
