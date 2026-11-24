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
	"fmt"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

type secret struct{}

// NewSecretValidator returns a validator for GDC cloudprovider credentials.
func NewSecretValidator() extensionswebhook.Validator {
	return &secret{}
}

// Validate validates credentials on both creation and update.
func (s *secret) Validate(_ context.Context, newObj, oldObj client.Object) error {
	return ValidateSecret(newObj, oldObj)
}

// ValidateSecret checks the service account without exposing credential values.
func ValidateSecret(newObj, oldObj client.Object) error {
	credentials, ok := newObj.(*corev1.Secret)
	if !ok {
		return fmt.Errorf("wrong object type %T", newObj)
	}
	if oldObj != nil {
		if _, ok := oldObj.(*corev1.Secret); !ok {
			return fmt.Errorf("wrong object type %T for old object", oldObj)
		}
	}
	path := field.NewPath("data").Key(gdcconstants.ServiceAccountJSONField)
	data, ok := credentials.Data[gdcconstants.ServiceAccountJSONField]
	if !ok || len(data) == 0 {
		return field.Required(path, "service account JSON must be provided")
	}
	account, err := gdc.GetServiceAccountFromJSON(data)
	if err != nil {
		return field.Invalid(path, "(hidden)", err.Error())
	}
	for _, entry := range []struct{ name, value string }{
		{"name", account.Name},
		{"private_key_id", account.PrivateKeyID},
		{"private_key", account.PrivateKey},
		{"token_uri", account.TokenURI},
	} {
		if entry.value == "" {
			return field.Required(path.Child(entry.name), "must be provided")
		}
	}
	return nil
}
