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
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"unicode"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"
)

var ErrMissingGDCHConfig = errors.New("secret doesn't have a gdch-config")
var ErrFetchingSecret = errors.New("failed to fetch secret")

// GetServiceAccountFromSecretReference retrieves the ServiceAccount from the secret with the given secret reference.
func GetServiceAccountFromSecretReference(ctx context.Context, c client.Client, secretRef corev1.SecretReference) (*auth.ServiceAccount, error) {
	secret, err := extensionscontroller.GetSecretByReference(ctx, c, &secretRef)
	if err != nil {
		return nil, err
	}

	return getServiceAccountFromSecret(secret)
}

// GetGDCHConfigFromSecretReference retrieves the GDCHConfig from the secret with the given secret reference.
func GetGDCHConfigFromSecretReference(ctx context.Context, c client.Client, secretRef corev1.SecretReference) (*gdcclient.OrgClusterConfig, error) {
	secret, err := extensionscontroller.GetSecretByReference(ctx, c, &secretRef)
	if err != nil {
		return nil, fmt.Errorf("%w:%w", ErrFetchingSecret, err)
	}

	return getGDCHConfigFromSecret(secret)
}

// getServiceAccountFromSecret retrieves the ServiceAccount from the secret.
func getServiceAccountFromSecret(secret *corev1.Secret) (*auth.ServiceAccount, error) {
	data, ok := secret.Data[gdcconstants.ServiceAccountJSONField]
	if !ok {
		return nil, fmt.Errorf("secret %s/%s doesn't have a service account json (expected field: %q)", secret.Namespace, secret.Name, gdcconstants.ServiceAccountJSONField)
	}

	return getServiceAccountFromJSON(data)
}

// getServiceAccountFromSecret retrieves the ServiceAccount from the secret.
func getGDCHConfigFromSecret(secret *corev1.Secret) (*gdcclient.OrgClusterConfig, error) {
	data, ok := secret.Data[gdcconstants.GDCHConfigJSONField]
	if !ok {
		return nil, fmt.Errorf("%w: secret %s/%s missing field %q", ErrMissingGDCHConfig, secret.Namespace, secret.Name, gdcconstants.GDCHConfigJSONField)
	}

	config := &gdcclient.OrgClusterConfig{}
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return config, nil
}

// GetServiceAccountFromJSON decodes and validates service account credentials.
// Admission and runtime consumers use the same validation boundary.
func GetServiceAccountFromJSON(data []byte) (*auth.ServiceAccount, error) {
	return getServiceAccountFromJSON(data)
}

// getServiceAccountFromJSON returns a validated ServiceAccount from the given JSON.
func getServiceAccountFromJSON(data []byte) (*auth.ServiceAccount, error) {
	serviceAccount := &auth.ServiceAccount{}
	if err := json.Unmarshal(data, serviceAccount); err != nil {
		return nil, err
	}
	if serviceAccount.Project == "" {
		return nil, fmt.Errorf("no project specified for service account")
	}
	if errs := validation.IsDNS1123Label(serviceAccount.Project); len(errs) > 0 {
		return nil, fmt.Errorf("service account project must be a DNS-1123 label: %s", strings.Join(errs, "; "))
	}

	// Check every string, including extension fields, without including credential
	// values in errors. PEM keys need their standard spaces and line breaks.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for name, raw := range fields {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			continue // Non-string extension fields are not interpolated.
		}
		if strings.ContainsAny(value, "\"'") {
			return nil, fmt.Errorf("service account field %q must not contain quotes", name)
		}
		if strings.ContainsFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			if name == "private_key" {
				block, rest := pem.Decode([]byte(value))
				if block != nil && (block.Type == "PRIVATE KEY" || block.Type == "RSA PRIVATE KEY" || block.Type == "EC PRIVATE KEY") &&
					len(block.Headers) == 0 && strings.HasPrefix(value, "-----BEGIN "+block.Type+"-----") && len(strings.TrimSpace(string(rest))) == 0 {
					continue
				}
			}
			return nil, fmt.Errorf("service account field %q must not contain whitespace or control characters (except PEM private key formatting)", name)
		}
	}

	return serviceAccount, nil
}
