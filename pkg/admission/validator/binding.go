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
	"fmt"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/gardener/gardener/pkg/apis/core"
	gardensecurity "github.com/gardener/gardener/pkg/apis/security"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type secretBindingValidator struct {
	apiReader client.Reader
}

// NewSecretBindingValidator returns a validator for GDC SecretBinding resources.
func NewSecretBindingValidator(mgr manager.Manager) extensionswebhook.Validator {
	reader := mgr.GetAPIReader()
	if reader == nil {
		reader = mgr.GetClient()
	}
	return &secretBindingValidator{apiReader: reader}
}

// Validate validates the referenced Secret when a new SecretBinding is created.
func (v *secretBindingValidator) Validate(ctx context.Context, newObj, oldObj client.Object) error {
	secretBinding, ok := newObj.(*core.SecretBinding) //nolint:staticcheck // SA1019: SecretBinding remains supported alongside CredentialsBinding.
	if !ok {
		return fmt.Errorf("wrong object type %T", newObj)
	}
	if oldObj != nil {
		// Only validate on creation so updates/finalizer removals on existing bindings remain non-breaking.
		return nil
	}

	secret := &corev1.Secret{}
	if err := v.apiReader.Get(ctx, client.ObjectKey{
		Namespace: secretBinding.SecretRef.Namespace,
		Name:      secretBinding.SecretRef.Name,
	}, secret); err != nil {
		return err
	}
	return ValidateSecret(secret, nil)
}

type credentialsBindingValidator struct {
	apiReader client.Reader
}

// NewCredentialsBindingValidator returns a validator for GDC CredentialsBinding resources.
func NewCredentialsBindingValidator(mgr manager.Manager) extensionswebhook.Validator {
	reader := mgr.GetAPIReader()
	if reader == nil {
		reader = mgr.GetClient()
	}
	return &credentialsBindingValidator{apiReader: reader}
}

// Validate validates the referenced Secret when a new CredentialsBinding is created.
func (v *credentialsBindingValidator) Validate(ctx context.Context, newObj, oldObj client.Object) error {
	credentialsBinding, ok := newObj.(*gardensecurity.CredentialsBinding)
	if !ok {
		return fmt.Errorf("wrong object type %T", newObj)
	}
	if oldObj != nil {
		// Only validate on creation so updates/finalizer removals on existing bindings remain non-breaking.
		return nil
	}
	if credentialsBinding.CredentialsRef.APIVersion != corev1.SchemeGroupVersion.String() ||
		credentialsBinding.CredentialsRef.Kind != "Secret" {
		return nil
	}

	secret := &corev1.Secret{}
	if err := v.apiReader.Get(ctx, client.ObjectKey{
		Namespace: credentialsBinding.CredentialsRef.Namespace,
		Name:      credentialsBinding.CredentialsRef.Name,
	}, secret); err != nil {
		return err
	}
	return ValidateSecret(secret, nil)
}
