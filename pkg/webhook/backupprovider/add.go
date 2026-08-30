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

package backupprovider

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
)

const (
	// WebhookName is the name of the webhook.
	WebhookName = "backupprovider"
)

var logger = log.Log.WithName("gdc-backupprovider-webhook")

// New creates a new backupprovider webhook.
func New(mgr manager.Manager) (*extensionswebhook.Webhook, error) {
	logger.Info("Creating webhook", "name", WebhookName)

	mutator := newMutator(mgr)

	types := []extensionswebhook.Type{
		{Obj: &druidv1alpha1.Etcd{}},
	}

	handler, err := extensionswebhook.NewBuilder(mgr, logger).WithMutator(mutator, types...).Build()
	if err != nil {
		return nil, err
	}

	namespaceSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			corev1.LabelMetadataName: v1beta1constants.GardenNamespace,
		},
	}

	webhook := &extensionswebhook.Webhook{
		Action: extensionswebhook.ActionMutating,
		Name:   WebhookName,
		// Using TargetSeed as a workaround because TargetGarden is not supported.
		// With using TargetSeed, the underlying functions operate correctly for the virtual garden use case.
		Target:            extensionswebhook.TargetSeed,
		Types:             types,
		Webhook:           &admission.Webhook{Handler: handler, RecoverPanic: ptr.To(true)},
		Path:              WebhookName,
		NamespaceSelector: namespaceSelector,
	}

	return webhook, nil
}
