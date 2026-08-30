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

package daemonset

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener/extensions/pkg/webhook"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
)

const (
	// WebhookName is the name of this webhook
	WebhookName = "daemonset"
	// WebhookPath is the URL path for this webhook
	WebhookPath = "/webhooks/daemonset"
)

var logger = log.Log.WithName("daemonset-webhook")

// New creates a new webhook for Fluent Bit.
func New(mgr manager.Manager) (*webhook.Webhook, error) {
	logger.Info("Adding webhook to manager")

	// Create the mutator instance
	daemonsetMutator := &mutator{
		client: mgr.GetClient(),
		logger: logger,
	}

	namespaceSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			corev1.LabelMetadataName: v1beta1constants.GardenNamespace,
		},
	}

	return webhook.New(mgr, webhook.Args{
		Name:   WebhookName,
		Path:   WebhookPath,
		Target: webhook.TargetSeed,
		Mutators: map[webhook.Mutator][]webhook.Type{
			daemonsetMutator: {{Obj: &appsv1.DaemonSet{}}},
		},
		NamespaceSelector: namespaceSelector,
	})
}
