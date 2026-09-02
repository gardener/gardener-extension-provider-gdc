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
	"fmt"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/gardener/gardener/pkg/apis/core"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	// Name is a name for a validation webhook.
	Name = "core-validator"
)

var logger = log.Log.WithName(fmt.Sprintf("gdch-%s-webhook", Name))

// New creates a new validation webhook for `core.gardener.cloud` resources.
func New(mgr manager.Manager) (*extensionswebhook.Webhook, error) {
	logger.Info("Setting up webhook", "name", Name)
	return extensionswebhook.New(mgr, extensionswebhook.Args{
		Name: Name,
		Path: "/webhooks/validate",
		Validators: map[extensionswebhook.Validator][]extensionswebhook.Type{
			NewShootValidator(mgr):                  {{Obj: &core.Shoot{}}},
			NewCloudProfileValidator(mgr):           {{Obj: &core.CloudProfile{}}},
			NewBackupBucketValidator(mgr):           {{Obj: &core.Seed{}}},
			NewNamespacedCloudProfileValidator(mgr): {{Obj: &core.NamespacedCloudProfile{}}},
		},
		Target: extensionswebhook.TargetSeed,
		ObjectSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"provider.extensions.gardener.cloud/gdch": "true"},
		},
	})
}
