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
	"context"
	"fmt"
	"strings"

	"github.com/gardener/gardener/extensions/pkg/webhook"
	gardenimagevector "github.com/gardener/gardener/imagevector"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"

	webhookutil "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/utils"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mutator struct {
	client client.Client
	logger logr.Logger
}

// Mutate implements webhook.Mutator
func (m *mutator) Mutate(ctx context.Context, newObj, _ client.Object) error {
	daemonSet, ok := newObj.(*appsv1.DaemonSet)
	if !ok {
		return fmt.Errorf("wrong object type %T", newObj)
	}

	if strings.Contains(daemonSet.Name, v1beta1constants.DaemonSetNameFluentBit) {
		m.mutateFluentbit(daemonSet)
	}

	return nil
}

// mutateFluentbit watch for fluent bit daemonset and add security context to fluent-bit container
func (m *mutator) mutateFluentbit(daemonSet *appsv1.DaemonSet) {
	m.logger.Info("Mutating Fluent Bit DaemonSet")

	if c := webhook.ContainerWithName(daemonSet.Spec.Template.Spec.Containers, gardenimagevector.ContainerImageNameFluentBit); c != nil {
		webhookutil.EnsureSELinuxSPCT(c)
	}
}
