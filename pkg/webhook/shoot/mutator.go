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

package shoot

import (
	"context"
	"fmt"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type mutator struct {
	logger logr.Logger
}

// NewMutator creates a new Mutator that mutates resources in the shoot cluster.
func NewMutator() extensionswebhook.Mutator {
	return &mutator{
		logger: log.Log.WithName("shoot-mutator"),
	}
}

// Mutate mutates resources.
func (m *mutator) Mutate(ctx context.Context, newObj, oldObj client.Object) error {
	newAcc, err := meta.Accessor(newObj)
	if err != nil {
		return fmt.Errorf("could not create accessor during webhook: %w", err)
	}
	// If the object does have a deletion timestamp then we don't want to mutate anything.
	if newAcc.GetDeletionTimestamp() != nil {
		return nil
	}

	switch x := newObj.(type) {
	case *corev1.Node:
		switch y := oldObj.(type) {
		case *corev1.Node:
			return m.mutateNetworkUnavailableNodeCondition(ctx, x, y, func() {
				extensionswebhook.LogMutation(logger, x.Kind, x.Namespace, x.Name)
			})
		}
	}
	return nil
}
