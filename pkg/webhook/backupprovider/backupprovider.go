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
	"context"
	"fmt"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	webhookutil "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/utils"
)

const (
	virtualGardenEtcdMainName = "virtual-garden-etcd-main"
)

type mutator struct {
	client client.Client
	logger logr.Logger
}

func newMutator(mgr manager.Manager) extensionswebhook.Mutator {
	return &mutator{
		client: mgr.GetClient(),
		logger: logger.WithName("backupprovider-mutator"),
	}
}

func (m *mutator) Mutate(ctx context.Context, newObj, old client.Object) error {
	if newObj.GetDeletionTimestamp() != nil {
		return nil
	}

	etcd, ok := newObj.(*druidv1alpha1.Etcd)
	if !ok {
		return fmt.Errorf("could not mutate: object is not of type %q", "Etcd")
	}

	if etcd.Name == virtualGardenEtcdMainName && etcd.Spec.Backup.Store != nil {
		if err := webhookutil.EnsureETCDBackup(ctx, m.client, m.logger, etcd); err != nil {
			return err
		}
	}

	extensionswebhook.LogMutation(m.logger, etcd.Kind, etcd.Namespace, etcd.Name)
	metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, "backupprovider.gdc.gardener.cloud/mutated", "true")

	return nil
}
