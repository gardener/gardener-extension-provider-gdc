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

package controlplane

import (
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/gardener/gardener/extensions/pkg/webhook/controlplane"
	"github.com/gardener/gardener/extensions/pkg/webhook/controlplane/genericmutator"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/gardener/gardener/pkg/component/extensions/operatingsystemconfig/original/components/kubelet"
	"github.com/gardener/gardener/pkg/component/extensions/operatingsystemconfig/utils"
	appsv1 "k8s.io/api/apps/v1"
	vpaautoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/config"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

var logger = log.Log.WithName("gdch-controlplane-webhook")

// AddToManager creates a new control plane webhook.
func AddToManager(mgr manager.Manager, config *config.ControllerConfiguration) (*webhook.Webhook, error) {
	logger.Info("Adding webhook to manager")
	fciCodec := utils.NewFileContentInlineCodec()
	return controlplane.New(mgr, controlplane.Args{
		Kind:     controlplane.KindShoot,
		Provider: gdc.Type,
		Types: []webhook.Type{
			{Obj: &appsv1.Deployment{}},
			{Obj: &vpaautoscalingv1.VerticalPodAutoscaler{}},
			{Obj: &extensionsv1alpha1.OperatingSystemConfig{}},
			{Obj: &druidv1alpha1.Etcd{}},
			{Obj: &appsv1.StatefulSet{}},
		},
		Mutator: genericmutator.NewMutator(mgr, NewEnsurer(mgr.GetClient(), logger, &config.ETCD.Storage), utils.NewUnitSerializer(),
			kubelet.NewConfigCodec(fciCodec), fciCodec, logger),
	})
}
