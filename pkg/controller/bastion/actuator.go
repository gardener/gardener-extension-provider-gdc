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

package bastion

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener/extensions/pkg/controller/bastion"

	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

type actuator struct {
	client              client.Client
	decoder             runtime.Decoder
	getClientAndProject func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error)
}

func newActuator(mgr manager.Manager) bastion.Actuator {
	c := mgr.GetClient()
	return &actuator{
		client:              c,
		decoder:             serializer.NewCodecFactory(c.Scheme(), serializer.EnableStrict).UniversalDecoder(),
		getClientAndProject: getClientAndProject,
	}
}

func getClientAndProject(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
	serviceAccount, err := gdc.GetServiceAccountFromSecretReference(ctx, c, sr)
	if err != nil {
		return nil, "", err
	}

	kubeclient, err := gdcclient.Get(orgClusterCfg, serviceAccount, scheme)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create kubeClient: %w", err)
	}

	return kubeclient, serviceAccount.Project, nil
}
