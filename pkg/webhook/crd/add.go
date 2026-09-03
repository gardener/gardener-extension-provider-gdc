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

package crd

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
	"github.com/gardener/gardener/extensions/pkg/webhook"
	versionutils "github.com/gardener/gardener/pkg/utils/version"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	// WebhookName is the name of this webhook
	WebhookName = "crd-mutator"
	// WebhookPath is the URL path for this webhook
	WebhookPath = "/webhooks/mutate-crd"
)

var (
	logger = log.Log.WithName("crd-webhook")

	// TODO: Remove this check in the future as Gardener v1.149.3+ only supports Kubernetes >= 1.32.
	constraintK8sLess132 = versionutils.MustNewConstraint("< 1.32-0")
)

// getSeedClusterVersion retrieves the Kubernetes version of the seed cluster.
func getSeedClusterVersion(mgr manager.Manager) (*semver.Version, error) {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(mgr.GetConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create discovery client: %w", err)
	}
	logger.Info("Fetching cluster version to determine webhook necessity")
	serverVersion, err := discoveryClient.ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to get server version: %w", err)
	}

	version, err := semver.NewVersion(serverVersion.GitVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to parse server version %s: %w", serverVersion.GitVersion, err)
	}

	return version, nil
}

// New creates a new webhook for CRD mutation.
func New(mgr manager.Manager) (*webhook.Webhook, error) {
	// Check seed cluster Kubernetes version
	seedVersion, err := getSeedClusterVersion(mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to get seed cluster version: %w", err)
	}

	mutator := getMutatorForVersion(seedVersion)
	if mutator == nil {
		return &webhook.Webhook{Name: WebhookName}, nil
	}
	logger.Info("Adding webhook to manager")

	return webhook.New(mgr, webhook.Args{
		Name:   WebhookName,
		Path:   WebhookPath,
		Target: webhook.TargetSeed,
		Mutators: map[webhook.Mutator][]webhook.Type{
			mutator: {{Obj: &apiextensionsv1.CustomResourceDefinition{}}},
		},
	})
}

// getMutatorForVersion returns the appropriate mutator based on the Kubernetes version.
// In Kubernetes < 1.32, CRDs need mutation to escape reserved CEL keywords.
func getMutatorForVersion(version *semver.Version) webhook.Mutator {
	// TODO: Remove this check in the future as Gardener v1.149.3+ only supports Kubernetes >= 1.32.
	if !constraintK8sLess132.Check(version) {
		logger.Info("Seed cluster version is >= 1.32, skipping CRD mutation", "seedVersion", version.String())
		return nil
	}
	logger.Info("Seed cluster version is < 1.32, using real mutator", "seedVersion", version.String())
	return NewCRDMutator()
}
