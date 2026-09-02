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

package app

import (
	"context"
	"fmt"
	"os"

	controllercmd "github.com/gardener/gardener/extensions/pkg/controller/cmd"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"

	"github.com/gardener/gardener/extensions/pkg/util"
	webhookcmd "github.com/gardener/gardener/extensions/pkg/webhook/cmd"
	"github.com/gardener/gardener/pkg/apis/core/install"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	gardenerhealthz "github.com/gardener/gardener/pkg/healthz"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	componentbaseconfig "k8s.io/component-base/config/v1alpha1"
	"k8s.io/component-base/version/verflag"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	admissioncmd "github.com/gardener/gardener-extension-provider-gdc/pkg/admission/cmd"
	gdcinstall "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/install"
)

// AdmissionName is the name of the admission component.
const (
	AdmissionName                           = "admission-gdch"
	shootNamespacedCloudProfileNameIndexKey = ".spec.cloudProfile.name"
)

var log = logf.Log.WithName("gardener-extension-admission-gdch")

// NewAdmissionCommand creates a new command for running a GDCH gardener-extension-admission-gdch webhook.
func NewAdmissionCommand(ctx context.Context) *cobra.Command {
	generalOpts := &controllercmd.GeneralOptions{}
	restOpts := &controllercmd.RESTOptions{}
	mgrOpts := &controllercmd.ManagerOptions{
		LeaderElection:          true,
		LeaderElectionID:        controllercmd.LeaderElectionNameID(AdmissionName),
		LeaderElectionNamespace: os.Getenv("LEADER_ELECTION_NAMESPACE"),
		WebhookServerPort:       443,
		MetricsBindAddress:      ":8080",
		HealthBindAddress:       ":8081",
		WebhookCertDir:          "/tmp/gardener-extensions-cert",
	}
	// options for the webhook server
	webhookServerOptions := &webhookcmd.ServerOptions{
		Namespace: os.Getenv("WEBHOOK_CONFIG_NAMESPACE"),
	}

	webhookSwitches := admissioncmd.GardenWebhookSwitchOptions()
	webhookOptions := webhookcmd.NewAddToManagerOptions(
		AdmissionName,
		"",
		nil,
		generalOpts,
		webhookServerOptions,
		webhookSwitches,
	)

	aggOption := controllercmd.NewOptionAggregator(
		generalOpts,
		restOpts,
		mgrOpts,
		webhookOptions,
	)

	cmd := &cobra.Command{
		Use: AdmissionName,

		RunE: func(_ *cobra.Command, _ []string) error {
			verflag.PrintAndExitIfRequested()

			if gardenKubeconfig := os.Getenv("GARDEN_KUBECONFIG"); gardenKubeconfig != "" {
				log.Info("Getting rest config for garden from GARDEN_KUBECONFIG", "path", gardenKubeconfig)
				restOpts.Kubeconfig = gardenKubeconfig
			}

			if err := aggOption.Complete(); err != nil {
				return fmt.Errorf("error completing options: %w", err)
			}

			util.ApplyClientConnectionConfigurationToRESTConfig(&componentbaseconfig.ClientConnectionConfiguration{
				QPS:   100.0,
				Burst: 130,
			}, restOpts.Completed().Config)

			// Force JSON content type to avoid protobuf issues with Shoot resources.
			// The API server might not support protobuf for Shoots in this version.
			restOpts.Completed().Config.ContentType = "application/json"

			managerOptions := mgrOpts.Completed().Options()
			// managerOptions are modified based on the whether the source cluster is enabled.
			sourceClusterConfig, err := getSourceClusterConfig(&managerOptions, webhookOptions.Server.Completed().Namespace)
			if err != nil {
				return err
			}
			mgr, err := manager.New(restOpts.Completed().Config, managerOptions)
			if err != nil {
				return fmt.Errorf("could not instantiate manager: %w", err)
			}

			install.Install(mgr.GetScheme())

			if err := gdcinstall.AddToScheme(mgr.GetScheme()); err != nil {
				return fmt.Errorf("could not update manager scheme: %w", err)
			}

			log.Info("Setting up field indexer for Shoot spec.cloudProfile.name")
			if err := mgr.GetFieldIndexer().IndexField(ctx, &gardencorev1beta1.Shoot{}, shootNamespacedCloudProfileNameIndexKey, func(obj client.Object) []string {
				shoot, ok := obj.(*gardencorev1beta1.Shoot)
				if !ok {
					return nil
				}
				// Only index if the shoot references a NamespacedCloudProfile.
				if shoot.Spec.CloudProfile != nil && shoot.Spec.CloudProfile.Kind == "NamespacedCloudProfile" {
					return []string{shoot.Spec.CloudProfile.Name}
				}
				return nil
			}); err != nil {
				return fmt.Errorf("failed to add field indexer for shoots: %w", err)
			}

			sourceCluster, err := getSourceCluster(sourceClusterConfig, mgr)
			if err != nil {
				return err
			}

			log.Info("Setting up webhook server")
			if _, err := webhookOptions.Completed().AddToManager(ctx, mgr, sourceCluster); err != nil {
				return err
			}

			if err := mgr.AddReadyzCheck("informer-sync", gardenerhealthz.NewCacheSyncHealthz(mgr.GetCache())); err != nil {
				return fmt.Errorf("could not add readycheck for informers: %w", err)
			}

			if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
				return fmt.Errorf("could not add healthcheck: %w", err)
			}

			if err := mgr.AddReadyzCheck("webhook-server", mgr.GetWebhookServer().StartedChecker()); err != nil {
				return fmt.Errorf("could not add readycheck of webhook to manager: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	verflag.AddFlags(cmd.Flags())
	aggOption.AddFlags(cmd.Flags())

	return cmd
}

func getSourceClusterConfig(managerOptions *manager.Options, webhookNamespace string) (*rest.Config, error) {
	// Operators can enable the source cluster option via SOURCE_CLUSTER environment variable.
	// In-cluster config will be used if no SOURCE_KUBECONFIG is specified.
	//
	// The source cluster is for instance used by Gardener's certificate controller, to maintain certificate
	// secrets in a different cluster ('runtime-garden') than the cluster where the webhook configurations
	// are maintained ('virtual-garden').

	// This logic is because runtime-garden and virtual-garden might be different depending on the real-life scenario.
	// Ref: https://github.com/gardener/gardener-extension-provider-aws/blob/f7adcee46b77bd3e207ffc82e200f048ca9f8aa6/docs/operations/deployment.md#deployment-of-the-aws-provider-extension
	var sourceClusterConfig *rest.Config
	if sourceClusterEnabled := os.Getenv("SOURCE_CLUSTER"); sourceClusterEnabled != "" {
		log.Info("Configuring source cluster option")
		var err error
		sourceClusterConfig, err = clientcmd.BuildConfigFromFlags("", os.Getenv("SOURCE_KUBECONFIG"))
		if err != nil {
			return nil, err
		}
		managerOptions.LeaderElectionConfig = sourceClusterConfig
	} else {
		// Restrict the cache for secrets to the configured namespace to avoid the need for cluster-wide list/watch permissions.
		managerOptions.Cache = cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {Namespaces: map[string]cache.Config{webhookNamespace: {}}},
			},
		}
	}
	return sourceClusterConfig, nil
}

func getSourceCluster(sourceClusterConfig *rest.Config, mgr manager.Manager) (cluster.Cluster, error) {
	if sourceClusterConfig == nil {
		return nil, nil
	}

	sourceCluster, err := cluster.New(sourceClusterConfig, func(opts *cluster.Options) {
		opts.Logger = log
		opts.Cache.DefaultNamespaces = map[string]cache.Config{v1beta1constants.GardenNamespace: {}}
	})
	if err != nil {
		return nil, err
	}

	if err := mgr.AddReadyzCheck("source-informer-sync", gardenerhealthz.NewCacheSyncHealthz(sourceCluster.GetCache())); err != nil {
		return nil, err
	}

	if err = mgr.Add(sourceCluster); err != nil {
		return nil, err
	}
	return sourceCluster, nil
}
