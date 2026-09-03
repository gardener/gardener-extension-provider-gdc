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

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	autoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/component-base/version/verflag"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener/extensions/pkg/controller"
	controllercmd "github.com/gardener/gardener/extensions/pkg/controller/cmd"
	"github.com/gardener/gardener/extensions/pkg/controller/controlplane/genericactuator"
	"github.com/gardener/gardener/extensions/pkg/util"
	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	webhookcmd "github.com/gardener/gardener/extensions/pkg/webhook/cmd"
	"github.com/gardener/gardener/pkg/client/kubernetes"
	gardenerhealthz "github.com/gardener/gardener/pkg/healthz"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"

	gdc "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"

	gdcinstall "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/install"
	gdccmd "github.com/gardener/gardener-extension-provider-gdc/pkg/cmd"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/backupbucket"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/backupentry"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/bastion"
	gdccontrolplane "github.com/gardener/gardener-extension-provider-gdc/pkg/controller/controlplane"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/dnsrecord"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/infrastructure"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/worker"
)

// NewControllerManagerCommand creates a new command for running a GDCH provider controller.
func NewControllerManagerCommand(ctx context.Context) *cobra.Command {
	generalOpts := &controllercmd.GeneralOptions{}
	restOpts := &controllercmd.RESTOptions{}
	mgrOpts := &controllercmd.ManagerOptions{
		LeaderElection:   true,
		LeaderElectionID: controllercmd.LeaderElectionNameID(gdc.ProviderName),
		// LeaderElectionNamespace is the namespace to do leader election in.
		LeaderElectionNamespace: os.Getenv("LEADER_ELECTION_NAMESPACE"),
		WebhookServerPort:       443,
		WebhookCertDir:          "/tmp/gardener-extensions-cert",
		MetricsBindAddress:      ":8080",
		HealthBindAddress:       ":8081",
	}

	// options for the controlplane controller
	controlPlaneCtrlOpts := &controllercmd.ControllerOptions{
		MaxConcurrentReconciles: 5,
	}

	// options for the dnsrecord controller
	dnsRecordCtrlOpts := &controllercmd.ControllerOptions{
		MaxConcurrentReconciles: 5,
	}

	// options for the infrastructure controller
	infraCtrlOpts := &controllercmd.ControllerOptions{
		MaxConcurrentReconciles: 5,
	}

	// options for the worker controller
	workerCtrlOpts := &controllercmd.ControllerOptions{
		MaxConcurrentReconciles: 5,
	}

	// options for the backupbucket controller
	backupBucketCtrlOpts := &controllercmd.ControllerOptions{
		MaxConcurrentReconciles: 5,
	}

	// options for the backupentry controller
	backupEntryCtrlOpts := &controllercmd.ControllerOptions{
		MaxConcurrentReconciles: 5,
	}

	// options for the webhook server
	webhookServerOptions := &webhookcmd.ServerOptions{
		Namespace: os.Getenv("WEBHOOK_CONFIG_NAMESPACE"),
	}

	configFileOpts := &gdccmd.ConfigOptions{}
	controllerSwitches := gdccmd.ControllerSwitchOptions()
	webhookSwitches := gdccmd.WebhookSwitchOptions(configFileOpts)
	webhookOptions := webhookcmd.NewAddToManagerOptions(
		gdc.ProviderName,
		genericactuator.ShootWebhooksResourceName,
		genericactuator.ShootWebhookNamespaceSelector(gdc.Type),
		generalOpts,
		webhookServerOptions,
		webhookSwitches,
	)
	reconcileOpts := &controllercmd.ReconcilerOptions{}

	// TODO(wenjung): Implementation of Backupbucket, backupentry, bastion, healthcheck, heartbeat controllers
	// might be needed for the Gardener's basic functionality.
	aggOption := controllercmd.NewOptionAggregator(
		generalOpts,
		restOpts,
		mgrOpts,
		controllercmd.PrefixOption("backupbucket-", backupBucketCtrlOpts),
		controllercmd.PrefixOption("backupentry-", backupEntryCtrlOpts),
		controllercmd.PrefixOption("controlplane-", controlPlaneCtrlOpts),
		controllercmd.PrefixOption("dnsrecord-", dnsRecordCtrlOpts),
		controllercmd.PrefixOption("infrastructure-", infraCtrlOpts),
		controllercmd.PrefixOption("worker-", workerCtrlOpts),
		configFileOpts,
		controllerSwitches,
		reconcileOpts,
		webhookOptions,
	)

	cmd := &cobra.Command{
		Use: fmt.Sprintf("%s-extension-controller-manager", gdc.ProviderName),

		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()

			if err := aggOption.Complete(); err != nil {
				return fmt.Errorf("error completing options: %w", err)
			}

			util.ApplyClientConnectionConfigurationToRESTConfig(configFileOpts.Completed().Config.ClientConnection, restOpts.Completed().Config)

			mopts := mgrOpts.Completed().Options()
			mopts.Client = client.Options{
				// Disable the Secret Caching considering the VM has limited capacity
				// https://github.com/gardener/gardener-extension-provider-aws/pull/790
				Cache: &client.CacheOptions{
					DisableFor: []client.Object{
						&corev1.Secret{},
					},
				},
			}
			mgr, err := manager.New(restOpts.Completed().Config, mopts)
			if err != nil {
				return fmt.Errorf("could not instantiate manager: %w", err)
			}

			scheme := mgr.GetScheme()
			if err := controller.AddToScheme(scheme); err != nil {
				return fmt.Errorf("could not add controller api to scheme: %w", err)
			}
			if err := gdcinstall.AddToScheme(scheme); err != nil {
				return fmt.Errorf("could not add gdch api to scheme: %w", err)
			}
			if err := druidv1alpha1.AddToScheme(scheme); err != nil {
				return fmt.Errorf("could not add druid api to scheme: %w", err)
			}
			if err := autoscalingv1.AddToScheme(scheme); err != nil {
				return fmt.Errorf("could not add autoscaling api to scheme: %w", err)
			}
			if err := machinev1alpha1.AddToScheme(scheme); err != nil {
				return fmt.Errorf("could not add machine api to scheme: %w", err)
			}
			// add common meta types to schema for controller-runtime to use v1.ListOptions
			metav1.AddToGroupVersion(scheme, machinev1alpha1.SchemeGroupVersion)

			log := mgr.GetLogger()
			log.Info("Getting rest config for garden")
			gardenRESTConfig, err := kubernetes.RESTConfigFromKubeconfigFile(os.Getenv("GARDEN_KUBECONFIG"), kubernetes.AuthTokenFile)
			if err != nil {
				return err
			}

			log.Info("Setting up cluster object for garden")
			gardenCluster, err := cluster.New(gardenRESTConfig, func(opts *cluster.Options) {
				opts.Scheme = kubernetes.GardenScheme
				opts.Logger = log
			})
			if err != nil {
				return fmt.Errorf("failed creating garden cluster object: %w", err)
			}

			log.Info("Adding garden cluster to manager")
			if err := mgr.Add(gardenCluster); err != nil {
				return fmt.Errorf("failed adding garden cluster to manager: %w", err)
			}

			log.Info("Adding controllers to manager")
			backupBucketCtrlOpts.Completed().Apply(&backupbucket.DefaultAddOptions.Controller)
			backupEntryCtrlOpts.Completed().Apply(&backupentry.DefaultAddOptions.Controller)
			worker.DefaultAddOptions.GardenCluster = gardenCluster
			controlPlaneCtrlOpts.Completed().Apply(&gdccontrolplane.DefaultAddOptions.Controller)
			infraCtrlOpts.Completed().Apply(&infrastructure.DefaultAddOptions.Controller)
			dnsRecordCtrlOpts.Completed().Apply(&dnsrecord.DefaultAddOptions.Controller)
			workerCtrlOpts.Completed().Apply(&worker.DefaultAddOptions.Controller)
			reconcileOpts.Completed().Apply(&infrastructure.DefaultAddOptions.IgnoreOperationAnnotation)
			reconcileOpts.Completed().Apply(&gdccontrolplane.DefaultAddOptions.IgnoreOperationAnnotation)
			reconcileOpts.Completed().Apply(&worker.DefaultAddOptions.IgnoreOperationAnnotation)
			reconcileOpts.Completed().Apply(&backupbucket.DefaultAddOptions.IgnoreOperationAnnotation)
			reconcileOpts.Completed().Apply(&backupentry.DefaultAddOptions.IgnoreOperationAnnotation)
			reconcileOpts.Completed().Apply(&dnsrecord.DefaultAddOptions.IgnoreOperationAnnotation)
			reconcileOpts.Completed().Apply(&bastion.DefaultAddOptions.IgnoreOperationAnnotation)

			infrastructure.DefaultAddOptions.ExtensionClasses = generalOpts.Completed().ExtensionClasses
			gdccontrolplane.DefaultAddOptions.ExtensionClasses = generalOpts.Completed().ExtensionClasses
			worker.DefaultAddOptions.ExtensionClasses = generalOpts.Completed().ExtensionClasses
			backupbucket.DefaultAddOptions.ExtensionClasses = generalOpts.Completed().ExtensionClasses
			backupentry.DefaultAddOptions.ExtensionClasses = generalOpts.Completed().ExtensionClasses
			dnsrecord.DefaultAddOptions.ExtensionClasses = generalOpts.Completed().ExtensionClasses
			bastion.DefaultAddOptions.ExtensionClasses = generalOpts.Completed().ExtensionClasses

			webhookConfig := webhookOptions.Completed()
			originalFactory := webhookConfig.Switch.WebhooksFactory
			webhookConfig.Switch.WebhooksFactory = func(mgr manager.Manager) ([]*extensionswebhook.Webhook, error) {
				webhooks, err := originalFactory(mgr)
				if err != nil {
					return nil, err
				}
				var filtered []*extensionswebhook.Webhook
				for _, wh := range webhooks {
					// Filter out the dummy webhook created in options.go when CRD webhook is not needed.
					// The dummy webhook has Name set but Webhook field is nil.
					if wh != nil && wh.Name == "crd-mutator" && wh.Webhook == nil {
						continue
					}
					filtered = append(filtered, wh)
				}
				return filtered, nil
			}

			shootWebhookConfig, err := webhookConfig.AddToManager(ctx, mgr, nil)
			if err != nil {
				return fmt.Errorf("could not add webhooks to manager: %w", err)
			}
			gdccontrolplane.DefaultAddOptions.ShootWebhookConfig = shootWebhookConfig
			gdccontrolplane.DefaultAddOptions.WebhookServerNamespace = webhookOptions.Server.Namespace

			if err := controllerSwitches.Completed().AddToManager(ctx, mgr); err != nil {
				return fmt.Errorf("could not add controllers to manager: %w", err)
			}

			if err := mgr.AddReadyzCheck("informer-sync", gardenerhealthz.NewCacheSyncHealthz(mgr.GetCache())); err != nil {
				return fmt.Errorf("could not add readycheck for informers: %w", err)
			}

			if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
				return fmt.Errorf("could not add health check to manager: %w", err)
			}

			if err := mgr.AddReadyzCheck("webhook-server", mgr.GetWebhookServer().StartedChecker()); err != nil {
				return fmt.Errorf("could not add ready check for webhook server to manager: %w", err)
			}

			if err := mgr.Start(ctx); err != nil {
				return fmt.Errorf("error running manager: %w", err)
			}

			return nil
		},
	}

	verflag.AddFlags(cmd.Flags())
	aggOption.AddFlags(cmd.Flags())

	return cmd
}
