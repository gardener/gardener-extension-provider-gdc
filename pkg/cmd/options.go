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

package cmd

import (
	extensionsbackupbucketcontroller "github.com/gardener/gardener/extensions/pkg/controller/backupbucket"
	extensionsbackupentrycontroller "github.com/gardener/gardener/extensions/pkg/controller/backupentry"
	extensionsbastioncontroller "github.com/gardener/gardener/extensions/pkg/controller/bastion"
	controllercmd "github.com/gardener/gardener/extensions/pkg/controller/cmd"
	extensionscontrolplanecontroller "github.com/gardener/gardener/extensions/pkg/controller/controlplane"
	extensionsdnsrecordcontroller "github.com/gardener/gardener/extensions/pkg/controller/dnsrecord"
	extensionsinfrastructurecontroller "github.com/gardener/gardener/extensions/pkg/controller/infrastructure"
	extensionsworkercontroller "github.com/gardener/gardener/extensions/pkg/controller/worker"
	"github.com/gardener/gardener/extensions/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	webhookcmd "github.com/gardener/gardener/extensions/pkg/webhook/cmd"
	extensioncontrolplanewebhook "github.com/gardener/gardener/extensions/pkg/webhook/controlplane"
	extensionshootwebhook "github.com/gardener/gardener/extensions/pkg/webhook/shoot"

	crdwebhook "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/crd"
	daemonsetwebhook "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/daemonset"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/backupbucket"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/backupentry"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/bastion"
	controlplanecontroller "github.com/gardener/gardener-extension-provider-gdc/pkg/controller/controlplane"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/dnsrecord"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/infrastructure"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/worker"
	backupproviderwebhook "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/backupprovider"
	controlplanewebhook "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/controlplane"
	shootwebhook "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/shoot"
	shootservicewebhook "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/shootservice"
)

const (
	// ProviderClientQPSFlag is the name of the command line flag to specify the client QPS for provider operations.
	ProviderClientQPSFlag = "provider-client-qps"
	// ProviderClientBurstFlag is the name of the command line flag to specify the client burst for provider operations.
	ProviderClientBurstFlag = "provider-client-burst"
	// ProviderClientWaitTimeoutFlag is the name of the command line flag to specify the client wait timeout for provider operations.
	ProviderClientWaitTimeoutFlag = "provider-client-wait-timeout"
)

// ControllerSwitchOptions are the controllercmd.SwitchOptions for the provider controllers.
func ControllerSwitchOptions() *controllercmd.SwitchOptions {
	return controllercmd.NewSwitchOptions(
		controllercmd.Switch(extensionsbastioncontroller.ControllerName, bastion.AddToManager),
		controllercmd.Switch(extensionscontrolplanecontroller.ControllerName, controlplanecontroller.AddToManager),
		controllercmd.Switch(extensionsinfrastructurecontroller.ControllerName, infrastructure.AddToManager),
		controllercmd.Switch(extensionsworkercontroller.ControllerName, worker.AddToManager),
		controllercmd.Switch(extensionsdnsrecordcontroller.ControllerName, dnsrecord.AddToManager),
		controllercmd.Switch(extensionsbackupbucketcontroller.ControllerName, backupbucket.AddToManager),
		controllercmd.Switch(extensionsbackupentrycontroller.ControllerName, backupentry.AddToManager),
	)
}

// WebhookSwitchOptions are the webhookcmd.SwitchOptions for the provider webhooks.
func WebhookSwitchOptions(configOptions *ConfigOptions) *webhookcmd.SwitchOptions {
	return webhookcmd.NewSwitchOptions(
		webhookcmd.Switch(extensioncontrolplanewebhook.WebhookName, func(mgr manager.Manager) (*webhook.Webhook, error) {
			return controlplanewebhook.AddToManager(mgr, configOptions.Completed().Config)
		}),
		webhookcmd.Switch(extensionshootwebhook.WebhookName, shootwebhook.AddToManager),
		webhookcmd.Switch(backupproviderwebhook.WebhookName, backupproviderwebhook.New),
		webhookcmd.Switch(shootservicewebhook.WebhookName, shootservicewebhook.AddToManager),
		webhookcmd.Switch(daemonsetwebhook.WebhookName, daemonsetwebhook.New),
		webhookcmd.Switch(crdwebhook.WebhookName, crdwebhook.New),
	)
}
