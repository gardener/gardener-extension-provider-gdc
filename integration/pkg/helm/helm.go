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

package helm

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"helm.sh/helm/v4/pkg/action"
	"helm.sh/helm/v4/pkg/chart"
	"helm.sh/helm/v4/pkg/chart/loader"
	"helm.sh/helm/v4/pkg/cli"
	"helm.sh/helm/v4/pkg/getter"
	"helm.sh/helm/v4/pkg/kube"
	"helm.sh/helm/v4/pkg/release"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

// InstallOptions contains the options for installing a Helm chart.
type InstallOptions struct {
	ChartPath      string
	KubeconfigPath string
	ReleaseName    string
	Namespace      string
	Values         map[string]interface{}
	Timeout        time.Duration
}

// UninstallOptions contains the options for uninstalling a Helm chart.
type UninstallOptions struct {
	KubeconfigPath string
	ReleaseName    string
	Namespace      string
	IgnoreNotFound bool
	// Wait will wait for all the resources to be deleted.
	Wait bool
}

// InstallOrUpgrade installs or upgrades a Helm chart.
func InstallOrUpgrade(opts InstallOptions) (release.Releaser, error) {
	chrt, err := loadChart(opts.ChartPath)
	if err != nil {
		return nil, err
	}

	actionConfig, err := newActionConfig(opts.KubeconfigPath, opts.Namespace)
	if err != nil {
		return nil, err
	}

	listClient := action.NewList(actionConfig)
	listClient.Filter = opts.ReleaseName
	releases, err := listClient.Run()
	if err != nil {
		return nil, fmt.Errorf("failed to list helm releases: %w", err)
	}

	if len(releases) == 0 {
		// Release does not exist, install it.
		return installChart(actionConfig, chrt, opts)
	}

	// Release exists, upgrade it.
	return upgradeChart(actionConfig, chrt, opts)
}

func installChart(actionConfig *action.Configuration, chrt chart.Charter, opts InstallOptions) (release.Releaser, error) {
	installClient := action.NewInstall(actionConfig)
	installClient.ReleaseName = opts.ReleaseName
	installClient.Namespace = opts.Namespace
	installClient.CreateNamespace = true
	installClient.Timeout = opts.Timeout
	installClient.WaitStrategy = kube.HookOnlyStrategy

	rel, err := installClient.Run(chrt, opts.Values)
	if err != nil {
		return nil, fmt.Errorf("failed to run helm install: %w", err)
	}
	return rel, nil
}

func upgradeChart(actionConfig *action.Configuration, chrt chart.Charter, opts InstallOptions) (release.Releaser, error) {
	upgradeClient := action.NewUpgrade(actionConfig)
	upgradeClient.Namespace = opts.Namespace
	upgradeClient.Timeout = opts.Timeout
	upgradeClient.WaitStrategy = kube.HookOnlyStrategy

	rel, err := upgradeClient.Run(opts.ReleaseName, chrt, opts.Values)
	if err != nil {
		return nil, fmt.Errorf("failed to run helm upgrade: %w", err)
	}
	return rel, nil
}

// Uninstall removes a specified Helm release from a cluster.
func Uninstall(opts UninstallOptions) (*release.UninstallReleaseResponse, error) {
	actionConfig, err := newActionConfig(opts.KubeconfigPath, opts.Namespace)
	if err != nil {
		return nil, err
	}

	uninstallClient := action.NewUninstall(actionConfig)
	if opts.Wait {
		uninstallClient.WaitStrategy = kube.LegacyStrategy
		uninstallClient.Timeout = 5 * time.Minute
	} else {
		uninstallClient.WaitStrategy = kube.HookOnlyStrategy
	}
	uninstallClient.IgnoreNotFound = opts.IgnoreNotFound
	res, err := uninstallClient.Run(opts.ReleaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to uninstall release %q in namespace %q: %w", opts.ReleaseName, opts.Namespace, err)
	}

	return res, nil
}

func newActionConfig(kubeconfigPath, namespace string) (*action.Configuration, error) {
	kubeConfigFlags := genericclioptions.NewConfigFlags(false)
	kubeConfigFlags.KubeConfig = &kubeconfigPath

	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(kubeConfigFlags, namespace, os.Getenv("HELM_DRIVER")); err != nil {
		return nil, fmt.Errorf("failed to initialize Helm action configuration: %w", err)
	}
	return actionConfig, nil
}

func isRemoteChart(chartPath string) bool {
	isOCI := strings.HasPrefix(chartPath, "oci://")
	isHTTP := strings.HasPrefix(chartPath, "http://")
	isHTTPS := strings.HasPrefix(chartPath, "https://")
	return isOCI || isHTTP || isHTTPS
}

func loadChart(chartPath string) (chart.Charter, error) {
	if isRemoteChart(chartPath) {
		settings := cli.New()
		providers := getter.All(settings)
		parsedUrl, err := url.ParseRequestURI(chartPath)
		if err != nil {
			return nil, fmt.Errorf("invalid chart URL %q: %w", chartPath, err)
		}

		provider, err := providers.ByScheme(parsedUrl.Scheme)
		if err != nil {
			return nil, fmt.Errorf("no getter for scheme %q: %w", parsedUrl.Scheme, err)
		}

		var getterOpts []getter.Option
		if parsedUrl.Scheme == "oci" {
			getterOpts = append(getterOpts, getter.WithURL(chartPath))
		}

		data, err := provider.Get(chartPath, getterOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to download chart from %q: %w", chartPath, err)
		}

		return loader.LoadArchive(data)
	}

	loadedChart, err := loader.Load(chartPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load chart from path %s: %w", chartPath, err)
	}
	return loadedChart, nil
}
