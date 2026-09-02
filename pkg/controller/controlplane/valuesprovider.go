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
	"context"
	"fmt"
	"path/filepath"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	"github.com/gardener/gardener/extensions/pkg/controller/controlplane/genericactuator"
	extensionssecretsmanager "github.com/gardener/gardener/extensions/pkg/util/secret/manager"
	gardencorev1beta1helper "github.com/gardener/gardener/pkg/api/core/v1beta1/helper"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/gardener/gardener/pkg/utils/chart"
	gutil "github.com/gardener/gardener/pkg/utils/gardener"
	kutil "github.com/gardener/gardener/pkg/utils/kubernetes"
	secretutils "github.com/gardener/gardener/pkg/utils/secrets"
	secretsmanager "github.com/gardener/gardener/pkg/utils/secrets/manager"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/charts"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/kubeconfig"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/cloudprofile"
	gdc "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/internal"
)

const (
	caNameControlPlane                   = "ca-" + gdc.ProviderName + "-controlplane"
	cloudControllerManagerDeploymentName = "cloud-controller-manager"
	cloudControllerManagerServerName     = "cloud-controller-manager-server"
	csiSnapshotValidationServerName      = gdc.CSISnapshotValidationName + "-server"
	topologyKey                          = "topology.kubernetes.io/zone"
)

func secretConfigsFunc(namespace string) []extensionssecretsmanager.SecretConfigWithOptions {
	return []extensionssecretsmanager.SecretConfigWithOptions{
		{
			Config: &secretutils.CertificateSecretConfig{
				Name:       caNameControlPlane,
				CommonName: caNameControlPlane,
				CertType:   secretutils.CACert,
			},
			Options: []secretsmanager.GenerateOption{secretsmanager.Persist()},
		},
		{
			Config: &secretutils.CertificateSecretConfig{
				Name:                        cloudControllerManagerServerName,
				CommonName:                  gdc.CloudControllerManagerName,
				DNSNames:                    kutil.DNSNamesForService(gdc.CloudControllerManagerName, namespace),
				CertType:                    secretutils.ServerCert,
				SkipPublishingCACertificate: true,
			},
			Options: []secretsmanager.GenerateOption{secretsmanager.SignedByCA(caNameControlPlane)},
		},
		{
			Config: &secretutils.CertificateSecretConfig{
				Name:                        csiSnapshotValidationServerName,
				CommonName:                  gdc.UsernamePrefix + gdc.CSISnapshotValidationName,
				DNSNames:                    kutil.DNSNamesForService(gdc.CSISnapshotValidationName, namespace),
				CertType:                    secretutils.ServerCert,
				SkipPublishingCACertificate: true,
			},
			// use current CA for signing server cert to prevent mismatches when dropping the old CA from the webhook
			// config in phase Completing
			Options: []secretsmanager.GenerateOption{secretsmanager.SignedByCA(caNameControlPlane, secretsmanager.UseCurrentCA)},
		},
	}
}

func shootAccessSecretsFunc(namespace string) []*gutil.AccessSecret {
	return []*gutil.AccessSecret{
		gutil.NewShootAccessSecret(cloudControllerManagerDeploymentName, namespace),
		gutil.NewShootAccessSecret(gdc.CSIProvisionerName, namespace),
		gutil.NewShootAccessSecret(gdc.CSIAttacherName, namespace),
		gutil.NewShootAccessSecret(gdc.CSISnapshotterName, namespace),
		gutil.NewShootAccessSecret(gdc.CSISnapshotControllerName, namespace),
		gutil.NewShootAccessSecret(gdc.CSIDriverName, namespace),
		gutil.NewShootAccessSecret(gdc.CSIResizerName, namespace),
	}
}

var (
	configChart = &chart.Chart{
		Name:       "cloud-provider-config",
		EmbeddedFS: charts.InternalChart,
		Path:       filepath.Join(charts.InternalChartsPath, "cloud-provider-config"),
		Objects: []*chart.Object{
			{Type: &corev1.ConfigMap{}, Name: internal.CloudProviderConfigName},
		},
	}

	controlPlaneChart = &chart.Chart{
		Name:       "seed-controlplane",
		EmbeddedFS: charts.InternalChart,
		Path:       filepath.Join(charts.InternalChartsPath, "seed-controlplane"),
		SubCharts: []*chart.Chart{
			{
				Name:   gdc.CloudControllerManagerName,
				Images: []string{gdc.CloudControllerManagerImageName},
				Objects: []*chart.Object{
					{Type: &corev1.Service{}, Name: "cloud-controller-manager"},
					{Type: &appsv1.Deployment{}, Name: "cloud-controller-manager"},
					{Type: &corev1.ConfigMap{}, Name: "cloud-controller-manager-observability-config"},
				},
			},
			{
				Name: gdc.CSIControllerName,
				Images: []string{
					gdc.CSIDriverImageName,
					gdc.CSIProvisionerImageName,
					gdc.CSIAttacherImageName,
					gdc.CSISnapshotterImageName,
					gdc.CSIResizerImageName,
					gdc.CSISnapshotControllerImageName,
					gdc.CSILivenessProbeImageName,
				},
				Objects: []*chart.Object{
					{Type: &appsv1.Deployment{}, Name: gdc.CSIControllerName},
					{Type: &corev1.ConfigMap{}, Name: gdc.CSIControllerConfigName},
					{Type: &appsv1.Deployment{}, Name: gdc.CSISnapshotControllerName},
					{Type: &corev1.ConfigMap{}, Name: gdc.InfraClusterKubeconfigName},
				},
			},
		},
	}

	controlPlaneShootChart = &chart.Chart{
		Name:       "shoot-system-components",
		EmbeddedFS: charts.InternalChart,
		Path:       filepath.Join(charts.InternalChartsPath, "shoot-system-components"),
		SubCharts: []*chart.Chart{
			{
				Name: "cloud-controller-manager",
				Objects: []*chart.Object{
					{Type: &rbacv1.ClusterRole{}, Name: "system:controller:cloud-node-controller"},
					{Type: &rbacv1.ClusterRoleBinding{}, Name: "system:controller:cloud-node-controller"},
					{Type: &rbacv1.ClusterRole{}, Name: "gvm:cloud-provider"},
					{Type: &rbacv1.ClusterRoleBinding{}, Name: "gvm:cloud-provider"},
				},
			},
			{
				Name: "csi-driver-node",
				Images: []string{
					gdc.CSIDriverImageName,
					gdc.CSILivenessProbeImageName,
					gdc.CSINodeDriverRegistrarImageName,
				},
				Objects: []*chart.Object{
					{Type: &appsv1.DaemonSet{}, Name: gdc.CSINodeName},
					{Type: &storagev1.CSIDriver{}, Name: "csi.kubevirt.io"},
					{Type: &corev1.ServiceAccount{}, Name: gdc.CSINodeName},
					{Type: &rbacv1.ClusterRole{}, Name: gdc.UsernamePrefix + gdc.CSIDriverName},
					{Type: &rbacv1.ClusterRoleBinding{}, Name: gdc.UsernamePrefix + gdc.CSIDriverName},
				},
			},
		},
	}

	controlPlaneShootCRDsChart = &chart.Chart{
		Name:       "shoot-crds",
		EmbeddedFS: charts.InternalChart,
		Path:       filepath.Join(charts.InternalChartsPath, "shoot-crds"),
		SubCharts: []*chart.Chart{
			{
				Name: "volumesnapshots",
				Objects: []*chart.Object{
					{Type: &apiextensionsv1.CustomResourceDefinition{}, Name: "volumesnapshotclasses.snapshot.storage.k8s.io"},
					{Type: &apiextensionsv1.CustomResourceDefinition{}, Name: "volumesnapshotcontents.snapshot.storage.k8s.io"},
					{Type: &apiextensionsv1.CustomResourceDefinition{}, Name: "volumesnapshots.snapshot.storage.k8s.io"},
				},
			},
		},
	}

	storageClassChart = &chart.Chart{
		Name:       "shoot-storageclasses",
		EmbeddedFS: charts.InternalChart,
		Path:       filepath.Join(charts.InternalChartsPath, "shoot-storageclasses"),
	}
)

// NewValuesProvider creates a new ValuesProvider for the generic actuator.
func NewValuesProvider(mgr manager.Manager) genericactuator.ValuesProvider {
	return &valuesProvider{
		client:  mgr.GetClient(),
		decoder: serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder(),
	}
}

// valuesProvider is a ValuesProvider that provides GDCH-specific values for the 2 charts applied by the generic actuator.
type valuesProvider struct {
	genericactuator.NoopValuesProvider
	client  client.Client
	decoder runtime.Decoder
}

// GetConfigChartValues returns the values for the config chart applied by the generic actuator.
func (vp *valuesProvider) GetConfigChartValues(
	ctx context.Context,
	cp *extensionsv1alpha1.ControlPlane,
	cluster *extensionscontroller.Cluster,
) (map[string]interface{}, error) {
	// Get service account
	serviceAccount, err := gdc.GetServiceAccountFromSecretReference(ctx, vp.client, cp.Spec.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("could not get service account from secret '%s/%s': %w", cp.Spec.SecretRef.Namespace, cp.Spec.SecretRef.Name, err)
	}
	cloudProfile, err := cloudprofile.GetFromCluster(cluster, vp.decoder)
	if err != nil {
		return nil, fmt.Errorf("could not get cloud profile from cluster: %w", err)
	}

	activeZones, err := vp.getActiveZones(cp)
	if err != nil {
		return nil, fmt.Errorf("could not get active zones from controle plane: %w", err)
	}
	// Get config chart values
	return getConfigChartValues(cp, cloudProfile, serviceAccount, activeZones)
}

// GetControlPlaneChartValues returns the values for the control plane chart applied by the generic actuator.
func (vp *valuesProvider) GetControlPlaneChartValues(
	ctx context.Context,
	cp *extensionsv1alpha1.ControlPlane,
	cluster *extensionscontroller.Cluster,
	secretsReader secretsmanager.Reader,
	checksums map[string]string,
	scaledDown bool,
) (
	map[string]interface{},
	error,
) {
	// Get service account
	serviceAccount, err := gdc.GetServiceAccountFromSecretReference(ctx, vp.client, cp.Spec.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("could not get service account from secret '%s/%s': %w", cp.Spec.SecretRef.Namespace, cp.Spec.SecretRef.Name, err)
	}

	return vp.getControlPlaneChartValues(cp, cluster, secretsReader, serviceAccount, checksums, scaledDown)
}

// GetControlPlaneShootChartValues returns the values for the control plane shoot chart applied by the generic actuator.
func (vp *valuesProvider) GetControlPlaneShootChartValues(
	_ context.Context,
	cp *extensionsv1alpha1.ControlPlane,
	cluster *extensionscontroller.Cluster,
	secretsReader secretsmanager.Reader,
	_ map[string]string,
) (
	map[string]interface{},
	error,
) {
	return vp.getControlPlaneShootChartValues(cluster, cp, secretsReader)
}

// getStorageClassChartValues collects and returns the shoot storage-class chart values.
func (vp *valuesProvider) GetStorageClassesChartValues(
	_ context.Context,
	cp *extensionsv1alpha1.ControlPlane,
	cluster *extensionscontroller.Cluster,
) (map[string]interface{}, error) {
	managedDefaultStorageClass := true
	managedDefaultVolumeSnapshotClass := true

	cpConfig := &apisgdc.ControlPlaneConfig{}
	if cp.Spec.ProviderConfig != nil {
		if _, _, err := vp.decoder.Decode(cp.Spec.ProviderConfig.Raw, nil, cpConfig); err != nil {
			return nil, fmt.Errorf("could not decode providerConfig of controlplane '%s': %w", gdc.ObjectName(cp), err)
		}
	}

	if cpConfig.Storage != nil {
		managedDefaultStorageClass = ptr.Deref(cpConfig.Storage.ManagedDefaultStorageClass, true)
		managedDefaultVolumeSnapshotClass = ptr.Deref(cpConfig.Storage.ManagedDefaultVolumeSnapshotClass, true)
	}

	return map[string]interface{}{
		"managedDefaultStorageClass":        managedDefaultStorageClass,
		"managedDefaultVolumeSnapshotClass": managedDefaultVolumeSnapshotClass,
	}, nil
}

// getConfigChartValues collects and returns the configuration chart values.
func getConfigChartValues(
	cp *extensionsv1alpha1.ControlPlane,
	cloudProfile *apisgdc.CloudProfileConfig,
	serviceAccount *auth.ServiceAccount,
	activeZones []string,
) (map[string]interface{}, error) {
	caData := cloudProfile.OrgConfig.CAData
	globalManagementServerURL := cloudProfile.OrgConfig.GlobalManagementAPI
	zones := filtereCloudProfileZones(cloudProfile.OrgConfig.Zones, activeZones)

	// Collect config chart values
	return map[string]interface{}{
		"project":                   serviceAccount.Project,
		"globalManagementServerURL": globalManagementServerURL,
		"zones":                     zones,
		"caData":                    caData,
		"nodeTags":                  cp.Namespace,
	}, nil
}

// Filter cloudprofile zones based on infra's active zones
func filtereCloudProfileZones(cloudProfileZones []*apisgdc.ZoneEndpoints, activeZones []string) []*apisgdc.ZoneEndpoints {
	zones := make([]*apisgdc.ZoneEndpoints, 0)
	allowedZonesSet := make(map[string]struct{}, len(activeZones))
	for _, zoneName := range activeZones {
		allowedZonesSet[zoneName] = struct{}{}
	}
	for _, zone := range cloudProfileZones {
		if _, ok := allowedZonesSet[zone.Name]; ok {
			zones = append(zones, zone)
		}
	}
	return zones
}

// getControlPlaneChartValues collects and returns the control plane chart values.
func (vp *valuesProvider) getControlPlaneChartValues(
	cp *extensionsv1alpha1.ControlPlane,
	cluster *extensionscontroller.Cluster,
	secretsReader secretsmanager.Reader,
	serviceAccount *auth.ServiceAccount,
	checksums map[string]string,
	scaledDown bool,
) (
	map[string]interface{},
	error,
) {
	ccm, err := vp.getCCMChartValues(cp, cluster, secretsReader, checksums, scaledDown)
	if err != nil {
		return nil, err
	}

	csi, err := vp.getCSIControllerChartValues(cp, cluster, secretsReader, serviceAccount, checksums, scaledDown)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"global": map[string]interface{}{
			"genericTokenKubeconfigSecretName": extensionscontroller.GenericTokenKubeconfigSecretNameFromCluster(cluster),
		},
		gdc.CloudControllerManagerName: ccm,
		gdc.CSIControllerName:          csi,
	}, nil
}

// getCCMChartValues collects and returns the CCM chart values.
func (vp *valuesProvider) getCCMChartValues(
	cp *extensionsv1alpha1.ControlPlane,
	cluster *extensionscontroller.Cluster,
	secretsReader secretsmanager.Reader,
	checksums map[string]string,
	scaledDown bool,
) (map[string]interface{}, error) {
	serverSecret, found := secretsReader.Get(cloudControllerManagerServerName)
	if !found {
		return nil, fmt.Errorf("secret %q not found", cloudControllerManagerServerName)
	}

	values := map[string]interface{}{
		"enabled":           true,
		"replicas":          extensionscontroller.GetControlPlaneReplicas(cluster, scaledDown, 1),
		"clusterName":       cp.Namespace,
		"kubernetesVersion": cluster.Shoot.Spec.Kubernetes.Version,
		// Defines the network CIDR for pods within the cluster.
		"podNetwork": extensionscontroller.GetPodNetwork(cluster),
		"podAnnotations": map[string]interface{}{
			// Ensures pods are restarted when the cloud provider secret changes
			"checksum/secret-" + v1beta1constants.SecretNameCloudProvider: checksums[v1beta1constants.SecretNameCloudProvider],
			// Triggers pod restarts upon changes to the cloud provider configuration, ensuring the component operates with the latest settings
			"checksum/configmap-" + internal.CloudProviderConfigName: checksums[internal.CloudProviderConfigName],
		},
		"podLabels": map[string]interface{}{
			// Used by automated processes to trigger pod restarts during maintenance windows
			v1beta1constants.LabelPodMaintenanceRestart: "true",
		},
		// Specifies the allowed TLS cipher suites for secure communications.
		"tlsCipherSuites": kutil.TLSCipherSuites,
		"secrets": map[string]interface{}{
			"server": serverSecret.Name,
		},
	}

	return values, nil
}

// getCSIControllerChartValues collects and returns the CSIController chart values.
func (vp *valuesProvider) getCSIControllerChartValues(
	cp *extensionsv1alpha1.ControlPlane,
	cluster *extensionscontroller.Cluster,
	secretsReader secretsmanager.Reader,
	serviceAccount *auth.ServiceAccount,
	checksums map[string]string,
	scaledDown bool,
) (map[string]interface{}, error) {
	serverSecret, found := secretsReader.Get(csiSnapshotValidationServerName)
	if !found {
		return nil, fmt.Errorf("secret %q not found", csiSnapshotValidationServerName)
	}
	cloudProfile, err := cloudprofile.GetFromCluster(cluster, vp.decoder)
	if err != nil {
		return nil, fmt.Errorf("could not get cloud profile from cluster: %w", err)
	}
	caData := cloudProfile.OrgConfig.CAData

	var infraClusterKubeconfigs = make(map[string]string)

	sa := &auth.ServiceAccount{
		Name:         serviceAccount.Name,
		PrivateKey:   serviceAccount.PrivateKey,
		PrivateKeyID: serviceAccount.PrivateKeyID,
		Project:      serviceAccount.Project,
		TokenURI:     serviceAccount.TokenURI,
	}

	generateKubeconfig := func(url string) (string, error) {
		orgInfraConfig := &gdcclient.OrgClusterConfig{
			OrgClusterURL: url,
			CAData:        caData,
		}
		return kubeconfig.Raw(orgInfraConfig, sa)
	}

	for _, zone := range cloudProfile.OrgConfig.Zones {
		infraKubeconfig, err := generateKubeconfig(zone.InfrastructureAPI)
		if err != nil {
			return nil, fmt.Errorf("could not generate kubeconfig for zone %s: %w", zone.Name, err)
		}
		infraClusterKubeconfigs[zone.Name] = infraKubeconfig
	}

	zones, err := vp.getActiveZones(cp)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"enabled":  true,
		"replicas": extensionscontroller.GetControlPlaneReplicas(cluster, scaledDown, 1),
		"project":  serviceAccount.Project,
		"podAnnotations": map[string]interface{}{
			// Triggers pod restarts when the cloud provider's secret changes
			"checksum/secret-" + v1beta1constants.SecretNameCloudProvider: checksums[v1beta1constants.SecretNameCloudProvider],
		},
		// Snapshot controller is responsible for managing snapshot objects
		"csiSnapshotController": map[string]interface{}{
			"replicas": extensionscontroller.GetControlPlaneReplicas(cluster, scaledDown, 1),
		},
		// Configuration for the CSI snapshot validation webhook. Webhook validates snapshot requests before they are processed, ensuring that only valid requests are executed.
		"csiSnapshotValidationWebhook": map[string]interface{}{
			"replicas": extensionscontroller.GetControlPlaneReplicas(cluster, scaledDown, 1),
			"secrets": map[string]interface{}{
				"server": serverSecret.Name,
			},
		},
		"infraClusterkubeconfig": infraClusterKubeconfigs,
		"topologyKey":            topologyKey,
		"zones":                  zones,
		"infraClusterNamespace":  serviceAccount.Project,
	}, nil
}

// getControlPlaneShootChartValues collects and returns the control plane shoot chart values.
func (vp *valuesProvider) getControlPlaneShootChartValues(
	cluster *extensionscontroller.Cluster,
	cp *extensionsv1alpha1.ControlPlane,
	secretsReader secretsmanager.Reader,
) (map[string]interface{}, error) {
	kubernetesVersion := cluster.Shoot.Spec.Kubernetes.Version
	caSecret, found := secretsReader.Get(caNameControlPlane)
	if !found {
		return nil, fmt.Errorf("secret %q not found", caNameControlPlane)
	}

	return map[string]interface{}{
		gdc.CloudControllerManagerName: map[string]interface{}{"enabled": true},
		gdc.CSINodeName: map[string]interface{}{
			"enabled":           true,
			"kubernetesVersion": kubernetesVersion,
			// Determines whether Vertical Pod Autoscaler (VPA) is enabled for the CSI components.
			"vpaEnabled": gardencorev1beta1helper.ShootWantsVerticalPodAutoscaler(cluster.Shoot),
			"webhookConfig": map[string]interface{}{
				// The URL for the CSI snapshot validation webhook, which is responsible for validating snapshot creation requests.
				"url": "https://" + gdc.CSISnapshotValidationName + "." + cp.Namespace + "/volumesnapshot",
				// The CA bundle used for the webhook server
				"caBundle": string(caSecret.Data[secretutils.DataKeyCertificateBundle]),
			},
			"topologyKey": topologyKey,
		},
	}, nil
}

func (vp *valuesProvider) getActiveZones(cp *extensionsv1alpha1.ControlPlane) ([]string, error) {
	infraStatus := &apisgdc.InfrastructureStatus{}
	if cp.Spec.InfrastructureProviderStatus != nil {
		if _, _, err := vp.decoder.Decode(cp.Spec.InfrastructureProviderStatus.Raw, nil, infraStatus); err != nil {
			return nil, fmt.Errorf("could not decode infrastructureProviderStatus of controlplane %q: %w", gdc.ObjectName(cp), err)
		}
	}

	zones := make([]string, len(infraStatus.Networks.Zones))
	for i, zone := range infraStatus.Networks.Zones {
		zones[i] = zone.Name
	}
	return zones, nil
}
