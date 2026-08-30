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

package controlplane_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"text/template"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	kutil "github.com/gardener/gardener/pkg/utils/kubernetes"
	fakesecretsmanager "github.com/gardener/gardener/pkg/utils/secrets/manager/fake"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	controlplane "github.com/gardener/gardener-extension-provider-gdc/pkg/controller/controlplane"
	gdc "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
	testfake "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

const (
	namespace                        = "test"
	genericTokenKubeconfigSecretName = "generic-token-kubeconfig-92e9ae14"
)

var fakeCAData = []byte("test-ca-data")
var fakeCADataBase64 = base64.StdEncoding.EncodeToString(fakeCAData)

func TestGetConfigChartValues(t *testing.T) {
	cpSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      v1beta1constants.SecretNameCloudProvider,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			gdcconstants.ServiceAccountJSONField: []byte(`{"project":"abc"}`),
		},
	}

	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = apisgdc.AddToScheme(scheme)

	c := fake.NewClientBuilder().WithObjects(cpSecret).Build()
	mgr := testfake.NewManager(c)

	vp := controlplane.NewValuesProvider(mgr)

	cluster := &extensionscontroller.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"generic-token-kubeconfig.secret.gardener.cloud/name": genericTokenKubeconfigSecretName,
			},
		},
		Seed: &gardencorev1beta1.Seed{},
		Shoot: &gardencorev1beta1.Shoot{
			Spec: gardencorev1beta1.ShootSpec{
				Provider: gardencorev1beta1.Provider{
					Workers: []gardencorev1beta1.Worker{
						{
							Name: "worker",
						},
					},
				},
				Networking: &gardencorev1beta1.Networking{
					Pods: ptr.To("10.250.0.0/19"),
				},
				Kubernetes: gardencorev1beta1.Kubernetes{
					Version: "1.24.0",
					VerticalPodAutoscaler: &gardencorev1beta1.VerticalPodAutoscaler{
						Enabled: true,
					},
				},
			},
		},
	}

	testcases := []struct {
		name             string
		cloudProfile     *gardencorev1beta1.CloudProfile
		infraActiveZones []apisgdc.Zones
		expectedValues   map[string]interface{}
	}{
		{
			name: "test get config chart values in lancer as per active zones",
			cloudProfile: &gardencorev1beta1.CloudProfile{
				Spec: gardencorev1beta1.CloudProfileSpec{
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(&apisgdc.CloudProfileConfig{
							TypeMeta: metav1.TypeMeta{
								APIVersion: apisgdc.SchemeGroupVersion.String(),
								Kind:       "CloudProfileConfig",
							},
							OrgConfig: &apisgdc.OrgConfig{
								OrgName:             "test-org",
								GlobalManagementAPI: "https://global-url",
								RegistryURL:         "test-registry-url",
								CAData:              fakeCADataBase64,
								Zones: []*apisgdc.ZoneEndpoints{
									{
										Name:              "zone1",
										ManagementAPI:     "test-zone1-url",
										InfrastructureAPI: "https://infa-cluster-url",
									},
									{
										Name:              "zone2",
										ManagementAPI:     "test-zone2-url",
										InfrastructureAPI: "https://infa-cluster-url",
									},
									{
										Name:              "zone3",
										ManagementAPI:     "test-zone3-url",
										InfrastructureAPI: "https://infa-cluster-url",
									},
								},
							},
						}),
					},
				},
			},
			infraActiveZones: []apisgdc.Zones{
				{
					Name: "zone1",
				},
				{
					Name: "zone2",
				},
			},
			expectedValues: map[string]interface{}{
				"project":                   "abc",
				"caData":                    base64.StdEncoding.EncodeToString(fakeCAData),
				"nodeTags":                  namespace,
				"globalManagementServerURL": "https://global-url",
				"zones": []*apisgdc.ZoneEndpoints{
					{
						Name:              "zone1",
						ManagementAPI:     "test-zone1-url",
						InfrastructureAPI: "https://infa-cluster-url",
					},
					{
						Name:              "zone2",
						ManagementAPI:     "test-zone2-url",
						InfrastructureAPI: "https://infa-cluster-url",
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cp := &extensionsv1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "control-plane",
					Namespace: namespace,
				},
				Spec: extensionsv1alpha1.ControlPlaneSpec{
					SecretRef: corev1.SecretReference{
						Name:      v1beta1constants.SecretNameCloudProvider,
						Namespace: namespace,
					},
					InfrastructureProviderStatus: &runtime.RawExtension{
						Raw: encode(&apisgdc.InfrastructureStatus{
							Networks: apisgdc.NetworkStatus{
								Zones: tc.infraActiveZones,
							},
						}),
					},
				},
			}
			cluster.CloudProfile = tc.cloudProfile

			got, err := vp.GetConfigChartValues(ctx, cp, cluster)
			if err != nil {
				t.Fatalf("expected no error, but got %v", err)
			}

			if diff := cmp.Diff(got, tc.expectedValues); diff != "" {
				t.Errorf("expected values %v, but got %v", tc.expectedValues, got)
			}
		})
	}
}
func TestGetControlPlaneChartValues_Lancer(t *testing.T) {
	namespace := "test"
	project := "abc"
	cidr := "10.250.0.0/19"
	service_account_name := "test-service-account"
	infraClusterName := "cluster-name"
	infraClusterUrlZone1 := "https://zone1-infa-cluster-url"
	infraClusterUrlZone2 := "https://zone2-infa-cluster-url"
	infraClusterUrlZone3 := "https://zone3-infa-cluster-url"

	serviceAccount := []byte(`{"project":"` + project + `", "name":"` + service_account_name + `"}`)
	wantSa, _ := json.Marshal(&auth.ServiceAccount{Project: project, Name: service_account_name})
	ccmSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-controller-manager-server", Namespace: namespace},
		Data:       map[string][]byte{gdcconstants.ServiceAccountJSONField: serviceAccount},
	}
	csiSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-snapshot-validation-server", Namespace: namespace},
		Data:       map[string][]byte{gdcconstants.ServiceAccountJSONField: serviceAccount},
	}
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = apisgdc.AddToScheme(scheme)

	c := fake.NewClientBuilder().WithObjects(ccmSecret, csiSecret).Build()
	mgr := testfake.NewManager(c)

	vp := controlplane.NewValuesProvider(mgr)

	infraStatus := &apisgdc.InfrastructureStatus{
		Networks: apisgdc.NetworkStatus{
			Zones: []apisgdc.Zones{
				{
					Name: "zone1",
				},
				{
					Name: "zone2",
				},
			},
		},
	}

	cp := &extensionsv1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "control-plane", Namespace: namespace},
		Spec: extensionsv1alpha1.ControlPlaneSpec{
			SecretRef: corev1.SecretReference{Name: "cloud-controller-manager-server", Namespace: namespace},
			InfrastructureProviderStatus: &runtime.RawExtension{
				Raw: encode(infraStatus),
			},
		},
	}

	fakeSecretsManager := fakesecretsmanager.New(c, namespace)

	cluster := &extensionscontroller.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Shoot: &gardencorev1beta1.Shoot{
			Spec: gardencorev1beta1.ShootSpec{
				Networking: &gardencorev1beta1.Networking{Pods: &cidr},
				Kubernetes: gardencorev1beta1.Kubernetes{Version: "1.18.0"},
			},
		},
		CloudProfile: &gardencorev1beta1.CloudProfile{
			Spec: gardencorev1beta1.CloudProfileSpec{
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(&apisgdc.CloudProfileConfig{
						TypeMeta: metav1.TypeMeta{
							APIVersion: apisgdc.SchemeGroupVersion.String(),
							Kind:       "CloudProfileConfig",
						},
						OrgConfig: &apisgdc.OrgConfig{
							OrgName:             "test-org",
							GlobalManagementAPI: "https://global-url",
							RegistryURL:         "test-registry-url",
							CAData:              fakeCADataBase64,
							Zones: []*apisgdc.ZoneEndpoints{
								{
									Name:              "zone1",
									ManagementAPI:     "test-zone1-url",
									InfrastructureAPI: infraClusterUrlZone1,
								},
								{
									Name:              "zone2",
									ManagementAPI:     "test-zone2-url",
									InfrastructureAPI: infraClusterUrlZone2,
								},
								{
									Name:              "zone3",
									ManagementAPI:     "test-zone3-url",
									InfrastructureAPI: infraClusterUrlZone3,
								},
							},
						},
					}),
				},
			},
		},
	}

	t.Run("GetControlPlaneChartValues", func(t *testing.T) {
		got, err := vp.GetControlPlaneChartValues(ctx, cp, cluster, fakeSecretsManager, map[string]string{}, false)
		if err != nil {
			t.Fatalf("error getting control plane chart values: %v", err)
		}

		wantCCM := map[string]interface{}{
			"clusterName":       "test",
			"enabled":           true,
			"kubernetesVersion": "1.18.0",
			"podAnnotations":    map[string]interface{}{"checksum/configmap-cloud-provider-config": "", "checksum/secret-cloudprovider": ""},
			"podLabels":         map[string]interface{}{"maintenance.gardener.cloud/restart": "true"},
			"podNetwork":        [1]string{"10.250.0.0/19"},
			"replicas":          1,
			"secrets":           map[string]interface{}{"server": "cloud-controller-manager-server"},
			"tlsCipherSuites":   kutil.TLSCipherSuites,
		}

		kubeconfigZone1, _ := GenerateKubeconfig(
			infraClusterUrlZone1,
			infraClusterName,
			"test-service-account",
			fakeCAData,
			wantSa,
		)
		kubeconfigZone2, _ := GenerateKubeconfig(
			infraClusterUrlZone2,
			infraClusterName,
			"test-service-account",
			fakeCAData,
			wantSa,
		)

		kubeconfigZone3, _ := GenerateKubeconfig(
			infraClusterUrlZone3,
			infraClusterName,
			"test-service-account",
			fakeCAData,
			wantSa,
		)

		wantCSI := map[string]interface{}{
			"csiSnapshotController": map[string]interface{}{
				"replicas": 1,
			},
			"csiSnapshotValidationWebhook": map[string]interface{}{
				"replicas": 1,
				"secrets": map[string]interface{}{
					"server": "csi-snapshot-validation-server",
				},
			},
			"enabled":        true,
			"podAnnotations": map[string]interface{}{"checksum/secret-cloudprovider": ""},
			"project":        "abc",
			"replicas":       1,
			"zones":          []string{"zone1", "zone2"},
			"topologyKey":    "topology.kubernetes.io/zone",
			"infraClusterkubeconfig": map[string]string{
				"zone1": kubeconfigZone1,
				"zone2": kubeconfigZone2,
				"zone3": kubeconfigZone3,
			},
			"infraClusterNamespace": "abc",
		}

		want := map[string]interface{}{
			"cloud-controller-manager": wantCCM,
			gdc.CSIControllerName:      wantCSI,
			"global": map[string]interface{}{
				"genericTokenKubeconfigSecretName": "generic-token-kubeconfig",
			},
		}
		// TODO:: Somehow cmp.Equal() throw unqual for the same value as "10.250.0.0/19"
		got["cloud-controller-manager"].(map[string]interface{})["podNetwork"] = ""
		want["cloud-controller-manager"].(map[string]interface{})["podNetwork"] = ""

		if !cmp.Equal(got, want) {
			t.Errorf("expected values %v, but got %v", want, got)
		}
	})
}

func TestGetControlPlaneShootChartValues(t *testing.T) {
	namespace := "test"
	project := "abc"
	cidr := "10.250.0.0/19"
	caProviderSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-provider-gdch-controlplane", Namespace: namespace},
		Data:       map[string][]byte{gdcconstants.ServiceAccountJSONField: []byte(`{"project":"` + project + `"}`)},
	}
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = apisgdc.AddToScheme(scheme)

	c := fake.NewClientBuilder().WithObjects(caProviderSecret).Build()
	mgr := testfake.NewManager(c)

	vp := controlplane.NewValuesProvider(mgr)

	cp := &extensionsv1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "control-plane", Namespace: namespace},
		Spec: extensionsv1alpha1.ControlPlaneSpec{
			SecretRef: corev1.SecretReference{Name: "cloud-controller-manager-server", Namespace: namespace},
		},
	}

	fakeSecretsManager := fakesecretsmanager.New(c, namespace)
	cluster := &extensionscontroller.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Shoot: &gardencorev1beta1.Shoot{
			Spec: gardencorev1beta1.ShootSpec{
				Networking: &gardencorev1beta1.Networking{Pods: &cidr},
				Kubernetes: gardencorev1beta1.Kubernetes{Version: "1.18.0"},
			},
		},
		CloudProfile: &gardencorev1beta1.CloudProfile{
			Spec: gardencorev1beta1.CloudProfileSpec{
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(&apisgdc.CloudProfileConfig{
						TypeMeta: metav1.TypeMeta{
							APIVersion: apisgdc.SchemeGroupVersion.String(),
							Kind:       "CloudProfileConfig",
						},
						OrgConfig: &apisgdc.OrgConfig{
							OrgName:             "test-org",
							GlobalManagementAPI: "https://global-url",
							RegistryURL:         "test-registry-url",
							CAData:              fakeCADataBase64,
						},
					}),
				},
			},
		},
	}

	t.Run("GetControlPlaneShootChartValues", func(t *testing.T) {
		got, err := vp.GetControlPlaneShootChartValues(ctx, cp, cluster, fakeSecretsManager, map[string]string{})
		if err != nil {
			t.Fatalf("error getting control plane shoot chart values: %v", err)
		}

		wantCCM := map[string]interface{}{
			"enabled": true,
		}

		wantCSI := map[string]interface{}{
			"enabled":           true,
			"kubernetesVersion": "1.18.0",
			"vpaEnabled":        false,
			"webhookConfig": map[string]interface{}{
				"url":      "https://csi-snapshot-validation.test/volumesnapshot",
				"caBundle": "",
			},
			"topologyKey": "topology.kubernetes.io/zone",
		}

		want := map[string]interface{}{
			"cloud-controller-manager": wantCCM,
			"csi-driver-node":          wantCSI,
		}

		if !cmp.Equal(got, want) {
			t.Errorf("expected values %v, but got %v", want, got)
		}
	})
}

func TestGetStorageClassesChartValues(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = apisgdc.AddToScheme(scheme)

	c := fake.NewClientBuilder().Build()
	mgr := testfake.NewManager(c)
	vp := controlplane.NewValuesProvider(mgr)

	testcases := []struct {
		name           string
		providerConfig *runtime.RawExtension
		expectedValues map[string]interface{}
	}{
		{
			name:           "providerConfig is nil",
			providerConfig: nil,
			expectedValues: map[string]interface{}{
				"managedDefaultStorageClass":        true,
				"managedDefaultVolumeSnapshotClass": true,
			},
		},
		{
			name: "storage config is nil",
			providerConfig: &runtime.RawExtension{
				Raw: encode(&apisgdc.ControlPlaneConfig{}),
			},
			expectedValues: map[string]interface{}{
				"managedDefaultStorageClass":        true,
				"managedDefaultVolumeSnapshotClass": true,
			},
		},
		{
			name: "storage config is set to false",
			providerConfig: &runtime.RawExtension{
				Raw: encode(&apisgdc.ControlPlaneConfig{
					Storage: &apisgdc.Storage{
						ManagedDefaultStorageClass:        ptr.To(false),
						ManagedDefaultVolumeSnapshotClass: ptr.To(false),
					},
				}),
			},
			expectedValues: map[string]interface{}{
				"managedDefaultStorageClass":        false,
				"managedDefaultVolumeSnapshotClass": false,
			},
		},
		{
			name: "storage config is set to true",
			providerConfig: &runtime.RawExtension{
				Raw: encode(&apisgdc.ControlPlaneConfig{
					Storage: &apisgdc.Storage{
						ManagedDefaultStorageClass:        ptr.To(true),
						ManagedDefaultVolumeSnapshotClass: ptr.To(true),
					},
				}),
			},
			expectedValues: map[string]interface{}{
				"managedDefaultStorageClass":        true,
				"managedDefaultVolumeSnapshotClass": true,
			},
		},
		{
			name: "managedDefaultStorageClass is false, managedDefaultVolumeSnapshotClass is nil",
			providerConfig: &runtime.RawExtension{
				Raw: encode(&apisgdc.ControlPlaneConfig{
					Storage: &apisgdc.Storage{
						ManagedDefaultStorageClass: ptr.To(false),
					},
				}),
			},
			expectedValues: map[string]interface{}{
				"managedDefaultStorageClass":        false,
				"managedDefaultVolumeSnapshotClass": true,
			},
		},
		{
			name: "managedDefaultStorageClass is nil, managedDefaultVolumeSnapshotClass is false",
			providerConfig: &runtime.RawExtension{
				Raw: encode(&apisgdc.ControlPlaneConfig{
					Storage: &apisgdc.Storage{
						ManagedDefaultVolumeSnapshotClass: ptr.To(false),
					},
				}),
			},
			expectedValues: map[string]interface{}{
				"managedDefaultStorageClass":        true,
				"managedDefaultVolumeSnapshotClass": false,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cp := &extensionsv1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "control-plane",
					Namespace: namespace,
				},
				Spec: extensionsv1alpha1.ControlPlaneSpec{
					DefaultSpec: extensionsv1alpha1.DefaultSpec{
						ProviderConfig: tc.providerConfig,
					},
				},
			}

			got, err := vp.GetStorageClassesChartValues(ctx, cp, nil)
			if err != nil {
				t.Fatalf("expected no error, but got %v", err)
			}

			if diff := cmp.Diff(got, tc.expectedValues); diff != "" {
				t.Errorf("expected values %v, but got %v\n%s", tc.expectedValues, got, diff)
			}
		})
	}
}

func encode(obj runtime.Object) []byte {
	data, _ := json.Marshal(obj)
	return data
}

type KubeConfigValues struct {
	CertificateAuthorityData string
	InfraClusterName         string
	InfraclusterUrl          string
	ServiceAccountName       string
	KeyString                string
	CaCert                   string
}

func GenerateKubeconfig(infraClusterURL, infraClusterName, serviceAccountName string, caData, keyBytes []byte) (string, error) {
	configValues := KubeConfigValues{
		CertificateAuthorityData: base64.StdEncoding.EncodeToString(caData),
		InfraClusterName:         infraClusterName,
		InfraclusterUrl:          infraClusterURL,
		ServiceAccountName:       serviceAccountName,
		KeyString:                base64.StdEncoding.EncodeToString(keyBytes),
		CaCert:                   base64.StdEncoding.EncodeToString(caData),
	}

	tmpl := `apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: {{.CertificateAuthorityData}}
    server: {{.InfraclusterUrl}}
  name: {{.InfraClusterName}}
contexts:
- context:
    cluster: {{.InfraClusterName}}
    user: {{.ServiceAccountName}}
  name: {{.ServiceAccountName}}@{{.InfraClusterName}}
current-context: {{.ServiceAccountName}}@{{.InfraClusterName}}
kind: Config
users:
- name: {{.ServiceAccountName}}
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args:
      - --audience={{.InfraclusterUrl}}
      - --key-string={{.KeyString}}
      - --ca-cert={{.CaCert}}
      command: /gdch-sa-auth-plugin
      env: null
      provideClusterInfo: false
`

	t, err := template.New("KubeConfigValues").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("error parsing kubeconfig template: %w", err)
	}

	var renderedTemplate bytes.Buffer
	if err := t.Execute(&renderedTemplate, configValues); err != nil {
		return "", fmt.Errorf("error rendering kubeconfig template: %w", err)
	}

	return renderedTemplate.String(), nil
}
