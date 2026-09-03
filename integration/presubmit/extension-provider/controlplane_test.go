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

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"

	resourcemanagerconfigv1alpha1 "github.com/gardener/gardener/pkg/apis/config/resourcemanager/v1alpha1"
	resourcesv1alpha1 "github.com/gardener/gardener/pkg/apis/resources/v1alpha1"
	"github.com/gardener/gardener/pkg/component/gardener/resourcemanager"
	"github.com/gardener/gardener/pkg/controllerutils"
	"github.com/gardener/gardener/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	k8sjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/kubernetes"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"

	cpactuator "github.com/gardener/gardener/extensions/pkg/controller/controlplane/genericactuator"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
)

const (
	yamlDecoderBufferSize = 4096
	kindConfigMap         = "ConfigMap"
	cloudProviderConfig   = "cloud-provider-config"
)

type controlPlaneTestFixture struct {
	*commonTestFixture

	controlPlaneNamespace string
	availableZones        []string
}

func (f *controlPlaneTestFixture) test(t *testing.T) {
	ctx := context.Background()

	// Create a dedicated, isolated vcluster client for this subtest
	f.vucClient = f.NewVClusterClient(t)

	if f.controlPlaneNamespace == "" {
		t.Fatalf("controlPlaneNamespace is not set")
	}

	// Create unique namespace for the ControlPlane object
	if err := f.vucClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.controlPlaneNamespace,
		},
	}); err != nil {
		t.Fatalf("cannot create namespace for controlplane test %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: f.controlPlaneNamespace,
			},
		}); err != nil {
			t.Fatalf("cannot delete namespace for controlplane test %v", err)
		}
	})

	// Deploy ResourceManager CRDs
	f.deployResourceManagerCRDs(t, ctx)

	t.Run("ControlPlaneCreation", func(t *testing.T) {
		// Arrange
		sa, err := os.ReadFile(*safile)
		if err != nil {
			t.Fatalf("cannot read service account file %v", err)
		}
		rawGDCHConfig, err := json.Marshal(f.gdchConfig)
		if err != nil {
			t.Fatalf("cannot marshal gdch-config %v", err)
		}
		saSecretName := "sa-" + ptr.Deref(commitHash, "")
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      saSecretName,
				Namespace: f.controlPlaneNamespace,
			},
			Data: map[string][]byte{
				"serviceaccount.json": sa,
				"gdch-config":         rawGDCHConfig,
			},
		}
		if err := f.vucClient.Create(ctx, secret); err != nil {
			t.Fatalf("unable to create Secret for controlplane test %v", err)
		}
		t.Cleanup(func() {
			if err := f.vucClient.Delete(ctx, secret); err != nil {
				t.Fatalf("unable to delete Secret for controlplane test %v", err)
			}
		})
		// Create Cluster resource
		zones := []*v1alpha1.ZoneEndpoints{}
		for _, z := range f.availableZones {
			zones = append(zones, &v1alpha1.ZoneEndpoints{
				Name:              z,
				ManagementAPI:     fmt.Sprintf("https://management-kube.apiserver.%s.%s.%s", *org, z, *labURL),
				InfrastructureAPI: fmt.Sprintf("https://infra-kube.apiserver.%s.%s.%s", *org, z, *labURL),
			})
		}
		gdchCloudProfile := &v1alpha1.CloudProfileConfig{
			OrgConfig: &v1alpha1.OrgConfig{
				OrgName:             *org,
				CAData:              f.gdchConfig.CAData,
				GlobalManagementAPI: fmt.Sprintf("https://global-api.%s.%s.%s", *org, *zone, *labURL),
				Zones:               zones,
			},
		}
		cluster := &extensionsv1alpha1.Cluster{
			TypeMeta: metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{
				Name: f.controlPlaneNamespace,
			},
			Spec: extensionsv1alpha1.ClusterSpec{
				Seed: &runtime.RawExtension{Raw: encode(t, &gardencorev1beta1.Seed{})},
				Shoot: runtime.RawExtension{Raw: encode(t, &gardencorev1beta1.Shoot{
					Spec: gardencorev1beta1.ShootSpec{
						Kubernetes: gardencorev1beta1.Kubernetes{
							// Same version as the Standard User Cluster
							Version: k8sVersion(),
						},
					},
				})},
				CloudProfile: runtime.RawExtension{
					Raw: encode(t, &gardencorev1beta1.CloudProfile{
						Spec: gardencorev1beta1.CloudProfileSpec{
							ProviderConfig: &runtime.RawExtension{
								Raw: encode(t, gdchCloudProfile),
							},
						},
					}),
				},
			},
		}
		if err := f.vucClient.Create(ctx, cluster); err != nil {
			t.Fatalf("unable to create Cluster obj %v", err)
		}
		t.Cleanup(func() {
			if err := f.vucClient.Delete(ctx, cluster); err != nil {
				t.Fatalf("unable to delete Cluster obj %v", err)
			}
		})

		// Action
		cp := &extensionsv1alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "controlplane-" + *commitHash,
				Namespace: f.controlPlaneNamespace,
			},
			Spec: extensionsv1alpha1.ControlPlaneSpec{
				DefaultSpec: extensionsv1alpha1.DefaultSpec{
					Type: "gdch",
				},
				Region: *region,
				SecretRef: corev1.SecretReference{
					Name:      saSecretName,
					Namespace: f.controlPlaneNamespace,
				},
			},
		}
		if err := f.vucClient.Create(ctx, cp); err != nil {
			t.Fatalf("failed to create ControlPlane %v", err)
		}

		// Bypass ManagedResource reconciliation since gardener-resource-manager is not running in vuc.
		// This asynchronously waits for ManagedResource secrets and manually applies the resources they contain.
		go f.bypassManagedResourceReconciliation(ctx, t)
		t.Cleanup(func() {
			t.Logf(`cleaning up controlplane object "%s/%s"`, cp.Namespace, cp.Name)
			if err := f.vucClient.Delete(ctx, cp); err != nil {
				if apierrors.IsNotFound(err) {
					return
				}
				t.Fatalf("cannot delete controlplane object %v", err)
			}
			if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
				if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(cp), cp); err != nil {
					if apierrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}
				return false, nil
			}); err != nil {
				t.Fatalf("error waiting for controlplane object to be deleted: %v", err)
			}
		})

		// Assert
		controlplaneList := &extensionsv1alpha1.ControlPlaneList{}
		listOptions := []client.ListOption{
			client.InNamespace(f.controlPlaneNamespace),
			client.MatchingFields{"metadata.name": cp.Name},
		}
		if err := kubernetes.WaitForCondition[*extensionsv1alpha1.ControlPlane](ctx, 5*time.Minute, func() (watch.Interface, error) {
			return f.vucClient.Watch(ctx, controlplaneList, listOptions...)
		}, func(obj *extensionsv1alpha1.ControlPlane) bool {
			t.Logf("Waiting for %q, LastError: %q",
				obj.Name,
				ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
			lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
			return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
		}); err != nil {
			t.Fatalf("ControlPlane is not Succeeded in 5 minutes %v", err)
		}
	})
}

func (f *controlPlaneTestFixture) deployResourceManagerCRDs(t *testing.T, ctx context.Context) {
	scheme := runtime.NewScheme()
	utilruntime.Must(resourcemanagerconfigv1alpha1.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
	ser := k8sjson.NewSerializerWithOptions(k8sjson.DefaultMetaFactory, scheme, scheme, k8sjson.SerializerOptions{
		Yaml:   true,
		Pretty: false,
		Strict: false,
	})
	versions := schema.GroupVersions([]schema.GroupVersion{
		resourcemanagerconfigv1alpha1.SchemeGroupVersion,
		apiextensionsv1.SchemeGroupVersion,
	})
	codec := serializer.NewCodecFactory(scheme).CodecForVersions(ser, ser, versions, versions)
	obj, err := runtime.Decode(codec, []byte(resourcemanager.CRD))
	if err != nil {
		t.Fatalf("cannot decode resource manager CRDs")
	}
	desiredCRD, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		t.Fatalf("expected *apiextensionsv1.CustomResourceDefinition but got %T", obj)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: desiredCRD.Name}}
	if _, err = controllerutils.GetAndCreateOrMergePatch(ctx, f.vucClient, crd, func() error {
		crd.Annotations = utils.MergeStringMaps(crd.Annotations, desiredCRD.Annotations)
		crd.Labels = utils.MergeStringMaps(crd.Labels, desiredCRD.Labels)
		crd.Spec = desiredCRD.Spec
		return nil
	}); err != nil {
		t.Fatalf("failed to deploy Resource Manager CRD %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, crd); err != nil {
			t.Fatalf("unable to clean up Resource Manager CRD, %v", err)
		}
	})
}

func (f *controlPlaneTestFixture) bypassManagedResourceReconciliation(ctx context.Context, t *testing.T) {
	mrNames := []string{cpactuator.ControlPlaneSeedConfigurationChartResourceName, cpactuator.ShootWebhooksResourceName}
	for _, mrName := range mrNames {
		go f.reconcileManagedResourceBypass(ctx, t, mrName)
	}
}

func (f *controlPlaneTestFixture) reconcileManagedResourceBypass(ctx context.Context, t *testing.T, name string) {
	secret, err := f.waitForManagedResourceSecret(ctx, name)
	if err != nil {
		t.Logf("Failed to wait for secret for MR %s: %v", name, err)
		return
	}

	if err := f.applyCloudProviderConfigFromSecret(ctx, secret); err != nil {
		t.Logf("Failed to apply cloud provider config from secret for MR %s: %v", name, err)
	}

	// Periodically mark the ManagedResource healthy as soon as it is created by the actuator,
	// and keep conditions updated throughout the test to bypass gardener-resource-manager.
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := f.markManagedResourceHealthy(ctx, name); err != nil && !apierrors.IsNotFound(err) {
				t.Logf("Failed to mark ManagedResource %s as healthy: %v", name, err)
			}
			time.Sleep(2 * time.Second)
		}
	}
}

func (f *controlPlaneTestFixture) waitForManagedResourceSecret(ctx context.Context, mrName string) (*corev1.Secret, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			secrets := &corev1.SecretList{}
			if err := f.vucClient.List(ctx, secrets, client.InNamespace(f.controlPlaneNamespace)); err == nil {
				for _, s := range secrets.Items {
					if strings.HasPrefix(s.Name, mrName) {
						return &s, nil
					}
				}
			}
			time.Sleep(2 * time.Second)
		}
	}
}

func (f *controlPlaneTestFixture) applyCloudProviderConfigFromSecret(ctx context.Context, secret *corev1.Secret) error {
	for _, data := range secret.Data {
		var reader io.Reader = bytes.NewReader(data)
		// Gardener v1.149.3 compresses ManagedResource secret payloads using gzip.
		// Passing raw gzipped binary data directly into the YAML decoder causes
		// "yaml: control characters are not allowed", so we decompress if a gzip header is detected.
		if bytes.HasPrefix(data, []byte("\x1f\x8b")) {
			gzReader, err := gzip.NewReader(bytes.NewReader(data))
			if err == nil {
				defer gzReader.Close()
				reader = gzReader
			}
		}
		decoder := yaml.NewYAMLOrJSONDecoder(reader, yamlDecoderBufferSize)
		for {
			var obj unstructured.Unstructured
			err := decoder.Decode(&obj)
			if err == io.EOF {
				break
			}
			if err != nil {
				// Skip non-YAML binary entries (e.g. helm charts or webhook configs) that cannot be parsed as YAML.
				break
			}
			if obj.GetKind() == kindConfigMap && obj.GetName() == cloudProviderConfig {
				configData, found, err := unstructured.NestedStringMap(obj.Object, "data")
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf("configmap %s has no data", cloudProviderConfig)
				}

				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      cloudProviderConfig,
						Namespace: f.controlPlaneNamespace,
					},
				}
				_, err = controllerutils.GetAndCreateOrMergePatch(ctx, f.vucClient, cm, func() error {
					cm.Data = configData
					return nil
				})
				return err
			}
		}
	}
	return nil
}

func (f *controlPlaneTestFixture) markManagedResourceHealthy(ctx context.Context, mrName string) error {
	mr := &resourcesv1alpha1.ManagedResource{}
	if err := f.vucClient.Get(ctx, client.ObjectKey{Name: mrName, Namespace: f.controlPlaneNamespace}, mr); err != nil {
		return err
	}
	patch := client.MergeFrom(mr.DeepCopy())
	mr.Status.Conditions = []gardencorev1beta1.Condition{
		{Type: "ResourcesApplied", Status: "True", LastUpdateTime: metav1.Now(), LastTransitionTime: metav1.Now()},
		{Type: "ResourcesHealthy", Status: "True", LastUpdateTime: metav1.Now(), LastTransitionTime: metav1.Now()},
		{Type: "ResourcesProgressing", Status: "False", LastUpdateTime: metav1.Now(), LastTransitionTime: metav1.Now()},
	}
	mr.Status.ObservedGeneration = mr.Generation
	return f.vucClient.Status().Patch(ctx, mr, patch)
}
