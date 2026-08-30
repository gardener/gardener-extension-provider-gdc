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

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/kubernetes"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
)

const (
	workerPollInterval = 5 * time.Second
	workerPollTimeout  = 2 * time.Minute

	cpuCapacity               = "2"
	memoryCapacity            = "8Gi"
	gpuVirtualCapacity        = "2"
	gpuVirtualCapacityUpdated = "4"
)

type workerControllerFixture struct {
	*commonTestFixture

	workerNamespace string
}

func (w *workerControllerFixture) test(t *testing.T) {
	ctx := context.Background()

	if w.workerNamespace == "" {
		t.Fatalf("Worker namespace not specified")
	}

	secret, err := w.setupPreRequisites(ctx, t)
	if err != nil {
		t.Fatalf("failed to create cluster and secret: %v", err)
	}

	t.Run("WorkerCreation", func(t *testing.T) {
		// Arrange: Infrastructure status and Worker configuration
		infraStatus := &apisgdc.InfrastructureStatus{
			TypeMeta: metav1.TypeMeta{
				APIVersion: apisgdc.SchemeGroupVersion.String(),
				Kind:       "InfrastructureStatus",
			},
			Networks: apisgdc.NetworkStatus{
				Zones: []apisgdc.Zones{
					{
						Name:   *zone,
						Subnet: "test-subnet",
					},
				},
			},
		}

		// Create UserData Secret
		userDataSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-userdata",
				Namespace: w.workerNamespace,
			},
			Data: map[string][]byte{
				"userData": []byte("test-user-data"),
			},
		}
		if err := w.vucClient.Create(ctx, userDataSecret); err != nil {
			t.Fatalf("failed to create user data secret: %v", err)
		}
		t.Cleanup(func() {
			if err := w.vucClient.Delete(ctx, userDataSecret); err != nil {
				t.Errorf("failed to delete user data secret: %v", err)
			}
		})

		const poolName = "pool1"

		workerConfig := &v1alpha1.WorkerConfig{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.SchemeGroupVersion.String(),
				Kind:       "WorkerConfig",
			},
			NodeTemplate: &extensionsv1alpha1.NodeTemplate{
				Capacity: corev1.ResourceList{
					"cpu":    resource.MustParse(cpuCapacity),
					"memory": resource.MustParse(memoryCapacity),
				},
				VirtualCapacity: corev1.ResourceList{
					"gpu": resource.MustParse(gpuVirtualCapacity),
				},
			},
		}

		// Action: Create Worker resource
		workerName := "test-worker-" + ptr.Deref(commitHash, "")
		worker := &extensionsv1alpha1.Worker{
			ObjectMeta: metav1.ObjectMeta{
				Name:      workerName,
				Namespace: w.workerNamespace,
			},
			Spec: extensionsv1alpha1.WorkerSpec{
				DefaultSpec: extensionsv1alpha1.DefaultSpec{
					Type: "gdch",
				},
				Region: *zone,
				SecretRef: corev1.SecretReference{
					Name:      secret.Name,
					Namespace: w.workerNamespace,
				},
				Pools: []extensionsv1alpha1.WorkerPool{
					{
						Name:        poolName,
						MachineType: "n3-standard-2-gdc",
						Minimum:     0,
						Maximum:     0,
						MachineImage: extensionsv1alpha1.MachineImage{
							Name:    "ubuntu",
							Version: "1.0",
						},
						MaxSurge:       intstr.FromInt(1),
						MaxUnavailable: intstr.FromInt(0),
						Zones:          []string{*zone},
						Labels:         map[string]string{"custom-label": "true"},
						Volume: &extensionsv1alpha1.Volume{
							Type: ptr.To("Performance"),
							Size: "50Gi",
						},
						DataVolumes: []extensionsv1alpha1.DataVolume{
							{
								Type: ptr.To("Standard"),
								Size: "100Gi",
							},
						},
						UserDataSecretRef: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: userDataSecret.Name,
							},
							Key: "userData",
						},
						NodeAgentSecretName: ptr.To(userDataSecret.Name),
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(t, workerConfig),
						},
					},
				},
				InfrastructureProviderStatus: &runtime.RawExtension{
					Raw: encode(t, infraStatus),
				},
			},
		}
		if err := w.vucClient.Create(ctx, worker); err != nil {
			t.Fatalf("failed to create worker: %v", err)
		}

		// Start MCM simulation to reconcile MachineDeployments
		ctxMCM, cancelMCM := context.WithCancel(ctx)
		t.Cleanup(cancelMCM)
		go SimulateMCM(ctxMCM, w.vucClient, w.workerNamespace, workerName)

		t.Cleanup(func() {
			if err := w.vucClient.Delete(ctx, worker); err != nil && !apierrors.IsNotFound(err) {
				t.Errorf("failed to delete worker during cleanup: %v", err)
			}
			if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
				if err := w.vucClient.Get(ctx, client.ObjectKeyFromObject(worker), worker); err != nil {
					if apierrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}
				return false, nil
			}); err != nil {
				t.Errorf("error waiting for worker object to be deleted during cleanup: %v", err)
			}
		})

		// Assert: Worker is in Succeeded state
		workerList := &extensionsv1alpha1.WorkerList{}
		listOptions := []client.ListOption{
			client.InNamespace(w.workerNamespace),
			client.MatchingFields{"metadata.name": workerName},
		}
		if err := kubernetes.WaitForCondition[*extensionsv1alpha1.Worker](ctx, 5*time.Minute, func() (watch.Interface, error) {
			return w.vucClient.Watch(ctx, workerList, listOptions...)
		}, func(obj *extensionsv1alpha1.Worker) bool {
			t.Logf("Waiting for %q, LastError: %q",
				obj.Name,
				ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
			lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
			return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
		}); err != nil {
			t.Fatalf("Worker is not Succeeded in 5 minutes: %v", err)
		}

		// Assert: Wait for the corresponding MachineClass and MachineDeployment to be created
		t.Log("Waiting for MachineClass creation...")
		targetMC := w.assertAndGetMachineClass(ctx, t, poolName, nil)

		t.Log("Waiting for MachineDeployment creation...")
		mdList := &machinev1alpha1.MachineDeploymentList{}
		if err := wait.PollUntilContextTimeout(ctx, workerPollInterval, workerPollTimeout, true, func(ctx context.Context) (bool, error) {
			if err := w.vucClient.List(ctx, mdList, client.InNamespace(w.workerNamespace)); err != nil {
				t.Logf("Error listing MachineDeployments: %v", err)
				return false, err
			}
			t.Logf("Found %d MachineDeployments", len(mdList.Items))
			for _, md := range mdList.Items {
				t.Logf("MachineDeployment found: %s", md.Name)
				if strings.Contains(md.Name, poolName) {
					if md.Spec.Template.Spec.Class.Name != targetMC.Name {
						t.Errorf("expected MachineDeployment to ref class %q, got %q", targetMC.Name, md.Spec.Template.Spec.Class.Name)
					}
					return true, nil
				}
			}
			return false, nil
		}); err != nil {
			t.Fatalf("timeout waiting for MachineDeployment: %v", err)
		}

		// Assert MachineClass NodeTemplate holds the expected capacity and virtualCapacity
		if targetMC.NodeTemplate == nil {
			t.Fatalf("expected MachineClass to have a NodeTemplate")
		}
		if targetMC.NodeTemplate.Capacity == nil || targetMC.NodeTemplate.Capacity.Cpu().Cmp(resource.MustParse(cpuCapacity)) != 0 {
			t.Errorf("expected MachineClass NodeTemplate Capacity CPU to be %s, got %v", cpuCapacity, targetMC.NodeTemplate.Capacity.Cpu().String())
		}
		if targetMC.NodeTemplate.VirtualCapacity == nil {
			t.Fatalf("expected MachineClass NodeTemplate to have VirtualCapacity")
		}
		gpuVal, ok := targetMC.NodeTemplate.VirtualCapacity["gpu"]
		if !ok {
			t.Errorf("expected MachineClass NodeTemplate VirtualCapacity to contain key 'gpu'")
		} else if gpuVal.Cmp(resource.MustParse(gpuVirtualCapacity)) != 0 {
			t.Errorf("expected MachineClass NodeTemplate VirtualCapacity GPU to be %s, got %v", gpuVirtualCapacity, gpuVal.String())
		}

		// Action: Update VirtualCapacity to test that the MachineClass is updated in place (without a name change)
		t.Log("Updating Worker Pool VirtualCapacity...")

		originalMCName := targetMC.Name

		updatedWorkerConfig := &v1alpha1.WorkerConfig{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.SchemeGroupVersion.String(),
				Kind:       "WorkerConfig",
			},
			NodeTemplate: &extensionsv1alpha1.NodeTemplate{
				Capacity: corev1.ResourceList{
					"cpu":    resource.MustParse(cpuCapacity),
					"memory": resource.MustParse(memoryCapacity),
				},
				VirtualCapacity: corev1.ResourceList{
					"gpu": resource.MustParse(gpuVirtualCapacityUpdated),
				},
			},
		}

		if _, err := controllerutil.CreateOrUpdate(ctx, w.vucClient, worker, func() error {
			worker.Spec.Pools[0].ProviderConfig = &runtime.RawExtension{
				Raw: encode(t, updatedWorkerConfig),
			}
			return nil
		}); err != nil {
			t.Fatalf("failed to update worker virtual capacity: %v", err)
		}

		// Assert: Wait for the MachineClass to update its VirtualCapacity to gpu: 4
		t.Log("Waiting for MachineClass to update its VirtualCapacity...")
		verifyGPUCount := func(mc *machinev1alpha1.MachineClass) bool {
			if mc.Name != originalMCName {
				t.Fatalf("expected MachineClass name to remain %q, but got %q (rollout detected)", originalMCName, mc.Name)
			}
			if mc.NodeTemplate == nil || mc.NodeTemplate.VirtualCapacity == nil {
				return false
			}
			gpuVal, ok := mc.NodeTemplate.VirtualCapacity["gpu"]
			if !ok {
				t.Logf("MachineClass %s VirtualCapacity has no 'gpu' key", originalMCName)
				return false
			}
			if gpuVal.Cmp(resource.MustParse(gpuVirtualCapacityUpdated)) == 0 {
				targetMC = mc
				return true
			}
			t.Logf("MachineClass %s VirtualCapacity GPU is still %v", originalMCName, gpuVal.String())
			return false
		}
		w.assertAndGetMachineClass(ctx, t, poolName, verifyGPUCount)
	})
}

func (w *workerControllerFixture) assertAndGetMachineClass(ctx context.Context, t *testing.T, poolName string, predicate func(*machinev1alpha1.MachineClass) bool) *machinev1alpha1.MachineClass {
	t.Helper()
	mcs := w.assertAndGetMachineClasses(ctx, t, poolName, 1 /* expectedCount */, predicate)
	return mcs[0]
}

func (w *workerControllerFixture) assertAndGetMachineClasses(ctx context.Context, t *testing.T, poolName string, expectedCount int, predicate func(*machinev1alpha1.MachineClass) bool) []*machinev1alpha1.MachineClass {
	t.Helper()
	var targetMCs []*machinev1alpha1.MachineClass
	if err := wait.PollUntilContextTimeout(ctx, workerPollInterval, workerPollTimeout, true, func(ctx context.Context) (bool, error) {
		mcList := &machinev1alpha1.MachineClassList{}
		if err := w.vucClient.List(ctx, mcList, client.InNamespace(w.workerNamespace)); err != nil {
			t.Logf("Error listing MachineClasses: %v", err)
			return false, err
		}

		var matchingClasses []*machinev1alpha1.MachineClass
		for i := range mcList.Items {
			mc := &mcList.Items[i]
			if strings.Contains(mc.Name, poolName) {
				if predicate == nil || predicate(mc) {
					matchingClasses = append(matchingClasses, mc)
				}
			}
		}

		if len(matchingClasses) > expectedCount {
			var names []string
			for _, mc := range matchingClasses {
				names = append(names, mc.Name)
			}
			return false, fmt.Errorf("found %d matching MachineClasses for pool %q: %v (expected %d)", len(matchingClasses), poolName, names, expectedCount)
		}

		if len(matchingClasses) == expectedCount {
			targetMCs = matchingClasses
			return true, nil
		}

		return false, nil
	}); err != nil {
		t.Fatalf("error asserting MachineClass: %v", err)
	}
	return targetMCs
}

func (w *workerControllerFixture) setupPreRequisites(ctx context.Context, t *testing.T) (*corev1.Secret, error) {
	// Common setup
	caData, err := os.ReadFile(*cafile)
	if err != nil {
		return nil, fmt.Errorf("cannot read CA file %v", err)
	}

	// Create unique namespace for the worker object
	if err := kubernetes.CreateNamespace(ctx, w.vucClient, w.workerNamespace); err != nil {
		return nil, fmt.Errorf("cannot create namespace %q, %v", w.workerNamespace, err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up namespace %q", w.workerNamespace)
		kubernetes.CleanupResources(t, w.vucClient, w.workerNamespace)
	})

	// Create Cluster resource
	gdchCloudProfile := &apisgdc.CloudProfileConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apisgdc.SchemeGroupVersion.String(),
			Kind:       "CloudProfileConfig",
		},
		OrgConfig: &apisgdc.OrgConfig{
			GlobalManagementAPI: w.commonTestFixture.gdchConfig.OrgClusterURL,
			CAData:              base64.StdEncoding.EncodeToString(caData),
			OrgName:             "test-org",
			RegistryURL:         "test-registry",
			Zones: []*apisgdc.ZoneEndpoints{
				{
					Name:              *zone,
					ManagementAPI:     fmt.Sprintf("https://management-kube.apiserver.%s.%s.%s", *org, *zone, *labURL),
					InfrastructureAPI: fmt.Sprintf("https://infra-kube.apiserver.%s.%s.%s", *org, *zone, *labURL),
				},
			},
		},
		MachineImages: []apisgdc.MachineImages{
			{
				Name: "ubuntu",
				Versions: []apisgdc.MachineImageVersion{
					{
						Version:      "1.0",
						Image:        "ubuntu-1.0-image",
						Architecture: ptr.To("amd64"),
					},
				},
			},
		},
	}
	cluster := &extensionsv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: w.workerNamespace,
		},
		Spec: extensionsv1alpha1.ClusterSpec{
			Seed: &runtime.RawExtension{Raw: encode(t, &gardencorev1beta1.Seed{})},
			Shoot: runtime.RawExtension{Raw: encode(t, &gardencorev1beta1.Shoot{
				Spec: gardencorev1beta1.ShootSpec{
					Kubernetes: gardencorev1beta1.Kubernetes{
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
	if err := w.vucClient.Create(ctx, cluster); err != nil {
		return nil, fmt.Errorf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() {
		if err := w.vucClient.Delete(ctx, cluster); err != nil {
			t.Errorf("failed to delete cluster: %v", err)
		}
	})

	// Create Secret referenced by Worker
	// Secret content must be valid JSON for credentials
	sa, err := os.ReadFile(*safile)
	if err != nil {
		return nil, fmt.Errorf("cannot read service account file %v", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-secret-" + ptr.Deref(commitHash, ""),
			Namespace: w.workerNamespace,
		},
		Data: map[string][]byte{
			"serviceaccount.json": sa,
		},
	}
	if err := w.vucClient.Create(ctx, secret); err != nil {
		return nil, fmt.Errorf("failed to create secret: %v", err)
	}
	t.Cleanup(func() {
		if err := w.vucClient.Delete(ctx, secret); err != nil {
			t.Errorf("failed to delete secret: %v", err)
		}
	})

	return secret, nil
}
