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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vmv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/virtualmachine/v1"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
	bastioncontroller "github.com/gardener/gardener-extension-provider-gdc/pkg/controller/bastion"
)

const (
	bastionTimeout        = 10 * time.Minute
	bastionCleanupTimeout = 5 * time.Minute

	defaultMachineImage    = "ubuntu"
	defaultMachineVersion  = "22.04"
	defaultMachineImageRef = "ubuntu-22.04-v20250809-gdch"
	defaultMachineType     = "n3-standard-2-gdc"
)

// bastionTestFixture holds the test fixture for Bastion tests.
type bastionTestFixture struct {
	*commonTestFixture

	bastionNamespace string
}

// test runs the Bastion test suite.
func (f *bastionTestFixture) test(t *testing.T) {
	ctx := context.Background()

	// Create a dedicated, isolated vcluster client for this subtest
	f.vucClient = f.NewVClusterClient(t)

	if f.bastionNamespace == "" {
		t.Fatalf("bastionNamespace is not set")
	}

	// Prerequisite: Ensure cluster and dependencies exist
	// This setup creates shared resources and must be cleaned up after the test.
	f.ensureClusterAndDependencies(ctx, t)

	t.Run("ReconcileBastion", func(t *testing.T) {
		// Arrange: Bastion resource
		bastion := f.createBastionObject()

		// Action: Create Bastion resource
		if err := f.vucClient.Create(ctx, bastion); err != nil {
			t.Fatalf("cannot create bastion object %v", err)
		}

		// Cleanup: Delete Bastion and wait for deletion
		// This ensures that the Bastion resource is removed even if the test fails.
		t.Cleanup(func() {
			if err := f.cleanupBastion(ctx, bastion); err != nil {
				t.Fatalf("failed to cleanup bastion: %v", err)
			}
			f.assertBastionResourcesDeleted(ctx, t, bastion)
		})

		// Assert: Bastion is in Succeeded state and has Ingress
		f.assertBastionReady(ctx, t, bastion)
	})

}

// createBastionObject creates a Bastion object with a unique name.
func (f *bastionTestFixture) createBastionObject() *extensionsv1alpha1.Bastion {
	bastionName := "bastion-" + *commitHash
	return &extensionsv1alpha1.Bastion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bastionName,
			Namespace: f.bastionNamespace,
		},
		Spec: extensionsv1alpha1.BastionSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
			},
			UserData: []byte("echo hello"),
			Ingress: []extensionsv1alpha1.BastionIngressPolicy{
				{
					IPBlock: networkingv1.IPBlock{
						CIDR: "0.0.0.0/0",
					},
				},
			},
		},
	}
}

// cleanupBastion deletes the Bastion object and waits for it to be deleted.
func (f *bastionTestFixture) cleanupBastion(ctx context.Context, bastion *extensionsv1alpha1.Bastion) error {
	if err := f.vucClient.Delete(ctx, bastion); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete bastion object %w", err)
		}
	}
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, bastionCleanupTimeout, true, func(ctx context.Context) (bool, error) {
		if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(bastion), bastion); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("error waiting for bastion object to be deleted: %w", err)
	}
	return nil
}

// assertBastionResourcesDeleted verifies that all dependent resources (VM, Disk, ExternalAccess) are deleted.
func (f *bastionTestFixture) assertBastionResourcesDeleted(ctx context.Context, t *testing.T, bastion *extensionsv1alpha1.Bastion) {
	t.Helper()
	vmName := bastioncontroller.VMName(bastion.Name)
	diskName := bastioncontroller.DiskName(bastion.Name)

	resources := []client.Object{
		&vmv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: f.project}},
		&vmv1.VirtualMachineExternalAccess{ObjectMeta: metav1.ObjectMeta{Name: vmName, Namespace: f.project}},
		&vmv1.VirtualMachineDisk{ObjectMeta: metav1.ObjectMeta{Name: diskName, Namespace: f.project}},
	}

	for _, obj := range resources {
		// Use mgmtClient for checking deletion of dependent resources
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, bastionCleanupTimeout, true, func(ctx context.Context) (bool, error) {
			if err := f.mgmtClient.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		}); err != nil {
			t.Fatalf("failed to wait for %T %s to be deleted: %v", obj, obj.GetName(), err)
		} else {
			t.Logf("%T %s deleted successfully", obj, obj.GetName())
		}
	}
}

// assertBastionReady verifies that the Bastion is in Succeeded state and all dependent resources are healthy.
func (f *bastionTestFixture) assertBastionReady(ctx context.Context, t *testing.T, bastion *extensionsv1alpha1.Bastion) {
	t.Helper()

	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, bastionTimeout, true, func(ctx context.Context) (bool, error) {
		if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(bastion), bastion); err != nil {
			t.Logf("failed to get Bastion %s: %v", bastion.Name, err)
			return false, nil
		}

		t.Logf("Waiting for %q, LastError: %q",
			bastion.Name,
			ptr.Deref(bastion.Status.LastError, gardencorev1beta1.LastError{}).Description)
		lastOptState := ptr.Deref(bastion.Status.LastOperation, gardencorev1beta1.LastOperation{}).State

		if lastOptState != gardencorev1beta1.LastOperationStateSucceeded || bastion.Status.Ingress == nil {
			return false, nil
		}

		// Assert: VirtualMachine is Running
		vmName := bastioncontroller.VMName(bastion.Name)
		vm := &vmv1.VirtualMachine{}
		// Use mgmtClient for VirtualMachine
		if err := f.mgmtClient.Get(ctx, client.ObjectKey{Namespace: f.project, Name: vmName}, vm); err != nil {
			t.Logf("failed to get VirtualMachine %s: %v", vmName, err)
			return false, nil
		}
		if vm.Status.State != vmv1.VirtualMachineStateRunning {
			t.Logf("VirtualMachine %s state is %s, waiting for Running", vmName, vm.Status.State)
			return false, nil
		}
		t.Logf("VirtualMachine %s is Running", vmName)

		// Assert: VirtualMachineExternalAccess has IngressIP
		vmea := &vmv1.VirtualMachineExternalAccess{}
		// Use mgmtClient for VirtualMachineExternalAccess
		if err := f.mgmtClient.Get(ctx, client.ObjectKey{Namespace: f.project, Name: vmName}, vmea); err != nil {
			t.Logf("failed to get VirtualMachineExternalAccess %s: %v", vmName, err)
			return false, nil
		}
		if vmea.Status.IngressIP == "" {
			t.Logf("VirtualMachineExternalAccess %s has no IngressIP", vmName)
			return false, nil
		}
		t.Logf("VirtualMachineExternalAccess %s has IngressIP: %s", vmName, vmea.Status.IngressIP)

		// Assert: VirtualMachineDisk is Succeeded
		diskName := bastioncontroller.DiskName(bastion.Name)
		disk := &vmv1.VirtualMachineDisk{}
		// Use mgmtClient for VirtualMachineDisk
		if err := f.mgmtClient.Get(ctx, client.ObjectKey{Namespace: f.project, Name: diskName}, disk); err != nil {
			t.Logf("failed to get VirtualMachineDisk %s: %v", diskName, err)
			return false, nil
		}
		if disk.Status.Phase != vmv1.DiskPhaseSucceeded {
			t.Logf("VirtualMachineDisk %s phase is %s, waiting for Succeeded", diskName, disk.Status.Phase)
			return false, nil
		}
		t.Logf("VirtualMachineDisk %s is Succeeded", diskName)

		return true, nil
	}); err != nil {
		t.Fatalf("Bastion is not ready in %v: %v", bastionTimeout, err)
	}
}

// ensureClusterAndDependencies ensures that the Cluster resource and its dependencies (Secret, Configs) exist.
func (f *bastionTestFixture) ensureClusterAndDependencies(ctx context.Context, t *testing.T) {
	t.Helper()
	// Arrange:

	t.Cleanup(func() { f.cleanupTestNamespace(ctx, t) })
	if err := f.createTestNamespace(ctx); err != nil {
		t.Fatalf("cannot create test namespace: %v", err)
	}

	t.Cleanup(func() { f.cleanupCloudProviderSecret(ctx, t) })
	if err := f.createCloudProviderSecret(ctx); err != nil {
		t.Fatalf("cannot create cloud provider secret: %v", err)
	}

	infraConfig, err := f.createInfrastructureConfig()
	if err != nil {
		t.Fatalf("cannot create infrastructure config: %v", err)
	}

	cloudProfileConfig, err := f.createCloudProfileConfig()
	if err != nil {
		t.Fatalf("cannot create cloud profile config: %v", err)
	}

	cloudProfile, err := f.createCloudProfile(cloudProfileConfig)
	if err != nil {
		t.Fatalf("cannot create cloud profile: %v", err)
	}

	shoot, err := f.createShoot(infraConfig)
	if err != nil {
		t.Fatalf("cannot create shoot: %v", err)
	}

	seedBytes, err := json.Marshal(&gardencorev1beta1.Seed{})
	if err != nil {
		t.Fatalf("cannot marshal seed: %v", err)
	}

	cluster := &extensionsv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.bastionNamespace,
		},
		Spec: extensionsv1alpha1.ClusterSpec{
			CloudProfile: runtime.RawExtension{Raw: cloudProfile},
			Shoot:        runtime.RawExtension{Raw: shoot},
			Seed:         &runtime.RawExtension{Raw: seedBytes},
		},
	}

	t.Cleanup(func() { f.cleanupCluster(ctx, t) })
	existingCluster := &extensionsv1alpha1.Cluster{}
	if err := f.vucClient.Get(ctx, client.ObjectKey{Name: f.bastionNamespace}, existingCluster); err != nil {
		if apierrors.IsNotFound(err) {
			if err := f.vucClient.Create(ctx, cluster); err != nil {
				t.Fatalf("cannot create cluster object: %v", err)
			}
		} else {
			t.Fatalf("cannot get cluster object for update: %v", err)
		}
	} else {
		existingCluster.Spec = cluster.Spec
		if err := f.vucClient.Update(ctx, existingCluster); err != nil {
			t.Fatalf("cannot update cluster object: %v", err)
		}
	}
}

func (f *bastionTestFixture) cleanupTestNamespace(ctx context.Context, t *testing.T) {
	bastionNamespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.bastionNamespace,
		},
	}
	if err := f.vucClient.Delete(ctx, bastionNamespace); err != nil {
		if !apierrors.IsNotFound(err) {
			t.Errorf("cannot delete bastion namespace: %v", err)
		}
	}
}

func (f *bastionTestFixture) cleanupCloudProviderSecret(ctx context.Context, t *testing.T) {
	providerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudprovider",
			Namespace: f.bastionNamespace,
		},
	}
	if err := f.vucClient.Delete(ctx, providerSecret); err != nil {
		if !apierrors.IsNotFound(err) {
			t.Errorf("cannot delete provider secret: %v", err)
		}
	}
}

func (f *bastionTestFixture) cleanupCluster(ctx context.Context, t *testing.T) {
	cluster := &extensionsv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.bastionNamespace,
		},
	}
	if err := f.vucClient.Delete(ctx, cluster); err != nil {
		if !apierrors.IsNotFound(err) {
			t.Errorf("cannot delete cluster object: %v", err)
		}
	}
}

// createCloudProviderSecret creates the cloudprovider secret containing the service account.
func (f *bastionTestFixture) createCloudProviderSecret(ctx context.Context) error {
	sa, err := os.ReadFile(*safile)
	if err != nil {
		return fmt.Errorf("cannot read service account file: %w", err)
	}
	providerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudprovider",
			Namespace: f.bastionNamespace,
		},
		Data: map[string][]byte{
			"serviceaccount.json": sa,
		},
	}
	if err := f.vucClient.Create(ctx, providerSecret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("cannot create provider secret: %w", err)
		}
	}
	return nil
}

func (f *bastionTestFixture) createTestNamespace(ctx context.Context) error {
	// Create unique namespace for the infrastructure object
	if err := f.vucClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.bastionNamespace,
		},
	}); err != nil {
		return fmt.Errorf("cannot create namespace for bastion test %v", err)
	}
	return nil
}

// createInfrastructureConfig creates the InfrastructureConfig for the test.
func (f *bastionTestFixture) createInfrastructureConfig() ([]byte, error) {
	infraConfig := &v1alpha1.InfrastructureConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "InfrastructureConfig",
		},
		Networks: v1alpha1.NetworkConfig{
			Zones: []v1alpha1.Zone{
				{Name: *zone},
			},
		},
	}
	return json.Marshal(infraConfig)
}

// createCloudProfileConfig creates the CloudProfileConfig for the test.
func (f *bastionTestFixture) createCloudProfileConfig() ([]byte, error) {
	cloudProfileConfig := &v1alpha1.CloudProfileConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "CloudProfileConfig",
		},
		MachineImages: []v1alpha1.MachineImages{
			{
				Name:    defaultMachineImage,
				Project: "vm-system",
				Versions: []v1alpha1.MachineImageVersion{
					{
						Version: defaultMachineVersion,
						Image:   defaultMachineImageRef,
					},
				},
			},
		},
		OrgConfig: &v1alpha1.OrgConfig{
			Zones: []*v1alpha1.ZoneEndpoints{
				{
					Name:          *zone,
					ManagementAPI: "https://management-kube.apiserver." + *org + "." + *zone + "." + *labURL,
				},
			},
		},
	}
	return json.Marshal(cloudProfileConfig)
}

// createCloudProfile creates the CloudProfile resource.
func (f *bastionTestFixture) createCloudProfile(cloudProfileConfigBytes []byte) ([]byte, error) {
	supported := gardencorev1beta1.ClassificationSupported
	cloudProfile := &gardencorev1beta1.CloudProfile{
		Spec: gardencorev1beta1.CloudProfileSpec{
			MachineTypes: []gardencorev1beta1.MachineType{
				{
					Name:         defaultMachineType,
					CPU:          resource.MustParse("4"),
					Memory:       resource.MustParse("16Gi"),
					Architecture: ptr.To("amd64"),
				},
			},
			MachineImages: []gardencorev1beta1.MachineImage{
				{
					Name: defaultMachineImage,
					Versions: []gardencorev1beta1.MachineImageVersion{
						{
							ExpirableVersion: gardencorev1beta1.ExpirableVersion{
								Version:        defaultMachineVersion,
								Classification: &supported,
							},
							Architectures: []string{"amd64"},
						},
					},
				},
			},
			ProviderConfig: &runtime.RawExtension{
				Raw: cloudProfileConfigBytes,
			},
		},
	}
	return json.Marshal(cloudProfile)
}

// createShoot creates the Shoot resource.
func (f *bastionTestFixture) createShoot(infraConfigBytes []byte) ([]byte, error) {
	shoot := &gardencorev1beta1.Shoot{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.bastionNamespace,
		},
		Spec: gardencorev1beta1.ShootSpec{
			Provider: gardencorev1beta1.Provider{
				InfrastructureConfig: &runtime.RawExtension{
					Raw: infraConfigBytes,
				},
			},
		},
	}
	return json.Marshal(shoot)
}
