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
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ipamglobalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/ipam/v1"
	globalnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/networking/v1"
	ipamv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/ipam/v1"
	networkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"

	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/kubernetes"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
)

const (
	defaultNodeCIDRPrefixLength = 29
	infraTimeout                = 10 * time.Minute
)

type infraTestFixture struct {
	*commonTestFixture

	mgmtClient      client.WithWatch
	zoneMgmtClients map[string]client.WithWatch
	globalClient    client.WithWatch
	availableZones  []string
}

func (f *infraTestFixture) test(t *testing.T) {
	ctx := context.Background()
	// Common setup
	caData, err := os.ReadFile(*cafile)
	if err != nil {
		t.Fatalf("cannot read CA file %v", err)
	}
	if len(f.availableZones) < 2 {
		t.Fatalf("test requires at least 2 zones for Multi-zone test cases, %v", f.availableZones)
	}

	// Generate go client for the management cluster
	scheme := runtime.NewScheme()
	if err := extensionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register Gardener Extensions scheme %v", err)
	}

	if err := ipamv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register ipamv1 scheme %v", err)
	}
	if err := ipamglobalv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register ipamglobalv1 scheme %v", err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register networkingv1 scheme %v", err)
	}
	if err := globalnetworkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register globalnetworkingv1 scheme %v", err)
	}

	f.zoneMgmtClients = make(map[string]client.WithWatch)
	for _, z := range f.availableZones {
		zClient, err := GetManagementClient(scheme, z)
		if err != nil {
			t.Fatalf("failed to create management client for zone %s: %v", z, err)
		}
		f.zoneMgmtClients[z] = zClient
	}
	f.mgmtClient = f.zoneMgmtClients[*zone]

	// Generate go client for the global cluster
	globalSchema := runtime.NewScheme()
	if err := globalnetworkingv1.AddToScheme(globalSchema); err != nil {
		t.Fatalf("unable to register globalnetworkingv1 scheme %v", err)
	}
	if err := ipamglobalv1.AddToScheme(globalSchema); err != nil {
		t.Fatalf("unable to register ipamglobalv1 scheme %v", err)
	}
	globalClient, err := gdc.GetGlobalClient(f.gdcClient, globalSchema)
	if err != nil {
		t.Fatalf("cannot create client for Global API %v", err)
	}
	f.globalClient = globalClient

	// Instantiate a new vucClient for better isolation using the vcluster kubeconfig
	f.vucClient = f.NewVClusterClient(t)

	// Run all tests in parallel
	t.Run("InfrastructureAll", func(t *testing.T) {
		t.Run("SingleZoneInfrastructureCreation", func(t *testing.T) {
			t.Parallel()
			namespace, saSecretName := f.setupInfraTestEnvironment(ctx, t, caData)
			f.testSingleZoneInfrastructureCreation(ctx, t, namespace, saSecretName)
		})

		t.Run("MultiZoneInfrastructureCreation", func(t *testing.T) {
			t.Parallel()
			namespace, saSecretName := f.setupInfraTestEnvironment(ctx, t, caData)
			f.testMultiZoneInfrastructureCreation(ctx, t, namespace, saSecretName)
		})

		t.Run("MultiZoneInfrastructureCloudNAT", func(t *testing.T) {
			t.Parallel()
			namespace, saSecretName := f.setupInfraTestEnvironment(ctx, t, caData)
			f.testMultiZoneInfrastructureCloudNAT(ctx, t, namespace, saSecretName)
		})

		t.Run("SameNameInfrastructureKeepsSubnets", func(t *testing.T) {
			t.Parallel()
			namespace, saSecretName := f.setupInfraTestEnvironment(ctx, t, caData)
			otherNamespace, otherSASecretName := f.setupInfraTestEnvironment(ctx, t, caData)
			f.testSameNameInfrastructureKeepsSubnets(ctx, t, namespace, saSecretName, otherNamespace, otherSASecretName)
		})
	})
}

func (f *infraTestFixture) setupInfraTestEnvironment(ctx context.Context, t *testing.T, caData []byte) (string, string) {
	// Generate unique namespace for the infrastructure test
	namespace := fmt.Sprintf("%s-infra-%s", f.namespace, rand.String(5))

	if err := f.vucClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}); err != nil {
		t.Fatalf("cannot create namespace for infra test %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}); err != nil {
			t.Fatalf("cannot delete namespace for infra test %v", err)
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
			CAData:              base64.StdEncoding.EncodeToString(caData),
			GlobalManagementAPI: fmt.Sprintf("https://global-api.%s.%s.%s", *org, *zone, *labURL),
			Zones:               zones,
		},
	}
	cluster := &extensionsv1alpha1.Cluster{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
		Spec: extensionsv1alpha1.ClusterSpec{
			Seed:  &runtime.RawExtension{Raw: encode(t, &gardencorev1beta1.Shoot{})},
			Shoot: runtime.RawExtension{Raw: encode(t, &gardencorev1beta1.Seed{})},
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

	// Create secret with service account and gdch-config
	sa, err := os.ReadFile(*safile)
	if err != nil {
		t.Fatalf("cannot read service account file %v", err)
	}

	rawGDCHConfig, err := json.Marshal(f.gdchConfig)
	if err != nil {
		t.Fatalf("cannot marshal gdch-config %v", err)
	}
	saSecretName := "sa-" + ptr.Deref(commitHash, "") + "-" + rand.String(5)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saSecretName,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			"serviceaccount.json": sa,
			"gdch-config":         rawGDCHConfig,
		},
	}
	if err := f.vucClient.Create(ctx, secret); err != nil {
		t.Fatalf("unable to create Secret for infra test %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, secret); err != nil {
			t.Fatalf("unable to delete Secret for infra test %v", err)
		}
	})

	return namespace, saSecretName
}

func (f *infraTestFixture) testSingleZoneInfrastructureCreation(ctx context.Context, t *testing.T, namespace string, saSecretName string) {
	testSubnet, nodeCIDR := f.allocateTestSubnet(ctx, t, defaultNodeCIDRPrefixLength)
	zones := splitNodeCIDRToZones(t, nodeCIDR, []string{*zone})

	// Arrange: Infrastructure with single zone network configuration
	infraName := fmt.Sprintf("sz-infra-%s-%s", *commitHash, rand.String(5))
	infra := &extensionsv1alpha1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name:      infraName,
			Namespace: namespace,
		},
		Spec: extensionsv1alpha1.InfrastructureSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(t, &v1alpha1.InfrastructureConfig{
						Networks: v1alpha1.NetworkConfig{
							ParentReference: &v1alpha1.SubnetReference{
								Name:      testSubnet.Name,
								Namespace: ptr.To(testSubnet.Namespace),
								Type:      "SingleSubnet",
							},
							NodeCIDR: nodeCIDR,
							Zones:    zones,
						},
					}),
				},
			},
			SecretRef: corev1.SecretReference{
				Name:      saSecretName,
				Namespace: namespace,
			},
		},
	}

	// Action: Create Infrastructure resource
	if err := f.vucClient.Create(ctx, infra); err != nil {
		t.Fatalf("cannot create infrastructure object %v", err)
	}
	t.Cleanup(func() { f.cleanupInfraCR(ctx, t, infra) })

	// Assert: Infrastructure is in Succeeded state
	infraList := &extensionsv1alpha1.InfrastructureList{}
	listOptions := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingFields{"metadata.name": infraName},
	}
	if err := kubernetes.WaitForCondition[*extensionsv1alpha1.Infrastructure](ctx, infraTimeout, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, infraList, listOptions...)
	}, func(obj *extensionsv1alpha1.Infrastructure) bool {
		t.Logf("Waiting for %q, LastError: %q",
			obj.Name,
			ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
		lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
		return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
	}); err != nil {
		t.Fatalf("Infrastructure is not Succeeded in %d minutes %v", int(infraTimeout.Minutes()), err)
	}

	if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(infra), infra); err != nil {
		t.Fatalf("cannot get infrastructure object %v", err)
	}

	// Assert: ProviderStatus
	if infra.Status.ProviderStatus == nil {
		t.Fatalf("Infrastructure ProviderStatus is nil")
	}
	status := &v1alpha1.InfrastructureStatus{}
	if err := json.Unmarshal(infra.Status.ProviderStatus.Raw, status); err != nil {
		t.Fatalf("Could not decode ProviderStatus: %v", err)
	}
	if status.Networks.NodeCIDR != nodeCIDR {
		t.Errorf("Expected NodeCIDR %s, got %s", nodeCIDR, status.Networks.NodeCIDR)
	}
	if len(status.Networks.Zones) != 1 {
		t.Errorf("Expected 1 zone, got %d", len(status.Networks.Zones))
	} else {
		if status.Networks.Zones[0].Name != *zone {
			t.Errorf("Expected zone %s, got %s", *zone, status.Networks.Zones[0].Name)
		}
		if status.Networks.Zones[0].Subnet == "" {
			t.Errorf("Expected Subnet name to be populated")
		}
	}
	if len(infra.Status.EgressCIDRs) != 0 {
		t.Errorf("Expected 0 egress CIDRs, got %d", len(infra.Status.EgressCIDRs))
	}

	// Assert GDCH Resources: Global Root Subnet
	rootSubnetName := infra.Name
	rootSubnet := &ipamglobalv1.Subnet{}
	if err := f.globalClient.Get(ctx, client.ObjectKey{Name: rootSubnetName, Namespace: f.project}, rootSubnet); err != nil {
		t.Errorf("Failed to get global root subnet %s: %v", rootSubnetName, err)
	} else {
		if !isSubnetReady(rootSubnet.Status.Conditions) {
			t.Errorf("Global root subnet %s is not Ready", rootSubnetName)
		}
		if rootSubnet.Status.IPv4Allocation == nil || rootSubnet.Status.IPv4Allocation.CIDR != nodeCIDR {
			t.Errorf("Global root subnet %s has incorrect CIDR: expected %s, got %v", rootSubnetName, nodeCIDR, getCIDR(rootSubnet.Status.IPv4Allocation))
		}
	}

	// Assert GDCH Resources: Global Zone Subnet
	globalZoneSubnetName := infra.Name + "-" + *zone
	globalZoneSubnet := &ipamglobalv1.Subnet{}
	if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := f.globalClient.Get(ctx, client.ObjectKey{Name: globalZoneSubnetName, Namespace: f.project}, globalZoneSubnet); err != nil {
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Errorf("Failed to get global zone subnet %s: %v", globalZoneSubnetName, err)
	} else {
		if !isSubnetReady(globalZoneSubnet.Status.Conditions) {
			t.Errorf("Global zone subnet %s is not Ready", globalZoneSubnetName)
		}
		if globalZoneSubnet.Status.IPv4Allocation == nil || globalZoneSubnet.Status.IPv4Allocation.CIDR != zones[0].CIDR {
			t.Errorf("Global zone subnet %s has incorrect CIDR: expected %s, got %v", globalZoneSubnetName, zones[0].CIDR, getCIDR(globalZoneSubnet.Status.IPv4Allocation))
		}
	}

	// Assert GDCH Resources: Zonal Subnet
	zonalSubnetName := "z-" + infra.Name + "-" + *zone
	zonalSubnet := &ipamv1.Subnet{}
	if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := f.mgmtClient.Get(ctx, client.ObjectKey{Name: zonalSubnetName, Namespace: f.project}, zonalSubnet); err != nil {
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Errorf("Failed to get zonal subnet %s: %v", zonalSubnetName, err)
	} else {
		if !isSubnetReady(zonalSubnet.Status.Conditions) {
			t.Errorf("Zonal subnet %s is not Ready", zonalSubnetName)
		}
		if zonalSubnet.Status.IPv4Allocation == nil || zonalSubnet.Status.IPv4Allocation.CIDR != zones[0].CIDR {
			t.Errorf("Zonal subnet %s has incorrect CIDR: expected %s, got %v", zonalSubnetName, zones[0].CIDR, getCIDR(zonalSubnet.Status.IPv4Allocation))
		}
	}
}

func (f *infraTestFixture) testMultiZoneInfrastructureCreation(ctx context.Context, t *testing.T, namespace string, saSecretName string) {
	testSubnet, nodeCIDR := f.allocateTestSubnet(ctx, t, defaultNodeCIDRPrefixLength)
	zones := splitNodeCIDRToZones(t, nodeCIDR, f.availableZones[:2])

	// Arrange: Infrastructure with multi-zone network configuration
	infraName := fmt.Sprintf("mz-infra-%s-%s", *commitHash, rand.String(5))
	infra := &extensionsv1alpha1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name:      infraName,
			Namespace: namespace,
		},
		Spec: extensionsv1alpha1.InfrastructureSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(t, &v1alpha1.InfrastructureConfig{
						Networks: v1alpha1.NetworkConfig{
							ParentReference: &v1alpha1.SubnetReference{
								Name:      testSubnet.Name,
								Namespace: ptr.To(testSubnet.Namespace),
								Type:      "SingleSubnet",
							},
							NodeCIDR: nodeCIDR,
							Zones:    zones,
						},
					}),
				},
			},
			SecretRef: corev1.SecretReference{
				Name:      saSecretName,
				Namespace: namespace,
			},
		},
	}

	// Action: Create Infrastructure resource
	if err := f.vucClient.Create(ctx, infra); err != nil {
		t.Fatalf("cannot create infrastructure object %v", err)
	}
	t.Cleanup(func() { f.cleanupInfraCR(ctx, t, infra) })

	// Assert: Infrastructure is in Succeeded state
	infraList := &extensionsv1alpha1.InfrastructureList{}
	listOptions := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingFields{"metadata.name": infraName},
	}
	if err := kubernetes.WaitForCondition[*extensionsv1alpha1.Infrastructure](ctx, infraTimeout, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, infraList, listOptions...)
	}, func(obj *extensionsv1alpha1.Infrastructure) bool {
		t.Logf("Waiting for %q, LastError: %q",
			obj.Name,
			ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
		lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
		return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
	}); err != nil {
		t.Fatalf("Infrastructure is not Succeeded in %d minutes %v", int(infraTimeout.Minutes()), err)
	}

	if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(infra), infra); err != nil {
		t.Fatalf("cannot get infrastructure object %v", err)
	}

	// Assert: ProviderStatus
	expectedZones := []struct {
		Name string
		CIDR string
	}{
		{Name: zones[0].Name, CIDR: zones[0].CIDR},
		{Name: zones[1].Name, CIDR: zones[1].CIDR},
	}
	if infra.Status.ProviderStatus == nil {
		t.Fatalf("Infrastructure ProviderStatus is nil")
	}
	status := &v1alpha1.InfrastructureStatus{}
	if err := json.Unmarshal(infra.Status.ProviderStatus.Raw, status); err != nil {
		t.Fatalf("Could not decode ProviderStatus: %v", err)
	}
	if status.Networks.NodeCIDR != nodeCIDR {
		t.Errorf("Expected NodeCIDR %s, got %s", nodeCIDR, status.Networks.NodeCIDR)
	}
	if len(status.Networks.Zones) != 2 {
		t.Errorf("Expected 2 zones, got %d", len(status.Networks.Zones))
	} else {
		for _, expected := range expectedZones {
			var found bool
			for _, z := range status.Networks.Zones {
				if z.Name == expected.Name {
					found = true
					if z.Subnet == "" {
						t.Errorf("Expected Subnet name to be populated for zone %s", expected.Name)
					}
					break
				}
			}
			if !found {
				t.Errorf("Expected zone %s not found in status", expected.Name)
			}
		}
	}

	if len(infra.Status.EgressCIDRs) != 0 {
		t.Errorf("Expected 0 egress CIDRs, got %d", len(infra.Status.EgressCIDRs))
	}

	// Assert GDCH Resources: Global Root Subnet
	rootSubnetName := infra.Name
	rootSubnet := &ipamglobalv1.Subnet{}
	if err := f.globalClient.Get(ctx, client.ObjectKey{Name: rootSubnetName, Namespace: f.project}, rootSubnet); err != nil {
		t.Errorf("Failed to get global root subnet %s: %v", rootSubnetName, err)
	} else {
		if !isSubnetReady(rootSubnet.Status.Conditions) {
			t.Errorf("Global root subnet %s is not Ready", rootSubnetName)
		}
		if rootSubnet.Status.IPv4Allocation == nil || rootSubnet.Status.IPv4Allocation.CIDR != nodeCIDR {
			t.Errorf("Global root subnet %s has incorrect CIDR: expected %s, got %v", rootSubnetName, nodeCIDR, getCIDR(rootSubnet.Status.IPv4Allocation))
		}
	}

	// Assert GDCH Resources: Global Zone Subnet
	for _, z := range expectedZones {
		// Assert GDCH Resources: Global Zone Subnet
		globalZoneSubnetName := infra.Name + "-" + z.Name
		globalZoneSubnet := &ipamglobalv1.Subnet{}
		if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := f.globalClient.Get(ctx, client.ObjectKey{Name: globalZoneSubnetName, Namespace: f.project}, globalZoneSubnet); err != nil {
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Errorf("Failed to get global zone subnet %s: %v", globalZoneSubnetName, err)
		} else {
			if !isSubnetReady(globalZoneSubnet.Status.Conditions) {
				t.Errorf("Global zone subnet %s is not Ready", globalZoneSubnetName)
			}
			if globalZoneSubnet.Status.IPv4Allocation == nil || globalZoneSubnet.Status.IPv4Allocation.CIDR != z.CIDR {
				t.Errorf("Global zone subnet %s has incorrect CIDR: expected %s, got %v", globalZoneSubnetName, z.CIDR, getCIDR(globalZoneSubnet.Status.IPv4Allocation))
			}
		}

		// Assert GDCH Resources: Zonal Subnet
		zonalSubnetName := "z-" + infra.Name + "-" + z.Name
		zonalSubnet := &ipamv1.Subnet{}

		zoneMgmtClient, ok := f.zoneMgmtClients[z.Name]
		if !ok {
			t.Fatalf("no management client cached for zone %s", z.Name)
		}

		if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := zoneMgmtClient.Get(ctx, client.ObjectKey{Name: zonalSubnetName, Namespace: f.project}, zonalSubnet); err != nil {
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Errorf("Failed to get zonal subnet %s in zone %s: %v", zonalSubnetName, z.Name, err)
		} else {
			if !isSubnetReady(zonalSubnet.Status.Conditions) {
				t.Errorf("Zonal subnet %s in zone %s is not Ready", zonalSubnetName, z.Name)
			}
			if zonalSubnet.Status.IPv4Allocation == nil || zonalSubnet.Status.IPv4Allocation.CIDR != z.CIDR {
				t.Errorf("Zonal subnet %s in zone %s has incorrect CIDR: expected %s, got %v", zonalSubnetName, z.Name, z.CIDR, getCIDR(zonalSubnet.Status.IPv4Allocation))
			}
		}
	}
}

func (f *infraTestFixture) testMultiZoneInfrastructureCloudNAT(ctx context.Context, t *testing.T, namespace string, saSecretName string) {
	testSubnet, nodeCIDR := f.allocateTestSubnet(ctx, t, defaultNodeCIDRPrefixLength)
	zones := splitNodeCIDRToZones(t, nodeCIDR, f.availableZones[:2])

	// Arrange: Infrastructure with Multizon setup and CloudNAT enabled
	infraName := fmt.Sprintf("mz-cloudnat-infra-%s-%s", *commitHash, rand.String(5))
	infra := &extensionsv1alpha1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name:      infraName,
			Namespace: namespace,
		},
		Spec: extensionsv1alpha1.InfrastructureSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch",
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(t, &v1alpha1.InfrastructureConfig{
						EnableEgress: ptr.To(true),
						Networks: v1alpha1.NetworkConfig{
							ParentReference: &v1alpha1.SubnetReference{
								Name:      testSubnet.Name,
								Namespace: ptr.To(testSubnet.Namespace),
								Type:      "SingleSubnet",
							},
							NodeCIDR: nodeCIDR,
							Zones:    zones,
						},
					}),
				},
			},
			SecretRef: corev1.SecretReference{
				Name:      saSecretName,
				Namespace: namespace,
			},
		},
	}

	// Action: Create Infrastructure resource
	if err := f.vucClient.Create(ctx, infra); err != nil {
		t.Fatalf("cannot create infrastructure object %v", err)
	}
	t.Cleanup(func() { f.cleanupInfraCR(ctx, t, infra) })

	// Assert: Infrastructure is in Succeeded state
	infraList := &extensionsv1alpha1.InfrastructureList{}
	listOptions := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingFields{"metadata.name": infraName},
	}
	if err := kubernetes.WaitForCondition[*extensionsv1alpha1.Infrastructure](ctx, infraTimeout, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, infraList, listOptions...)
	}, func(obj *extensionsv1alpha1.Infrastructure) bool {
		t.Logf("Waiting for %q, LastError: %q",
			obj.Name,
			ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
		lastOptState := ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State
		return lastOptState == gardencorev1beta1.LastOperationStateSucceeded
	}); err != nil {
		t.Fatalf("Infrastructure is not Succeeded in %d minutes %v", int(infraTimeout.Minutes()), err)
	}

	if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(infra), infra); err != nil {
		t.Fatalf("cannot get infrastructure object %v", err)
	}

	// Assert: ProviderStatus
	if infra.Status.ProviderStatus == nil {
		t.Fatalf("Infrastructure ProviderStatus is nil")
	}
	status := &v1alpha1.InfrastructureStatus{}
	if err := json.Unmarshal(infra.Status.ProviderStatus.Raw, status); err != nil {
		t.Fatalf("Could not decode ProviderStatus: %v", err)
	}
	if status.Networks.NodeCIDR != nodeCIDR {
		t.Errorf("Expected NodeCIDR %s, got %s", nodeCIDR, status.Networks.NodeCIDR)
	}
	expectedZones := []struct {
		Name string
		CIDR string
	}{
		{Name: zones[0].Name, CIDR: zones[0].CIDR},
		{Name: zones[1].Name, CIDR: zones[1].CIDR},
	}
	if len(status.Networks.Zones) != 2 {
		t.Errorf("Expected 2 zones, got %d", len(status.Networks.Zones))
	} else {
		for _, expected := range expectedZones {
			var found bool
			for _, z := range status.Networks.Zones {
				if z.Name == expected.Name {
					found = true
					if z.Subnet == "" {
						t.Errorf("Expected Subnet name to be populated for zone %s", expected.Name)
					}
					break
				}
			}
			if !found {
				t.Errorf("Expected zone %s not found in status", expected.Name)
			}
		}
	}

	if len(infra.Status.EgressCIDRs) != len(expectedZones) {
		t.Errorf("Expected %d egress CIDRs, got %d", len(expectedZones), len(infra.Status.EgressCIDRs))
	}

	expectedEgressCIDRs := make([]string, 0, len(expectedZones))

	// Assert GDCH Resources: Global Root Subnet
	rootSubnetName := infra.Name
	rootSubnet := &ipamglobalv1.Subnet{}
	if err := f.globalClient.Get(ctx, client.ObjectKey{Name: rootSubnetName, Namespace: f.project}, rootSubnet); err != nil {
		t.Errorf("Failed to get global root subnet %s: %v", rootSubnetName, err)
	} else {
		if !isSubnetReady(rootSubnet.Status.Conditions) {
			t.Errorf("Global root subnet %s is not Ready", rootSubnetName)
		}
		if rootSubnet.Status.IPv4Allocation == nil || rootSubnet.Status.IPv4Allocation.CIDR != nodeCIDR {
			t.Errorf("Global root subnet %s has incorrect CIDR: expected %s, got %v", rootSubnetName, nodeCIDR, getCIDR(rootSubnet.Status.IPv4Allocation))
		}
	}

	// Assert GDCH Resources: Global Zone Subnet
	for _, z := range expectedZones {
		// Assert GDCH Resources: Global Zone Subnet
		globalZoneSubnetName := infra.Name + "-" + z.Name
		globalZoneSubnet := &ipamglobalv1.Subnet{}
		if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := f.globalClient.Get(ctx, client.ObjectKey{Name: globalZoneSubnetName, Namespace: f.project}, globalZoneSubnet); err != nil {
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Errorf("Failed to get global zone subnet %s: %v", globalZoneSubnetName, err)
		} else {
			if !isSubnetReady(globalZoneSubnet.Status.Conditions) {
				t.Errorf("Global zone subnet %s is not Ready", globalZoneSubnetName)
			}
			if globalZoneSubnet.Status.IPv4Allocation == nil || globalZoneSubnet.Status.IPv4Allocation.CIDR != z.CIDR {
				t.Errorf("Global zone subnet %s has incorrect CIDR: expected %s, got %v", globalZoneSubnetName, z.CIDR, getCIDR(globalZoneSubnet.Status.IPv4Allocation))
			}
		}

		// Assert GDCH Resources: Zonal Subnet
		zonalSubnetName := "z-" + infra.Name + "-" + z.Name
		zonalSubnet := &ipamv1.Subnet{}

		zoneMgmtClient, ok := f.zoneMgmtClients[z.Name]
		if !ok {
			t.Fatalf("no management client cached for zone %s", z.Name)
		}

		if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := zoneMgmtClient.Get(ctx, client.ObjectKey{Name: zonalSubnetName, Namespace: f.project}, zonalSubnet); err != nil {
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Errorf("Failed to get zonal subnet %s in zone %s: %v", zonalSubnetName, z.Name, err)
		} else {
			if !isSubnetReady(zonalSubnet.Status.Conditions) {
				t.Errorf("Zonal subnet %s in zone %s is not Ready", zonalSubnetName, z.Name)
			}
			if zonalSubnet.Status.IPv4Allocation == nil || zonalSubnet.Status.IPv4Allocation.CIDR != z.CIDR {
				t.Errorf("Zonal subnet %s in zone %s has incorrect CIDR: expected %s, got %v", zonalSubnetName, z.Name, z.CIDR, getCIDR(zonalSubnet.Status.IPv4Allocation))
			}
		}

		// Assert CloudNAT Resources
		egressResourceName := infra.Namespace + "-" + z.Name
		egressSubnet := &ipamv1.Subnet{}
		if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := zoneMgmtClient.Get(ctx, client.ObjectKey{Name: egressResourceName, Namespace: f.project}, egressSubnet); err != nil {
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Errorf("Failed to get egress subnet %s in zone %s: %v", egressResourceName, z.Name, err)
		} else {
			if !isSubnetReady(egressSubnet.Status.Conditions) {
				t.Errorf("Egress subnet %s in zone %s is not Ready", egressResourceName, z.Name)
			}
			if egressSubnet.Status.IPv4Allocation != nil {
				expectedEgressCIDRs = append(expectedEgressCIDRs, egressSubnet.Status.IPv4Allocation.CIDR)
			}
		}

		cloudNATGateway := &networkingv1.CloudNATGateway{}
		if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := zoneMgmtClient.Get(ctx, client.ObjectKey{Name: egressResourceName, Namespace: f.project}, cloudNATGateway); err != nil {
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Errorf("Failed to get CloudNATGateway %s in zone %s: %v", egressResourceName, z.Name, err)
		} else {
			if !isSubnetReady(cloudNATGateway.Status.Conditions) {
				t.Errorf("CloudNATGateway %s in zone %s is not Ready", egressResourceName, z.Name)
			}
		}
	}

	if len(infra.Status.EgressCIDRs) == len(expectedEgressCIDRs) {
		for i := range infra.Status.EgressCIDRs {
			if infra.Status.EgressCIDRs[i] != expectedEgressCIDRs[i] {
				t.Errorf("Egress CIDR mismatch at index %d: expected %s, got %s", i, expectedEgressCIDRs[i], infra.Status.EgressCIDRs[i])
			}
		}
	} else {
		t.Errorf("Egress CIDRs length mismatch after collection: expected %d, got %d", len(expectedEgressCIDRs), len(infra.Status.EgressCIDRs))
	}
}

// infraSubnets are the subnets the extension creates for a single-zone Infrastructure.
type infraSubnets struct {
	root       *ipamglobalv1.Subnet
	zone       *ipamglobalv1.Subnet
	propagated *ipamv1.Subnet
	network    *ipamv1.Subnet
}

// getInfraSubnets reads the subnets of the Infrastructure called infraName in the test zone.
func (f *infraTestFixture) getInfraSubnets(ctx context.Context, infraName string) (*infraSubnets, error) {
	subnets := &infraSubnets{
		root:       &ipamglobalv1.Subnet{},
		zone:       &ipamglobalv1.Subnet{},
		propagated: &ipamv1.Subnet{},
		network:    &ipamv1.Subnet{},
	}
	zoneSubnetName := infraName + "-" + *zone
	if err := f.globalClient.Get(ctx, client.ObjectKey{Name: infraName, Namespace: f.project}, subnets.root); err != nil {
		return nil, fmt.Errorf("root subnet %s: %w", infraName, err)
	}
	if err := f.globalClient.Get(ctx, client.ObjectKey{Name: zoneSubnetName, Namespace: f.project}, subnets.zone); err != nil {
		return nil, fmt.Errorf("zone subnet %s: %w", zoneSubnetName, err)
	}
	if err := f.mgmtClient.Get(ctx, client.ObjectKey{Name: zoneSubnetName, Namespace: f.project}, subnets.propagated); err != nil {
		return nil, fmt.Errorf("propagated zone subnet %s: %w", zoneSubnetName, err)
	}
	if err := f.mgmtClient.Get(ctx, client.ObjectKey{Name: "z-" + zoneSubnetName, Namespace: f.project}, subnets.network); err != nil {
		return nil, fmt.Errorf("network subnet z-%s: %w", zoneSubnetName, err)
	}
	return subnets, nil
}

// testSameNameInfrastructureKeepsSubnets creates two Infrastructures with the same name in
// two shoot namespaces that use the same GDC project, as two shoots with the same name in
// different garden projects do. Subnets are named after the shoot, so the second
// Infrastructure meets the subnets of the first one under the names it would use itself.
// It must fail to reconcile, and deleting it must leave those subnets untouched.
func (f *infraTestFixture) testSameNameInfrastructureKeepsSubnets(ctx context.Context, t *testing.T, namespace, saSecretName, otherNamespace, otherSASecretName string) {
	const ownerLabel = "infra-uid"
	testSubnet, nodeCIDR := f.allocateTestSubnet(ctx, t, defaultNodeCIDRPrefixLength)
	zones := splitNodeCIDRToZones(t, nodeCIDR, []string{*zone})
	infraName := fmt.Sprintf("sn-infra-%s-%s", *commitHash, rand.String(5))

	newInfra := func(namespace, saSecretName string) *extensionsv1alpha1.Infrastructure {
		return &extensionsv1alpha1.Infrastructure{
			ObjectMeta: metav1.ObjectMeta{
				Name:      infraName,
				Namespace: namespace,
			},
			Spec: extensionsv1alpha1.InfrastructureSpec{
				DefaultSpec: extensionsv1alpha1.DefaultSpec{
					Type: "gdch",
					ProviderConfig: &runtime.RawExtension{
						Raw: encode(t, &v1alpha1.InfrastructureConfig{
							Networks: v1alpha1.NetworkConfig{
								ParentReference: &v1alpha1.SubnetReference{
									Name:      testSubnet.Name,
									Namespace: ptr.To(testSubnet.Namespace),
									Type:      "SingleSubnet",
								},
								NodeCIDR: nodeCIDR,
								Zones:    zones,
							},
						}),
					},
				},
				SecretRef: corev1.SecretReference{
					Name:      saSecretName,
					Namespace: namespace,
				},
			},
		}
	}
	waitForInfra := func(infra *extensionsv1alpha1.Infrastructure, description string, done func(*extensionsv1alpha1.Infrastructure) bool) {
		t.Helper()
		infraList := &extensionsv1alpha1.InfrastructureList{}
		listOptions := []client.ListOption{
			client.InNamespace(infra.Namespace),
			client.MatchingFields{"metadata.name": infra.Name},
		}
		if err := kubernetes.WaitForCondition[*extensionsv1alpha1.Infrastructure](ctx, infraTimeout, func() (watch.Interface, error) {
			return f.vucClient.Watch(ctx, infraList, listOptions...)
		}, func(obj *extensionsv1alpha1.Infrastructure) bool {
			t.Logf("Waiting for %q in %q, LastError: %q",
				obj.Name, obj.Namespace,
				ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description)
			return done(obj)
		}); err != nil {
			t.Fatalf("Infrastructure %s/%s did not reach the expected state (%s) in %d minutes: %v", infra.Namespace, infra.Name, description, int(infraTimeout.Minutes()), err)
		}
	}

	// Arrange: the first Infrastructure owns the subnets.
	owner := newInfra(namespace, saSecretName)
	if err := f.vucClient.Create(ctx, owner); err != nil {
		t.Fatalf("cannot create infrastructure object %v", err)
	}
	// Registered before the cleanup of the Infrastructure, so it runs after it: once the
	// owner is deleted its subnets have to go away.
	t.Cleanup(func() {
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
			err := f.globalClient.Get(ctx, client.ObjectKey{Name: infraName, Namespace: f.project}, &ipamglobalv1.Subnet{})
			return apierrors.IsNotFound(err), nil
		}); err != nil {
			t.Errorf("root subnet %s still exists after its Infrastructure was deleted: %v", infraName, err)
		}
	})
	t.Cleanup(func() { f.cleanupInfraCR(ctx, t, owner) })
	waitForInfra(owner, "Succeeded", func(obj *extensionsv1alpha1.Infrastructure) bool {
		return ptr.Deref(obj.Status.LastOperation, gardencorev1beta1.LastOperation{}).State == gardencorev1beta1.LastOperationStateSucceeded
	})
	if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(owner), owner); err != nil {
		t.Fatalf("cannot get infrastructure object %v", err)
	}

	before, err := f.getInfraSubnets(ctx, infraName)
	if err != nil {
		t.Fatalf("cannot read the subnets of the first Infrastructure: %v", err)
	}
	// Assert: the subnets the extension creates carry the UID of their Infrastructure.
	for name, labels := range map[string]map[string]string{
		before.root.Name:               before.root.Labels,
		before.zone.Name:               before.zone.Labels,
		"zonal " + before.network.Name: before.network.Labels,
	} {
		if labels[ownerLabel] != string(owner.UID) {
			t.Errorf("subnet %s has label %s=%q, want the UID of its Infrastructure %q", name, ownerLabel, labels[ownerLabel], owner.UID)
		}
	}
	t.Logf("Propagated zone subnet %s: labels %v, annotations %v", before.propagated.Name, before.propagated.Labels, before.propagated.Annotations)

	// Action: a second Infrastructure with the same name in another shoot namespace.
	duplicate := newInfra(otherNamespace, otherSASecretName)
	if err := f.vucClient.Create(ctx, duplicate); err != nil {
		t.Fatalf("cannot create second infrastructure object %v", err)
	}
	t.Cleanup(func() { f.cleanupInfraCR(ctx, t, duplicate) })

	// Assert: it cannot take over the subnets of the first one.
	waitForInfra(duplicate, "error about the occupied root subnet", func(obj *extensionsv1alpha1.Infrastructure) bool {
		return strings.Contains(ptr.Deref(obj.Status.LastError, gardencorev1beta1.LastError{}).Description, "occupied by another shoot")
	})

	// Action: delete the second Infrastructure.
	f.cleanupInfraCR(ctx, t, duplicate)

	// Assert: the subnets of the first Infrastructure are the same objects as before and
	// are not being deleted.
	after, err := f.getInfraSubnets(ctx, infraName)
	if err != nil {
		t.Fatalf("subnets of the first Infrastructure are missing after the second one was deleted: %v", err)
	}
	for _, subnet := range []struct {
		name          string
		before, after metav1.Object
		ready         bool
	}{
		{"root subnet " + after.root.Name, before.root, after.root, isSubnetReady(after.root.Status.Conditions)},
		{"zone subnet " + after.zone.Name, before.zone, after.zone, isSubnetReady(after.zone.Status.Conditions)},
		{"propagated zone subnet " + after.propagated.Name, before.propagated, after.propagated, isSubnetReady(after.propagated.Status.Conditions)},
		{"network subnet " + after.network.Name, before.network, after.network, isSubnetReady(after.network.Status.Conditions)},
	} {
		if subnet.after.GetUID() != subnet.before.GetUID() {
			t.Errorf("%s was recreated: UID %s, was %s", subnet.name, subnet.after.GetUID(), subnet.before.GetUID())
		}
		if subnet.after.GetDeletionTimestamp() != nil {
			t.Errorf("%s is being deleted", subnet.name)
		}
		if !subnet.ready {
			t.Errorf("%s is not Ready", subnet.name)
		}
	}

	// Action: remove the owner labels from the zone and network subnets, which is the state
	// of subnets created before the labels were introduced, and reconcile the first
	// Infrastructure again.
	zoneSubnetPatch := client.MergeFrom(after.zone.DeepCopy())
	delete(after.zone.Labels, ownerLabel)
	networkSubnetPatch := client.MergeFrom(after.network.DeepCopy())
	delete(after.network.Labels, ownerLabel)
	// Removing the labels needs the permission to patch subnets, which the extension also
	// uses to label them. Without it this part of the case cannot run.
	if err := f.globalClient.Patch(ctx, after.zone, zoneSubnetPatch); err != nil {
		if apierrors.IsForbidden(err) {
			t.Logf("Skipping the relabelling check: not allowed to patch subnets in the global API: %v", err)
			return
		}
		t.Fatalf("cannot remove label %s from zone subnet %s: %v", ownerLabel, after.zone.Name, err)
	}
	if err := f.mgmtClient.Patch(ctx, after.network, networkSubnetPatch); err != nil {
		if apierrors.IsForbidden(err) {
			t.Logf("Skipping the relabelling check: not allowed to patch subnets in the zonal API: %v", err)
			return
		}
		t.Fatalf("cannot remove label %s from network subnet %s: %v", ownerLabel, after.network.Name, err)
	}
	ownerPatch := client.MergeFrom(owner.DeepCopy())
	if owner.Annotations == nil {
		owner.Annotations = map[string]string{}
	}
	owner.Annotations["gardener.cloud/operation"] = "reconcile"
	if err := f.vucClient.Patch(ctx, owner, ownerPatch); err != nil {
		t.Fatalf("cannot trigger a reconciliation of infrastructure %s/%s: %v", owner.Namespace, owner.Name, err)
	}

	// Assert: the Infrastructure takes its unlabelled subnets over again.
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		relabelled, err := f.getInfraSubnets(ctx, infraName)
		if err != nil {
			t.Logf("Waiting for the subnets of %s: %v", infraName, err)
			return false, nil
		}
		return relabelled.zone.Labels[ownerLabel] == string(owner.UID) && relabelled.network.Labels[ownerLabel] == string(owner.UID), nil
	}); err != nil {
		t.Errorf("zone and network subnets of %s were not labelled with their owner again: %v", infraName, err)
	}
}

func (f *infraTestFixture) cleanupInfraCR(ctx context.Context, t *testing.T, infra *extensionsv1alpha1.Infrastructure) {
	t.Helper()
	t.Logf(`cleaning up infrastructure object "%s/%s"`, infra.Namespace, infra.Name)
	if err := f.vucClient.Delete(ctx, infra); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		t.Errorf("cannot delete infrastructure object %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, infraTimeout, true, func(ctx context.Context) (bool, error) {
		if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(infra), infra); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	}); err != nil {
		t.Errorf("error waiting for infrastructure object to be deleted: %v", err)
	}
}

func encode(t *testing.T, obj runtime.Object) []byte {
	t.Helper()
	data, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("failed to marshal object to JSON: %v", err)
	}
	return data
}

func isSubnetReady(conditions []metav1.Condition) bool {
	readyCond := meta.FindStatusCondition(conditions, "Ready")
	return readyCond != nil && readyCond.Status == metav1.ConditionTrue
}

func getCIDR(allocation *ipamv1.SubnetAllocation) string {
	if allocation == nil {
		return "<nil>"
	}
	return allocation.CIDR
}

// allocateTestSubnet creates and allocates a new subnet for a test run.
func (f *infraTestFixture) allocateTestSubnet(ctx context.Context, t *testing.T, prefixLength int32) (*ipamglobalv1.Subnet, string) {
	t.Helper()

	subnetName := "infra-presubmit-subnet-" + rand.String(5) + "-" + *commitHash
	testSubnet := &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subnetName,
			Namespace: f.project,
		},
		Spec: ipamglobalv1.SubnetSpec{
			Type: ipamv1.Branch,
			IPv4Request: &ipamv1.SubnetRequest{
				PrefixLength: ptr.To[int32](prefixLength),
			},
			ParentReference: &ipamv1.SubnetReference{
				Name:      "gardener-parent-subnet-count-presubmit-25",
				Type:      ipamv1.SingleSubnet,
				Namespace: ptr.To(f.project),
			},
			PropagationStrategy: ipamv1.None,
		},
	}

	t.Cleanup(func() {
		// Retry deletion because of IPAM child subnet cleanup
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := f.globalClient.Delete(ctx, testSubnet); err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				// Return false to keep retrying (like admission webhook denials for child subnets)
				t.Logf("retrying subnet deletion for %s: %v", testSubnet.Name, err)
				return false, nil
			}
			return true, nil
		}); err != nil {
			t.Errorf("failed to delete dynamic test subnet after wait: %v", err)
		}
	})
	if err := f.globalClient.Create(ctx, testSubnet); err != nil {
		t.Fatalf("failed to create dynamic test subnet: %v", err)
	}

	// Wait for IPAM to assign the CIDR and mark it Ready
	if err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := f.globalClient.Get(ctx, client.ObjectKeyFromObject(testSubnet), testSubnet); err != nil {
			return false, err
		}
		// Uses the existing isSubnetReady helper from infra_test.go
		if isSubnetReady(testSubnet.Status.Conditions) && testSubnet.Status.IPv4Allocation != nil {
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("timed out waiting for dynamic subnet allocation: %v", err)
	}

	return testSubnet, testSubnet.Status.IPv4Allocation.CIDR
}

// splitNodeCIDRToZones splits a NodeCIDR into /32 subnets for each available zone.
func splitNodeCIDRToZones(t *testing.T, nodeCIDR string, availableZones []string) []v1alpha1.Zone {
	t.Helper()

	prefix, err := netip.ParsePrefix(nodeCIDR)
	if err != nil {
		t.Fatalf("failed to parse allocated NodeCIDR %q: %v", nodeCIDR, err)
	}

	// We want to create /32 prefixes out of this block for each zone
	var zones []v1alpha1.Zone
	addr := prefix.Addr()

	for i, zoneName := range availableZones {
		// Because it's a /32, each zone just takes the next sequential IP address
		zonePrefix := netip.PrefixFrom(addr, 32)
		zones = append(zones, v1alpha1.Zone{
			Name: zoneName,
			CIDR: zonePrefix.String(),
		})

		// Increment the IP address for the next zone
		addr = addr.Next()

		// Safety check: ensure we don't exceed the allocated block
		if !prefix.Contains(addr) && i < len(availableZones)-1 {
			t.Fatalf("allocated NodeCIDR %q is too small to provide %d zones", nodeCIDR, len(availableZones))
		}
	}

	return zones
}
