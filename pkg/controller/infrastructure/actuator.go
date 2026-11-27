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

// package infrastructure implements the infrastructure controller to manage infrastructure, such as iam, network.
package infrastructure

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	ptrutil "k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener/extensions/pkg/controller"
	"github.com/gardener/gardener/extensions/pkg/controller/infrastructure"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/gardener/gardener/pkg/controllerutils/reconciler"
	ipamglobalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/ipam/v1"
	ipamv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/ipam/v1"
	networkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"
	gdcapis "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/cloudprofile"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/errors"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/taskrunner"
)

const (
	subnetAllocationFailure            = "failed to allocate dynamic IPv4 CIDR"
	defaultSubnetCreationTime          = 5 * time.Second
	defaultCloudNATGatewayCreationTime = 5 * time.Second
)

type orgAdminClientLoaderFunc func(config *gdcclient.OrgClusterConfig, serviceAccount *auth.ServiceAccount, scheme *runtime.Scheme) (client.WithWatch, error)

// actuator implenents the Actuator in https://github.com/gardener/gardener/blob/master/extensions/pkg/controller/infrastructure/actuator.go
type actuator struct {
	client               client.Client
	decoder              runtime.Decoder
	restConfig           *rest.Config
	orgAdminClientLoader orgAdminClientLoaderFunc
}

type actuatorOption func(*actuator)

func withKubeClientGetter(loader orgAdminClientLoaderFunc) actuatorOption {
	return func(a *actuator) {
		a.orgAdminClientLoader = loader
	}
}

// NewActuator creates a new infrastructure.Actuator.
func NewActuator(mgr manager.Manager, opts ...actuatorOption) infrastructure.Actuator {
	decoder := serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder()
	a := &actuator{
		client:               mgr.GetClient(),
		decoder:              decoder,
		restConfig:           mgr.GetConfig(),
		orgAdminClientLoader: gdcclient.Get,
	}

	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Delete the infrastrucutre resource.
func (a *actuator) Delete(ctx context.Context, log logr.Logger, infrastructure *extensionsv1alpha1.Infrastructure, cluster *controller.Cluster) error {
	cp, err := cloudprofile.GetFromCluster(cluster, a.decoder)
	if err != nil {
		return fmt.Errorf("could not get cloud profile: %w", err)
	}

	serviceAccount, err := gdc.GetServiceAccountFromSecretReference(ctx, a.client, infrastructure.Spec.SecretRef)
	if err != nil {
		return fmt.Errorf("could not retrieve service account from secret reference '%s/%s': %w",
			infrastructure.Spec.SecretRef.Namespace, infrastructure.Spec.SecretRef.Name, err)
	}

	infraConfig, err := a.decodeInfrastructureConfig(infrastructure)
	if err != nil {
		return err
	}
	globalKubeclient, zonalKubeClients, err := a.createKubeClients(ctx, cp, infraConfig, serviceAccount)
	if err != nil {
		return err
	}
	runner := taskrunner.NewRunner()
	runner.AddTask("deleteAllSubnets", func() (any, error) {
		return nil, deleteAllSubnets(ctx, infrastructure, infraConfig, globalKubeclient, zonalKubeClients, serviceAccount)
	})
	runner.AddTask("deleteEgressOnShootVMs", func() (any, error) {
		return nil, deleteEgressOnShootVMs(ctx, infrastructure, infraConfig, zonalKubeClients, serviceAccount)
	})

	// Run all tasks in parallel and wait
	_, err = runner.Run()
	if err != nil {
		return err
	}

	return nil
}

// ForceDelete deletes the infrastructure, we bypass this logic in GDCH.
func (a *actuator) ForceDelete(ctx context.Context, log logr.Logger, infrastructure *extensionsv1alpha1.Infrastructure, cluster *controller.Cluster) error {
	return nil
}

// Based on https://github.com/gardener/gardener/blob/master/extensions/pkg/controller/infrastructure/actuator.go#L36C2-L36C114, Migrate
// should clean up terraform k8s resource, which is not supported in GDCH.
// Migrate logic is bypassed in GDCH infrastructure controller.
func (a *actuator) Migrate(ctx context.Context, log logr.Logger, infrastructure *extensionsv1alpha1.Infrastructure, cluster *controller.Cluster) error {
	return nil
}

func (a *actuator) Reconcile(ctx context.Context, log logr.Logger, infrastructure *extensionsv1alpha1.Infrastructure, cluster *controller.Cluster) error {
	cp, err := cloudprofile.GetFromCluster(cluster, a.decoder)
	if err != nil {
		return fmt.Errorf("could not get cloud profile: %w", err)
	}

	serviceAccount, err := gdc.GetServiceAccountFromSecretReference(ctx, a.client, infrastructure.Spec.SecretRef)
	if err != nil {
		return fmt.Errorf("could not retrieve service account from secret reference '%s/%s': %w",
			infrastructure.Spec.SecretRef.Namespace, infrastructure.Spec.SecretRef.Name, err)
	}
	seedName := "unknown-seed"
	if cluster != nil && cluster.Seed != nil && cluster.Seed.Name != "" {
		seedName = cluster.Seed.Name
	}

	return errors.DetermineError(a.reconcile(ctx, log, infrastructure, cp, serviceAccount, seedName))
}

// reconcile reconciles infrastructure resources for GDCH
func (a *actuator) reconcile(ctx context.Context, log logr.Logger, infrastructure *extensionsv1alpha1.Infrastructure, cp *gdcapis.CloudProfileConfig, serviceAccount *auth.ServiceAccount, seedName string) error {
	infraConfig, err := a.decodeInfrastructureConfig(infrastructure)
	if err != nil {
		return err
	}
	globalKubeClient, zonalKubeClients, err := a.createKubeClients(ctx, cp, infraConfig, serviceAccount)
	if err != nil {
		return fmt.Errorf("could not create kubeclients: %w", err)
	}

	var zonalKubeClientZones []string
	for zonalKubeClientZone := range zonalKubeClients {
		zonalKubeClientZones = append(zonalKubeClientZones, zonalKubeClientZone)
	}
	log.Info("Created kubeclients",
		"serviceAccountProject", serviceAccount.Project,
		"namespace", infrastructure.Namespace,
		"globalManagementAPI", cp.OrgConfig.GlobalManagementAPI,
		"zonalKubeClientZones", zonalKubeClientZones,
	)

	var (
		zoneSubnets     []v1alpha1.Zones
		egressCIDRs     map[string]string
		egressCIDRSlice []string
	)

	runner := taskrunner.NewRunner()
	runner.AddTask("createShootVMSubnets", func() (any, error) {
		return a.createShootVMSubnets(ctx, globalKubeClient, zonalKubeClients, infrastructure, infraConfig, serviceAccount, seedName)
	})

	switch {
	case infraConfig.EnableEgress == nil:
		// For backward compatibility, if EnableEgress is not defined then don't create the CloudNat resources.
		// But to cleanup the previously created resources, we need to delete them.
		log.Info("EnableEgress is not defined on infrastructure", "namespace", infrastructure.Namespace)
		runner.AddTask("deleteEgressOnShootVMs", func() (any, error) {
			return nil, deleteEgressOnShootVMs(ctx, infrastructure, infraConfig, zonalKubeClients, serviceAccount)
		})
	case *infraConfig.EnableEgress:
		log.Info("Enabling egress on infrastructure", "namespace", infrastructure.Namespace)
		runner.AddTask("createEgressOnShootVMs", func() (any, error) {
			return createEgressOnShootVMs(ctx, zonalKubeClients, infrastructure, infraConfig, serviceAccount)
		})
	default:
		log.Info("Disabling egress on infrastructure", "namespace", infrastructure.Namespace)
		runner.AddTask("deleteEgressOnShootVMs", func() (any, error) {
			return nil, deleteEgressOnShootVMs(ctx, infrastructure, infraConfig, zonalKubeClients, serviceAccount)
		})
	}

	// Run all tasks in parallel and wait
	success, err := runner.Run()
	if err != nil {
		return err
	}

	// Process successful results from the map
	if val, ok := success["createShootVMSubnets"]; ok {
		if subnets, ok := val.([]v1alpha1.Zones); ok {
			zoneSubnets = subnets
		}
	}
	if val, ok := success["createEgressOnShootVMs"]; ok {
		if cidrs, ok := val.(map[string]string); ok {
			egressCIDRs = cidrs
		}
	}

	if egressCIDRs != nil {
		egressCIDRSlice = make([]string, 0, len(zoneSubnets))
		for _, zone := range zoneSubnets {
			if cidr, ok := egressCIDRs[zone.Name]; ok {
				egressCIDRSlice = append(egressCIDRSlice, cidr)
			}
		}
	}

	status := &v1alpha1.InfrastructureStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "InfrastructureStatus",
		},
		Networks: v1alpha1.NetworkStatus{
			NodeCIDR: infraConfig.Networks.NodeCIDR,
			Zones:    zoneSubnets,
		},
		EnableEgress: infraConfig.EnableEgress,
	}

	bytes, _ := NewFlowState().ToJSON()
	state := &runtime.RawExtension{
		Raw: bytes,
	}
	return a.updateProviderStatus(ctx, infrastructure, status, state, egressCIDRSlice)
}

func (a *actuator) createShootVMSubnets(ctx context.Context, globalKubeClient client.Client, zonalKubeClients map[string]client.WithWatch, infrastructure *extensionsv1alpha1.Infrastructure, infraConfig *gdcapis.InfrastructureConfig, serviceAccount *auth.ServiceAccount, seedName string) ([]v1alpha1.Zones, error) {
	log := logr.FromContextOrDiscard(ctx)
	log.Info("Creating shoot VM subnets",
		"infrastructureName", infrastructure.Name,
		"infrastructureNamespace", infrastructure.Namespace,
		"nodeCIDR", infraConfig.Networks.NodeCIDR,
		"zones", infraConfig.Networks.Zones,
		"serviceAccountProject", serviceAccount.Project,
	)

	infraUID := string(infrastructure.UID)
	_, err := reconcileRootSubnet(ctx, globalKubeClient, zonalKubeClients, infrastructure, infraConfig, serviceAccount, seedName)
	if err != nil {
		return nil, fmt.Errorf("error creating infrastructure node subnet %q (project: %s, nodeCIDR: %s): %w",
			infrastructure.Name, serviceAccount.Project, infraConfig.Networks.NodeCIDR, err)
	}
	// if infraRootSubnet can be created, create zoneSubnet inherit from that
	zoneSubnets := make([]v1alpha1.Zones, 0)
	for _, zone := range infraConfig.Networks.Zones {
		// zoneSubnet need to be propagated to its corresponding zone
		zoneSubnetName := infrastructure.Name + "-" + zone.Name
		_, err := reconcileGlobalSubnet(ctx, globalKubeClient, zonalKubeClients, &zone.Name, zone.CIDR, infrastructure, infraConfig, serviceAccount)
		if err != nil {
			return nil, fmt.Errorf("error creating infrastructure zone subnet %q (zone: %s, cidr: %s): %w",
				zoneSubnetName, zone.Name, zone.CIDR, err)
		}

		// Wait for the global subnet to be propagated to the zone
		propagatedSubnet, err := waitForZonalSubnetReady(ctx, zoneSubnetName, serviceAccount.Project, zonalKubeClients[zone.Name])
		if err != nil {
			return nil, fmt.Errorf("timeout waiting for global subnet %q to propagate to zone %s (project: %s): %w",
				zoneSubnetName, zone.Name, serviceAccount.Project, err)
		}
		// A zonal subnet with this name can also belong to another shoot. The network subnet
		// must only be created below the copy of the zone subnet of this shoot.
		if !ownsPropagatedSubnet(propagatedSubnet, infrastructure.Name, serviceAccount.Project, true, true) {
			return nil, fmt.Errorf("terminal error: zonal subnet %q in zone %s is not the propagated copy of the zone subnet of this shoot. Please use a different shoot name to fix",
				zoneSubnetName, zone.Name)
		}

		// network subnet name example: z-shoot-garden-cluster1-europe-east1
		zoneNetworkSubnetName := "z-" + infrastructure.Name + "-" + zone.Name
		zoneNetworkSubnet, err := ReconcileZonalSubnet(ctx, zonalKubeClients[zone.Name], zone.CIDR, zoneNetworkSubnetName, zoneSubnetName, serviceAccount.Project, infraUID)
		if err != nil {
			return nil, fmt.Errorf("error creating infrastructure zone network subnet %q (zone: %s, cidr: %s): %w",
				zoneNetworkSubnetName, zone.Name, zone.CIDR, err)
		}

		zoneSubnets = append(zoneSubnets, v1alpha1.Zones{
			Name:   zone.Name,
			Subnet: zoneNetworkSubnet.Name,
		})
	}
	return zoneSubnets, nil
}

func createEgressOnShootVMs(ctx context.Context, zonalKubeClients map[string]client.WithWatch, infrastructure *extensionsv1alpha1.Infrastructure, infraConfig *gdcapis.InfrastructureConfig, serviceAccount *auth.ServiceAccount) (map[string]string, error) {
	egressCIDRs := make(map[string]string)
	for _, zone := range infraConfig.Networks.Zones {
		zonalClient, ok := zonalKubeClients[zone.Name]
		if !ok {
			return nil, fmt.Errorf("no kubeclient for zone %s", zone.Name)
		}

		egressResourceName := getEgressResourceName(infrastructure.Namespace, zone.Name)

		parentSubnetName := fmt.Sprintf("data-network-segment-%s-group", zone.Name)
		subnet := &ipamv1.Subnet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      egressResourceName,
				Namespace: serviceAccount.Project,
			},
			Spec: ipamv1.SubnetSpec{
				Type: ipamv1.Leaf,
				IPv4Request: &ipamv1.SubnetRequest{
					PrefixLength: ptrutil.To(int32(32)),
				},
				ParentReference: &ipamv1.SubnetReference{
					Name:      parentSubnetName,
					Namespace: ptrutil.To("platform"),
					Type:      "SubnetGroup",
				},
			},
		}
		if _, err := controllerutil.CreateOrUpdate(ctx, zonalClient, subnet, func() error { return nil }); err != nil {
			return nil, fmt.Errorf("failed to create egress subnet in zone %s: %w", zone.Name, err)
		}

		readySubnet, err := waitForZonalSubnetReady(ctx, egressResourceName, serviceAccount.Project, zonalClient)
		if err != nil {
			return nil, fmt.Errorf("error waiting for egress subnet to be ready in zone %s: %w", zone.Name, err)
		}

		cloudNATGateway := &networkingv1.CloudNATGateway{
			ObjectMeta: metav1.ObjectMeta{
				Name:      egressResourceName,
				Namespace: serviceAccount.Project,
			},
			Spec: networkingv1.CloudNATGatewaySpec{
				WorkloadSelector: &networkingv1.WorkloadSelector{
					LabelSelector: &networkingv1.WorkloadLabelSelector{
						Workloads: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								gdcconstants.WorkloadLabelSelectorKey: infrastructure.Namespace,
							},
						},
					},
				},
				SubnetRefs: []string{egressResourceName},
			},
		}
		if _, err := controllerutil.CreateOrUpdate(ctx, zonalClient, cloudNATGateway, func() error { return nil }); err != nil {
			return nil, fmt.Errorf("failed to create CloudNATGateway in zone %s: %w", zone.Name, err)
		}

		if _, err := waitForCloudNATGatewayReady(ctx, egressResourceName, serviceAccount.Project, zonalClient); err != nil {
			return nil, fmt.Errorf("timeout waiting for CloudNATGateway %q to be ready in zone %s: %w", egressResourceName, zone.Name, err)
		}

		egressCIDRs[zone.Name] = readySubnet.Status.IPv4Allocation.CIDR
	}

	return egressCIDRs, nil
}

// Restore implements the restoration of an infrastructure resource during the controlplane migration,
// GDCH reconciles the infrastructure again.
func (a *actuator) Restore(ctx context.Context, log logr.Logger, infrastructure *extensionsv1alpha1.Infrastructure, cluster *controller.Cluster) error {
	return a.Reconcile(ctx, log, infrastructure, cluster)
}

func (a *actuator) updateProviderStatus(
	ctx context.Context,
	infra *extensionsv1alpha1.Infrastructure,
	status *v1alpha1.InfrastructureStatus,
	state *runtime.RawExtension,
	egressCIDRs []string,
) error {
	patch := client.MergeFrom(infra.DeepCopy())
	infra.Status.ProviderStatus = &runtime.RawExtension{Object: status}
	infra.Status.EgressCIDRs = egressCIDRs
	infra.Status.State = state
	if err := a.client.Status().Patch(ctx, infra, patch); err != nil {
		return fmt.Errorf("patch status for infrastructure %q: %w", infra.Name, err)
	}
	return nil
}

func reconcileRootSubnet(
	ctx context.Context,
	globalKubeclient client.Client,
	zonalKubeClients map[string]client.WithWatch,
	infrastructure *extensionsv1alpha1.Infrastructure,
	infraConfig *gdcapis.InfrastructureConfig,
	serviceAccount *auth.ServiceAccount,
	seedName string,
) (*ipamglobalv1.Subnet, error) {
	var parentSubnetProject string
	var parentSubnetName string
	var parentSubnetType ipamv1.ReferenceType
	if infraConfig.Networks.ParentReference != nil {
		parentSubnetName = infraConfig.Networks.ParentReference.Name
		parentSubnetType = ipamv1.ReferenceType(string(infraConfig.Networks.ParentReference.Type))
		if infraConfig.Networks.ParentReference.Namespace != nil {
			parentSubnetProject = *infraConfig.Networks.ParentReference.Namespace
		}
	} else {
		parentSubnetName = infraConfig.Networks.ParentSubnet
		parentSubnetType = ipamv1.SingleSubnet
		parentSubnetProject = infraConfig.Networks.ParentSubnetProject
	}
	if parentSubnetProject == "" {
		parentSubnetProject = serviceAccount.Project
	}

	parentRef := &ipamv1.SubnetReference{
		Name:      parentSubnetName,
		Namespace: &parentSubnetProject,
		Type:      parentSubnetType,
	}
	zone := infraConfig.Networks.Zones[0].Name
	labelKey, labelValue := getOwnershipLabels(serviceAccount.Project, infrastructure.Name, seedName, zone)
	infraUID := string(infrastructure.UID)

	desiredSubnet := &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      infrastructure.Name,
			Namespace: serviceAccount.Project,
			Labels: map[string]string{
				labelKey:      labelValue,
				infraUIDLabel: infraUID,
			},
		},
		Spec: ipamglobalv1.SubnetSpec{
			Type: ipamv1.Branch,
			IPv4Request: &ipamv1.SubnetRequest{
				CIDR: &infraConfig.Networks.NodeCIDR,
			},
			ParentReference: parentRef,
		},
	}

	curSubnet := &ipamglobalv1.Subnet{}
	if err := globalKubeclient.Get(ctx, client.ObjectKey{Name: desiredSubnet.Name, Namespace: desiredSubnet.Namespace}, curSubnet); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("unable to get Subnet: %w", err)
		}
		if _, err = createRootSubnet(ctx, globalKubeclient, desiredSubnet); err != nil {
			return nil, fmt.Errorf("unable to create new subnet: %w", err)
		}
	} else {
		// if found subnet with label "infra-uid",  we check if the UID matches
		// In case of the UIDs are not matched, it means this subnet belongs to another shoot, throw an error
		existingUID, exists := curSubnet.Labels[infraUIDLabel]

		if exists {
			// If the label exists but DOES NOT match, it belongs to someone else. Reject.
			if existingUID != infraUID {
				existingOwner := getExistingOwnerLabel(curSubnet.Labels)
				return nil, fmt.Errorf("terminal error: global root subnet is occupied by another shoot, the existing fetched label's shoot: %s", existingOwner)
			}
			// If it exists and matches, do nothing! It's our healthy subnet.
		} else {
			// If the label DOES NOT exist, this is an unlabelled legacy subnet.

			// If ProviderStatus is nil, this is a brand new shoot with subnet name collides. Reject.
			if infrastructure.Status.ProviderStatus == nil {
				return nil, fmt.Errorf("terminal error: root subnet %q already exists and belongs to an older legacy shoot. Please use a different shoot name to fix", curSubnet.Name)
			}

			// If ProviderStatus is NOT nil, it's the legacy shoot reconciling itself. Adopt it.
			patch := client.MergeFrom(curSubnet.DeepCopy())
			if curSubnet.Labels == nil {
				curSubnet.Labels = make(map[string]string)
			}
			curSubnet.Labels[infraUIDLabel] = infraUID
			curSubnet.Labels[labelKey] = labelValue

			if err := globalKubeclient.Patch(ctx, curSubnet, patch); err != nil {
				return nil, fmt.Errorf("failed to adopt legacy subnet with ownership labels: %w", err)
			}
		}

		cidrChanged := !ptrutil.Equal(desiredSubnet.Spec.IPv4Request.CIDR, curSubnet.Spec.IPv4Request.CIDR)
		parentNameChanged := desiredSubnet.Spec.ParentReference.Name != curSubnet.Spec.ParentReference.Name
		parentNamespaceChanged := !ptrutil.Equal(desiredSubnet.Spec.ParentReference.Namespace, curSubnet.Spec.ParentReference.Namespace)

		if cidrChanged || parentNameChanged || parentNamespaceChanged {
			if err := deleteAllSubnets(ctx, infrastructure, infraConfig, globalKubeclient, zonalKubeClients, serviceAccount); err != nil {
				return nil, err
			}
			if _, err = createRootSubnet(ctx, globalKubeclient, desiredSubnet); err != nil {
				return nil, err
			}
		}
	}

	subnet, err := waitForGlobalSubnetReady(ctx, desiredSubnet.Name, desiredSubnet.Namespace, globalKubeclient)
	if err != nil {
		return nil, err
	}
	return subnet, nil
}

func createRootSubnet(
	ctx context.Context,
	globalKubeclient client.Client,
	subnet *ipamglobalv1.Subnet,
) (*ipamglobalv1.Subnet, error) {
	if subnet.Labels == nil {
		subnet.Labels = map[string]string{}
	}
	subnet.Labels["ipam.gdc.goog/vpc"] = "default-vpc"
	if err := globalKubeclient.Create(ctx, subnet); err != nil {
		return nil, fmt.Errorf("failed to create Subnet: %w", err)
	}
	return subnet, nil
}

func reconcileGlobalSubnet(
	ctx context.Context,
	globalKubeclient client.Client,
	zonalKubeClients map[string]client.WithWatch,
	propagationZone *string,
	subnetCIDR string,
	infrastructure *extensionsv1alpha1.Infrastructure,
	infraConfig *gdcapis.InfrastructureConfig,
	serviceAccount *auth.ServiceAccount,
) (*ipamglobalv1.Subnet, error) {
	subnetName := infrastructure.Name + "-" + *propagationZone
	infraUID := string(infrastructure.UID)
	subnet := &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subnetName,
			Namespace: serviceAccount.Project,
		},
	}
	curSubnet := &ipamglobalv1.Subnet{}
	if err := globalKubeclient.Get(ctx, client.ObjectKey{Name: subnet.Name, Namespace: subnet.Namespace}, curSubnet); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("unable to get Subnet: %w", err)
		}
		if _, err = createGlobalSubnet(ctx, globalKubeclient, infrastructure, infraConfig, serviceAccount, subnet, propagationZone, subnetCIDR); err != nil {
			return nil, err
		}
	} else {
		// A subnet with this name can also belong to another shoot. It is only changed or
		// deleted once it is known to be a subnet of this shoot.
		if err := claimSubnet(ctx, globalKubeclient, curSubnet, curSubnet.Spec.ParentReference, infraUID, infrastructure.Name, serviceAccount.Project); err != nil {
			return nil, err
		}
		if !ptrutil.Equal(curSubnet.Spec.IPv4Request.CIDR, &subnetCIDR) || curSubnet.Spec.ParentReference.Name != infrastructure.Name {
			zonalSubnet := "z-" + infrastructure.Name + "-" + *propagationZone
			zonalClient := zonalKubeClients[*propagationZone]
			networkSubnet := &ipamv1.Subnet{}
			exists, err := getSubnet(ctx, zonalClient, serviceAccount.Project, zonalSubnet, networkSubnet)
			if err != nil {
				return nil, err
			}
			if exists {
				if !ownsChildSubnet(networkSubnet.Labels, networkSubnet.Spec.ParentReference, infraUID, subnetName, serviceAccount.Project, true) {
					return nil, fmt.Errorf("terminal error: zonal subnet %q is occupied by another shoot. Please use a different shoot name to fix", zonalSubnet)
				}
				if err := deleteZonalSubnet(ctx, zonalClient, serviceAccount.Project, zonalSubnet); err != nil {
					return nil, err
				}
			}
			err = deleteSubnet(ctx, globalKubeclient, serviceAccount.Project, subnetName)
			if err != nil {
				return nil, err
			}
			if _, err = createGlobalSubnet(ctx, globalKubeclient, infrastructure, infraConfig, serviceAccount, subnet, propagationZone, subnetCIDR); err != nil {
				return nil, err
			}
		}
	}
	subnet, err := waitForGlobalSubnetReady(ctx, subnetName, serviceAccount.Project, globalKubeclient)
	if err != nil {
		return nil, err
	}
	return subnet, nil
}

func createGlobalSubnet(
	ctx context.Context,
	globalKubeclient client.Client,
	infrastructure *extensionsv1alpha1.Infrastructure,
	infraConfig *gdcapis.InfrastructureConfig,
	serviceAccount *auth.ServiceAccount,
	subnet *ipamglobalv1.Subnet,
	propagationZone *string,
	subnetCIDR string,
) (*ipamglobalv1.Subnet, error) {
	subnet.Spec = ipamglobalv1.SubnetSpec{
		Type:                ipamv1.Branch,
		Zone:                propagationZone,
		PropagationStrategy: "SingleZone",
		IPv4Request: &ipamv1.SubnetRequest{
			CIDR: &subnetCIDR,
		},
		ParentReference: &ipamv1.SubnetReference{
			Name:      infrastructure.Name,
			Namespace: &serviceAccount.Project,
		},
	}
	if subnet.Labels == nil {
		subnet.Labels = map[string]string{}
	}
	subnet.Labels["ipam.gdc.goog/vpc"] = "default-vpc"
	subnet.Labels["ipam.gdc.goog/usage"] = "zone-network-root-range"
	subnet.Labels[infraUIDLabel] = string(infrastructure.UID)

	if err := globalKubeclient.Create(ctx, subnet); err != nil {
		return nil, fmt.Errorf("failed to create Subnet: %w", err)
	}
	return subnet, nil
}

// ReconcileZonalSubnet creates the network subnet of a zone below the subnet called
// parentSubnetName, or takes over the existing one if it belongs to the Infrastructure
// with the given UID.
func ReconcileZonalSubnet(
	ctx context.Context,
	zonalKubeclient client.Client,
	subnetCIDR string,
	subnetName string,
	parentSubnetName string,
	namespace string,
	infraUID string,
) (*ipamv1.Subnet, error) {
	subnet := &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subnetName,
			Namespace: namespace,
		},
	}
	if subnet.Labels == nil {
		subnet.Labels = map[string]string{}
	}
	subnet.Labels["ipam.gdc.goog/vpc"] = "default-vpc"
	subnet.Labels[infraUIDLabel] = infraUID

	subnet.Spec = ipamv1.SubnetSpec{
		Type: ipamv1.Branch,
		IPv4Request: &ipamv1.SubnetRequest{
			CIDR: &subnetCIDR,
		},
		ParentReference: &ipamv1.SubnetReference{
			Name:      parentSubnetName,
			Namespace: &namespace,
		},
		NetworkSpec: &ipamv1.NetworkSpec{
			// Essential flag for usage by VMs.
			EnableGateway: true,
		},
	}
	curSubnet := &ipamv1.Subnet{}
	err := zonalKubeclient.Get(ctx, client.ObjectKey{Name: subnet.Name, Namespace: subnet.Namespace}, curSubnet)
	if apierrors.IsNotFound(err) {
		err := zonalKubeclient.Create(ctx, subnet)
		if err != nil {
			return nil, fmt.Errorf("failed to create subnet: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("unable to get Subnet: %w", err)
	} else if err := claimSubnet(ctx, zonalKubeclient, curSubnet, curSubnet.Spec.ParentReference, infraUID, parentSubnetName, namespace); err != nil {
		// A subnet with this name can also belong to another shoot, which must not be used
		// as the node network of this one.
		return nil, err
	}
	subnet, err = waitForZonalSubnetReady(ctx, subnetName, namespace, zonalKubeclient)
	if err != nil {
		return nil, err
	}

	return subnet, nil
}

func waitForZonalSubnetReady(ctx context.Context, subnetName, namespace string, kubeclient client.Client) (*ipamv1.Subnet, error) {
	subnet := &ipamv1.Subnet{}
	key := client.ObjectKey{Name: subnetName, Namespace: namespace}
	if err := kubeclient.Get(ctx, key, subnet); err != nil {
		return nil, fmt.Errorf("unable to get Subnet '%s/%s': %w", namespace, subnetName, err)
	}

	readyCond := meta.FindStatusCondition(subnet.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Status != metav1.ConditionTrue {
		message := ""
		if readyCond != nil {
			message = readyCond.Message
		}
		if strings.Contains(message, subnetAllocationFailure) {
			return nil, fmt.Errorf("the specified CIDR range of subnet '%s/%s' is already being used: %s", subnet.GetNamespace(), subnet.GetName(), message)
		}
		return nil, &reconciler.RequeueAfterError{
			Cause:        fmt.Errorf("zonal subnet '%s/%s' not ready: %s", subnet.GetNamespace(), subnet.GetName(), message),
			RequeueAfter: defaultSubnetCreationTime,
		}
	}

	return subnet, nil
}

func waitForCloudNATGatewayReady(ctx context.Context, name, namespace string, kubeclient client.Client) (*networkingv1.CloudNATGateway, error) {
	cng := &networkingv1.CloudNATGateway{}
	key := client.ObjectKey{Name: name, Namespace: namespace}
	if err := kubeclient.Get(ctx, key, cng); err != nil {
		return nil, fmt.Errorf("unable to get CloudNATGateway '%s/%s': %w", namespace, name, err)
	}

	readyCond := meta.FindStatusCondition(cng.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Status != metav1.ConditionTrue {
		message := ""
		if readyCond != nil {
			message = readyCond.Message
		}
		return nil, &reconciler.RequeueAfterError{
			Cause:        fmt.Errorf("CloudNATGateway '%s/%s' not ready: %s", cng.GetNamespace(), cng.GetName(), message),
			RequeueAfter: defaultCloudNATGatewayCreationTime,
		}
	}

	return cng, nil
}

func waitForGlobalSubnetReady(ctx context.Context, subnetName, namespace string, kubeclient client.Client) (*ipamglobalv1.Subnet, error) {
	subnet := &ipamglobalv1.Subnet{}
	key := client.ObjectKey{Name: subnetName, Namespace: namespace}
	if err := kubeclient.Get(ctx, key, subnet); err != nil {
		return nil, fmt.Errorf("unable to get Subnet %q: %w", key.Name, err)
	}
	readyCond := meta.FindStatusCondition(subnet.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Status != metav1.ConditionTrue {
		message := ""
		if readyCond != nil {
			message = readyCond.Message
		}
		if strings.Contains(message, subnetAllocationFailure) {
			return nil, fmt.Errorf("the specified CIDR range of subnet '%s/%s' is already being used: %s", subnet.GetNamespace(), subnet.GetName(), message)
		}
		return nil, &reconciler.RequeueAfterError{
			Cause:        fmt.Errorf("global subnet '%s/%s' not ready: %s", subnet.GetNamespace(), subnet.GetName(), message),
			RequeueAfter: defaultSubnetCreationTime,
		}
	}

	return subnet, nil
}

// Helper function to delete a subnet
func deleteSubnet(ctx context.Context, kubeclient client.Client, namespace, subnetName string) error {
	nodeSubnet := &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subnetName,
			Namespace: namespace,
		},
	}

	if err := kubeclient.Delete(ctx, nodeSubnet); err != nil && !apierrors.IsNotFound(err) {
		wrappedErr := fmt.Errorf("failed to delete global subnet %q: %w", subnetName, err)
		if strings.Contains(err.Error(), "children") {
			return &reconciler.RequeueAfterError{
				Cause:        wrappedErr,
				RequeueAfter: defaultSubnetCreationTime,
			}
		}
		return wrappedErr
	}
	return nil
}

func deleteZonalSubnet(ctx context.Context, zonalKubeClient client.Client, namespace, subnetName string) error {
	nodeSubnet := &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subnetName,
			Namespace: namespace,
		},
	}

	if err := zonalKubeClient.Delete(ctx, nodeSubnet); err != nil && !apierrors.IsNotFound(err) {
		wrappedErr := fmt.Errorf("failed to delete zonal subnet %q: %w", subnetName, err)
		if strings.Contains(err.Error(), "children") {
			return &reconciler.RequeueAfterError{
				Cause:        wrappedErr,
				RequeueAfter: defaultSubnetCreationTime,
			}
		}
		return wrappedErr
	}
	return nil
}

// Helper method to create the kubeclients
func (a *actuator) createKubeClients(ctx context.Context, cp *gdcapis.CloudProfileConfig, infraConfig *gdcapis.InfrastructureConfig, serviceAccount *auth.ServiceAccount) (client.Client, map[string]client.WithWatch, error) {
	zonalKubeClients := map[string]client.WithWatch{}
	cfg := &gdcclient.OrgClusterConfig{
		CAData: cp.OrgConfig.CAData,
	}
	if cp.OrgConfig.GlobalManagementAPI == "" {
		return nil, nil, fmt.Errorf("could not get global management API from cloud profile")
	}
	cfg.OrgClusterURL = cp.OrgConfig.GlobalManagementAPI
	globalClient, err := a.orgAdminClientLoader(cfg, serviceAccount, a.client.Scheme())
	if err != nil {
		return nil, nil, fmt.Errorf("could not create kubeclient: %w", err)
	}
	if len(infraConfig.Networks.Zones) == 0 {
		return nil, nil, fmt.Errorf("could not get zones from infrastructure config")
	}
	for _, zone := range infraConfig.Networks.Zones {
		managementURL, err := findManagementURL(zone.Name, cp)
		if err != nil {
			return nil, nil, err
		}
		cfg.OrgClusterURL = managementURL
		kubeclient, err := a.orgAdminClientLoader(cfg, serviceAccount, a.client.Scheme())
		if err != nil {
			return nil, nil, fmt.Errorf("could not create kubeclient for zone %s: %w", zone.Name, err)
		}
		zonalKubeClients[zone.Name] = kubeclient
	}
	return globalClient, zonalKubeClients, nil
}

// decodeInfrastructureConfig extracts the raw object from the Infrastructure CR
// and decodes the raw bytes into an `InfrastructureConfig` object.
func (a *actuator) decodeInfrastructureConfig(infra *extensionsv1alpha1.Infrastructure) (*gdcapis.InfrastructureConfig, error) {
	pc := infra.Spec.ProviderConfig
	if pc == nil || len(pc.Raw) == 0 {
		return nil, fmt.Errorf("provider config is not specified")
	}

	config := &gdcapis.InfrastructureConfig{}
	if _, _, err := a.decoder.Decode(pc.Raw, nil, config); err != nil {
		return nil, fmt.Errorf("decode infrastructure config: %w", err)
	}
	return config, nil
}

// check if zone configured in infrastructure config also exist in cloudprofile
// if cloudprofile has such zone, return the management API for that zone
func findManagementURL(zoneName string, cp *gdcapis.CloudProfileConfig) (string, error) {
	for _, zone := range cp.OrgConfig.Zones {
		if zone.Name == zoneName {
			return zone.ManagementAPI, nil
		}
	}
	return "", fmt.Errorf("could not find zone %s in cloud profile", zoneName)
}

// deleteAllSubnets deletes the subnets of the Infrastructure. Subnets are named after the
// shoot, so a subnet with one of the expected names can belong to another shoot. Only
// subnets that are known to belong to this Infrastructure are deleted; the others are left
// untouched and are not treated as an error, so that a shoot which never owned them can be
// deleted.
func deleteAllSubnets(ctx context.Context, infrastructure *extensionsv1alpha1.Infrastructure, infraConfig *gdcapis.InfrastructureConfig, globalKubeclient client.Client, zonalKubeClients map[string]client.WithWatch, serviceAccount *auth.ServiceAccount) error {
	log := logr.FromContextOrDiscard(ctx).WithValues("infrastructureName", infrastructure.Name, "infrastructureNamespace", infrastructure.Namespace, "project", serviceAccount.Project)
	project := serviceAccount.Project
	infraUID := string(infrastructure.UID)

	rootSubnet := &ipamglobalv1.Subnet{}
	rootExists, err := getSubnet(ctx, globalKubeclient, project, infrastructure.Name, rootSubnet)
	if err != nil {
		return errors.DetermineError(err)
	}
	rootOwned := rootExists && isRootSubnetOwned(rootSubnet, infrastructure)
	if rootExists && !rootOwned {
		log.Info("Root subnet does not belong to this Infrastructure, leaving it and its subnets in place", "subnet", infrastructure.Name)
	}

	for _, zone := range infraConfig.Networks.Zones {
		zonalClient := zonalKubeClients[zone.Name]
		zoneSubnetName := infrastructure.Name + "-" + zone.Name
		zoneNetworkSubnetName := "z-" + zoneSubnetName

		// The zone subnet decides about the unlabelled subnets in the zone: GDC propagates
		// a copy of it into the zone, and the network subnet is a child of that copy.
		zoneSubnet := &ipamglobalv1.Subnet{}
		zoneSubnetExists, err := getSubnet(ctx, globalKubeclient, project, zoneSubnetName, zoneSubnet)
		if err != nil {
			return errors.DetermineError(err)
		}
		zoneSubnetOwned := zoneSubnetExists && ownsChildSubnet(zoneSubnet.Labels, zoneSubnet.Spec.ParentReference, infraUID, infrastructure.Name, project, rootOwned)
		zoneOwned := rootOwned
		if zoneSubnetExists {
			zoneOwned = zoneSubnetOwned
		}

		// The copy that GDC propagates into the zone is the parent of the network subnet.
		// While it exists, it decides about a network subnet without the owner label.
		propagatedSubnet := &ipamv1.Subnet{}
		propagatedExists, err := getSubnet(ctx, zonalClient, project, zoneSubnetName, propagatedSubnet)
		if err != nil {
			return errors.DetermineError(err)
		}
		propagatedOwned := propagatedExists && ownsPropagatedSubnet(propagatedSubnet, infrastructure.Name, project, zoneSubnetExists, zoneOwned)
		networkParentOwned := zoneOwned
		if propagatedExists {
			networkParentOwned = propagatedOwned
		}

		networkSubnet := &ipamv1.Subnet{}
		exists, err := getSubnet(ctx, zonalClient, project, zoneNetworkSubnetName, networkSubnet)
		if err != nil {
			return errors.DetermineError(err)
		}
		if exists {
			if ownsChildSubnet(networkSubnet.Labels, networkSubnet.Spec.ParentReference, infraUID, zoneSubnetName, project, networkParentOwned) {
				if err := deleteZonalSubnet(ctx, zonalClient, project, zoneNetworkSubnetName); err != nil {
					return errors.DetermineError(err)
				}
			} else {
				log.Info("Zonal subnet does not belong to this Infrastructure, leaving it in place", "subnet", zoneNetworkSubnetName, "zone", zone.Name)
			}
		}

		if propagatedExists {
			if propagatedOwned {
				if err := deleteZonalSubnet(ctx, zonalClient, project, zoneSubnetName); err != nil {
					return errors.DetermineError(err)
				}
			} else {
				log.Info("Zonal subnet does not belong to this Infrastructure, leaving it in place", "subnet", zoneSubnetName, "zone", zone.Name)
			}
		}

		if zoneSubnetExists {
			if zoneSubnetOwned {
				if err := deleteSubnet(ctx, globalKubeclient, project, zoneSubnetName); err != nil {
					return errors.DetermineError(err)
				}
			} else {
				log.Info("Zone subnet does not belong to this Infrastructure, leaving it in place", "subnet", zoneSubnetName)
			}
		}
	}

	if rootOwned {
		if err := deleteSubnet(ctx, globalKubeclient, project, infrastructure.Name); err != nil {
			return errors.DetermineError(err)
		}
	}
	return nil
}

func deleteEgressOnShootVMs(ctx context.Context, infrastructure *extensionsv1alpha1.Infrastructure, infraConfig *gdcapis.InfrastructureConfig, zonalKubeClients map[string]client.WithWatch, serviceAccount *auth.ServiceAccount) error {
	for _, zone := range infraConfig.Networks.Zones {
		zonalClient, ok := zonalKubeClients[zone.Name]
		if !ok {
			return fmt.Errorf("no kubeclient for zone %s", zone.Name)
		}

		egressResourceName := getEgressResourceName(infrastructure.Namespace, zone.Name)

		cloudNATGateway := &networkingv1.CloudNATGateway{
			ObjectMeta: metav1.ObjectMeta{
				Name:      egressResourceName,
				Namespace: serviceAccount.Project,
			},
		}
		if err := zonalClient.Delete(ctx, cloudNATGateway); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsForbidden(err) {
			return fmt.Errorf("failed to delete CloudNATGateway in zone %s: %w", zone.Name, err)
		}

		subnet := &ipamv1.Subnet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      egressResourceName,
				Namespace: serviceAccount.Project,
			},
		}
		if err := zonalClient.Delete(ctx, subnet); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsForbidden(err) {
			return fmt.Errorf("failed to delete egress subnet in zone %s: %w", zone.Name, err)
		}
	}
	return nil
}

func getEgressResourceName(shootName string, zoneName string) string {
	return fmt.Sprintf("%s-%s", shootName, zoneName)
}

// getOwnershipLabelKey constructs the ownership label key safely.
func getOwnershipLabels(project, shootName, seedName, zone string) (string, string) {
	key := fmt.Sprintf("shootcontrolplane-%s-%s", project, shootName)
	value := fmt.Sprintf("%s-%s", zone, seedName)
	return key, value
}

func getExistingOwnerLabel(labels map[string]string) string {
	for k, v := range labels {
		if strings.HasPrefix(k, "shootcontrolplane-") {
			return fmt.Sprintf("%s: %s", k, v)
		}
	}
	return "unknown"
}
