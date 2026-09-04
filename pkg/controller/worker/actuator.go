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

// Package worker contains the cloud provider specific implementations for worker delegate.
package worker

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"strconv"
	"strings"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	"github.com/gardener/gardener/extensions/pkg/controller/worker"
	"github.com/gardener/gardener/extensions/pkg/controller/worker/genericactuator"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	v1betaconstants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/gardener/gardener/pkg/client/kubernetes"
	"github.com/gardener/gardener/pkg/utils"
	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/charts"
	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

// NewActuator creates a new Actuator that updates the status of the handled WorkerPoolConfigs.
func NewActuator(mgr manager.Manager, gardenCluster cluster.Cluster) worker.Actuator {
	workerDelegate := &delegateFactory{
		gardenReader: gardenCluster.GetAPIReader(),
		seedClient:   mgr.GetClient(),
		restConfig:   mgr.GetConfig(),
		scheme:       mgr.GetScheme(),
	}
	return genericactuator.NewActuator(
		mgr,
		gardenCluster,
		workerDelegate,
		func(err error) []gardencorev1beta1.ErrorCode {
			// TODO(samngo): add logic to map error messages that Gardener can understand
			return nil
		})
}

type delegateFactory struct {
	gardenReader client.Reader
	seedClient   client.Client
	restConfig   *rest.Config
	scheme       *runtime.Scheme
}

func (d *delegateFactory) WorkerDelegate(_ context.Context, worker *extensionsv1alpha1.Worker, cluster *extensionscontroller.Cluster) (genericactuator.WorkerDelegate, error) {
	seedChartApplier, err := kubernetes.NewChartApplierForConfig(d.restConfig)
	if err != nil {
		return nil, err
	}

	return newWorkerDelegate(
		d.seedClient,
		d.scheme,
		seedChartApplier,
		worker,
		cluster,
	)
}

type workerDelegate struct {
	client           client.Client
	decoder          runtime.Decoder
	scheme           *runtime.Scheme
	seedChartApplier EmbeddedChartApplier

	machineDeployments worker.MachineDeployments
	machineClasses     []machineClass
	machineImages      []apisgdc.MachineImage
	cloudProfileConfig *apisgdc.CloudProfileConfig

	worker  *extensionsv1alpha1.Worker
	cluster *extensionscontroller.Cluster
}

// EmbeddedChartApplier is an interface that describes needed method to apply
// Helm charts in Kubernetes clusters from an embedded file system
type EmbeddedChartApplier interface {
	// ApplyFromEmbeddedFS applies a chart from the provided embedded file system.
	ApplyFromEmbeddedFS(ctx context.Context, embeddedFS embed.FS, chartPath, namespace, name string, opts ...kubernetes.ApplyOption) error
}

func newWorkerDelegate(
	client client.Client,
	scheme *runtime.Scheme,
	seedChartApplier EmbeddedChartApplier,
	worker *extensionsv1alpha1.Worker,
	cluster *extensionscontroller.Cluster,
) (genericactuator.WorkerDelegate, error) {
	decoder := serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder()

	config, err := toCloudProfileConfig(decoder, cluster)
	if err != nil {
		return nil, err
	}

	return &workerDelegate{
		client:             client,
		scheme:             scheme,
		decoder:            decoder,
		seedChartApplier:   seedChartApplier,
		cloudProfileConfig: config,
		cluster:            cluster,
		worker:             worker,
	}, nil
}

// DeployMachineClasses generates and creates the GDCH specific machine classes.
func (w *workerDelegate) DeployMachineClasses(ctx context.Context) error {
	if w.machineClasses == nil {
		if err := w.generateMachineClasses(ctx); err != nil {
			return err
		}
	}

	return w.seedChartApplier.ApplyFromEmbeddedFS(ctx, charts.InternalChart, filepath.Join(charts.InternalChartsPath, "machineclass"), w.worker.Namespace, "machineclass", kubernetes.Values(map[string]interface{}{"machineClasses": w.machineClasses}))
}

func (w *workerDelegate) GenerateMachineDeployments(ctx context.Context) (worker.MachineDeployments, error) {
	if w.machineDeployments == nil {
		if err := w.generateMachineDeployments(ctx); err != nil {
			return nil, err
		}
	}
	return w.machineDeployments, nil
}

func (w *workerDelegate) generateMachineClasses(ctx context.Context) error {
	var machineClasses []machineClass

	sa, err := gdc.GetServiceAccountFromSecretReference(ctx, w.client, w.worker.Spec.SecretRef)
	if err != nil {
		return fmt.Errorf("could not get service account from secret '%s/%s': %w", w.worker.Spec.SecretRef.Namespace, w.worker.Spec.SecretRef.Name, err)
	}

	infraStatus := &apisgdc.InfrastructureStatus{}
	if _, _, err := w.decoder.Decode(w.worker.Spec.InfrastructureProviderStatus.Raw, nil, infraStatus); err != nil {
		return err
	}

	for _, pool := range w.worker.Spec.Pools {
		workerConfig := &apisgdc.WorkerConfig{}
		if pool.ProviderConfig != nil && pool.ProviderConfig.Raw != nil {
			if _, _, err := w.decoder.Decode(pool.ProviderConfig.Raw, nil, workerConfig); err != nil {
				return fmt.Errorf("could not decode provider config: %w", err)
			}
		}

		zones := pool.Zones
		poolLabels := getGDCHPoolLabels(w.worker, pool, w.cluster.ObjectMeta.Name)
		arch := ptr.Deref(pool.Architecture, v1betaconstants.ArchitectureAMD64)
		machineImage, machineImageProject, err := w.findMachineImage(pool.MachineImage.Name, pool.MachineImage.Version, &arch)
		if err != nil {
			return fmt.Errorf("could not find machine image with name %q, version %q, and arch %q: %w", pool.MachineImage.Name, pool.MachineImage.Version, arch, err)
		}

		for zoneIndex, zone := range zones {
			additionalData := []string{}
			disks, err := generateDisks(pool, machineImage, machineImageProject, poolLabels)
			if err != nil {
				return err
			}

			userData, err := worker.FetchUserData(ctx, w.client, w.worker.Spec.SecretRef.Namespace, pool)
			if err != nil {
				return fmt.Errorf("failed to get user data from worker: %w", err)
			}

			deploymentName, err := w.determineDeploymentName(ctx, pool, int32(zoneIndex))
			if err != nil {
				return err
			}

			additionalData = append(additionalData, zone)
			if infraStatus.EnableEgress != nil {
				additionalData = append(additionalData, strconv.FormatBool(*infraStatus.EnableEgress))
			}

			workerPoolHash, err := w.workerPoolHash(pool, additionalData, []string{})
			if err != nil {
				return fmt.Errorf("get hash for worker pool %v: %w", pool, err)
			}
			className := fmt.Sprintf("%s-%s", deploymentName, workerPoolHash)

			subnet, err := findSubnet(zone, infraStatus)
			if err != nil {
				return err
			}
			managementURL, err := findManagementURL(zone, w.cloudProfileConfig)
			if err != nil {
				return err
			}

			machineClass := machineClass{
				Name:        className,
				Project:     sa.Project,
				Labels:      poolLabels,
				CAData:      w.cloudProfileConfig.OrgConfig.CAData,
				RegistryURL: w.cloudProfileConfig.OrgConfig.RegistryURL,
				ResourceLabels: map[string]string{
					v1betaconstants.GardenerPurpose: v1betaconstants.GardenPurposeMachineClass,
				},
				Annotations: map[string]string{
					"description": fmt.Sprintf("Machine of Shoot %s created by machine-controller-manager", w.worker.Name),
				},
				Disks:  disks,
				Secret: &secret{CloudConfig: string(userData)},
				CredentialsSecretRef: &credentialsSecretRef{
					Name:      w.worker.Spec.SecretRef.Name,
					Namespace: w.worker.Spec.SecretRef.Namespace,
				},
				MachineType:   pool.MachineType,
				EnableEgress:  infraStatus.EnableEgress,
				SubnetName:    subnet,
				OrgClusterURL: managementURL,
			}

			var nodeTemplate *extensionsv1alpha1.NodeTemplate
			if pool.NodeTemplate != nil {
				nodeTemplate = pool.NodeTemplate.DeepCopy()
			}
			if workerConfig.NodeTemplate != nil {
				if nodeTemplate == nil {
					nodeTemplate = &extensionsv1alpha1.NodeTemplate{}
				}
				if nodeTemplate.Capacity == nil {
					nodeTemplate.Capacity = corev1.ResourceList{}
				}
				maps.Copy(nodeTemplate.Capacity, workerConfig.NodeTemplate.Capacity)

				if nodeTemplate.VirtualCapacity == nil {
					nodeTemplate.VirtualCapacity = corev1.ResourceList{}
				}
				maps.Copy(nodeTemplate.VirtualCapacity, workerConfig.NodeTemplate.VirtualCapacity)
			}

			if nodeTemplate != nil {
				machineClass.NodeTemplate = &machinev1alpha1.NodeTemplate{
					Capacity:        nodeTemplate.Capacity.DeepCopy(),
					VirtualCapacity: nodeTemplate.VirtualCapacity.DeepCopy(),
					InstanceType:    pool.MachineType,
					Region:          w.worker.Spec.Region,
					Zone:            zone,
				}
			}

			machineClasses = append(machineClasses, machineClass)
		}
	}

	w.machineClasses = machineClasses
	return nil
}

func (w *workerDelegate) generateMachineDeployments(ctx context.Context) error {
	sa, err := gdc.GetServiceAccountFromSecretReference(ctx, w.client, w.worker.Spec.SecretRef)
	if err != nil {
		return fmt.Errorf("could not get service account from secret '%s/%s': %w", w.worker.Spec.SecretRef.Namespace, w.worker.Spec.SecretRef.Name, err)
	}
	machineDeployments := worker.MachineDeployments{}

	infraStatus := &apisgdc.InfrastructureStatus{}
	if _, _, err := w.decoder.Decode(w.worker.Spec.InfrastructureProviderStatus.Raw, nil, infraStatus); err != nil {
		return err
	}

	for _, pool := range w.worker.Spec.Pools {
		zones := pool.Zones
		zoneLen := int32(len(zones))
		for zoneIndex, zone := range zones {
			zoneIdx := int32(zoneIndex)
			additionalData := []string{}
			deploymentName, err := w.determineDeploymentName(ctx, pool, zoneIdx)
			if err != nil {
				return err
			}

			additionalData = append(additionalData, zone)
			if infraStatus.EnableEgress != nil {
				additionalData = append(additionalData, strconv.FormatBool(*infraStatus.EnableEgress))
			}

			workerPoolHash, err := w.workerPoolHash(pool, additionalData, []string{})
			if err != nil {
				return fmt.Errorf("get hash for worker pool %v: %w", pool, err)
			}
			className := fmt.Sprintf("%s-%s", deploymentName, workerPoolHash)

			deployment := worker.MachineDeployment{
				Name:                 deploymentName,
				PoolName:             pool.Name,
				ClassName:            className,
				SecretName:           className,
				Minimum:              worker.DistributeOverZones(zoneIdx, pool.Minimum, zoneLen),
				Maximum:              worker.DistributeOverZones(zoneIdx, pool.Maximum, zoneLen),
				Labels:               addBaremetalNamespaceLabel(pool.Labels, sa.Project),
				Annotations:          pool.Annotations,
				Taints:               pool.Taints,
				MachineConfiguration: genericactuator.ReadMachineConfiguration(pool),
				Strategy: machinev1alpha1.MachineDeploymentStrategy{
					Type: machinev1alpha1.RollingUpdateMachineDeploymentStrategyType,
					RollingUpdate: &machinev1alpha1.RollingUpdateMachineDeployment{
						UpdateConfiguration: machinev1alpha1.UpdateConfiguration{
							MaxUnavailable: ptr.To(worker.DistributePositiveIntOrPercent(zoneIdx, pool.MaxUnavailable, zoneLen, pool.Maximum)),
							MaxSurge:       ptr.To(worker.DistributePositiveIntOrPercent(zoneIdx, pool.MaxSurge, zoneLen, pool.Maximum)),
						},
					},
				},
			}

			machineDeployments = append(machineDeployments, deployment)
		}
	}

	w.machineDeployments = machineDeployments
	return nil
}

// determineDeploymentName checks if a MachineDeployment with the old name pattern (shoot--<shoot_ns>-<pool_name>) exists and returns it if so.
// Otherwise, it returns the new, shorter name for new machineDeployments and machineClasses
func (w *workerDelegate) determineDeploymentName(ctx context.Context, pool extensionsv1alpha1.WorkerPool, zoneIdx int32) (string, error) {
	trimmedNamespace := strings.TrimPrefix(w.worker.Namespace, "shoot--")

	var newDeploymentName, oldDeploymentName string

	newDeploymentName = fmt.Sprintf("%s-%s-%d", trimmedNamespace, pool.Name, zoneIdx+1)
	oldDeploymentName = fmt.Sprintf("%s-%s-%d", w.worker.Namespace, pool.Name, zoneIdx+1)

	existingLegacyDeployment := &machinev1alpha1.MachineDeployment{}
	err := w.client.Get(ctx, client.ObjectKey{Namespace: w.worker.Namespace, Name: oldDeploymentName}, existingLegacyDeployment)

	if err == nil {
		// Found legacy deployment, use the old name to prevent disruption.
		return oldDeploymentName, nil
	}

	if !errors.IsNotFound(err) {
		return "", fmt.Errorf("failed to check for existing machine deployment %q: %w", oldDeploymentName, err)
	}

	// old deployment not found, use the new, shorter name.
	return newDeploymentName, nil
}

func (w *workerDelegate) stripVirtualCapacity(pool *extensionsv1alpha1.WorkerPool) error {
	if pool.NodeTemplate != nil {
		pool.NodeTemplate.VirtualCapacity = nil
	}
	if pool.ProviderConfig != nil && pool.ProviderConfig.Raw != nil {
		workerConfig := &apisgdc.WorkerConfig{}
		if _, _, err := w.decoder.Decode(pool.ProviderConfig.Raw, nil, workerConfig); err == nil {
			if workerConfig.NodeTemplate != nil {
				workerConfig.NodeTemplate.VirtualCapacity = nil
			}
			raw, err := json.Marshal(workerConfig)
			if err != nil {
				return fmt.Errorf("failed to marshal workerConfig: %w", err)
			}
			pool.ProviderConfig.Raw = raw
		}
	}
	return nil
}

func (w *workerDelegate) workerPoolHash(pool extensionsv1alpha1.WorkerPool, additionalData, additionalDataInPlace []string) (string, error) {
	poolForHash := pool.DeepCopy()
	if err := w.stripVirtualCapacity(poolForHash); err != nil {
		return "", err
	}
	return worker.WorkerPoolHash(*poolForHash, w.cluster, additionalData, additionalDataInPlace)
}

func generateDisks(pool extensionsv1alpha1.WorkerPool, image, project string, labels map[string]string) ([]*disk, error) {
	disks := []*disk{}
	if pool.Volume != nil {
		d, err := createDiskSpecForVolume(*pool.Volume, image, project, true, labels)
		if err != nil {
			return nil, fmt.Errorf("could not create root volume: %w", err)
		}
		disks = append(disks, d)
	}
	for _, v := range pool.DataVolumes {
		d, err := createDiskSpecForDataVolume(v, false, labels)
		if err != nil {
			return nil, fmt.Errorf("could not create data volume: %w", err)
		}
		disks = append(disks, d)
	}
	return disks, nil
}

// PreReconcileHook implements genericactuator.WorkerDelegate.
func (w *workerDelegate) PreReconcileHook(_ context.Context) error {
	return nil
}

// PostReconcileHook implements genericactuator.WorkerDelegate.
func (w *workerDelegate) PostReconcileHook(_ context.Context) error {
	return nil
}

// PreDeleteHook implements genericactuator.WorkerDelegate.
func (w *workerDelegate) PreDeleteHook(_ context.Context) error {
	return nil
}

// PostDeleteHook implements genericactuator.WorkerDelegate.
func (w *workerDelegate) PostDeleteHook(_ context.Context) error {
	return nil
}

// toCloudProfileConfig decodes the provider specific cloud profile
// configuration for a cluster.
func toCloudProfileConfig(decoder runtime.Decoder, cluster *extensionscontroller.Cluster) (*apisgdc.CloudProfileConfig, error) {
	if cluster == nil || cluster.CloudProfile == nil ||
		cluster.CloudProfile.Spec.ProviderConfig == nil ||
		cluster.CloudProfile.Spec.ProviderConfig.Raw == nil {
		return nil, fmt.Errorf("no cloud profile config")
	}

	cloudProfileConfig := &apisgdc.CloudProfileConfig{}
	if _, _, err := decoder.Decode(cluster.CloudProfile.Spec.ProviderConfig.Raw, nil, cloudProfileConfig); err != nil {
		return nil, fmt.Errorf("decode raw cloud profile config for '%s': %w", gdc.ObjectName(cluster.CloudProfile), err)
	}
	return cloudProfileConfig, nil
}

func findSubnet(name string, infraStatus *apisgdc.InfrastructureStatus) (string, error) {
	for _, zone := range infraStatus.Networks.Zones {
		if zone.Name == name && zone.Subnet != "" {
			return zone.Subnet, nil
		}
	}
	return "", fmt.Errorf("cannot find zone network subnet for zone %s", name)
}

func findManagementURL(name string, cloudProfile *apisgdc.CloudProfileConfig) (string, error) {
	for _, zone := range cloudProfile.OrgConfig.Zones {
		if zone.Name == name {
			return zone.ManagementAPI, nil
		}
	}
	return "", fmt.Errorf("cannot find zone management url for zone %s", name)
}

func addBaremetalNamespaceLabel(labels map[string]string, project string) map[string]string {
	return utils.MergeStringMaps(labels, map[string]string{"baremetal.cluster.gke.io/namespace": project})
}
