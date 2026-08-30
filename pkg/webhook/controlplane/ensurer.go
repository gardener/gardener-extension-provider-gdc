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
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/coreos/go-systemd/v22/unit"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/gardener/gardener/extensions/pkg/webhook"
	gcontext "github.com/gardener/gardener/extensions/pkg/webhook/context"
	"github.com/gardener/gardener/extensions/pkg/webhook/controlplane/genericmutator"
	gardenimagevector "github.com/gardener/gardener/imagevector"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	"github.com/gardener/gardener/pkg/component/nodemanagement/machinecontrollermanager"
	gutil "github.com/gardener/gardener/pkg/utils/gardener"
	versionutils "github.com/gardener/gardener/pkg/utils/version"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	vpaautoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	webhookutil "github.com/gardener/gardener-extension-provider-gdc/pkg/webhook/utils"

	"github.com/gardener/gardener-extension-provider-gdc/imagevector"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/config"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

// NewEnsurer creates a new controlplane ensurer.
func NewEnsurer(c client.Client, logger logr.Logger, etcdStorage *config.ETCDStorage) genericmutator.Ensurer {
	return &ensurer{
		logger:      logger.WithName("gdc-controlplane-ensurer"),
		client:      c,
		etcdStorage: etcdStorage,
	}
}

type ensurer struct {
	genericmutator.NoopEnsurer
	logger      logr.Logger
	client      client.Client
	etcdStorage *config.ETCDStorage
}

var (
	// ImageVector is the image vector that contains all the needed images.
	ImageVector = imagevector.ImageVector()

	// TODO: Remove this check in the future as Gardener v1.149.3+ only supports Kubernetes >= 1.32.
	constraintK8sLess131 = versionutils.MustNewConstraint("< 1.31-0")
)

// EnsureMachineControllerManagerDeployment ensures that the machine-controller-manager deployment conforms to the provider requirements.
func (e *ensurer) EnsureMachineControllerManagerDeployment(ctx context.Context, gctx gcontext.GardenContext, newObj, _ *appsv1.Deployment) error {
	image, err := ImageVector.FindImage(gdc.MCMProviderGDCImageName)
	if err != nil {
		return err
	}

	cluster, err := gctx.GetCluster(ctx)
	if err != nil {
		return fmt.Errorf("failed reading Cluster: %w", err)
	}

	newObj.Spec.Template.Spec.Containers = webhook.EnsureContainerWithName(
		newObj.Spec.Template.Spec.Containers,
		machinecontrollermanager.ProviderSidecarContainer(cluster.Shoot, newObj.Namespace, gdc.ProviderName, image.String()),
	)
	return nil
}

// EnsureMachineControllerManagerVPA ensures that the machine-controller-manager VPA conforms to the provider requirements.
func (e *ensurer) EnsureMachineControllerManagerVPA(_ context.Context, _ gcontext.GardenContext, newAutoscaler, _ *vpaautoscalingv1.VerticalPodAutoscaler) error {
	if newAutoscaler.Spec.ResourcePolicy == nil {
		newAutoscaler.Spec.ResourcePolicy = &vpaautoscalingv1.PodResourcePolicy{}
	}

	newAutoscaler.Spec.ResourcePolicy.ContainerPolicies = webhook.EnsureVPAContainerResourcePolicyWithName(
		newAutoscaler.Spec.ResourcePolicy.ContainerPolicies,
		machinecontrollermanager.ProviderSidecarVPAContainerPolicy(gdc.ProviderName),
	)
	return nil
}

// EnsureKubeAPIServerDeployment ensures that the kube-apiserver deployment conforms to the provider requirements.
func (e *ensurer) EnsureKubeAPIServerDeployment(ctx context.Context, gctx gcontext.GardenContext, new, _ *appsv1.Deployment) error {
	template := &new.Spec.Template
	ps := &template.Spec

	// TODO(valerii): Make sure the line below no longer needed
	// TODO(valerii): This label approach is deprecated and no longer needed in the future. Remove it as soon as gardener/gardener@v1.75 has been released.
	metav1.SetMetaDataLabel(&new.Spec.Template.ObjectMeta, gutil.NetworkPolicyLabel(gdc.CSISnapshotValidationName, 443), v1beta1constants.LabelNetworkPolicyAllowed)

	cluster, err := gctx.GetCluster(ctx)
	if err != nil {
		return err
	}

	k8sVersion, err := semver.NewVersion(cluster.Shoot.Spec.Kubernetes.Version)
	if err != nil {
		return err
	}

	if c := webhook.ContainerWithName(ps.Containers, "kube-apiserver"); c != nil {
		ensureKubeAPIServerCommandLineArgs(c, k8sVersion)
	}

	vpnClientName := gardenimagevector.ContainerImageNameVpnClient
	for i := range ps.Containers {
		if strings.HasPrefix(ps.Containers[i].Name, vpnClientName) {
			webhookutil.EnsureSELinuxSPCT(&ps.Containers[i])
		}
	}

	return nil
}

// EnsureKubeControllerManagerDeployment ensures that the kube-controller-manager deployment conforms to the provider requirements.
func (e *ensurer) EnsureKubeControllerManagerDeployment(ctx context.Context, gctx gcontext.GardenContext, deployment, _ *appsv1.Deployment) error {
	etcSSLName := "etc-ssl"

	etcSSLVolumeMount := corev1.VolumeMount{
		Name:      etcSSLName,
		MountPath: "/etc/ssl",
		ReadOnly:  true,
	}

	usrShareCaCerts := "usr-share-cacerts"
	directoryOrCreate := corev1.HostPathDirectoryOrCreate
	usrShareCaCertsVolumeMount := corev1.VolumeMount{
		Name:      usrShareCaCerts,
		MountPath: "/usr/share/ca-certificates",
		ReadOnly:  true,
	}
	usrShareCaCertsVolume := corev1.Volume{
		Name: usrShareCaCerts,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/usr/share/ca-certificates",
				Type: &directoryOrCreate,
			},
		},
	}
	etcSSLVolume := corev1.Volume{
		Name: etcSSLName,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/etc/ssl",
				Type: &directoryOrCreate,
			},
		},
	}

	template := &deployment.Spec.Template
	ps := &template.Spec

	if c := webhook.ContainerWithName(ps.Containers, "kube-controller-manager"); c != nil {
		ensureKubeControllerManagerCommandLineArgs(c)
		ensureKubeControllerManagerVolumeMounts(c, etcSSLVolumeMount.Name, usrShareCaCertsVolumeMount.Name)
	}

	ensureKubeControllerManagerLabels(template)
	ensureKubeControllerManagerVolumes(ps, etcSSLVolume.Name, usrShareCaCertsVolume.Name)
	return nil
}

func ensureKubeControllerManagerCommandLineArgs(c *corev1.Container) {
	// Kubernetes installation is using an external cloud provider rather than an in-tree
	// cloud provider.
	c.Command = webhook.EnsureStringWithPrefix(c.Command, "--cloud-provider=", "external")
	// Ensures that the --cloud-config flag is not set, which is used to specify a config
	// file for the in-tree cloud provider.
	c.Command = webhook.EnsureNoStringWithPrefix(c.Command, "--cloud-config=")
	// This flag was used in earlier versions of Kubernetes to specify external cloud
	// volume plugins
	c.Command = webhook.EnsureNoStringWithPrefix(c.Command, "--external-cloud-volume-plugin=")
}

func ensureKubeControllerManagerLabels(t *corev1.PodTemplateSpec) {
	if t.Labels != nil {
		// This label indicates that the pod should be subjected to network policies that
		// block certain CIDR ranges. Removing this label means the pod will not be automatically
		// blocked from accessing these CIDRs.
		delete(t.Labels, v1beta1constants.LabelNetworkPolicyToBlockedCIDRs)
		// This label is used to restrict or allow network traffic to public networks.
		delete(t.Labels, v1beta1constants.LabelNetworkPolicyToPublicNetworks)
		// This label is associated with access to private networks.
		delete(t.Labels, v1beta1constants.LabelNetworkPolicyToPrivateNetworks)
	}
}

func ensureKubeControllerManagerVolumeMounts(c *corev1.Container, etcSSLVolumeMountName string, usrShareCaCertsVolumeMountName string) {
	// Ensures that a volume mount with the name etcSSLVolumeMount.Name is removed from
	// the container's volume mounts.
	// Removing access to SSL/TLS certificates stored in the /etc/ssl directory,
	// to enforce security policies or to comply with a specific configuration where such mounts
	// should not be exposed to this container.
	c.VolumeMounts = webhook.EnsureNoVolumeMountWithName(c.VolumeMounts, etcSSLVolumeMountName)
	// Ensures that a volume mount with the name usrShareCaCertsVolumeMount.Name is removed from
	// the container’s volume mounts.
	// refers to the mount for CA certificates stored in /usr/share/ca-certificates to prevent
	// the container from using system-wide CA certificates
	// to enforce the use of a more controlled set of certificates.
	c.VolumeMounts = webhook.EnsureNoVolumeMountWithName(c.VolumeMounts, usrShareCaCertsVolumeMountName)
}

func ensureKubeControllerManagerVolumes(ps *corev1.PodSpec, etcSSLVolumeName string, usrShareCaCertsVolumeName string) {
	// Ensures that the volume named etcSSLVolume.Name, associated with the /etc/ssl directory, is removed from the pod's list of volumes for security reasons.
	ps.Volumes = webhook.EnsureNoVolumeWithName(ps.Volumes, etcSSLVolumeName)
	// Ensures the removal of the volume named usrShareCaCertsVolume.Name, which maps to the /usr/share/ca-certificates directory to restrict the certificates
	// that the controller manager can trust, ensuring it only uses specifically provided certificates rather than those installed system-wide.
	ps.Volumes = webhook.EnsureNoVolumeWithName(ps.Volumes, usrShareCaCertsVolumeName)
}

func ensureKubeAPIServerCommandLineArgs(c *corev1.Container, k8sVersion *semver.Version) {
	// Gardener ensures these flags are absent to prevent the Kubernetes API server from being tied to specific cloud implementations.
	c.Command = webhook.EnsureNoStringWithPrefix(c.Command, "--cloud-provider=")
	c.Command = webhook.EnsureNoStringWithPrefix(c.Command, "--cloud-config=")

	// TODO: Remove this check in the future as Gardener v1.149.3+ only supports Kubernetes >= 1.32.
	// In Kubernetes < 1.31, disable the PersistentVolumeLabel admission plugin.
	if constraintK8sLess131.Check(k8sVersion) {
		c.Command = webhook.EnsureStringWithPrefixContains(c.Command, "--disable-admission-plugins=", "PersistentVolumeLabel", ",")
	}
}

// EnsureKubeletServiceUnitOptions ensures that the kubelet.service unit options conform to the provider requirements.
func (e *ensurer) EnsureKubeletServiceUnitOptions(_ context.Context, _ gcontext.GardenContext, _ *semver.Version, newOptions, _ []*unit.UnitOption) ([]*unit.UnitOption, error) {
	if opt := webhook.UnitOptionWithSectionAndName(newOptions, "Service", "ExecStart"); opt != nil {
		command := webhook.DeserializeCommandLine(opt.Value)
		// Gardener ensures these flags are absent to prevent the Kubernetes API server from being tied to specific cloud implementations.
		command = webhook.EnsureStringWithPrefix(command, "--cloud-provider=", "external")
		opt.Value = webhook.SerializeCommandLine(command, 1, " \\\n    ")
	}

	ensuredOptions := webhook.EnsureUnitOption(newOptions, &unit.UnitOption{
		Section: "Service",
		Name:    "ExecStartPre",
		Value:   `/bin/sh -c 'hostnamectl set-hostname $(hostname)'`,
	})

	return ensuredOptions, nil
}

// EnsureKubeletConfiguration ensures that the kubelet configuration conforms to the provider requirements.
func (e *ensurer) EnsureKubeletConfiguration(_ context.Context, _ gcontext.GardenContext, _ *semver.Version, kubletConfig, _ *kubeletconfigv1beta1.KubeletConfiguration) error {
	// Allows the Kubelet to manage the attach/detach operations for volumes automatically
	kubletConfig.EnableControllerAttachDetach = ptr.To(true)

	return nil
}

// EnsureETCD ensures that the etcd stateful sets conform to the provider requirements.
// For internal context, see: go/gardener-etcd-webhook
func (e *ensurer) EnsureETCD(ctx context.Context, _ gcontext.GardenContext, newObj, oldObj *druidv1alpha1.Etcd) error {
	if newObj.Name == v1beta1constants.ETCDMain {
		// Ensure that the etcd storage is configured correctly.
		e.ensureETCDStorage(newObj, oldObj)

		// Ensure that the etcd backup is configured correctly.
		if err := e.ensureETCDBackup(ctx, newObj); err != nil {
			return err
		}
	}

	return nil
}

// ensureETCDStorage ensures that the etcd storage is configured correctly.
// It sets the storage class and capacity for new objects and preserves them for existing objects to avoid immutability errors.
func (e *ensurer) ensureETCDStorage(newObj, oldObj *druidv1alpha1.Etcd) {
	if oldObj != nil {
		// ETCD resource update: preserve existing storage class and capacity if not specified in the new object.
		// This prevents the "immutable field" error when the upstream controller updates the resource without these fields.
		if newObj.Spec.StorageClass == nil {
			newObj.Spec.StorageClass = oldObj.Spec.StorageClass
		}
		if newObj.Spec.StorageCapacity == nil {
			newObj.Spec.StorageCapacity = oldObj.Spec.StorageCapacity
		}
		return
	}

	// New ETCD resource creation: set default storage class and capacity if configured.
	if e.etcdStorage == nil {
		return
	}
	if e.etcdStorage.ClassName != nil {
		newObj.Spec.StorageClass = e.etcdStorage.ClassName
	}
	if e.etcdStorage.Capacity != nil {
		newObj.Spec.StorageCapacity = e.etcdStorage.Capacity
	}
}

// ensureETCDBackup ensures that the etcd backup is configured correctly.
// It sets the backup bucket container and provider.
func (e *ensurer) ensureETCDBackup(ctx context.Context, newObj *druidv1alpha1.Etcd) error {
	return webhookutil.EnsureETCDBackup(ctx, e.client, e.logger, newObj)
}

// EnsureVPNSeedServerDeployment ensures that the vpn-seed-server deployment conform to the provider requirements.
func (e *ensurer) EnsureVPNSeedServerDeployment(ctx context.Context, _ gcontext.GardenContext, newObj, _ *appsv1.Deployment) error {
	if newObj.Name == v1beta1constants.DeploymentNameVPNSeedServer {
		if c := webhook.ContainerWithName(newObj.Spec.Template.Spec.Containers, "vpn-seed-server"); c != nil {
			webhookutil.EnsureSELinuxSPCT(c)
		}

		if c := webhook.ContainerWithName(newObj.Spec.Template.Spec.InitContainers, "setup"); c != nil {
			webhookutil.EnsureSELinuxSPCT(c)
		}
	}
	return nil
}

// EnsureVPNSeedServerStatefulSet ensures that the vpn-seed-server stateful set conform to the provider requirements.
func (e *ensurer) EnsureVPNSeedServerStatefulSet(ctx context.Context, gctx gcontext.GardenContext, newObj, _ *appsv1.StatefulSet) error {
	if newObj.Name == v1beta1constants.DeploymentNameVPNSeedServer {
		if c := webhook.ContainerWithName(newObj.Spec.Template.Spec.Containers, "vpn-seed-server"); c != nil {
			webhookutil.EnsureSELinuxSPCT(c)
		}
		if c := webhook.ContainerWithName(newObj.Spec.Template.Spec.InitContainers, "setup"); c != nil {
			webhookutil.EnsureSELinuxSPCT(c)
		}
	}
	return nil
}
