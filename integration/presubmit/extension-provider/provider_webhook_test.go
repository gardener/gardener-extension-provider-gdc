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
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	gardencorev1beta1const "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	vpaautoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/kubernetes"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

type extensionProviderWebhookTestFixture struct {
	*commonTestFixture
}

func (f *extensionProviderWebhookTestFixture) test(t *testing.T) {
	ctx := context.Background()

	// Setup common environment
	f.setup(t, ctx)

	// ControlPlane Webhook Tests
	t.Run("MutatesKubeAPIServer", func(t *testing.T) {
		f.testMutatesKubeAPIServer(t, ctx)
	})
	t.Run("MutatesKubeControllerManager", func(t *testing.T) {
		f.testMutatesKubeControllerManager(t, ctx)
	})
	t.Run("MutatesMachineControllerManager", func(t *testing.T) {
		f.testMutatesMachineControllerManager(t, ctx)
	})
	t.Run("MutatesMachineControllerManagerVPA", func(t *testing.T) {
		f.testMutatesMachineControllerManagerVPA(t, ctx)
	})
	t.Run("MutatesOperatingSystemConfig", func(t *testing.T) {
		f.testMutatesOperatingSystemConfig(t, ctx)
	})
	t.Run("MutatesETCDMain", func(t *testing.T) {
		f.testMutatesETCDMain(t, ctx)
	})
	t.Run("MutatesVPNSeedServer", func(t *testing.T) {
		f.testMutatesVPNSeedServer(t, ctx)
	})

	// BackupProvider Webhook Tests
	t.Run("MutatesVirtualGardenETCDMain", func(t *testing.T) {
		f.testMutatesVirtualGardenETCDMain(t, ctx)
	})

	// DaemonSet Webhook Tests
	t.Run("MutatesFluentBit", func(t *testing.T) {
		f.testMutatesFluentBit(t, ctx)
	})
}

func (f *extensionProviderWebhookTestFixture) testMutatesKubeControllerManager(t *testing.T, ctx context.Context) {
	// Arrange
	deployName := "kube-controller-manager"
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: f.namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "kubernetes", "role": "controller-manager"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":  "kubernetes",
						"role": "controller-manager",
						gardencorev1beta1const.LabelNetworkPolicyToBlockedCIDRs:    "allowed",
						gardencorev1beta1const.LabelNetworkPolicyToPublicNetworks:  "allowed",
						gardencorev1beta1const.LabelNetworkPolicyToPrivateNetworks: "allowed",
					},
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{Name: "etc-ssl"},
						{Name: "usr-share-cacerts"},
					},
					Containers: []corev1.Container{
						{
							Name:  "kube-controller-manager",
							Image: fmt.Sprintf("registry.k8s.io/kube-controller-manager:%s", *vclusterK8sTag),
							Command: []string{
								"kube-controller-manager",
								"--cloud-provider=old-provider",
								"--cloud-config=/etc/kubernetes/cloud.conf",
								"--external-cloud-volume-plugin=gdch",
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "etc-ssl", MountPath: "/etc/ssl"},
								{Name: "usr-share-cacerts", MountPath: "/usr/share/ca-certificates"},
							},
						},
					},
				},
			},
		},
	}

	// Action
	if err := f.vucClient.Create(ctx, deploy); err != nil {
		t.Fatalf("failed to create Deployment: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, deploy); err != nil {
			t.Errorf("failed to delete deployment: %v", err)
		}
	})

	// Assert
	deployList := &appsv1.DeploymentList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": deployName},
		client.InNamespace(f.namespace),
	}
	err := kubernetes.WaitForCondition[*appsv1.Deployment](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, deployList, listOptions...)
	}, func(obj *appsv1.Deployment) bool {

		// Check labels
		if _, ok := obj.Spec.Template.Labels[gardencorev1beta1const.LabelNetworkPolicyToBlockedCIDRs]; ok {
			t.Logf("Deployment %q has forbidden label %s", obj.Name, gardencorev1beta1const.LabelNetworkPolicyToBlockedCIDRs)
			return false
		}

		if _, ok := obj.Spec.Template.Labels[gardencorev1beta1const.LabelNetworkPolicyToPublicNetworks]; ok {
			t.Logf("Deployment %q has forbidden label %s", obj.Name, gardencorev1beta1const.LabelNetworkPolicyToPublicNetworks)
			return false
		}

		if _, ok := obj.Spec.Template.Labels[gardencorev1beta1const.LabelNetworkPolicyToPrivateNetworks]; ok {
			t.Logf("Deployment %q has forbidden label %s", obj.Name, gardencorev1beta1const.LabelNetworkPolicyToPrivateNetworks)
			return false
		}

		// Check container args
		if len(obj.Spec.Template.Spec.Containers) == 0 {
			return false
		}
		c := obj.Spec.Template.Spec.Containers[0]

		cloudProviderOk := false
		for _, arg := range c.Command {
			if arg == "--cloud-provider=external" {
				cloudProviderOk = true
			}
			if strings.HasPrefix(arg, "--cloud-config") {
				t.Logf("Deployment %q has forbidden cloud-config arg: %s", obj.Name, arg)
				return false
			}
			if strings.HasPrefix(arg, "--external-cloud-volume-plugin") {
				t.Logf("Deployment %q has forbidden external-cloud-volume-plugin arg: %s", obj.Name, arg)
				return false
			}
		}
		if !cloudProviderOk {
			t.Logf("Deployment %q missing --cloud-provider=external", obj.Name)
			return false
		}

		// Check volumes/mounts
		for _, v := range obj.Spec.Template.Spec.Volumes {
			if v.Name == "etc-ssl" || v.Name == "usr-share-cacerts" {
				t.Logf("Deployment %q has forbidden volume: %s", obj.Name, v.Name)
				return false
			}
		}
		for _, m := range c.VolumeMounts {
			if m.Name == "etc-ssl" || m.Name == "usr-share-cacerts" {
				t.Logf("Deployment %q has forbidden volume mount: %s", obj.Name, m.Name)
				return false
			}
		}

		return true
	})
	if err != nil {
		t.Fatalf("Timeout waiting for Kube Controller Manager mutation: %v", err)
	}
}

func (f *extensionProviderWebhookTestFixture) setup(t *testing.T, ctx context.Context) {
	// Patch namespace to have provider label
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.namespace}}
	if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(ns), ns); err != nil {
		t.Fatalf("failed to get namespace %q: %v", f.namespace, err)
	}
	if ns.Labels == nil {
		ns.Labels = make(map[string]string)
	}
	ns.Labels[gardencorev1beta1const.LabelShootProvider] = "gdch"
	if err := f.vucClient.Update(ctx, ns); err != nil {
		t.Fatalf("failed to update namespace %q: %v", f.namespace, err)
	}
	t.Cleanup(func() {
		// Revert namespace label
		nsCleanup := &corev1.Namespace{}
		if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(ns), nsCleanup); err == nil {
			if nsCleanup.Labels != nil {
				delete(nsCleanup.Labels, gardencorev1beta1const.LabelShootProvider)
				if err := f.vucClient.Update(ctx, nsCleanup); err != nil {
					t.Errorf("failed to update namespace cleanup: %v", err)
				}
			}
		}
	})

	cluster := &extensionsv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: f.namespace,
		},
		Spec: extensionsv1alpha1.ClusterSpec{
			Shoot: runtime.RawExtension{
				Raw: encode(t, &gardencorev1beta1.Shoot{
					Spec: gardencorev1beta1.ShootSpec{
						Kubernetes: gardencorev1beta1.Kubernetes{
							Version: k8sVersion(),
						},
					},
				}),
			},
			Seed: &runtime.RawExtension{
				Raw: encode(t, &gardencorev1beta1.Seed{}),
			},
			CloudProfile: runtime.RawExtension{
				Raw: encode(t, &gardencorev1beta1.CloudProfile{}),
			},
		},
	}
	if err := f.vucClient.Create(ctx, cluster); err != nil {
		t.Fatalf("failed to create Cluster %q: %v", cluster.Name, err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, cluster); err != nil {
			t.Errorf("failed to delete cluster: %v", err)
		}
	})
}

func (f *extensionProviderWebhookTestFixture) testMutatesKubeAPIServer(t *testing.T, ctx context.Context) {
	// Arrange
	deployName := "kube-apiserver"
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: f.namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "kubernetes", "role": "apiserver"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "kubernetes", "role": "apiserver"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "kube-apiserver",
							Image: fmt.Sprintf("registry.k8s.io/kube-apiserver:%s", *vclusterK8sTag),
							Command: []string{
								"--cloud-provider=old-provider",
								"--enable-admission-plugins=NodeRestriction",
							},
						},
						{
							Name:  "vpn-client",
							Image: "europe-docker.pkg.dev/gardener-project/public/gardener/vpn-client:v1.0.0",
						},
					},
				},
			},
		},
	}

	// Action
	if err := f.vucClient.Create(ctx, deploy); err != nil {
		t.Fatalf("failed to create Deployment: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, deploy); err != nil {
			t.Errorf("failed to delete deployment: %v", err)
		}
	})

	// Assert
	deployList := &appsv1.DeploymentList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": deployName},
		client.InNamespace(f.namespace),
	}
	err := kubernetes.WaitForCondition[*appsv1.Deployment](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, deployList, listOptions...)
	}, func(obj *appsv1.Deployment) bool {
		// Check for Pod labels
		if obj.Spec.Template.Labels["networking.resources.gardener.cloud/to-csi-snapshot-validation-tcp-443"] != "allowed" {
			t.Logf("Deployment %q missing label. Has: %v", obj.Name, obj.Spec.Template.Labels)
			return false
		}

		// Check for admission plugins and cloud-provider removal
		if len(obj.Spec.Template.Spec.Containers) == 0 {
			return false
		}
		nodeRestrictionEnabled := false
		for _, arg := range obj.Spec.Template.Spec.Containers[0].Command {
			if strings.HasPrefix(arg, "--cloud-provider=") {
				t.Logf("Deployment %q still has --cloud-provider flag: %v", obj.Name, arg)
				return false
			}
			if strings.HasPrefix(arg, "--enable-admission-plugins=") {
				if strings.Contains(arg, "NodeRestriction") {
					nodeRestrictionEnabled = true
				}
			}
		}
		if !nodeRestrictionEnabled {
			t.Logf("Deployment %q missing NodeRestriction in --enable-admission-plugins", obj.Name)
			return false
		}

		// Check for vpn-client SELinux
		foundVpnClient := false
		for _, c := range obj.Spec.Template.Spec.Containers {
			if c.Name == "vpn-client" {
				foundVpnClient = true
				if c.SecurityContext == nil || c.SecurityContext.SELinuxOptions == nil || c.SecurityContext.SELinuxOptions.Type != "spc_t" {
					t.Logf("Deployment %q vpn-client has incorrect SELinux options: %v", obj.Name, c.SecurityContext)
					return false
				}
			}
		}
		if !foundVpnClient {
			t.Logf("Deployment %q missing vpn-client container", obj.Name)
		}
		return foundVpnClient
	})
	if err != nil {
		t.Fatalf("Timeout waiting for Kube APIServer mutation: %v", err)
	}
}

func (f *extensionProviderWebhookTestFixture) testMutatesMachineControllerManager(t *testing.T, ctx context.Context) {
	// Arrange
	deployName := "machine-controller-manager"
	labels := map[string]string{"app": "kubernetes", "role": "machine-controller-manager"}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: f.namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "kubeconfig",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "machine-controller-manager",
							Image: "registry.k8s.io/machine-controller-manager",
						},
					},
				},
			},
		},
	}

	// Action
	if err := f.vucClient.Create(ctx, deploy); err != nil {
		t.Fatalf("failed to create Deployment: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, deploy); err != nil {
			t.Fatalf("failed to delete deployment: %v", err)
		}
	})

	// Assert
	deployList := &appsv1.DeploymentList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": deployName},
		client.InNamespace(f.namespace),
	}
	err := kubernetes.WaitForCondition[*appsv1.Deployment](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, deployList, listOptions...)
	}, func(obj *appsv1.Deployment) bool {
		for _, c := range obj.Spec.Template.Spec.Containers {
			if c.Name == "machine-controller-manager-provider-gdch" {
				return true
			}
		}
		t.Logf("Deployment %q missing machine-controller-manager-provider-gdch sidecar", obj.Name)
		return false
	})
	if err != nil {
		t.Fatalf("Timeout waiting for Machine Controller Manager mutation: %v", err)
	}
}

func (f *extensionProviderWebhookTestFixture) testMutatesMachineControllerManagerVPA(t *testing.T, ctx context.Context) {
	// Arrange
	vpaName := "machine-controller-manager-vpa"
	vpa := &vpaautoscalingv1.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vpaName,
			Namespace: f.namespace,
		},
		Spec: vpaautoscalingv1.VerticalPodAutoscalerSpec{
			TargetRef: &autoscalingv1.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "machine-controller-manager",
			},
		},
	}

	// Action
	if err := f.vucClient.Create(ctx, vpa); err != nil {
		t.Fatalf("failed to create VPA: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, vpa); err != nil {
			t.Fatalf("failed to delete VPA: %v", err)
		}
	})

	// Assert
	vpaList := &vpaautoscalingv1.VerticalPodAutoscalerList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": vpaName},
		client.InNamespace(f.namespace),
	}
	err := kubernetes.WaitForCondition[*vpaautoscalingv1.VerticalPodAutoscaler](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, vpaList, listOptions...)
	}, func(obj *vpaautoscalingv1.VerticalPodAutoscaler) bool {
		if obj.Spec.ResourcePolicy != nil {
			for _, policy := range obj.Spec.ResourcePolicy.ContainerPolicies {
				if policy.ContainerName == "machine-controller-manager-provider-gdch" {
					return true
				}
			}
		}
		t.Logf("VPA %q missing machine-controller-manager-provider-gdch sidecar container policy", obj.Name)
		return false
	})
	if err != nil {
		t.Fatalf("Timeout waiting for Machine Controller Manager VPA mutation: %v", err)
	}
}

func (f *extensionProviderWebhookTestFixture) testMutatesOperatingSystemConfig(t *testing.T, ctx context.Context) {
	// Arrange
	kubeletConfigYAML := `apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
enableControllerAttachDetach: false`
	encodedKubeletConfig := base64.StdEncoding.EncodeToString([]byte(kubeletConfigYAML))

	oscName := "test-osc"
	osc := &extensionsv1alpha1.OperatingSystemConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      oscName,
			Namespace: f.namespace,
		},
		Spec: extensionsv1alpha1.OperatingSystemConfigSpec{
			Purpose: extensionsv1alpha1.OperatingSystemConfigPurposeReconcile,
			Units: []extensionsv1alpha1.Unit{
				{
					Name:    "kubelet.service",
					Command: ptr.To(extensionsv1alpha1.CommandStart),
					Content: ptr.To(`[Unit]
Description=kubelet
[Service]
ExecStart=/opt/bin/kubelet --config=/var/lib/kubelet/config/kubelet`),
				},
			},
			Files: []extensionsv1alpha1.File{
				{
					Path:        "/var/lib/kubelet/config/kubelet",
					Permissions: ptr.To(uint32(0644)),
					Content: extensionsv1alpha1.FileContent{
						Inline: &extensionsv1alpha1.FileContentInline{
							Encoding: "b64",
							Data:     encodedKubeletConfig,
						},
					},
				},
			},
		},
	}

	// Action
	if err := f.vucClient.Create(ctx, osc); err != nil {
		t.Fatalf("failed to create OperatingSystemConfig: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, osc); err != nil {
			t.Fatalf("failed to delete OperatingSystemConfig: %v", err)
		}
	})

	// Assert
	oscList := &extensionsv1alpha1.OperatingSystemConfigList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": oscName},
		client.InNamespace(f.namespace),
	}
	err := kubernetes.WaitForCondition[*extensionsv1alpha1.OperatingSystemConfig](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, oscList, listOptions...)
	}, func(obj *extensionsv1alpha1.OperatingSystemConfig) bool {
		foundMutatedUnit := false
		foundExecStartPre := false
		for _, unit := range obj.Spec.Units {
			if unit.Name == "kubelet.service" && unit.Content != nil {
				if strings.Contains(*unit.Content, "--cloud-provider=external") {
					foundMutatedUnit = true
				}
				if strings.Contains(*unit.Content, "ExecStartPre=/bin/sh -c 'hostnamectl set-hostname $(hostname)'") {
					foundExecStartPre = true
				}
			}
		}
		if !foundMutatedUnit {
			t.Logf("OSC %q missing --cloud-provider=external in kubelet.service unit", obj.Name)
			return false
		}
		if !foundExecStartPre {
			t.Logf("OSC %q missing ExecStartPre for hostnamectl in kubelet.service unit", obj.Name)
			return false
		}

		foundMutatedFile := false
		for _, file := range obj.Spec.Files {
			if file.Path == "/var/lib/kubelet/config/kubelet" && file.Content.Inline != nil {
				// Base64 for `apiVersion: kubelet.config.k8s.io/v1beta1\nenableControllerAttachDetach: true\nkind: KubeletConfiguration\n`
				decoded, err := base64.StdEncoding.DecodeString(file.Content.Inline.Data)
				if err == nil && strings.Contains(string(decoded), "enableControllerAttachDetach: true") {
					foundMutatedFile = true
				}
			}
		}
		if !foundMutatedFile {
			t.Logf("OSC %q missing enableControllerAttachDetach: true in kubelet config file", obj.Name)
			return false
		}
		return true
	})
	if err != nil {
		t.Fatalf("Timeout waiting for OperatingSystemConfig mutation: %v", err)
	}
}

func (f *extensionProviderWebhookTestFixture) testMutatesETCDMain(t *testing.T, ctx context.Context) {
	// Arrange
	backupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: "00-test-backup-bucket",
			Annotations: map[string]string{
				gdc.FullyQualifiedBucketNameAnnotationKey: "my-fq-bucket-name",
			},
		},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: gdc.Type,
			},
			Region: "us-west6",
			SecretRef: corev1.SecretReference{
				Name:      "some-secret",
				Namespace: "some-namespace",
			},
		},
	}
	if err := f.vucClient.Create(ctx, backupBucket); err != nil {
		t.Fatalf("failed to create BackupBucket: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, backupBucket); err != nil {
			if !apierrors.IsNotFound(err) {
				t.Logf("failed to delete BackupBucket: %v", err)
			}
		} else {
			if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
				if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(backupBucket), backupBucket); err != nil {
					if apierrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}
				return false, nil
			}); err != nil {
				t.Fatalf("error waiting for BackupBucket object to be deleted: %v", err)
			}
		}
	})

	etcdName := "etcd-main"
	var etcd *druidv1alpha1.Etcd

	// Action
	if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		etcd = &druidv1alpha1.Etcd{
			ObjectMeta: metav1.ObjectMeta{
				Name:      etcdName,
				Namespace: f.namespace,
			},
			Spec: druidv1alpha1.EtcdSpec{
				Labels: map[string]string{
					"name": "etcd",
				},
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"name": "etcd",
					},
				},
				Replicas: 1,
				Backup: druidv1alpha1.BackupSpec{
					Store: &druidv1alpha1.StoreSpec{
						Container: ptr.To("00-test-backup-bucket"),
						SecretRef: &corev1.SecretReference{
							Name: "etcd-backup-secret",
						},
					},
				},
			},
		}

		if err := f.vucClient.Create(ctx, etcd); err != nil {
			if strings.Contains(err.Error(), "no backup bucket found for seed") {
				t.Log("Webhook denied Etcd creation (BackupBucket not in cache yet), retrying...")
				return false, nil
			}
			return false, err
		}
		return true, nil
	}); err != nil {
		t.Fatalf("failed to create Etcd: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, etcd); err != nil {
			if !apierrors.IsNotFound(err) {
				t.Errorf("failed to delete Etcd: %v", err)
			}
		}
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(etcd), etcd); err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		}); err != nil {
			t.Errorf("error waiting for Etcd object to be deleted: %v", err)
		}
	})

	// Assert
	etcdList := &druidv1alpha1.EtcdList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": etcdName},
		client.InNamespace(f.namespace),
	}
	err := kubernetes.WaitForCondition[*druidv1alpha1.Etcd](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, etcdList, listOptions...)
	}, func(obj *druidv1alpha1.Etcd) bool {
		if obj.Spec.Backup.Store == nil {
			t.Logf("Etcd %s: Backup store is nil", obj.Name)
			return false
		}
		if obj.Spec.Backup.Store.Container == nil {
			t.Logf("Etcd %s: Backup store container is nil", obj.Name)
			return false
		}
		if obj.Spec.Backup.Store.Provider == nil {
			t.Logf("Etcd %s: Backup store provider is nil", obj.Name)
			return false
		}
		if obj.Spec.StorageClass == nil {
			t.Logf("Etcd %s: Storage class is nil", obj.Name)
			return false
		}
		if obj.Spec.StorageCapacity == nil {
			t.Logf("Etcd %s: Storage capacity is nil", obj.Name)
			return false
		}

		if *obj.Spec.Backup.Store.Container == "my-fq-bucket-name" &&
			*obj.Spec.Backup.Store.Provider == "aws" &&
			*obj.Spec.StorageClass == "performance-rwo" &&
			obj.Spec.StorageCapacity.String() == "10Gi" {
			return true
		}

		return false
	})
	if err != nil {
		t.Fatalf("Timeout waiting for Etcd mutation: %v", err)
	}

	t.Run("UpdateETCDMain", func(t *testing.T) {
		// Fetch the latest Etcd object
		latestEtcd := &druidv1alpha1.Etcd{}
		if err := f.vucClient.Get(ctx, client.ObjectKey{Namespace: f.namespace, Name: etcdName}, latestEtcd); err != nil {
			t.Fatalf("failed to get latest Etcd: %v", err)
		}

		// Update the Etcd object without specifying StorageClass and StorageCapacity
		latestEtcd.Spec.StorageClass = nil
		latestEtcd.Spec.StorageCapacity = nil

		if err := f.vucClient.Update(ctx, latestEtcd); err != nil {
			t.Fatalf("failed to update Etcd: %v", err)
		}

		// Assert that the webhook preserved the previous StorageClass and StorageCapacity
		err := kubernetes.WaitForCondition[*druidv1alpha1.Etcd](ctx, 1*time.Minute, func() (watch.Interface, error) {
			return f.vucClient.Watch(ctx, etcdList, listOptions...)
		}, func(obj *druidv1alpha1.Etcd) bool {
			if obj.Spec.StorageClass == nil {
				t.Logf("Etcd %s: Storage class is nil after update", obj.Name)
				return false
			}
			if obj.Spec.StorageCapacity == nil {
				t.Logf("Etcd %s: Storage capacity is nil after update", obj.Name)
				return false
			}

			if *obj.Spec.StorageClass == "performance-rwo" &&
				obj.Spec.StorageCapacity.String() == "10Gi" {
				return true
			}

			return false
		})
		if err != nil {
			t.Fatalf("Timeout waiting for Etcd update mutation to preserve storage: %v", err)
		}
	})
}

func (f *extensionProviderWebhookTestFixture) testMutatesVPNSeedServer(t *testing.T, ctx context.Context) {
	// Arrange
	deployName := "vpn-seed-server"
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: f.namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "vpn-seed-server"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "vpn-seed-server"},
				},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Name:  "setup",
							Image: "alpine:latest",
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "vpn-seed-server",
							Image: "europe-docker.pkg.dev/gardener-project/public/gardener/vpn-seed-server:v1.0.0",
						},
					},
				},
			},
		},
	}

	// Action
	if err := f.vucClient.Create(ctx, deploy); err != nil {
		t.Fatalf("failed to create Deployment: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, deploy); err != nil {
			t.Fatalf("failed to delete deployment: %v", err)
		}
	})

	// Assert
	deployList := &appsv1.DeploymentList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": deployName},
		client.InNamespace(f.namespace),
	}
	err := kubernetes.WaitForCondition[*appsv1.Deployment](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, deployList, listOptions...)
	}, func(obj *appsv1.Deployment) bool {
		// Check that both containers have SELinuxOptions.Type == "spc_t"
		setupOk := false
		for _, c := range obj.Spec.Template.Spec.InitContainers {
			if c.Name == "setup" && c.SecurityContext != nil && c.SecurityContext.SELinuxOptions != nil && c.SecurityContext.SELinuxOptions.Type == "spc_t" {
				setupOk = true
				break
			}
		}

		vpnSeedServerOk := false
		for _, c := range obj.Spec.Template.Spec.Containers {
			if c.Name == "vpn-seed-server" && c.SecurityContext != nil && c.SecurityContext.SELinuxOptions != nil && c.SecurityContext.SELinuxOptions.Type == "spc_t" {
				vpnSeedServerOk = true
				break
			}
		}

		if !setupOk {
			t.Logf("Deployment %q init container 'setup' missing SELinuxOptions.Type == 'spc_t'", obj.Name)
			return false
		}
		if !vpnSeedServerOk {
			t.Logf("Deployment %q container 'vpn-seed-server' missing SELinuxOptions.Type == 'spc_t'", obj.Name)
			return false
		}

		return true
	})
	if err != nil {
		t.Fatalf("Timeout waiting for VPN Seed Server mutation: %v", err)
	}
}

func (f *extensionProviderWebhookTestFixture) testMutatesVirtualGardenETCDMain(t *testing.T, ctx context.Context) {
	// Arrange
	// Use "garden" namespace as required by the webhook selector
	gardenNamespace := "garden"

	backupBucketName := "01-test-bucket-" + *commitHash
	backupBucket := &extensionsv1alpha1.BackupBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: backupBucketName,
			Annotations: map[string]string{
				gdc.FullyQualifiedBucketNameAnnotationKey: "my-fq-bucket-name",
			},
		},
		Spec: extensionsv1alpha1.BackupBucketSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{Type: gdc.Type},
			Region:      *region,
			SecretRef: corev1.SecretReference{
				Name:      "non-existent-secret",
				Namespace: gardenNamespace,
			},
		},
	}
	if err := f.vucClient.Create(ctx, backupBucket); err != nil {
		t.Fatalf("failed to create BackupBucket: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, backupBucket); err != nil {
			if !apierrors.IsNotFound(err) {
				t.Errorf("failed to delete BackupBucket: %v", err)
			}
		} else {
			if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
				if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(backupBucket), backupBucket); err != nil {
					if apierrors.IsNotFound(err) {
						return true, nil
					}
					return false, err
				}
				return false, nil
			}); err != nil {
				t.Errorf("error waiting for BackupBucket object to be deleted: %v", err)
			}
		}
	})

	etcdName := "virtual-garden-etcd-main"
	var etcd *druidv1alpha1.Etcd

	// Action
	t.Logf("Creating Etcd %q in namespace %q", etcdName, gardenNamespace)
	if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		etcd = &druidv1alpha1.Etcd{
			ObjectMeta: metav1.ObjectMeta{
				Name:      etcdName,
				Namespace: gardenNamespace,
			},
			Spec: druidv1alpha1.EtcdSpec{
				Labels: map[string]string{
					"name": "etcd",
				},
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"name": "etcd",
					},
				},
				Replicas: 1,
				Backup: druidv1alpha1.BackupSpec{
					Store: &druidv1alpha1.StoreSpec{
						Container: ptr.To(backupBucketName),
					},
				},
			},
		}

		if err := f.vucClient.Create(ctx, etcd); err != nil {
			if strings.Contains(err.Error(), "no backup bucket found") || strings.Contains(err.Error(), "No matching backup bucket found") {
				t.Log("Webhook denied Etcd creation (BackupBucket not in cache yet), retrying...")
				return false, nil
			}
			return false, err
		}
		return true, nil
	}); err != nil {
		t.Fatalf("failed to create Etcd: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, etcd); err != nil {
			if !apierrors.IsNotFound(err) {
				t.Errorf("failed to delete Etcd: %v", err)
			}
		}
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(etcd), etcd); err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		}); err != nil {
			t.Errorf("error waiting for Etcd object to be deleted: %v", err)
		}
	})

	// Assert
	etcdList := &druidv1alpha1.EtcdList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": etcdName},
		client.InNamespace(gardenNamespace),
	}
	err := kubernetes.WaitForCondition[*druidv1alpha1.Etcd](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, etcdList, listOptions...)
	}, func(obj *druidv1alpha1.Etcd) bool {
		if obj.Annotations == nil || obj.Annotations["backupprovider.gdc.gardener.cloud/mutated"] != "true" {
			return false
		}

		if obj.Spec.Backup.Store == nil {
			return false
		}

		if obj.Spec.Backup.Store.Container == nil || obj.Spec.Backup.Store.Provider == nil {
			return false
		}

		return *obj.Spec.Backup.Store.Container == "my-fq-bucket-name" && *obj.Spec.Backup.Store.Provider == "aws"
	})
	if err != nil {
		t.Fatalf("Timeout waiting for Etcd mutation: %v", err)
	}
}

func (f *extensionProviderWebhookTestFixture) testMutatesFluentBit(t *testing.T, ctx context.Context) {
	// Arrange
	// Use "garden" namespace as required by the webhook selector
	gardenNamespace := "garden"

	dsName := "fluent-bit"
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dsName,
			Namespace: gardenNamespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "fluent-bit"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "fluent-bit"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "fluent-bit",
							Image: "fluent-bit:latest",
						},
					},
				},
			},
		},
	}

	// Action
	if err := f.vucClient.Create(ctx, ds); err != nil {
		t.Fatalf("failed to create DaemonSet: %v", err)
	}
	t.Cleanup(func() {
		if err := f.vucClient.Delete(ctx, ds); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("failed to delete DaemonSet: %v", err)
		}
	})

	// Assert
	dsList := &appsv1.DaemonSetList{}
	listOptions := []client.ListOption{
		client.MatchingFields{"metadata.name": dsName},
		client.InNamespace(gardenNamespace),
	}
	err := kubernetes.WaitForCondition[*appsv1.DaemonSet](ctx, 1*time.Minute, func() (watch.Interface, error) {
		return f.vucClient.Watch(ctx, dsList, listOptions...)
	}, func(obj *appsv1.DaemonSet) bool {
		found := false
		for _, c := range obj.Spec.Template.Spec.Containers {
			if c.Name == "fluent-bit" {
				found = true
				if c.SecurityContext == nil || c.SecurityContext.SELinuxOptions == nil || c.SecurityContext.SELinuxOptions.Type != "spc_t" {
					t.Logf("DaemonSet %q container 'fluent-bit' missing SELinuxOptions.Type == 'spc_t'", obj.Name)
					return false
				}
			}
		}
		if !found {
			t.Logf("DaemonSet %q missing 'fluent-bit' container", obj.Name)
			return false
		}
		return true
	})
	if err != nil {
		t.Fatalf("Timeout waiting for Fluent Bit mutation: %v", err)
	}
}
