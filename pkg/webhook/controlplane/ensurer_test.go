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
	"errors"
	"testing"

	gardenimagevector "github.com/gardener/gardener/imagevector"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"

	"github.com/coreos/go-systemd/v22/unit"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/gardener/gardener/extensions/pkg/controller"
	gcontext "github.com/gardener/gardener/extensions/pkg/webhook/context"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	imagevectorutils "github.com/gardener/gardener/pkg/utils/imagevector"
	testutils "github.com/gardener/gardener/pkg/utils/test"
	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	vpaautoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const namespace = "test"

func TestEnsureKubeAPIServerDeployment(t *testing.T) {
	c := fake.NewClientBuilder().WithObjects().Build()
	ensurer := NewEnsurer(c, logger, nil)
	ctx := context.Background()
	tests := []struct {
		name       string
		deployment appsv1.Deployment
		want       appsv1.Deployment
	}{
		{
			name: "ensure kube-apiserver deployment success",
			deployment: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      v1beta1constants.DeploymentNameKubeAPIServer,
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "some-other-container",
								},
								{
									Name: "kube-apiserver",
								},
							},
						},
					},
				},
			},

			want: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      v1beta1constants.DeploymentNameKubeAPIServer,
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"networking.resources.gardener.cloud/to-csi-snapshot-validation-tcp-443": "allowed",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "some-other-container",
								},
								{
									Name:    "kube-apiserver",
									Command: []string{"--disable-admission-plugins=PersistentVolumeLabel"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "ensure kube-apiserver deployment success - with vpn-client container",
			deployment: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      v1beta1constants.DeploymentNameKubeAPIServer,
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "some-other-container",
								},
								{
									Name: "kube-apiserver",
								},
								{
									Name: gardenimagevector.ContainerImageNameVpnClient,
								},
							},
						},
					},
				},
			},

			want: appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      v1beta1constants.DeploymentNameKubeAPIServer,
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"networking.resources.gardener.cloud/to-csi-snapshot-validation-tcp-443": "allowed",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "some-other-container",
								},
								{
									Name:    "kube-apiserver",
									Command: []string{"--disable-admission-plugins=PersistentVolumeLabel"},
								},
								{
									Name: "vpn-client",
									SecurityContext: &corev1.SecurityContext{
										SELinuxOptions: &corev1.SELinuxOptions{
											Type: "spc_t",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ensurer.EnsureKubeAPIServerDeployment(ctx, createContext("1.28.2"), &tt.deployment, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.deployment, tt.want); diff != "" {
				t.Errorf("expected values %v, but got %v, differences %v", tt.want, tt.deployment, diff)
			}
		})
	}
}

func TestEnsureMachineControllerManagerDeployment(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "foo",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"networking.resources.gardener.cloud/to-csi-snapshot-validation-tcp-443": "allowed",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "some-other-container",
						},
					},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithObjects().Build()
	ensurer := NewEnsurer(c, logger, nil)
	ctx := context.Background()
	repositoryValue := "foo"
	testutils.WithVar(&ImageVector, imagevectorutils.ImageVector{{
		Name:       "machine-controller-manager-provider-gdch",
		Repository: &repositoryValue,
		Tag:        ptr.To("bar"),
	}})

	want := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "foo",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"networking.resources.gardener.cloud/to-csi-snapshot-validation-tcp-443": "allowed",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "some-other-container",
						},
						{
							Name:  "machine-controller-manager-provider-gdch",
							Image: "foo:bar",
							Args: []string{
								"--control-kubeconfig=inClusterConfig",
								"--kube-api-qps=100",
								"--kube-api-burst=200",
								"--machine-creation-timeout=20m",
								"--machine-drain-timeout=2h",
								"--machine-health-timeout=10m",
								"--machine-safety-apiserver-statuscheck-timeout=30s",
								"--machine-safety-apiserver-statuscheck-period=1m",
								"--machine-safety-orphan-vms-period=30m",
								"--namespace=foo",
								"--port=10259",
								"--target-kubeconfig=/var/run/secrets/gardener.cloud/shoot/generic-kubeconfig/kubeconfig",
								"--v=3",
							},
							Ports: []corev1.ContainerPort{{Name: "providermetrics", ContainerPort: 10259, Protocol: "TCP"}},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									"cpu":    resource.MustParse("10m"),
									"memory": resource.MustParse("20Mi"),
								},
							},
							ImagePullPolicy: "IfNotPresent",
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "kubeconfig",
									ReadOnly:  true,
									MountPath: "/var/run/secrets/gardener.cloud/shoot/generic-kubeconfig",
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path:   "/healthz",
										Port:   intstr.FromInt(10259),
										Scheme: "HTTP",
									},
								},
								InitialDelaySeconds: 30,
								TimeoutSeconds:      5,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								FailureThreshold:    3,
							},
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: ptr.To(false),
							},
						},
					},
				},
			},
		},
	}

	gctx := createContext("1.28.2")
	err := ensurer.EnsureMachineControllerManagerDeployment(ctx, gctx, deployment, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if diff := cmp.Diff(deployment, want); diff != "" {
		t.Errorf("expected values %v, but got %v, differences %v", want, deployment, diff)
	}
}

func TestEnsureKubeControllerManagerDeployment(t *testing.T) {
	directoryOrCreate := corev1.HostPathDirectoryOrCreate
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: v1beta1constants.DeploymentNameKubeControllerManager},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"networking.gardener.cloud/to-blocked-cidrs":    "true",
						"networking.gardener.cloud/to-public-networks":  "true",
						"networking.gardener.cloud/to-private-networks": "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "kube-controller-manager",
							Command: []string{
								"--cloud-config=cfg",
								"--external-cloud-volume-plugin=volume-plugin",
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "etc-ssl",
									MountPath: "/etc/ssl",
									ReadOnly:  true,
								},
								{
									Name:      "usr-share-cacerts",
									MountPath: "/usr/share/ca-certificates",
									ReadOnly:  true,
								},
							},
						},
						{
							Name: "other-container",
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "usr-share-cacerts",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/usr/share/ca-certificates",
									Type: &directoryOrCreate,
								},
							},
						},
						{
							Name: "etc-ssl",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/etc/ssl",
									Type: &directoryOrCreate,
								},
							},
						},
					},
				},
			},
		},
	}

	eContextK8s128 := createContext("1.28.2")

	c := fake.NewClientBuilder().WithObjects().Build()
	ensurer := NewEnsurer(c, logger, nil)
	ctx := context.Background()

	err := ensurer.EnsureKubeControllerManagerDeployment(ctx, eContextK8s128, deployment, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      v1beta1constants.DeploymentNameKubeControllerManager,
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:         "kube-controller-manager",
							Command:      []string{"--cloud-provider=external"},
							VolumeMounts: []corev1.VolumeMount{},
						},
						{
							Name: "other-container",
						},
					},
					Volumes: []corev1.Volume{},
				},
			},
		},
	}

	if diff := cmp.Diff(deployment, want); diff != "" {
		t.Errorf("expected values %v, but got %v, differences %v", want, deployment, diff)
	}
}

func TestEnsureKubeletConfiguration(t *testing.T) {
	kubletConfig := &kubeletconfigv1beta1.KubeletConfiguration{
		EnableControllerAttachDetach: ptr.To(false),
	}
	eContext := createContext("1.28.2")

	c := fake.NewClientBuilder().WithObjects().Build()
	ensurer := NewEnsurer(c, logger, nil)
	ctx := context.Background()

	err := ensurer.EnsureKubeletConfiguration(ctx, eContext, nil, kubletConfig, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := &kubeletconfigv1beta1.KubeletConfiguration{
		EnableControllerAttachDetach: ptr.To(true),
	}

	if diff := cmp.Diff(kubletConfig, want); diff != "" {
		t.Errorf("expected values %v, but got %v, differences %v", want, kubletConfig, diff)
	}
}
func TestEnsureKubeletServiceUnitOptions(t *testing.T) {
	options := []*unit.UnitOption{
		{
			Section: "Section1",
			Name:    "Test",
			Value:   "test",
		},
		{
			Section: "Service",
			Name:    "ExecStart",
			Value:   `--param1=123`,
		},
	}
	eContext := createContext("1.28.2")

	c := fake.NewClientBuilder().WithObjects().Build()
	ensurer := NewEnsurer(c, logger, nil)
	ctx := context.Background()

	got, err := ensurer.EnsureKubeletServiceUnitOptions(ctx, eContext, nil, options, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []*unit.UnitOption{
		{
			Section: "Section1",
			Name:    "Test",
			Value:   "test",
		},
		{
			Section: "Service",
			Name:    "ExecStart",
			Value:   `--param1=123 --cloud-provider=external`,
		},
		{
			Section: "Service",
			Name:    "ExecStartPre",
			Value:   `/bin/sh -c 'hostnamectl set-hostname $(hostname)'`,
		},
	}

	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("expected values %v, but got %v, differences %v", want, options, diff)
	}
}

func TestEnsureMachineControllerManagerVPA(t *testing.T) {
	autoscaler := vpaautoscalingv1.VerticalPodAutoscaler{
		Spec: vpaautoscalingv1.VerticalPodAutoscalerSpec{
			ResourcePolicy: &vpaautoscalingv1.PodResourcePolicy{
				ContainerPolicies: []vpaautoscalingv1.ContainerResourcePolicy{
					{
						ContainerName: "some-other-container-gdc",
					},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithObjects().Build()
	ensurer := NewEnsurer(c, logger, nil)
	ctx := context.Background()
	eContext := createContext("1.28.2")
	err := ensurer.EnsureMachineControllerManagerVPA(ctx, eContext, &autoscaler, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	requestOnly := vpaautoscalingv1.ContainerControlledValuesRequestsOnly
	want := vpaautoscalingv1.VerticalPodAutoscaler{
		Spec: vpaautoscalingv1.VerticalPodAutoscalerSpec{
			ResourcePolicy: &vpaautoscalingv1.PodResourcePolicy{
				ContainerPolicies: []vpaautoscalingv1.ContainerResourcePolicy{
					{
						ContainerName: "some-other-container-gdc",
					},
					{
						ContainerName:    "machine-controller-manager-provider-gdch",
						ControlledValues: &requestOnly,
					},
				},
			},
		},
	}

	if diff := cmp.Diff(autoscaler, want); diff != "" {
		t.Errorf("expected values %v, but got %v, differences %v", want, autoscaler, diff)
	}
}

func TestEnsureETCD_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = druidv1alpha1.AddToScheme(scheme)
	_ = extensionsv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	ctx := context.TODO()

	tests := []struct {
		name          string
		newObj        *druidv1alpha1.Etcd
		oldObj        *druidv1alpha1.Etcd
		backupBuckets []*extensionsv1alpha1.BackupBucket
		expectedETCD  *druidv1alpha1.Etcd
	}{
		{
			name: "BackupBucket exists and bucket annotation is present; modify",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("backup-bucket"),
							Provider:  pointerToProvider("gdch"),
						},
					},
				},
			},
			oldObj: nil,
			backupBuckets: []*extensionsv1alpha1.BackupBucket{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "backup-bucket",
						Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-backup-bucket-name"},
					},
				},
			},
			expectedETCD: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("fq-backup-bucket-name"),
							Provider:  pointerToProvider("aws"),
						},
					},
				},
			},
		},
		{
			name: "Multiple BackupBuckets present; matches correct bucket by name",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("shoot-backup-bucket"),
							Provider:  pointerToProvider("gdch"),
						},
					},
				},
			},
			oldObj: nil,
			backupBuckets: []*extensionsv1alpha1.BackupBucket{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "garden-backup-bucket",
						Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-garden-bucket-name"},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "shoot-backup-bucket",
						Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-shoot-backup-name"},
					},
				},
			},
			expectedETCD: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("fq-shoot-backup-name"),
							Provider:  pointerToProvider("aws"),
						},
					},
				},
			},
		},
		{
			name: "Etcd is not etcd-main; do not modify",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-events",
					Namespace: "shoot-namespace",
				},
			},
			oldObj:        nil,
			backupBuckets: nil,
			expectedETCD: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-events",
					Namespace: "shoot-namespace",
				},
			},
		},
		{
			name: "Etcd update; preserve storage fields",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("backup-bucket"),
							Provider:  pointerToProvider("gdch"),
						},
					},
				},
			},
			oldObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					StorageClass:    pointerToString("fast"),
					StorageCapacity: pointerToQuantity("10Gi"),
				},
			},
			backupBuckets: []*extensionsv1alpha1.BackupBucket{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "backup-bucket",
						Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-backup-bucket-name"},
					},
				},
			},
			expectedETCD: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					StorageClass:    pointerToString("fast"),
					StorageCapacity: pointerToQuantity("10Gi"),
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("fq-backup-bucket-name"),
							Provider:  pointerToProvider("aws"),
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []runtime.Object{}
			for _, bb := range tt.backupBuckets {
				objs = append(objs, bb)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

			ensurer := NewEnsurer(c, logger, nil)
			err := ensurer.EnsureETCD(ctx, nil, tt.newObj, tt.oldObj)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.newObj, tt.expectedETCD); diff != "" {
				t.Errorf("\nexpected ETCD: %v, \ngot: %v \n differences %v", tt.expectedETCD, tt.newObj, diff)
			}
		})
	}
}

func TestEnsureETCD_Fail(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = druidv1alpha1.AddToScheme(scheme)
	_ = extensionsv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	ctx := context.TODO()

	tests := []struct {
		name          string
		newObj        *druidv1alpha1.Etcd
		oldObj        *druidv1alpha1.Etcd
		backupBuckets []*extensionsv1alpha1.BackupBucket
		expectedError error
	}{
		{
			name: "BackupBucket annotation is missing",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("backup-bucket"),
						},
					},
				},
			},
			oldObj: nil,
			backupBuckets: []*extensionsv1alpha1.BackupBucket{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "backup-bucket",
					},
				},
			},
			expectedError: errors.New("fqBucketName annotation not found in BackupBucket backup-bucket"),
		},
		{
			name: "BackupBucket does not exist; do not modify",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("backup-bucket"),
							Provider:  pointerToProvider("gdch"),
						},
					},
				},
			},
			oldObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
			},
			backupBuckets: nil,
			expectedError: errors.New("no backup bucket found for etcd shoot-namespace/etcd-main"),
		},
		{
			name: "No matching BackupBucket found for container name",
			newObj: &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-main",
					Namespace: "shoot-namespace",
				},
				Spec: druidv1alpha1.EtcdSpec{
					Backup: druidv1alpha1.BackupSpec{
						Store: &druidv1alpha1.StoreSpec{
							Container: pointerToString("non-existent-bucket"),
						},
					},
				},
			},
			oldObj: nil,
			backupBuckets: []*extensionsv1alpha1.BackupBucket{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "other-bucket",
						Annotations: map[string]string{gdc.FullyQualifiedBucketNameAnnotationKey: "fq-other-bucket"},
					},
				},
			},
			expectedError: errors.New("no matching backup bucket found for etcd shoot-namespace/etcd-main with container name non-existent-bucket"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []runtime.Object{}
			for _, bb := range tt.backupBuckets {
				objs = append(objs, bb)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

			ensurer := NewEnsurer(c, logger, nil)
			err := ensurer.EnsureETCD(ctx, nil, tt.newObj, tt.oldObj)

			if err == nil || err.Error() != tt.expectedError.Error() {
				t.Fatalf("expected error: %v, got: %v", tt.expectedError, err)
			}
		})
	}
}

func pointerToString(s string) *string {
	return &s
}

func pointerToProvider(p druidv1alpha1.StorageProvider) *druidv1alpha1.StorageProvider {
	return &p
}

func pointerToQuantity(q string) *resource.Quantity {
	qty := resource.MustParse(q)
	return &qty
}

func createContext(version string) gcontext.GardenContext {
	return gcontext.NewInternalGardenContext(
		&controller.Cluster{
			Shoot: &gardencorev1beta1.Shoot{
				Spec: gardencorev1beta1.ShootSpec{
					Kubernetes: gardencorev1beta1.Kubernetes{
						Version: version,
					},
				},
			},
		},
	)
}
