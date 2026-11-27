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

package bastion

import (
	"context"
	"strings"
	"testing"
	"time"

	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gdchnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"
	vmv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/virtualmachine/v1"
)

const policyTestProject = "garden"

func newPolicyTestClient(objects ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	runtimeutil.Must(corev1.AddToScheme(scheme))
	runtimeutil.Must(extensionsv1alpha1.AddToScheme(scheme))
	runtimeutil.Must(gdchnetworkingv1.AddToScheme(scheme))
	runtimeutil.Must(vmv1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func newPolicyTestBastion(name string, cidrs ...string) *extensionsv1alpha1.Bastion {
	bastion := &extensionsv1alpha1.Bastion{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "shoot--project--name"},
	}
	for _, cidr := range cidrs {
		bastion.Spec.Ingress = append(bastion.Spec.Ingress, extensionsv1alpha1.BastionIngressPolicy{
			IPBlock: networkingv1.IPBlock{CIDR: cidr},
		})
	}
	return bastion
}

func newPolicyTestVM(bastionName string, labels map[string]string) *vmv1.VirtualMachine {
	return &vmv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:              VMName(bastionName),
			Namespace:         policyTestProject,
			Labels:            labels,
			CreationTimestamp: metav1.NewTime(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)),
		},
		Status: vmv1.VirtualMachineStatus{State: vmv1.VirtualMachineStateRunning},
	}
}

func sshOnly() []gdchnetworkingv1.ProjectNetworkPolicyPort {
	return []gdchnetworkingv1.ProjectNetworkPolicyPort{
		{Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(intstr.FromInt32(22))},
	}
}

func bastionVMSelector(bastionName string) *gdchnetworkingv1.WorkloadSelector {
	return &gdchnetworkingv1.WorkloadSelector{
		LabelSelector: &gdchnetworkingv1.WorkloadLabelSelector{
			Workloads: &metav1.LabelSelector{MatchLabels: VMLabels(bastionName)},
		},
	}
}

func TestVMLabels(t *testing.T) {
	t.Run("short name", func(t *testing.T) {
		got := VMLabels("cli-1a2b3c4d")
		want := map[string]string{VMLabelKey: "cli-1a2b3c4d-bastion-vm"}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("VMLabels() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("name that is too long for a label value", func(t *testing.T) {
		long := strings.Repeat("a", 60)
		other := strings.Repeat("a", 59) + "b"
		value := VMLabels(long)[VMLabelKey]
		if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
			t.Errorf("VMLabels() value %q is not a valid label value: %v", value, errs)
		}
		if value != VMLabels(long)[VMLabelKey] {
			t.Errorf("VMLabels() is not stable for the same name")
		}
		if value == VMLabels(other)[VMLabelKey] {
			t.Errorf("VMLabels() returns the same value %q for different names", value)
		}
	})

	t.Run("label key", func(t *testing.T) {
		if errs := validation.IsQualifiedName(VMLabelKey); len(errs) > 0 {
			t.Errorf("label key %q is not valid: %v", VMLabelKey, errs)
		}
	})
}

func Test_createOrUpdateProjectNetworkPolicy(t *testing.T) {
	const bastionName = "bastion"
	labelledVM := newPolicyTestVM(bastionName, VMLabels(bastionName))

	tests := []struct {
		name     string
		bastion  *extensionsv1alpha1.Bastion
		vm       *vmv1.VirtualMachine
		existing []client.Object
		want     *gdchnetworkingv1.ProjectNetworkPolicySpec
		wantErr  string
	}{
		{
			name:    "one CIDR reaches only the bastion VM on the SSH port",
			bastion: newPolicyTestBastion(bastionName, "203.0.113.7/32"),
			vm:      labelledVM,
			want: &gdchnetworkingv1.ProjectNetworkPolicySpec{
				Subject: gdchnetworkingv1.ProjectNetworkPolicySubject{
					SubjectType:          gdchnetworkingv1.PolicySubjectTypeUserWorkload,
					UserWorkloadSelector: bastionVMSelector(bastionName),
				},
				PolicyType: gdchnetworkingv1.PolicyTypeIngress,
				Ingress: []gdchnetworkingv1.ProjectNetworkPolicyIngressRule{{
					Ports: sshOnly(),
					From: []gdchnetworkingv1.ProjectNetworkPolicyPeer{{
						IPBlock: &networkingv1.IPBlock{CIDR: "203.0.113.7/32"},
					}},
				}},
			},
		},
		{
			name:    "several CIDRs share the single peer of the rule",
			bastion: newPolicyTestBastion(bastionName, "203.0.113.7/32", "198.51.100.0/24", "2001:db8::1/128"),
			vm:      labelledVM,
			want: &gdchnetworkingv1.ProjectNetworkPolicySpec{
				Subject: gdchnetworkingv1.ProjectNetworkPolicySubject{
					SubjectType:          gdchnetworkingv1.PolicySubjectTypeUserWorkload,
					UserWorkloadSelector: bastionVMSelector(bastionName),
				},
				PolicyType: gdchnetworkingv1.PolicyTypeIngress,
				Ingress: []gdchnetworkingv1.ProjectNetworkPolicyIngressRule{{
					Ports: sshOnly(),
					From: []gdchnetworkingv1.ProjectNetworkPolicyPeer{{
						IPBlocks: []networkingv1.IPBlock{
							{CIDR: "203.0.113.7/32"},
							{CIDR: "198.51.100.0/24"},
							{CIDR: "2001:db8::1/128"},
						},
					}},
				}},
			},
		},
		{
			name:    "an existing policy for all workloads and ports is narrowed",
			bastion: newPolicyTestBastion(bastionName, "203.0.113.7/32"),
			vm:      labelledVM,
			existing: []client.Object{&gdchnetworkingv1.ProjectNetworkPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: ProjectNetworkPolicyName(bastionName), Namespace: policyTestProject},
				Spec: gdchnetworkingv1.ProjectNetworkPolicySpec{
					Subject:    gdchnetworkingv1.ProjectNetworkPolicySubject{SubjectType: gdchnetworkingv1.PolicySubjectTypeUserWorkload},
					PolicyType: gdchnetworkingv1.PolicyTypeIngress,
					Ingress: []gdchnetworkingv1.ProjectNetworkPolicyIngressRule{{
						From: []gdchnetworkingv1.ProjectNetworkPolicyPeer{{
							IPBlock: &networkingv1.IPBlock{CIDR: "0.0.0.0/0"},
						}},
					}},
				},
			}},
			want: &gdchnetworkingv1.ProjectNetworkPolicySpec{
				Subject: gdchnetworkingv1.ProjectNetworkPolicySubject{
					SubjectType:          gdchnetworkingv1.PolicySubjectTypeUserWorkload,
					UserWorkloadSelector: bastionVMSelector(bastionName),
				},
				PolicyType: gdchnetworkingv1.PolicyTypeIngress,
				Ingress: []gdchnetworkingv1.ProjectNetworkPolicyIngressRule{{
					Ports: sshOnly(),
					From: []gdchnetworkingv1.ProjectNetworkPolicyPeer{{
						IPBlock: &networkingv1.IPBlock{CIDR: "203.0.113.7/32"},
					}},
				}},
			},
		},
		{
			name:    "a VM that runs without the labels keeps a policy for all workloads, limited to the SSH port",
			bastion: newPolicyTestBastion(bastionName, "203.0.113.7/32"),
			vm:      newPolicyTestVM(bastionName, map[string]string{"other": "label"}),
			want: &gdchnetworkingv1.ProjectNetworkPolicySpec{
				Subject:    gdchnetworkingv1.ProjectNetworkPolicySubject{SubjectType: gdchnetworkingv1.PolicySubjectTypeUserWorkload},
				PolicyType: gdchnetworkingv1.PolicyTypeIngress,
				Ingress: []gdchnetworkingv1.ProjectNetworkPolicyIngressRule{{
					Ports: sshOnly(),
					From: []gdchnetworkingv1.ProjectNetworkPolicyPeer{{
						IPBlock: &networkingv1.IPBlock{CIDR: "203.0.113.7/32"},
					}},
				}},
			},
		},
		{
			name:    "a VM labelled for another bastion is not selected",
			bastion: newPolicyTestBastion(bastionName, "203.0.113.7/32"),
			vm:      newPolicyTestVM(bastionName, VMLabels("another-bastion")),
			want: &gdchnetworkingv1.ProjectNetworkPolicySpec{
				Subject:    gdchnetworkingv1.ProjectNetworkPolicySubject{SubjectType: gdchnetworkingv1.PolicySubjectTypeUserWorkload},
				PolicyType: gdchnetworkingv1.PolicyTypeIngress,
				Ingress: []gdchnetworkingv1.ProjectNetworkPolicyIngressRule{{
					Ports: sshOnly(),
					From: []gdchnetworkingv1.ProjectNetworkPolicyPeer{{
						IPBlock: &networkingv1.IPBlock{CIDR: "203.0.113.7/32"},
					}},
				}},
			},
		},
		{
			name:    "a bastion without ingress CIDRs does not get a policy that matches every source",
			bastion: newPolicyTestBastion(bastionName),
			vm:      labelledVM,
			wantErr: "does not specify any ingress CIDR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			c := newPolicyTestClient(tt.existing...)

			err := createOrUpdateProjectNetworkPolicy(ctx, c, policyTestProject, tt.bastion, tt.vm)

			pnp := &gdchnetworkingv1.ProjectNetworkPolicy{}
			getErr := c.Get(ctx, client.ObjectKey{Name: ProjectNetworkPolicyName(tt.bastion.Name), Namespace: policyTestProject}, pnp)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("createOrUpdateProjectNetworkPolicy() error = %v, want an error containing %q", err, tt.wantErr)
				}
				if !apierrors.IsNotFound(getErr) {
					t.Fatalf("no policy must be created, get returned: %v", getErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("createOrUpdateProjectNetworkPolicy() error = %v", err)
			}
			if getErr != nil {
				t.Fatalf("failed to get project network policy: %v", getErr)
			}
			if diff := cmp.Diff(tt.want, &pnp.Spec); diff != "" {
				t.Errorf("project network policy spec mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_createOrUpdateVirtualMachine_labels(t *testing.T) {
	const bastionName = "bastion"
	readyDisk := func() *vmv1.VirtualMachineDisk {
		return &vmv1.VirtualMachineDisk{
			ObjectMeta: metav1.ObjectMeta{Name: DiskName(bastionName), Namespace: policyTestProject},
			Status:     vmv1.VirtualMachineDiskStatus{Phase: vmv1.DiskPhaseSucceeded},
		}
	}

	tests := []struct {
		name       string
		existingVM *vmv1.VirtualMachine
		wantLabels map[string]string
		// wantSelected says whether the returned VM can be selected by the policy.
		wantSelected bool
	}{
		{
			name:         "a new VM is created with the labels",
			wantLabels:   VMLabels(bastionName),
			wantSelected: true,
		},
		{
			name:         "a VM that already runs without the labels is not relabelled",
			existingVM:   newPolicyTestVM(bastionName, map[string]string{"other": "label"}),
			wantLabels:   map[string]string{"other": "label"},
			wantSelected: false,
		},
		{
			name:         "a VM that runs with the labels keeps them",
			existingVM:   newPolicyTestVM(bastionName, VMLabels(bastionName)),
			wantLabels:   VMLabels(bastionName),
			wantSelected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			objects := []client.Object{readyDisk()}
			if tt.existingVM != nil {
				objects = append(objects, tt.existingVM)
			}
			c := newPolicyTestClient(objects...)

			// A new VM is not running in the fake API, so the call ends with a requeue
			// after the VM has been created. The returned VM is the stored one either way.
			vm, _ := createOrUpdateVirtualMachine(ctx, c, policyTestProject, newPolicyTestBastion(bastionName, "203.0.113.7/32"), "image", "image-project", "machine-type")

			stored := &vmv1.VirtualMachine{}
			if err := c.Get(ctx, client.ObjectKey{Name: VMName(bastionName), Namespace: policyTestProject}, stored); err != nil {
				t.Fatalf("failed to get vm: %v", err)
			}
			if diff := cmp.Diff(tt.wantLabels, stored.Labels); diff != "" {
				t.Errorf("vm labels mismatch (-want +got):\n%s", diff)
			}
			if got := hasVMLabels(vm, bastionName); got != tt.wantSelected {
				t.Errorf("hasVMLabels() = %v, want %v", got, tt.wantSelected)
			}
		})
	}
}
