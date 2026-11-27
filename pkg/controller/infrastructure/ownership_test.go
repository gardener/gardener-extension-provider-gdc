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

package infrastructure

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gardener/gardener/extensions/pkg/controller"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ipamglobalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/ipam/v1"
	ipamv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/ipam/v1"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
	gdcfake "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

const (
	otherInfraUID = "other-infra-uid"
	testProject   = "test-project"
	rootName      = "test-infrastructure"
)

var (
	ownedLabels   = map[string]string{infraUIDLabel: infraUID}
	foreignLabels = map[string]string{infraUIDLabel: otherInfraUID}
	// GDC marks the copy of a global subnet that it propagates into a zone.
	propagatedAnnotations = map[string]string{ipamv1.CreationOriginAnnotation: string(ipamv1.GlobalOrg)}
)

func readyConditions() []metav1.Condition {
	return []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
}

func newGlobalSubnet(name, parent, cidr string, labels map[string]string) *ipamglobalv1.Subnet {
	return &ipamglobalv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testProject, Labels: labels},
		Spec: ipamglobalv1.SubnetSpec{
			Type:            ipamv1.Branch,
			IPv4Request:     &ipamv1.SubnetRequest{CIDR: ptr.To(cidr)},
			ParentReference: &ipamv1.SubnetReference{Name: parent, Namespace: ptr.To(testProject)},
		},
		Status: ipamglobalv1.SubnetStatus{Conditions: readyConditions()},
	}
}

func newZonalSubnet(name, parent, cidr string, labels, annotations map[string]string) *ipamv1.Subnet {
	return &ipamv1.Subnet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testProject, Labels: labels, Annotations: annotations},
		Spec: ipamv1.SubnetSpec{
			Type:            ipamv1.Branch,
			IPv4Request:     &ipamv1.SubnetRequest{CIDR: ptr.To(cidr)},
			ParentReference: &ipamv1.SubnetReference{Name: parent, Namespace: ptr.To(testProject)},
		},
		Status: ipamv1.SubnetStatus{Conditions: readyConditions()},
	}
}

func newOwnershipTestInfra(zones ...v1alpha1.Zone) *extensionsv1alpha1.Infrastructure {
	return &extensionsv1alpha1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rootName,
			Namespace: "test-infrastructure-ns",
			UID:       types.UID(infraUID),
		},
		Spec: extensionsv1alpha1.InfrastructureSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(&v1alpha1.InfrastructureConfig{
						TypeMeta: metav1.TypeMeta{
							APIVersion: v1alpha1.SchemeGroupVersion.String(),
							Kind:       "InfrastructureConfig",
						},
						Networks: v1alpha1.NetworkConfig{
							ParentSubnet:        "test-ip-pool",
							ParentSubnetProject: "platform",
							NodeCIDR:            "10.0.212.1/29",
							Zones:               zones,
						},
						EnableEgress: ptr.To(false),
					}),
				},
			},
			SecretRef: corev1.SecretReference{
				Name:      "my-secret",
				Namespace: "test-infrastructure-ns",
			},
		},
	}
}

func newOwnershipTestSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "my-secret", Namespace: "test-infrastructure-ns"},
		Data: map[string][]byte{
			"serviceaccount.json": []byte(`{"Project": "test-project"}`),
			"gdch-config":         []byte(`{}`),
		},
	}
}

func TestLabelledOwner(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   subnetOwner
	}{
		{name: "no labels", labels: nil, want: ownerUnknown},
		{name: "other labels only", labels: map[string]string{"ipam.gdc.goog/vpc": "default-vpc"}, want: ownerUnknown},
		{name: "own UID", labels: map[string]string{infraUIDLabel: infraUID}, want: ownerSelf},
		{name: "other UID", labels: map[string]string{infraUIDLabel: otherInfraUID}, want: ownerOther},
		{name: "empty label value", labels: map[string]string{infraUIDLabel: ""}, want: ownerOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := labelledOwner(tt.labels, infraUID); got != tt.want {
				t.Errorf("labelledOwner() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReferencesSubnet(t *testing.T) {
	tests := []struct {
		name string
		ref  *ipamv1.SubnetReference
		want bool
	}{
		{name: "nil reference", ref: nil, want: false},
		{name: "same name without namespace", ref: &ipamv1.SubnetReference{Name: rootName}, want: true},
		{name: "same name with empty namespace", ref: &ipamv1.SubnetReference{Name: rootName, Namespace: ptr.To("")}, want: true},
		{name: "same name in the project", ref: &ipamv1.SubnetReference{Name: rootName, Namespace: ptr.To(testProject)}, want: true},
		{name: "same name with explicit single subnet type", ref: &ipamv1.SubnetReference{Name: rootName, Namespace: ptr.To(testProject), Type: ipamv1.SingleSubnet}, want: true},
		{name: "same name in another project", ref: &ipamv1.SubnetReference{Name: rootName, Namespace: ptr.To("other-project")}, want: false},
		{name: "other name", ref: &ipamv1.SubnetReference{Name: "other", Namespace: ptr.To(testProject)}, want: false},
		{name: "subnet group with the same name", ref: &ipamv1.SubnetReference{Name: rootName, Namespace: ptr.To(testProject), Type: ipamv1.SubnetGroup}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := referencesSubnet(tt.ref, rootName, testProject); got != tt.want {
				t.Errorf("referencesSubnet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsRootSubnetOwned(t *testing.T) {
	reconciled := newOwnershipTestInfra()
	reconciled.Status.ProviderStatus = &runtime.RawExtension{Raw: []byte(`{}`)}
	neverReconciled := newOwnershipTestInfra()

	tests := []struct {
		name   string
		labels map[string]string
		infra  *extensionsv1alpha1.Infrastructure
		want   bool
	}{
		{name: "own label", labels: ownedLabels, infra: neverReconciled, want: true},
		{name: "label of another Infrastructure", labels: foreignLabels, infra: reconciled, want: false},
		{name: "no label and reconciled before", labels: nil, infra: reconciled, want: true},
		{name: "no label and never reconciled", labels: nil, infra: neverReconciled, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newGlobalSubnet(rootName, "test-ip-pool", "10.0.212.1/29", tt.labels)
			if got := isRootSubnetOwned(root, tt.infra); got != tt.want {
				t.Errorf("isRootSubnetOwned() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOwnsPropagatedSubnet(t *testing.T) {
	createdInZone := map[string]string{ipamv1.CreationOriginAnnotation: string(ipamv1.ZonalOrg)}
	tests := []struct {
		name             string
		subnet           *ipamv1.Subnet
		zoneSubnetExists bool
		zoneOwned        bool
		want             bool
	}{
		{
			name:             "copy of the owned zone subnet",
			subnet:           newZonalSubnet(rootName+"-zone1", rootName, "", nil, propagatedAnnotations),
			zoneSubnetExists: true,
			zoneOwned:        true,
			want:             true,
		},
		{
			name:             "copy without the origin annotation is recognised by its parent",
			subnet:           newZonalSubnet(rootName+"-zone1", rootName, "", nil, nil),
			zoneSubnetExists: true,
			zoneOwned:        true,
			want:             true,
		},
		{
			name:             "copy with the origin annotation does not need the parent",
			subnet:           newZonalSubnet(rootName+"-zone1", "something-else", "", nil, propagatedAnnotations),
			zoneSubnetExists: true,
			zoneOwned:        true,
			want:             true,
		},
		{
			name:             "copy of a zone subnet that is not owned",
			subnet:           newZonalSubnet(rootName+"-zone1", rootName, "", nil, propagatedAnnotations),
			zoneSubnetExists: true,
			zoneOwned:        false,
			want:             false,
		},
		{
			name:             "own label on the copy does not replace the zone subnet",
			subnet:           newZonalSubnet(rootName+"-zone1", rootName, "", ownedLabels, propagatedAnnotations),
			zoneSubnetExists: true,
			zoneOwned:        false,
			want:             false,
		},
		{
			name:             "out-of-date label on the copy of the owned zone subnet",
			subnet:           newZonalSubnet(rootName+"-zone1", rootName, "", foreignLabels, propagatedAnnotations),
			zoneSubnetExists: true,
			zoneOwned:        true,
			want:             true,
		},
		{
			name:             "subnet created in the zone itself",
			subnet:           newZonalSubnet(rootName+"-zone1", rootName, "", nil, createdInZone),
			zoneSubnetExists: true,
			zoneOwned:        true,
			want:             false,
		},
		{
			name:             "labelled subnet created in the zone itself",
			subnet:           newZonalSubnet(rootName+"-zone1", rootName, "", ownedLabels, createdInZone),
			zoneSubnetExists: true,
			zoneOwned:        true,
			want:             false,
		},
		{
			name:             "subnet without origin below another parent",
			subnet:           newZonalSubnet(rootName+"-zone1", "another-shoot-zone1", "", nil, nil),
			zoneSubnetExists: true,
			zoneOwned:        true,
			want:             false,
		},
		{
			name:             "zone subnet is gone: copy below the owned root subnet",
			subnet:           newZonalSubnet(rootName+"-zone1", rootName, "", nil, propagatedAnnotations),
			zoneSubnetExists: false,
			zoneOwned:        true,
			want:             true,
		},
		{
			name:             "zone subnet is gone: copy below another root subnet",
			subnet:           newZonalSubnet(rootName+"-zone1", "another-shoot", "", nil, propagatedAnnotations),
			zoneSubnetExists: false,
			zoneOwned:        true,
			want:             false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ownsPropagatedSubnet(tt.subnet, rootName, testProject, tt.zoneSubnetExists, tt.zoneOwned); got != tt.want {
				t.Errorf("ownsPropagatedSubnet() = %v, want %v", got, tt.want)
			}
		})
	}
}

type patchErrorClient struct {
	client.WithWatch
	err error
}

func (c *patchErrorClient) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return c.err
}

func TestClaimSubnet(t *testing.T) {
	ctx := context.Background()
	zoneSubnetName := rootName + "-zone1"

	t.Run("owned subnet is left as it is", func(t *testing.T) {
		subnet := newGlobalSubnet(zoneSubnetName, "another-parent", "", ownedLabels)
		c := &patchErrorClient{WithWatch: createFakeClient(t, []client.Object{subnet}, newOwnershipTestInfra()), err: errors.New("patch must not be called")}
		if err := claimSubnet(ctx, c, subnet, subnet.Spec.ParentReference, infraUID, rootName, testProject); err != nil {
			t.Fatalf("claimSubnet() error = %v", err)
		}
	})

	t.Run("subnet of another Infrastructure is refused", func(t *testing.T) {
		subnet := newGlobalSubnet(zoneSubnetName, rootName, "", foreignLabels)
		c := &patchErrorClient{WithWatch: createFakeClient(t, []client.Object{subnet}, newOwnershipTestInfra()), err: errors.New("patch must not be called")}
		err := claimSubnet(ctx, c, subnet, subnet.Spec.ParentReference, infraUID, rootName, testProject)
		if err == nil || !strings.Contains(err.Error(), "terminal error") || !strings.Contains(err.Error(), "occupied by another shoot") {
			t.Fatalf("claimSubnet() error = %v, want a terminal error about another shoot", err)
		}
	})

	t.Run("unlabelled subnet below another parent is refused", func(t *testing.T) {
		subnet := newGlobalSubnet(zoneSubnetName, "another-parent", "", nil)
		c := &patchErrorClient{WithWatch: createFakeClient(t, []client.Object{subnet}, newOwnershipTestInfra()), err: errors.New("patch must not be called")}
		err := claimSubnet(ctx, c, subnet, subnet.Spec.ParentReference, infraUID, rootName, testProject)
		if err == nil || !strings.Contains(err.Error(), "terminal error") || !strings.Contains(err.Error(), "is not a child of subnet") {
			t.Fatalf("claimSubnet() error = %v, want a terminal error about the parent", err)
		}
	})

	t.Run("unlabelled subnet below the expected parent is labelled", func(t *testing.T) {
		subnet := newGlobalSubnet(zoneSubnetName, rootName, "", map[string]string{"ipam.gdc.goog/vpc": "default-vpc"})
		c := createFakeClient(t, []client.Object{subnet}, newOwnershipTestInfra())
		current := &ipamglobalv1.Subnet{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(subnet), current); err != nil {
			t.Fatalf("failed to get subnet: %v", err)
		}
		if err := claimSubnet(ctx, c, current, current.Spec.ParentReference, infraUID, rootName, testProject); err != nil {
			t.Fatalf("claimSubnet() error = %v", err)
		}
		stored := &ipamglobalv1.Subnet{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(subnet), stored); err != nil {
			t.Fatalf("failed to get subnet: %v", err)
		}
		if stored.Labels[infraUIDLabel] != infraUID || stored.Labels["ipam.gdc.goog/vpc"] != "default-vpc" {
			t.Errorf("labels after claimSubnet() = %v, want the owner label next to the existing ones", stored.Labels)
		}
	})

	// Labelling is best effort: the parent already shows that the subnet belongs to
	// the Infrastructure, so no failure to write the label may block the shoot.
	for name, patchErr := range map[string]error{
		"missing permission to label does not block": apierrors.NewForbidden(schema.GroupResource{Group: "ipam.gdc.goog", Resource: "subnets"}, "z-"+zoneSubnetName, nil),
		"rejected label patch does not block":        apierrors.NewInvalid(schema.GroupKind{Group: "ipam.gdc.goog", Kind: "Subnet"}, "z-"+zoneSubnetName, nil),
		"failed label patch does not block":          errors.New("connection refused"),
	} {
		t.Run(name, func(t *testing.T) {
			subnet := newZonalSubnet("z-"+zoneSubnetName, zoneSubnetName, "", map[string]string{"ipam.gdc.goog/vpc": "default-vpc"}, nil)
			c := &patchErrorClient{WithWatch: createFakeClient(t, []client.Object{subnet}, newOwnershipTestInfra()), err: patchErr}
			if err := claimSubnet(ctx, c, subnet, subnet.Spec.ParentReference, infraUID, zoneSubnetName, testProject); err != nil {
				t.Fatalf("claimSubnet() error = %v, want nil when the label cannot be written", err)
			}
			if _, labelled := subnet.Labels[infraUIDLabel]; labelled || subnet.Labels["ipam.gdc.goog/vpc"] != "default-vpc" {
				t.Errorf("labels after a failed patch = %v, want them as they are stored", subnet.Labels)
			}
		})
	}
}

func TestNewSubnetsCarryTheOwnerLabel(t *testing.T) {
	ctx := context.Background()
	infra := newOwnershipTestInfra(v1alpha1.Zone{Name: "zone1", CIDR: "10.0.212.1/30"})
	c := createFakeClient(t, nil, infra)

	zoneSubnet := &ipamglobalv1.Subnet{ObjectMeta: metav1.ObjectMeta{Name: rootName + "-zone1", Namespace: testProject}}
	if _, err := createGlobalSubnet(ctx, c, infra, nil, &auth.ServiceAccount{Project: testProject}, zoneSubnet, ptr.To("zone1"), "10.0.212.1/30"); err != nil {
		t.Fatalf("createGlobalSubnet() error = %v", err)
	}
	storedZoneSubnet := &ipamglobalv1.Subnet{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(zoneSubnet), storedZoneSubnet); err != nil {
		t.Fatalf("failed to get zone subnet: %v", err)
	}
	if storedZoneSubnet.Labels[infraUIDLabel] != infraUID {
		t.Errorf("zone subnet labels = %v, want %s=%s", storedZoneSubnet.Labels, infraUIDLabel, infraUID)
	}

	// The fake API never reports the new subnet as ready, so the call ends with a requeue.
	// The subnet has been created by then.
	_, _ = ReconcileZonalSubnet(ctx, c, "10.0.212.1/30", "z-"+rootName+"-zone1", rootName+"-zone1", testProject, infraUID)
	storedNetworkSubnet := &ipamv1.Subnet{}
	if err := c.Get(ctx, client.ObjectKey{Name: "z-" + rootName + "-zone1", Namespace: testProject}, storedNetworkSubnet); err != nil {
		t.Fatalf("failed to get network subnet: %v", err)
	}
	if storedNetworkSubnet.Labels[infraUIDLabel] != infraUID {
		t.Errorf("network subnet labels = %v, want %s=%s", storedNetworkSubnet.Labels, infraUIDLabel, infraUID)
	}
}

// Test_Delete_SubnetOwnership covers shoots that share subnet names: a shoot with the same
// name in another garden project, and shoots whose names produce the same subnet names.
func Test_Delete_SubnetOwnership(t *testing.T) {
	const (
		zone1Subnet        = rootName + "-zone1"
		zone2Subnet        = rootName + "-zone2"
		zone1NetworkSubnet = "z-" + zone1Subnet
		zone2NetworkSubnet = "z-" + zone2Subnet
	)

	tests := []struct {
		name string
		// reconciled says whether the Infrastructure was reconciled successfully before.
		reconciled    bool
		globalObjects []client.Object
		zone1Objects  []client.Object
		zone2Objects  []client.Object
		// The maps list every subnet of the fixture and say whether it must survive Delete.
		wantGlobal map[string]bool
		wantZone1  map[string]bool
		wantZone2  map[string]bool
	}{
		{
			name: "subnets labelled for this Infrastructure are deleted",
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", ownedLabels),
				newGlobalSubnet(zone1Subnet, rootName, "", ownedLabels),
				newGlobalSubnet(zone2Subnet, rootName, "", ownedLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", ownedLabels, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", ownedLabels, nil),
			},
			wantGlobal: map[string]bool{rootName: false, zone1Subnet: false, zone2Subnet: false},
			wantZone1:  map[string]bool{zone1Subnet: false, zone1NetworkSubnet: false},
			wantZone2:  map[string]bool{zone2Subnet: false, zone2NetworkSubnet: false},
		},
		{
			name: "subnets of a shoot with the same name are left alone",
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", foreignLabels),
				newGlobalSubnet(zone1Subnet, rootName, "", foreignLabels),
				newGlobalSubnet(zone2Subnet, rootName, "", foreignLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", foreignLabels, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", foreignLabels, nil),
			},
			wantGlobal: map[string]bool{rootName: true, zone1Subnet: true, zone2Subnet: true},
			wantZone1:  map[string]bool{zone1Subnet: true, zone1NetworkSubnet: true},
			wantZone2:  map[string]bool{zone2Subnet: true, zone2NetworkSubnet: true},
		},
		{
			name: "unlabelled subnets below the root of a shoot with the same name are left alone",
			// Even an Infrastructure that was reconciled before does not own them: the root
			// subnet carries the UID of the other shoot.
			reconciled: true,
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", foreignLabels),
				newGlobalSubnet(zone1Subnet, rootName, "", nil),
				newGlobalSubnet(zone2Subnet, rootName, "", nil),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", nil, nil),
			},
			wantGlobal: map[string]bool{rootName: true, zone1Subnet: true, zone2Subnet: true},
			wantZone1:  map[string]bool{zone1Subnet: true, zone1NetworkSubnet: true},
			wantZone2:  map[string]bool{zone2Subnet: true, zone2NetworkSubnet: true},
		},
		{
			name: "unlabelled subnets are left alone when the Infrastructure was never reconciled",
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", nil),
				newGlobalSubnet(zone1Subnet, rootName, "", nil),
				newGlobalSubnet(zone2Subnet, rootName, "", nil),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", nil, nil),
			},
			wantGlobal: map[string]bool{rootName: true, zone1Subnet: true, zone2Subnet: true},
			wantZone1:  map[string]bool{zone1Subnet: true, zone1NetworkSubnet: true},
			wantZone2:  map[string]bool{zone2Subnet: true, zone2NetworkSubnet: true},
		},
		{
			name:       "unlabelled subnets of an Infrastructure that was reconciled before are deleted",
			reconciled: true,
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", nil),
				newGlobalSubnet(zone1Subnet, rootName, "", nil),
				newGlobalSubnet(zone2Subnet, rootName, "", nil),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, nil),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", nil, nil),
			},
			wantGlobal: map[string]bool{rootName: false, zone1Subnet: false, zone2Subnet: false},
			wantZone1:  map[string]bool{zone1Subnet: false, zone1NetworkSubnet: false},
			wantZone2:  map[string]bool{zone2Subnet: false, zone2NetworkSubnet: false},
		},
		{
			name: "a zone subnet name that is the root subnet of another shoot is left alone",
			// A shoot called "test-infrastructure-zone1" has a root subnet with the name of
			// this shoot's zone subnet for zone1.
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", ownedLabels),
				newGlobalSubnet(zone1Subnet, "test-ip-pool", "", foreignLabels),
				newGlobalSubnet(zone2Subnet, rootName, "", ownedLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", ownedLabels, nil),
			},
			wantGlobal: map[string]bool{rootName: false, zone1Subnet: true, zone2Subnet: false},
			wantZone1:  map[string]bool{zone1NetworkSubnet: true},
			wantZone2:  map[string]bool{zone2Subnet: false, zone2NetworkSubnet: false},
		},
		{
			name: "zonal subnets of other shoots with the expected names are left alone",
			// zone1: the name of the propagated copy is taken by a subnet that hangs below
			// another parent, and the network subnet carries the UID of another shoot.
			// zone2: the name of the propagated copy is taken by a subnet created in the zone.
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", ownedLabels),
				newGlobalSubnet(zone1Subnet, rootName, "", ownedLabels),
				newGlobalSubnet(zone2Subnet, rootName, "", ownedLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, "another-shoot-zone1", "", nil, nil),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", foreignLabels, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, map[string]string{ipamv1.CreationOriginAnnotation: string(ipamv1.ZonalOrg)}),
				newZonalSubnet(zone2NetworkSubnet, "another-parent", "", nil, nil),
			},
			wantGlobal: map[string]bool{rootName: false, zone1Subnet: false, zone2Subnet: false},
			wantZone1:  map[string]bool{zone1Subnet: true, zone1NetworkSubnet: true},
			wantZone2:  map[string]bool{zone2Subnet: true, zone2NetworkSubnet: true},
		},
		{
			name: "without a root subnet only labelled subnets and their copies are deleted",
			globalObjects: []client.Object{
				newGlobalSubnet(zone1Subnet, rootName, "", ownedLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", nil, nil),
			},
			wantGlobal: map[string]bool{zone1Subnet: false},
			wantZone1:  map[string]bool{zone1Subnet: false, zone1NetworkSubnet: false},
			wantZone2:  map[string]bool{zone2Subnet: true, zone2NetworkSubnet: true},
		},
		{
			name: "copies with an out-of-date owner label go with their zone subnet",
			// GDC gives a copy the labels its zone subnet had when it was propagated.
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", ownedLabels),
				newGlobalSubnet(zone1Subnet, rootName, "", ownedLabels),
				newGlobalSubnet(zone2Subnet, rootName, "", ownedLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", foreignLabels, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", foreignLabels, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", ownedLabels, nil),
			},
			wantGlobal: map[string]bool{rootName: false, zone1Subnet: false, zone2Subnet: false},
			wantZone1:  map[string]bool{zone1Subnet: false, zone1NetworkSubnet: false},
			wantZone2:  map[string]bool{zone2Subnet: false, zone2NetworkSubnet: false},
		},
		{
			name: "the copy of a zone subnet of another shoot is left alone even if it carries the own label",
			// The zone subnet was handed over to another shoot after it was propagated.
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", ownedLabels),
				newGlobalSubnet(zone1Subnet, rootName, "", foreignLabels),
				newGlobalSubnet(zone2Subnet, rootName, "", ownedLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", ownedLabels, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", nil, nil),
			},
			wantGlobal: map[string]bool{rootName: false, zone1Subnet: true, zone2Subnet: false},
			wantZone1:  map[string]bool{zone1Subnet: true, zone1NetworkSubnet: true},
			wantZone2:  map[string]bool{zone2Subnet: false, zone2NetworkSubnet: false},
		},
		{
			name: "an unlabelled network subnet below a zonal subnet of somebody else is left alone",
			// zone1: the name of the propagated copy is taken by a subnet created in the
			// zone, and the network subnet hangs below that subnet.
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", ownedLabels),
				newGlobalSubnet(zone1Subnet, rootName, "", ownedLabels),
				newGlobalSubnet(zone2Subnet, rootName, "", ownedLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, map[string]string{ipamv1.CreationOriginAnnotation: string(ipamv1.ZonalOrg)}),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", nil, nil),
			},
			wantGlobal: map[string]bool{rootName: false, zone1Subnet: false, zone2Subnet: false},
			wantZone1:  map[string]bool{zone1Subnet: true, zone1NetworkSubnet: true},
			wantZone2:  map[string]bool{zone2Subnet: false, zone2NetworkSubnet: false},
		},
		{
			name: "unlabelled subnets with the expected names below other parents are left alone",
			// zone1: the network subnet hangs below a subnet that is not the copy of the
			// zone subnet. zone2: the name of the zone subnet is taken by an unlabelled
			// subnet that is not a child of the root subnet.
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", ownedLabels),
				newGlobalSubnet(zone1Subnet, rootName, "", ownedLabels),
				newGlobalSubnet(zone2Subnet, "test-ip-pool", "", nil),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, "another-parent", "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", nil, nil),
			},
			wantGlobal: map[string]bool{rootName: false, zone1Subnet: false, zone2Subnet: true},
			wantZone1:  map[string]bool{zone1Subnet: false, zone1NetworkSubnet: true},
			wantZone2:  map[string]bool{zone2NetworkSubnet: true},
		},
		{
			name: "when the zone subnet is gone, a zonal subnet is its copy only below the own root subnet",
			globalObjects: []client.Object{
				newGlobalSubnet(rootName, "test-ip-pool", "", ownedLabels),
			},
			zone1Objects: []client.Object{
				newZonalSubnet(zone1Subnet, rootName, "", nil, propagatedAnnotations),
				newZonalSubnet(zone1NetworkSubnet, zone1Subnet, "", nil, nil),
			},
			zone2Objects: []client.Object{
				newZonalSubnet(zone2Subnet, "another-shoot", "", nil, propagatedAnnotations),
				newZonalSubnet(zone2NetworkSubnet, zone2Subnet, "", nil, nil),
			},
			wantGlobal: map[string]bool{rootName: false},
			wantZone1:  map[string]bool{zone1Subnet: false, zone1NetworkSubnet: false},
			wantZone2:  map[string]bool{zone2Subnet: true, zone2NetworkSubnet: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			infra := newOwnershipTestInfra(v1alpha1.Zone{Name: "zone1"}, v1alpha1.Zone{Name: "zone2"})
			if tt.reconciled {
				infra.Status.ProviderStatus = &runtime.RawExtension{Raw: []byte(`{}`)}
			}
			c := createFakeClient(t, []client.Object{newOwnershipTestSecret(), infra}, infra)
			globalClient := createFakeClient(t, tt.globalObjects, infra)
			zone1Client := createFakeClient(t, tt.zone1Objects, infra)
			zone2Client := createFakeClient(t, tt.zone2Objects, infra)
			loader := func(config *gdcclient.OrgClusterConfig, _ *auth.ServiceAccount, _ *runtime.Scheme) (client.WithWatch, error) {
				switch config.OrgClusterURL {
				case "test-global-url":
					return globalClient, nil
				case "test-zone1-url":
					return zone1Client, nil
				case "test-zone2-url":
					return zone2Client, nil
				default:
					return c, nil
				}
			}
			a := NewActuator(gdcfake.NewManager(c), withKubeClientGetter(loader))

			if err := a.Delete(ctx, logr.Discard(), infra, gdcfake.CreateClusterWithCloudProfile()); err != nil {
				t.Fatalf("actuator.Delete() error = %v", err)
			}

			assertSubnets(ctx, t, "global API", globalClient, func() client.Object { return &ipamglobalv1.Subnet{} }, tt.wantGlobal)
			assertSubnets(ctx, t, "zone1", zone1Client, func() client.Object { return &ipamv1.Subnet{} }, tt.wantZone1)
			assertSubnets(ctx, t, "zone2", zone2Client, func() client.Object { return &ipamv1.Subnet{} }, tt.wantZone2)
		})
	}
}

// assertSubnets checks for every subnet name whether the subnet still exists.
func assertSubnets(ctx context.Context, t *testing.T, api string, c client.Client, newSubnet func() client.Object, want map[string]bool) {
	t.Helper()
	for name, wantExists := range want {
		err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: testProject}, newSubnet())
		switch {
		case err != nil && !apierrors.IsNotFound(err):
			t.Errorf("%s: failed to get subnet %q: %v", api, name, err)
		case wantExists && err != nil:
			t.Errorf("%s: subnet %q was deleted, but it does not belong to this Infrastructure", api, name)
		case !wantExists && err == nil:
			t.Errorf("%s: subnet %q still exists, but it belongs to this Infrastructure", api, name)
		}
	}
}

// Test_Reconcile_SubnetOwnership covers existing subnets that carry the expected names but
// do not belong to the Infrastructure. They must neither be reused nor deleted.
func Test_Reconcile_SubnetOwnership(t *testing.T) {
	const (
		zoneSubnet    = rootName + "-zone1"
		networkSubnet = "z-" + zoneSubnet
		zoneCIDR      = "10.0.212.1/30"
		otherCIDR     = "10.0.99.1/30"
	)
	// The root subnet of the Infrastructure, as its configuration describes it.
	root := func() client.Object { return testInfraSubnet.DeepCopy() }

	tests := []struct {
		name        string
		objects     []client.Object
		wantErrCont string
		// verify runs after Reconcile, whether it failed or not.
		verify func(ctx context.Context, t *testing.T, c client.Client)
	}{
		{
			name: "zone subnet of another shoot is neither deleted nor recreated",
			objects: []client.Object{
				root(),
				newGlobalSubnet(zoneSubnet, "test-ip-pool", otherCIDR, foreignLabels),
			},
			wantErrCont: `terminal error: subnet "test-infrastructure-zone1" is occupied by another shoot`,
			verify: func(ctx context.Context, t *testing.T, c client.Client) {
				subnet := &ipamglobalv1.Subnet{}
				if err := c.Get(ctx, client.ObjectKey{Name: zoneSubnet, Namespace: testProject}, subnet); err != nil {
					t.Fatalf("zone subnet of the other shoot is gone: %v", err)
				}
				if subnet.Labels[infraUIDLabel] != otherInfraUID || ptr.Deref(subnet.Spec.IPv4Request.CIDR, "") != otherCIDR || subnet.Spec.ParentReference.Name != "test-ip-pool" {
					t.Errorf("zone subnet of the other shoot was changed: labels %v, spec %+v", subnet.Labels, subnet.Spec)
				}
			},
		},
		{
			name: "unlabelled zone subnet below another parent is neither deleted nor recreated",
			objects: []client.Object{
				root(),
				newGlobalSubnet(zoneSubnet, "another-parent", otherCIDR, nil),
			},
			wantErrCont: `terminal error: subnet "test-infrastructure-zone1" already exists and is not a child of subnet "test-infrastructure"`,
			verify: func(ctx context.Context, t *testing.T, c client.Client) {
				subnet := &ipamglobalv1.Subnet{}
				if err := c.Get(ctx, client.ObjectKey{Name: zoneSubnet, Namespace: testProject}, subnet); err != nil {
					t.Fatalf("zone subnet is gone: %v", err)
				}
				if _, labelled := subnet.Labels[infraUIDLabel]; labelled || ptr.Deref(subnet.Spec.IPv4Request.CIDR, "") != otherCIDR || subnet.Spec.ParentReference.Name != "another-parent" {
					t.Errorf("zone subnet was changed: labels %v, spec %+v", subnet.Labels, subnet.Spec)
				}
			},
		},
		{
			name: "zonal subnet with the name of the propagated copy belongs to another shoot",
			objects: []client.Object{
				root(),
				newGlobalSubnet(zoneSubnet, rootName, zoneCIDR, ownedLabels),
				newZonalSubnet(zoneSubnet, "another-shoot-zone1", zoneCIDR, nil, nil),
			},
			wantErrCont: `terminal error: zonal subnet "test-infrastructure-zone1" in zone zone1 is not the propagated copy`,
			verify: func(ctx context.Context, t *testing.T, c client.Client) {
				err := c.Get(ctx, client.ObjectKey{Name: networkSubnet, Namespace: testProject}, &ipamv1.Subnet{})
				if !apierrors.IsNotFound(err) {
					t.Errorf("network subnet must not be created below a subnet of another shoot, get returned: %v", err)
				}
			},
		},
		{
			name: "network subnet of another shoot is not reused",
			objects: []client.Object{
				root(),
				newGlobalSubnet(zoneSubnet, rootName, zoneCIDR, ownedLabels),
				newZonalSubnet(zoneSubnet, rootName, zoneCIDR, nil, propagatedAnnotations),
				newZonalSubnet(networkSubnet, zoneSubnet, zoneCIDR, foreignLabels, nil),
			},
			wantErrCont: `terminal error: subnet "z-test-infrastructure-zone1" is occupied by another shoot`,
		},
		{
			name: "unlabelled network subnet below another parent is not reused",
			objects: []client.Object{
				root(),
				newGlobalSubnet(zoneSubnet, rootName, zoneCIDR, ownedLabels),
				newZonalSubnet(zoneSubnet, rootName, zoneCIDR, nil, propagatedAnnotations),
				newZonalSubnet(networkSubnet, "another-shoot-zone1", zoneCIDR, nil, nil),
			},
			wantErrCont: `terminal error: subnet "z-test-infrastructure-zone1" already exists and is not a child of subnet "test-infrastructure-zone1"`,
		},
		{
			name: "network subnet of another shoot is not deleted when the zone CIDR changes",
			objects: []client.Object{
				root(),
				newGlobalSubnet(zoneSubnet, rootName, otherCIDR, ownedLabels),
				newZonalSubnet(zoneSubnet, rootName, otherCIDR, nil, propagatedAnnotations),
				newZonalSubnet(networkSubnet, zoneSubnet, otherCIDR, foreignLabels, nil),
			},
			wantErrCont: `terminal error: zonal subnet "z-test-infrastructure-zone1" is occupied by another shoot`,
			verify: func(ctx context.Context, t *testing.T, c client.Client) {
				if err := c.Get(ctx, client.ObjectKey{Name: networkSubnet, Namespace: testProject}, &ipamv1.Subnet{}); err != nil {
					t.Errorf("network subnet of the other shoot is gone: %v", err)
				}
				if err := c.Get(ctx, client.ObjectKey{Name: zoneSubnet, Namespace: testProject}, &ipamglobalv1.Subnet{}); err != nil {
					t.Errorf("zone subnet must not be deleted while the network subnet cannot be removed: %v", err)
				}
			},
		},
		{
			name: "unlabelled subnets below the root subnet are taken over and labelled",
			objects: []client.Object{
				root(),
				newGlobalSubnet(zoneSubnet, rootName, zoneCIDR, map[string]string{"ipam.gdc.goog/vpc": "default-vpc"}),
				newZonalSubnet(zoneSubnet, rootName, zoneCIDR, nil, propagatedAnnotations),
				newZonalSubnet(networkSubnet, zoneSubnet, zoneCIDR, map[string]string{"ipam.gdc.goog/vpc": "default-vpc"}, nil),
			},
			verify: func(ctx context.Context, t *testing.T, c client.Client) {
				zone := &ipamglobalv1.Subnet{}
				if err := c.Get(ctx, client.ObjectKey{Name: zoneSubnet, Namespace: testProject}, zone); err != nil {
					t.Fatalf("failed to get zone subnet: %v", err)
				}
				if zone.Labels[infraUIDLabel] != infraUID || zone.Labels["ipam.gdc.goog/vpc"] != "default-vpc" {
					t.Errorf("zone subnet labels = %v, want the owner label next to the existing ones", zone.Labels)
				}
				network := &ipamv1.Subnet{}
				if err := c.Get(ctx, client.ObjectKey{Name: networkSubnet, Namespace: testProject}, network); err != nil {
					t.Fatalf("failed to get network subnet: %v", err)
				}
				if network.Labels[infraUIDLabel] != infraUID || network.Labels["ipam.gdc.goog/vpc"] != "default-vpc" {
					t.Errorf("network subnet labels = %v, want the owner label next to the existing ones", network.Labels)
				}
				// The copy is managed by GDC and is not labelled by the extension.
				propagated := &ipamv1.Subnet{}
				if err := c.Get(ctx, client.ObjectKey{Name: zoneSubnet, Namespace: testProject}, propagated); err != nil {
					t.Fatalf("failed to get propagated subnet: %v", err)
				}
				if _, labelled := propagated.Labels[infraUIDLabel]; labelled {
					t.Errorf("propagated subnet labels = %v, want no owner label", propagated.Labels)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			infra := newOwnershipTestInfra(v1alpha1.Zone{Name: "zone1", CIDR: zoneCIDR})
			c := createFakeClient(t, append([]client.Object{newOwnershipTestSecret(), infra}, tt.objects...), infra)
			loader := func(*gdcclient.OrgClusterConfig, *auth.ServiceAccount, *runtime.Scheme) (client.WithWatch, error) {
				return c, nil
			}
			a := NewActuator(gdcfake.NewManager(c), withKubeClientGetter(loader))

			err := a.Reconcile(ctx, logr.Discard(), infra, reconcileTestCluster())
			switch {
			case tt.wantErrCont == "" && err != nil:
				t.Fatalf("actuator.Reconcile() error = %v", err)
			case tt.wantErrCont != "" && err == nil:
				t.Fatalf("actuator.Reconcile() error = nil, want an error containing %q", tt.wantErrCont)
			case tt.wantErrCont != "" && !strings.Contains(err.Error(), tt.wantErrCont):
				t.Fatalf("actuator.Reconcile() error = %v, want an error containing %q", err, tt.wantErrCont)
			}
			if tt.verify != nil {
				tt.verify(ctx, t, c)
			}
		})
	}
}

func reconcileTestCluster() *controller.Cluster {
	return gdcfake.CreateClusterWithCloudProfile()
}
