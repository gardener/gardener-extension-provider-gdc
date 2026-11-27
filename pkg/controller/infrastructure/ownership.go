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
	"fmt"
	"maps"

	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"
	ipamglobalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/ipam/v1"
	ipamv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/ipam/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The subnets of an Infrastructure are named after the shoot: the root subnet
// "<shoot>", one zone subnet "<shoot>-<zone>" per zone in the global API, the
// copy of the zone subnet that GDC propagates into the zone, and the network
// subnet "z-<shoot>-<zone>" in the zone. Shoots of different garden projects
// can have the same name and use the same GDC project, and the names of
// different shoots can collide ("<shoot>-<zone>" is also the root subnet of a
// shoot with that name). A name therefore says nothing about who a subnet
// belongs to. Before a subnet is reused, changed or deleted, the functions in
// this file establish that it belongs to the Infrastructure at hand.

// infraUIDLabel records the owner of a subnet. Its value is the UID of the
// Infrastructure object the subnet was created for.
const infraUIDLabel = "infra-uid"

type subnetOwner int

const (
	// ownerUnknown means that the subnet does not carry the owner label.
	ownerUnknown subnetOwner = iota
	// ownerSelf means that the subnet is labelled with the UID of this Infrastructure.
	ownerSelf
	// ownerOther means that the subnet is labelled with the UID of another Infrastructure.
	ownerOther
)

// labelledOwner reports what the owner label of a subnet says.
func labelledOwner(labels map[string]string, infraUID string) subnetOwner {
	uid, ok := labels[infraUIDLabel]
	switch {
	case !ok:
		return ownerUnknown
	case uid == infraUID:
		return ownerSelf
	default:
		return ownerOther
	}
}

// referencesSubnet reports whether ref points at the subnet called name in
// project. A reference without a namespace points into the namespace of the
// subnet that holds it, which is always project here.
func referencesSubnet(ref *ipamv1.SubnetReference, name, project string) bool {
	if ref == nil || ref.Name != name {
		return false
	}
	if ref.Type != "" && ref.Type != ipamv1.SingleSubnet {
		return false
	}
	return ref.Namespace == nil || *ref.Namespace == "" || *ref.Namespace == project
}

// isRootSubnetOwned reports whether the root subnet belongs to the
// Infrastructure. It is the rule reconcileRootSubnet applies before it adopts a
// root subnet: the owner label decides, and a subnet without the label, which
// predates the label, belongs only to an Infrastructure that was reconciled
// successfully before.
func isRootSubnetOwned(root *ipamglobalv1.Subnet, infrastructure *extensionsv1alpha1.Infrastructure) bool {
	switch labelledOwner(root.Labels, string(infrastructure.UID)) {
	case ownerSelf:
		return true
	case ownerOther:
		return false
	default:
		return infrastructure.Status.ProviderStatus != nil
	}
}

// ownsChildSubnet reports whether a subnet below the root subnet belongs to the
// Infrastructure. The owner label decides. A subnet without the label belongs
// to the Infrastructure only if its parent is the subnet called parentName and
// that parent is known to be owned.
func ownsChildSubnet(labels map[string]string, parent *ipamv1.SubnetReference, infraUID, parentName, project string, parentOwned bool) bool {
	switch labelledOwner(labels, infraUID) {
	case ownerSelf:
		return true
	case ownerOther:
		return false
	default:
		return parentOwned && referencesSubnet(parent, parentName, project)
	}
}

// ownsPropagatedSubnet reports whether a zonal subnet is the copy that GDC
// propagated from the zone subnet of the Infrastructure. The extension does not
// create this copy. GDC gives it the labels the zone subnet had at that moment,
// so the owner label of a copy can be missing or out of date and is not
// consulted: the zone subnet decides. zoneSubnetExists tells whether the zone
// subnet still exists, and zoneOwned whether it is owned or, if it is gone,
// whether the root subnet is. The copy of an existing zone subnet is recognised
// by the origin that GDC records on it: a zonal subnet that came from the global
// API under this name can only be the copy of the zone subnet with this name.
// Where no origin is recorded, and when the zone subnet is gone, the copy is
// recognised by its parent, the root subnet called rootName.
func ownsPropagatedSubnet(subnet *ipamv1.Subnet, rootName, project string, zoneSubnetExists, zoneOwned bool) bool {
	if !zoneOwned {
		return false
	}
	origin, recorded := subnet.Annotations[ipamv1.CreationOriginAnnotation]
	if recorded && origin != string(ipamv1.GlobalOrg) {
		// Created in the zone itself, so it is not the copy of a global subnet.
		return false
	}
	if recorded && zoneSubnetExists {
		return true
	}
	return referencesSubnet(subnet.Spec.ParentReference, rootName, project)
}

// claimSubnet makes sure that an existing subnet belongs to the Infrastructure
// before the caller uses, changes or deletes it. A subnet that carries the UID
// of another Infrastructure is refused. A subnet without the owner label is
// taken over only if its parent is the subnet called parentName, which the
// caller has established as its own, and is then labelled. Labelling is best
// effort: the parent already shows whom the subnet belongs to.
func claimSubnet(ctx context.Context, c client.Client, subnet client.Object, parent *ipamv1.SubnetReference, infraUID, parentName, project string) error {
	switch labelledOwner(subnet.GetLabels(), infraUID) {
	case ownerSelf:
		return nil
	case ownerOther:
		return fmt.Errorf("terminal error: subnet %q is occupied by another shoot. Please use a different shoot name to fix", subnet.GetName())
	}
	if !referencesSubnet(parent, parentName, project) {
		return fmt.Errorf("terminal error: subnet %q already exists and is not a child of subnet %q, so it does not belong to this shoot. Please use a different shoot name, or label the subnet with %s=%s to hand it over to this shoot", subnet.GetName(), parentName, infraUIDLabel, infraUID)
	}

	unlabelled := subnet.GetLabels()
	patch := client.MergeFrom(subnet.DeepCopyObject().(client.Object))
	labels := maps.Clone(unlabelled)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[infraUIDLabel] = infraUID
	subnet.SetLabels(labels)
	if err := c.Patch(ctx, subnet, patch); err != nil {
		// The label only makes explicit what the parent already shows. If it cannot
		// be written, for example because the service account may not patch subnets,
		// that must not block a shoot that worked before. The next reconciliation
		// tries again.
		subnet.SetLabels(unlabelled)
		logr.FromContextOrDiscard(ctx).Info("Could not label subnet with its owner, continuing without the label", "subnet", subnet.GetName(), "reason", err.Error())
	}
	return nil
}

// getSubnet reads the subnet called name into obj and reports whether it exists.
func getSubnet(ctx context.Context, c client.Client, namespace, name string, obj client.Object) (bool, error) {
	if err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("unable to get subnet %q: %w", name, err)
	}
	return true, nil
}
