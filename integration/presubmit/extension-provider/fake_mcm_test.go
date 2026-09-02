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
	"fmt"
	"time"

	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// SimulateMCM acts as a fake Machine Controller Manager.
// It handles:
// 1. Simulating MCM ownership logic by adding/removing finalizers on the Worker credentials Secret.
// 2. Creating MachineSets for new MachineDeployments and marking them as Ready.
// 3. Cleaning up owned MachineSets and Machines when MachineDeployments are deleted.
func SimulateMCM(ctx context.Context, c client.Client, namespace, workerName string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fmt.Println("Starting MCM simulation (Reconcile & Delete)...")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping MCM simulation")
			return
		case <-ticker.C:
			if err := runMCM(ctx, c, namespace, workerName); err != nil {
				fmt.Printf("MCM Simulation Error: %v\n", err)
			}
		}
	}
}

func runMCM(ctx context.Context, c client.Client, namespace, workerName string) error {
	mdList := &machinev1alpha1.MachineDeploymentList{}
	if err := c.List(ctx, mdList, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("listing MachineDeployments: %w", err)
	}

	for _, md := range mdList.Items {
		var workerSecretRef *corev1.SecretReference
		workerObj := &extensionsv1alpha1.Worker{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: md.Namespace, Name: workerName}, workerObj); err == nil {
			workerSecretRef = &workerObj.Spec.SecretRef
		}

		// --- DELETION HANDLING ---
		if !md.ObjectMeta.DeletionTimestamp.IsZero() {
			// 1. Delete owned MachineSets (The GC usually does this, but we simulate it)
			if err := deleteOwnedMachineSets(ctx, c, &md); err != nil {
				return fmt.Errorf("deleting MachineSets for %s: %w", md.Name, err)
			}

			// 2. Remove Finalizers from the secret associated with the Worker
			// This simulates MCM releasing the secret (as expected by actuator_delete.go L95-L99)
			if workerSecretRef != nil {
				secret := &corev1.Secret{}
				if err := c.Get(ctx, client.ObjectKey{Namespace: workerSecretRef.Namespace, Name: workerSecretRef.Name}, secret); err == nil {
					if controllerutil.ContainsFinalizer(secret, "machine.sapcloud.io/machine-controller-manager") || controllerutil.ContainsFinalizer(secret, "machine.sapcloud.io/machine-controller-manager-provider") {
						patch := client.MergeFrom(secret.DeepCopy())
						controllerutil.RemoveFinalizer(secret, "machine.sapcloud.io/machine-controller-manager")
						controllerutil.RemoveFinalizer(secret, "machine.sapcloud.io/machine-controller-manager-provider")
						if err := c.Patch(ctx, secret, patch); err != nil {
							return fmt.Errorf("removing finalizer from Worker secret %s: %w", secret.Name, err)
						}
						fmt.Printf("[MCM Sim] Removed finalizers from Worker secret %s\n", secret.Name)
					}
				}
			}

			// 3. Remove Finalizers (MCM adds them, so we might need to remove them if we added any)
			// Even if acts as a "dumb" controller, ensuring finalizers are gone allows k8s to remove the object.
			if len(md.Finalizers) > 0 {
				patch := client.MergeFrom(md.DeepCopy())
				md.Finalizers = nil
				if err := c.Patch(ctx, &md, patch); err != nil {
					fmt.Printf("[MCM Sim Warning] Failed removing finalizers from %s: %v\n", md.Name, err)
					continue
				} else {
					fmt.Printf("[MCM Sim] Removed finalizers from terminating %s\n", md.Name)
				}
			}
			continue
		}

		// --- CREATION / UPDATE HANDLING ---

		// 1. Add Finalizer to MachineDeployment
		if !controllerutil.ContainsFinalizer(&md, "machine.sapcloud.io/machine-controller-manager") {
			patch := client.MergeFrom(md.DeepCopy())
			controllerutil.AddFinalizer(&md, "machine.sapcloud.io/machine-controller-manager")
			if err := c.Patch(ctx, &md, patch); err != nil {
				return err
			}
		}

		// 2. Add Finalizer to Worker Secret
		if workerSecretRef != nil {
			secret := &corev1.Secret{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: workerSecretRef.Namespace, Name: workerSecretRef.Name}, secret); err == nil {
				if !controllerutil.ContainsFinalizer(secret, "machine.sapcloud.io/machine-controller-manager") {
					patch := client.MergeFrom(secret.DeepCopy())
					controllerutil.AddFinalizer(secret, "machine.sapcloud.io/machine-controller-manager")
					if err := c.Patch(ctx, secret, patch); err != nil {
						return fmt.Errorf("adding finalizer to Worker secret %s: %w", secret.Name, err)
					}
				}
			}
		}

		// 3. Ensure a MachineSet exists
		if err := ensureMachineSetForAll(ctx, c, &md); err != nil {
			return fmt.Errorf("ensuring MachineSet for %s: %w", md.Name, err)
		}

		// 4. Update Status to Ready
		if err := updateMachineDeploymentStatus(ctx, c, &md); err != nil {
			return fmt.Errorf("updating status for %s: %w", md.Name, err)
		}
	}
	return nil
}

func ensureMachineSetForAll(ctx context.Context, c client.Client, md *machinev1alpha1.MachineDeployment) error {
	msList := &machinev1alpha1.MachineSetList{}
	if err := c.List(ctx, msList, client.InNamespace(md.Namespace), client.MatchingLabels(md.Spec.Template.Labels)); err != nil {
		return err
	}

	// MachineSet already exists
	if len(msList.Items) > 0 {
		return nil
	}

	// Create a dummy MachineSet
	ms := &machinev1alpha1.MachineSet{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName:    md.Name + "-",
			Namespace:       md.Namespace,
			Labels:          md.Spec.Template.Labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(md, machinev1alpha1.SchemeGroupVersion.WithKind("MachineDeployment"))},
		},
		Spec: machinev1alpha1.MachineSetSpec{
			Replicas: md.Spec.Replicas,
			Template: md.Spec.Template,
		},
	}

	if err := c.Create(ctx, ms); err != nil {
		return err
	}
	fmt.Printf("[MCM] Created MachineSet for %s\n", md.Name)
	return nil
}

func updateMachineDeploymentStatus(ctx context.Context, c client.Client, md *machinev1alpha1.MachineDeployment) error {
	if md.Status.ObservedGeneration == md.Generation &&
		md.Status.AvailableReplicas == md.Spec.Replicas &&
		md.Status.UpdatedReplicas == md.Spec.Replicas &&
		isConditionTrue(md.Status.Conditions, machinev1alpha1.MachineDeploymentAvailable) {
		return nil
	}

	patch := client.MergeFrom(md.DeepCopy())

	md.Status.ObservedGeneration = md.Generation
	md.Status.Replicas = md.Spec.Replicas
	md.Status.UpdatedReplicas = md.Spec.Replicas
	md.Status.AvailableReplicas = md.Spec.Replicas
	md.Status.ReadyReplicas = md.Spec.Replicas

	// Ensure the Available condition is set
	availableCond := machinev1alpha1.MachineDeploymentCondition{
		Type:               machinev1alpha1.MachineDeploymentAvailable,
		Status:             machinev1alpha1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "MCM_Simulation",
		Message:            "Simulated by integration test",
	}
	upsertCondition(&md.Status.Conditions, availableCond)

	if err := c.Status().Patch(ctx, md, patch); err != nil {
		return err
	}
	fmt.Printf("[MCM] Updated status for %s\n", md.Name)
	return nil
}

func isConditionTrue(conditions []machinev1alpha1.MachineDeploymentCondition, condType machinev1alpha1.MachineDeploymentConditionType) bool {
	for _, c := range conditions {
		if c.Type == condType && c.Status == machinev1alpha1.ConditionTrue {
			return true
		}
	}
	return false
}

func upsertCondition(conditions *[]machinev1alpha1.MachineDeploymentCondition, newCond machinev1alpha1.MachineDeploymentCondition) {
	for i, c := range *conditions {
		if c.Type == newCond.Type {
			(*conditions)[i] = newCond
			return
		}
	}
	*conditions = append(*conditions, newCond)
}

func deleteOwnedMachineSets(ctx context.Context, c client.Client, md *machinev1alpha1.MachineDeployment) error {
	msList := &machinev1alpha1.MachineSetList{}
	if err := c.List(ctx, msList, client.InNamespace(md.Namespace), client.MatchingLabels(md.Spec.Template.Labels)); err != nil {
		return err
	}

	for _, ms := range msList.Items {
		if ms.DeletionTimestamp.IsZero() {
			if err := c.Delete(ctx, &ms); err != nil {
				return err
			}
			fmt.Printf("[MCM] Deleted MachineSet %s\n", ms.Name)
		}

		// Also clean up Machines (simplification: assume 1-1 mapping or just delete all matching)
		if err := deleteMachinesForSet(ctx, c, &ms); err != nil {
			return err
		}

		// Remove Finalizers from MachineSet to let it disappear
		if len(ms.Finalizers) > 0 {
			patch := client.MergeFrom(ms.DeepCopy())
			ms.Finalizers = nil
			if err := c.Patch(ctx, &ms, patch); err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteMachinesForSet(ctx context.Context, c client.Client, ms *machinev1alpha1.MachineSet) error {
	mList := &machinev1alpha1.MachineList{}
	if err := c.List(ctx, mList, client.InNamespace(ms.Namespace), client.MatchingLabels(ms.Spec.Template.Labels)); err != nil {
		return err
	}
	for _, m := range mList.Items {
		if len(m.Finalizers) > 0 {
			patch := client.MergeFrom(m.DeepCopy())
			m.Finalizers = nil
			if err := c.Patch(ctx, &m, patch); err != nil {
				return err
			}
		}
		if m.DeletionTimestamp.IsZero() {
			if err := c.Delete(ctx, &m); err != nil {
				return err
			}
		}
	}
	return nil
}
