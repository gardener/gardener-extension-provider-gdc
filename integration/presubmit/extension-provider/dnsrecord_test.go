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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	globalnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/networking/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/kubernetes"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/controller/dnsrecord"
)

const testIP = "192.0.2.1"

type dnsrecordControllerFixture struct {
	*commonTestFixture
	secretName    string
	domainName    string
	dnsRecordName string
	timeout       time.Duration
	interval      time.Duration
	ip            string
}

func setupDNSRecordFixture(t *testing.T, common *commonTestFixture) *dnsrecordControllerFixture {
	t.Helper()
	t.Log("Setting up DNSRecord fixture")
	secretName := "dnsrecord-secret-" + *commitHash
	domainName := fmt.Sprintf("api.presubmit-dns.%s.%s", *commitHash, *managedDNSZone)
	dnsRecordName := "dnsrecord-" + *commitHash
	// Same SLO as agtest
	timeout := 5 * time.Minute
	interval := 5 * time.Second

	f := &dnsrecordControllerFixture{
		commonTestFixture: common,
		secretName:        secretName,
		domainName:        domainName,
		dnsRecordName:     dnsRecordName,
		timeout:           timeout,
		interval:          interval,
		ip:                testIP,
	}

	// Create a dedicated, isolated vcluster client for this subtest
	f.vucClient = f.NewVClusterClient(t)
	return f
}

func (f *dnsrecordControllerFixture) test(t *testing.T) {
	t.Logf("Running dnsrecord-controller presubmit test for commit %q", *commitHash)
	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()

	// Create DNSRecord Secret
	f.createDNSRecordSecret(t, ctx)

	// Create DNSRecord and Wait for Ready
	f.createAndVerifyDNSRecord(ctx, t)

	// Verify RRS Existence
	f.waitForRRSReady(ctx, t)

	// Delete DNSRecord and Verify Cleanup
	f.deleteAndVerifyCleanup(ctx, t)
}

func (f *dnsrecordControllerFixture) waitForRRSReady(ctx context.Context, t *testing.T) {
	rrsName := dnsrecord.GetDNSRecordSetName("A", f.domainName)
	t.Logf("Waiting for resourceRecordSet to be ready. Name: %s", rrsName)

	rrsKey := client.ObjectKey{
		Name:      rrsName,
		Namespace: *project,
	}
	err := wait.PollUntilContextTimeout(ctx, f.interval, f.timeout, true, func(ctx context.Context) (bool, error) {
		rrs := &globalnetworkingv1.ResourceRecordSet{}
		err := f.globalClient.Get(ctx, rrsKey, rrs)

		if err != nil {
			// If not found, continue polling until timeout
			return false, client.IgnoreNotFound(err)
		}

		// RRS found, verify the data is correct
		if len(rrs.Spec.RRData) > 0 && rrs.Spec.RRData[0] == f.ip {
			if len(rrs.Status.Zones) == 0 {
				t.Logf("Waiting: RRS has no status zones yet")
				return false, nil
			}

			for _, zone := range rrs.Status.Zones {
				// Replica Ready
				isReady := false
				for _, cond := range zone.ReplicaStatus.Conditions {
					if cond.Type == "Ready" && cond.Status == "True" {
						isReady = true
						break
					}
				}
				if !isReady {
					t.Logf("Waiting: RRS Zone %s is not Ready", zone.Name)
					return false, nil
				}

				// Rollout Synced
				isSynced := false
				for _, cond := range zone.RolloutStatus.Conditions {
					if cond.Type == "Synced" && cond.Status == "True" {
						isSynced = true
						break
					}
				}
				if !isSynced {
					t.Logf("Waiting: RRS Zone %s is not Synced", zone.Name)
					return false, nil
				}
			}
			return true, nil
		}

		// If RRS exists but IP is wrong (stale data), continue polling
		t.Logf("RRS exist but IP mismatch, want: %s, got: %v", f.ip, rrs.Spec.RRData)
		return false, nil
	})

	if err != nil {
		t.Fatalf("Timed out waiting for ResourceRecordSet %s to exist with IP %s: %v", rrsName, f.ip, err)
	}

	t.Logf("Success: ResourceRecordSet %v is ready", rrsName)
}

func (f *dnsrecordControllerFixture) waitForRRSDeleted(ctx context.Context, t *testing.T) {
	rrsName := dnsrecord.GetDNSRecordSetName("A", f.domainName)
	t.Logf("Waiting for resourceRecordSet to be deleted, Name: %s", rrsName)

	rrsKey := client.ObjectKey{
		Name:      rrsName,
		Namespace: *project,
	}
	err := wait.PollUntilContextTimeout(ctx, f.interval, f.timeout, true, func(ctx context.Context) (bool, error) {
		rrs := &globalnetworkingv1.ResourceRecordSet{}
		err := f.globalClient.Get(ctx, rrsKey, rrs)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		// RRS still exists, continue polling
		return false, nil
	})
	if err != nil {
		t.Fatalf("Timed out waiting for ResourceRecordSet %s to be deleted: %v", rrsName, err)
	}
	t.Logf("Success: ResourceRecordSet verified deleted")
	return
}

func (f *dnsrecordControllerFixture) createAndVerifyDNSRecord(ctx context.Context, t *testing.T) {
	dnsrecord := &extensionsv1alpha1.DNSRecord{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.dnsRecordName,
			Namespace: f.namespace,
		},
		Spec: extensionsv1alpha1.DNSRecordSpec{
			DefaultSpec: extensionsv1alpha1.DefaultSpec{
				Type: "gdch-dns",
			},
			Name: f.domainName,
			SecretRef: corev1.SecretReference{
				Name:      f.secretName,
				Namespace: f.namespace,
			},
			RecordType: extensionsv1alpha1.DNSRecordTypeA,
			Values:     []string{f.ip},
		},
	}

	t.Logf("Creating DNSRecord: %s", f.dnsRecordName)
	if err := f.vucClient.Create(ctx, dnsrecord); err != nil {
		t.Fatalf("Failed to create DNSRecord: %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleanup: Ensuring DNSRecord %s is deleted", f.dnsRecordName)
		cleanupContext, cancel := context.WithTimeout(context.Background(), f.timeout)
		defer cancel()
		err := f.vucClient.Delete(cleanupContext, dnsrecord)
		if client.IgnoreNotFound(err) != nil {
			t.Logf("Warning: Failed to clean up DNSRecord: %v", err)
		}

		// Wait until DNSRecord is cleaned up to prevent leaks.
		// Note: f.deleteAndVerifyCleanup already has a wait loop, but it only runs when the whole test succeeds.
		// This t.Cleanup block is executed on test failures (e.g. fatal assertion), and without this synchronous
		// wait, the subsequent teardown of the test environment would immediately kill the extension controller,
		// leaving orphaned GDCH resources behind.
		if err := wait.PollUntilContextTimeout(cleanupContext, f.interval, f.timeout, true, func(ctx context.Context) (bool, error) {
			obj := &extensionsv1alpha1.DNSRecord{}
			err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(dnsrecord), obj)
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				return false, err
			}
			return false, nil
		}); err != nil {
			t.Errorf("error waiting for DNSRecord object to be deleted: %v", err)
		}
	})

	listOptions := []client.ListOption{
		client.InNamespace(f.namespace),
	}

	err := kubernetes.WaitForCondition(
		ctx,
		f.timeout,
		func() (watch.Interface, error) {
			return f.vucClient.Watch(ctx, &extensionsv1alpha1.DNSRecordList{}, listOptions...)
		},
		func(obj *extensionsv1alpha1.DNSRecord) bool {
			if obj.Name != f.dnsRecordName {
				return false
			}
			if obj.Status.LastOperation != nil &&
				obj.Status.LastOperation.State == "Succeeded" {
				return true
			}
			if obj.Status.LastOperation != nil && obj.Status.LastOperation.State == "Error" {
				t.Logf("Warning: DNSRecord %s is in Error state: %s", obj.Name, obj.Status.LastOperation.Description)
			}

			return false
		},
	)
	if err != nil {
		t.Fatalf("Failed to create DNSRecord: %v", err)
	}
}

// deleteAndVerifyCleanup delete DNSRecord then verify DNSRecord and ResourceRecordSet do not exist
func (f *dnsrecordControllerFixture) deleteAndVerifyCleanup(ctx context.Context, t *testing.T) {
	t.Logf("Deleting DNSRecord: %s", f.dnsRecordName)

	dnsrecord := &extensionsv1alpha1.DNSRecord{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.dnsRecordName,
			Namespace: f.namespace,
		},
	}

	if err := f.vucClient.Delete(ctx, dnsrecord); err != nil {
		if !apierrors.IsNotFound(err) {
			t.Fatalf("Failed to delete DNSRecord: %v", err)
		}
	}

	// Wait until DNSRecord is cleaned up
	err := wait.PollUntilContextTimeout(ctx, f.interval, f.timeout, true, func(ctx context.Context) (bool, error) {
		obj := &extensionsv1alpha1.DNSRecord{}
		err := f.vucClient.Get(ctx, client.ObjectKeyFromObject(dnsrecord), obj)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})

	if err != nil {
		t.Fatalf("Timed out waiting for DNSRecord k8s object to be deleted: %v", err)
	}

	f.waitForRRSDeleted(ctx, t)
}

func (f *dnsrecordControllerFixture) createDNSRecordSecret(t *testing.T, ctx context.Context) {
	configBytes, err := json.Marshal(f.commonTestFixture.gdchConfig)
	if err != nil {
		t.Fatalf("failed to marshal gdch config: %v", err)
	}

	saContent, err := os.ReadFile(*safile)
	if err != nil {
		t.Fatalf("failed to read service account file at %s: %v", *safile, err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.secretName,
			Namespace: f.commonTestFixture.namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"gdch-config":         configBytes,
			"serviceaccount.json": saContent,
		},
	}

	t.Logf("Creating secret: %s/%s", f.commonTestFixture.namespace, secret.Name)
	err = f.commonTestFixture.vucClient.Create(ctx, secret)
	if err != nil {
		t.Fatalf("failed to create secret: %v", err)
	}

	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), f.timeout)
		defer cancel()
		if err := f.commonTestFixture.vucClient.Delete(cleanupContext, secret); err != nil {
			t.Logf("Warning: Failed to clean up DNSRecord secret: %v", err)
		}
	})
}
