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
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/gardener/gardener/pkg/component/autoscaling/vpa"
	"github.com/gardener/gardener/pkg/component/etcd/etcd"
	"github.com/gardener/gardener/pkg/component/extensions/crds"
	"github.com/gardener/gardener/pkg/component/nodemanagement/machinecontrollermanager"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func installCRDs(t *testing.T, ctx context.Context, vucClient client.WithWatch, k8sVersion *semver.Version) {

	// Deploy VPA CRDs and register clean up
	vpaCRDsDeployWaiter, err := vpa.NewCRD(vucClient, nil)
	if err != nil {
		t.Fatalf("error creating deployWaiter for VPA: %v", err)
	}
	if err := vpaCRDsDeployWaiter.Deploy(ctx); err != nil {
		t.Fatalf("cannot deploy VPA CRDs %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up VPA CRDs")
		if err := vpaCRDsDeployWaiter.Destroy(ctx); err != nil {
			t.Logf("unable to clean up VPA CRDs %v", err)
		}
	})

	// Deploy Extension CRDs and register clean up
	extCRDsDeployWaiter, err := crds.NewCRD(vucClient, true, true)
	if err != nil {
		t.Fatalf("error creating deployWaiter for Extension CRDs: %v", err)
	}
	if err := extCRDsDeployWaiter.Deploy(ctx); err != nil {
		t.Fatalf("cannot deploy Extension CRDs %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up Extension CRDs")
		if err := extCRDsDeployWaiter.Destroy(ctx); err != nil {
			t.Logf("unable to clean up Extension CRDs %v", err)
		}
	})

	// Deploy ETCD CRDs and register clean up
	etcdCRDsDeployWaiter, err := etcd.NewCRD(vucClient, k8sVersion)
	if err != nil {
		t.Fatalf("error creating deployWaiter for ETCD CRDs: %v", err)
	}
	if err := etcdCRDsDeployWaiter.Deploy(ctx); err != nil {
		t.Fatalf("cannot deploy ETCD CRDs %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up ETCD CRDs")
		if err := etcdCRDsDeployWaiter.Destroy(ctx); err != nil {
			t.Logf("unable to clean up ETCD CRDs %v", err)
		}
	})

	// Deploy Machine CRDs and register clean up
	machineCRDsDeployWaiter, err := machinecontrollermanager.NewCRD(vucClient)
	if err != nil {
		t.Fatalf("error creating deployWaiter for Machine CRDs: %v", err)
	}
	if err := machineCRDsDeployWaiter.Deploy(ctx); err != nil {
		t.Fatalf("cannot deploy Machine CRDs %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up Machine CRDs")
		if err := machineCRDsDeployWaiter.Destroy(ctx); err != nil {
			t.Logf("unable to clean up Machine CRDs %v", err)
		}
	})
}
