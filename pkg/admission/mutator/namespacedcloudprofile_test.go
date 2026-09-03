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

package mutator

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gardener/gardener/pkg/apis/core"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	gocmp "github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	fakemanager "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

var (
	// new "machine-img-2" in spec and spec.providerConfig
	validNCPNewImage = buildNCP(
		[]gardencorev1beta1.MachineImage{buildv1beta1MachineImage("machine-img-2", "1.2.3")},
		[]apisgdc.MachineImages{buildGDCHMachineImage("machine-img-2", "1.2.3")},
		[]gardencorev1beta1.MachineImage{buildv1beta1MachineImage("machine-img-1", "1.2.3")},
		[]apisgdc.MachineImages{buildGDCHMachineImage("machine-img-1", "1.2.3")},
		false,
	)

	validNCPExistingImage = buildNCP(
		[]gardencorev1beta1.MachineImage{buildv1beta1MachineImage("machine-img-1", "1.2.4")},
		[]apisgdc.MachineImages{buildGDCHMachineImage("machine-img-1", "1.2.4")},
		nil,
		[]apisgdc.MachineImages{buildGDCHMachineImage("machine-img-1", "1.2.3")},
		true,
	)
	expirationTime = time.Now().AddDate(1, 0, 0)
)

func TestNamespacedcloudprofileMutateSuccess(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = gardencorev1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)
	validNCPNewImageMutatedStatus := validNCPNewImage.Status.DeepCopy()
	validNCPNewImageMutatedStatus.CloudProfileSpec.ProviderConfig = buildMergedProviderConfig(
		[]apisgdc.MachineImages{
			buildGDCHMachineImage("machine-img-1", "1.2.3"),
			buildGDCHMachineImage("machine-img-2", "1.2.3"),
		},
	)
	validNCPExistingImageMutatedStatus := validNCPExistingImage.Status.DeepCopy()
	validNCPExistingImageMutatedStatus.CloudProfileSpec.ProviderConfig = buildMergedProviderConfig(
		[]apisgdc.MachineImages{
			buildGDCHMachineImage("machine-img-1", "1.2.3"),
			buildGDCHMachineImage("machine-img-1", "1.2.4"),
		},
	)
	validNCPExistingImageMutatedStatus.CloudProfileSpec.Kubernetes = gardencorev1beta1.KubernetesSettings{
		Versions: []gardencorev1beta1.ExpirableVersion{
			buildKuberneteSettings("1.23.4"),
		},
	}

	tests := []struct {
		name      string
		newObj    *gardencorev1beta1.NamespacedCloudProfile
		expectObj *gardencorev1beta1.NamespacedCloudProfileStatus
	}{
		{
			name:      "mutate successfully with new image",
			newObj:    validNCPNewImage,
			expectObj: validNCPNewImageMutatedStatus,
		},
		{
			name:      "mutate successfully with existing image",
			newObj:    validNCPExistingImage,
			expectObj: validNCPExistingImageMutatedStatus,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			err := NewNamespacedCloudProfileMutator(fakemanager.NewManager(c)).Mutate(context.Background(), tt.newObj, nil)
			if err != nil {
				t.Fatalf("mutator.mutator() error = %v", err.Error())
			}
			if diff := gocmp.Diff(tt.expectObj, &tt.newObj.Status); diff != "" {
				t.Fatalf("Mutate() Status.CloudProfileSpec fields mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNamespacedcloudprofileMutateError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = core.AddToScheme(scheme)
	_ = gardencorev1beta1.AddToScheme(scheme)
	_ = apisgdc.AddToScheme(scheme)
	ncpInvalidSpecProviderConfig := validNCPNewImage.DeepCopy()
	ncpInvalidSpecProviderConfig.Spec.ProviderConfig = &runtime.RawExtension{
		Raw: []byte("this is not valid json"),
	}
	ncpInvalidStatusProviderConfig := validNCPNewImage.DeepCopy()
	ncpInvalidStatusProviderConfig.Status.CloudProfileSpec.ProviderConfig = &runtime.RawExtension{
		Raw: []byte("this is not valid json"),
	}
	ncpWithDeletionTimestamp := validNCPNewImage.DeepCopy()
	ncpWithDeletionTimestamp.DeletionTimestamp = &metav1.Time{Time: time.Now()}

	tests := []struct {
		name      string
		newObj    client.Object
		expectErr error
	}{
		{
			name: "wrong object type",
			newObj: &core.NamespacedCloudProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-ncp",
					Namespace: "test-project",
				},
			},
			expectErr: fmt.Errorf("wrong object type %T", &core.NamespacedCloudProfile{}),
		},
		{
			name:      "invalid spec providerConfig JSON",
			newObj:    ncpInvalidSpecProviderConfig,
			expectErr: fmt.Errorf("could not decode providerConfig of namespacedCloudProfile spec"),
		},
		{
			name:      "invalid status providerConfig JSON",
			newObj:    ncpInvalidStatusProviderConfig,
			expectErr: fmt.Errorf("could not decode providerConfig of namespacedCloudProfile status"),
		},
		{
			name:      "should not mutate if deletion timestamp is set",
			newObj:    ncpWithDeletionTimestamp,
			expectErr: nil, // Expect no error
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			err := NewNamespacedCloudProfileMutator(fakemanager.NewManager(c)).Mutate(context.Background(), tt.newObj, nil)
			originalObject := tt.newObj.DeepCopyObject()
			if tt.expectErr != nil {
				if err == nil {
					t.Fatalf("Expected error %q but got none", tt.expectErr)
				}
				if errors.Is(err, tt.expectErr) || (err != nil && !strings.Contains(err.Error(), tt.expectErr.Error())) {
					t.Fatalf("Expect error = %v, but got = %v", tt.expectErr, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, but got: %v", err)
				}
				if diff := gocmp.Diff(originalObject, tt.newObj); diff != "" {
					t.Errorf("Mutate() unexpectedly changed the object when no error was expected (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func encode(obj runtime.Object) []byte {
	data, _ := json.Marshal(obj)
	return data
}

func buildGDCHMachineImage(name, version string) apisgdc.MachineImages {
	return apisgdc.MachineImages{
		Name: name,
		Versions: []apisgdc.MachineImageVersion{
			{
				Version:      version,
				Image:        "path/to/gdch/image",
				Architecture: ptr.To("amd64"),
			},
		},
	}
}

func buildv1beta1MachineImage(name, version string) gardencorev1beta1.MachineImage {
	return gardencorev1beta1.MachineImage{
		Name: name,
		Versions: []gardencorev1beta1.MachineImageVersion{
			{
				ExpirableVersion: gardencorev1beta1.ExpirableVersion{Version: version},
			},
		},
	}
}

func buildProviderConfig(machineImages []apisgdc.MachineImages) *runtime.RawExtension {
	return &runtime.RawExtension{
		Raw: encode(&apisgdc.CloudProfileConfig{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "gdch.provider.extensions.gardener.gdc.goog/__internal",
				Kind:       "CloudProfileConfig",
			},
			MachineImages: machineImages,
			OrgConfig: &apisgdc.OrgConfig{
				GlobalManagementAPI: "test-global-api",
				Zones: []*apisgdc.ZoneEndpoints{
					{
						Name:              "test-region-1-zone-1",
						ManagementAPI:     "test-management-api",
						InfrastructureAPI: "test-infra-api",
					},
				},
				RegistryURL: "test-registry-url",
				CAData:      "test-ca-data",
			},
		}),
	}
}

// buildMergedProviderConfig dynamically generates a provider config by merging multiple
// machine image lists. If images have the same name, their versions are combined.
func buildMergedProviderConfig(imageLists ...[]apisgdc.MachineImages) *runtime.RawExtension {
	merged := make(map[string]apisgdc.MachineImages)

	for _, imageList := range imageLists {
		for _, image := range imageList {
			existing, ok := merged[image.Name]
			if ok {
				existing.Versions = append(existing.Versions, image.Versions...)
				merged[image.Name] = existing
			} else {
				merged[image.Name] = image
			}
		}
	}

	// Convert map back to slice for consistent ordering in tests.
	finalList := make([]apisgdc.MachineImages, 0, len(merged))
	for _, image := range merged {
		finalList = append(finalList, image)
	}
	slices.SortFunc(finalList, func(a, b apisgdc.MachineImages) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return buildProviderConfig(finalList)
}

func buildKuberneteSettings(version string) gardencorev1beta1.ExpirableVersion {
	return gardencorev1beta1.ExpirableVersion{
		Version: version,
		ExpirationDate: &metav1.Time{
			Time: expirationTime,
		},
	}
}

func buildNCP(specMachineImages []gardencorev1beta1.MachineImage, specProviderMachineImages []apisgdc.MachineImages, statusMachineImages []gardencorev1beta1.MachineImage, statusProviderMachineImages []apisgdc.MachineImages, withStatusKubernetes bool) *gardencorev1beta1.NamespacedCloudProfile {
	ncp := &gardencorev1beta1.NamespacedCloudProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-ncp",
			Namespace: "test-project",
		},
		Spec: gardencorev1beta1.NamespacedCloudProfileSpec{
			Parent: gardencorev1beta1.CloudProfileReference{
				Kind: "CloudProfile",
				Name: "test-cloud-profile",
			},
			ProviderConfig: buildMergedProviderConfig(specProviderMachineImages),
			MachineImages:  specMachineImages,
			Kubernetes: &gardencorev1beta1.KubernetesSettings{
				Versions: []gardencorev1beta1.ExpirableVersion{
					buildKuberneteSettings("1.23.4"),
				},
			},
		},
		Status: gardencorev1beta1.NamespacedCloudProfileStatus{
			CloudProfileSpec: gardencorev1beta1.CloudProfileSpec{
				MachineImages:  statusMachineImages,
				ProviderConfig: buildMergedProviderConfig(statusProviderMachineImages),
			},
		},
	}
	if withStatusKubernetes {
		ncp.Status.CloudProfileSpec.Kubernetes = gardencorev1beta1.KubernetesSettings{
			Versions: []gardencorev1beta1.ExpirableVersion{
				buildKuberneteSettings("1.23.4"),
			},
		}
	}
	return ncp
}
