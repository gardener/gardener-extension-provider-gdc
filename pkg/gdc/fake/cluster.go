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

package fake

import (
	"encoding/json"

	"github.com/gardener/gardener/extensions/pkg/controller"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	"github.com/gardener/gardener/pkg/extensions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	apisgdc "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

func encode(obj runtime.Object) []byte {
	data, _ := json.Marshal(obj)
	return data
}

func CreateClusterWithCloudProfile() *controller.Cluster {
	return &extensions.Cluster{
		Seed: &gardencorev1beta1.Seed{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-seed",
			},
		},
		CloudProfile: &gardencorev1beta1.CloudProfile{
			Spec: gardencorev1beta1.CloudProfileSpec{
				ProviderConfig: &runtime.RawExtension{
					Raw: encode(&apisgdc.CloudProfileConfig{
						TypeMeta: metav1.TypeMeta{
							APIVersion: apisgdc.SchemeGroupVersion.String(),
							Kind:       "CloudProfileConfig",
						},
						OrgConfig: &apisgdc.OrgConfig{
							OrgName:             "test-org",
							GlobalManagementAPI: "test-global-url",
							RegistryURL:         "test-registry-url",
							CAData:              "test-ca-data",
							Zones: []*apisgdc.ZoneEndpoints{
								{
									Name:              "zone1",
									ManagementAPI:     "test-zone1-url",
									InfrastructureAPI: "test-infra-url",
								},
								{
									Name:              "zone2",
									ManagementAPI:     "test-zone2-url",
									InfrastructureAPI: "test-infra-url",
								},
							},
						},
					}),
				},
			},
		},
	}
}
