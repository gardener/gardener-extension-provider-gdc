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

package cloudprofile

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/gardener/gardener/extensions/pkg/controller"
	"github.com/gardener/gardener/pkg/apis/core/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	gdcapis "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

func Test_GetFromCluster_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(gdcapis.AddToScheme(scheme))
	codecFactory := serializer.NewCodecFactory(scheme)
	decoder := codecFactory.UniversalDecoder()

	tests := []struct {
		name          string
		cluster       *controller.Cluster
		want          *gdcapis.CloudProfileConfig
		expectedError string
	}{
		{
			name: "success - correctly decode cloudprofile in lancer",
			cluster: &controller.Cluster{
				CloudProfile: &v1beta1.CloudProfile{
					Spec: v1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: encode(t, &gdcapis.CloudProfileConfig{
								TypeMeta: metav1.TypeMeta{
									APIVersion: gdcapis.SchemeGroupVersion.String(),
									Kind:       "CloudProfileConfig",
								},
								OrgConfig: &gdcapis.OrgConfig{
									OrgName:             "test-org",
									GlobalManagementAPI: "test-global-url",
									RegistryURL:         "test-registry-url",
									CAData:              "test-ca-data",
									Zones: []*gdcapis.ZoneEndpoints{
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
			},
			want: &gdcapis.CloudProfileConfig{
				TypeMeta: metav1.TypeMeta{
					APIVersion: gdcapis.SchemeGroupVersion.String(),
					Kind:       "CloudProfileConfig",
				},
				OrgConfig: &gdcapis.OrgConfig{
					OrgName:             "test-org",
					GlobalManagementAPI: "test-global-url",
					RegistryURL:         "test-registry-url",
					CAData:              "test-ca-data",
					Zones: []*gdcapis.ZoneEndpoints{
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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetFromCluster(tt.cluster, decoder)
			if err != nil {
				t.Fatalf("GetFromCluster() unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFromCluster() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_GetFromCluster_Error(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(gdcapis.AddToScheme(scheme))
	codecFactory := serializer.NewCodecFactory(scheme)
	decoder := codecFactory.UniversalDecoder()

	tests := []struct {
		name          string
		cluster       *controller.Cluster
		expectedError string
	}{
		{
			name: "no cloud profile",
			cluster: &controller.Cluster{
				CloudProfile: nil,
			},
			expectedError: "cloud profile is not set",
		},
		{
			name: "invalid cloud profile config",
			cluster: &controller.Cluster{
				CloudProfile: &v1beta1.CloudProfile{
					Spec: v1beta1.CloudProfileSpec{
						ProviderConfig: &runtime.RawExtension{
							Raw: []byte("invalid"),
						},
					},
				},
			},
			expectedError: "decode cloud profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := GetFromCluster(tt.cluster, decoder)
			if err == nil {
				t.Fatalf("Test %s expected an error but got no error", tt.name)
			}
			if tt.expectedError != "" && !checkError(err, tt.expectedError) {
				t.Fatalf("Test %s expected error %q, but got %q", tt.name, tt.expectedError, err.Error())
			}
		})
	}
}

func encode(t *testing.T, obj runtime.Object) []byte {
	t.Helper()
	data, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Failed to marshal object: %v", err)
	}
	return data
}

func checkError(err error, expectedError string) bool {
	return err != nil && (err.Error() == expectedError || strings.Contains(err.Error(), expectedError))
}
