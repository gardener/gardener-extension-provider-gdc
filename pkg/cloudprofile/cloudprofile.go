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
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/gardener/gardener/extensions/pkg/controller"

	gdcapis "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
)

// GetFromCluster decodes cluster's providerConfig and return cloudprofile object
func GetFromCluster(cluster *controller.Cluster, decoder runtime.Decoder) (*gdcapis.CloudProfileConfig, error) {
	if cluster.CloudProfile == nil {
		return nil, fmt.Errorf("cloud profile is not set")
	}
	cloudprofile := &gdcapis.CloudProfileConfig{}
	if _, _, err := decoder.Decode(cluster.CloudProfile.Spec.ProviderConfig.Raw, nil, cloudprofile); err != nil {
		return nil, fmt.Errorf("decode cloud profile: %w", err)
	}

	return cloudprofile, nil
}
