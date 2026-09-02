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

package constants

const (
	// WorkloadLabelSelectorKey is the label selector key for CloudNATGateway and shoot worker machine.
	WorkloadLabelSelectorKey = "provider.extensions.gardener.gdc.goog/cluster"

	// ServiceAccountJSONField is the field in a secret where the service account JSON is stored at.
	ServiceAccountJSONField = "serviceaccount.json"

	// GDCHConfigJSONField is the field in a secret where the GDCH configuration is stored at.
	GDCHConfigJSONField = "gdch-config"
)
