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

package keys

// JSONCredentials is the definition of the credential JSON file.
type JSONCredentials struct {
	Type          string `json:"type"` // is credential type
	FormatVersion string `json:"format_version"`
	Project       string `json:"project"` // refers to the project namespace in the organization
	PrivateKeyID  string `json:"private_key_id"`
	PrivateKey    string `json:"private_key"`
	Name          string `json:"name"` // is the name of the service identity
	CaCertPath    string `json:"ca_cert_path,omitempty"`
	TokenURI      string `json:"token_uri"` // is the address of the authentication endpoint
}
