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

package kubeconfig

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
)

func TestOrgInfrastuctureCluster(t *testing.T) {
	caData := []byte("fake-ca-data")
	projectName := "test-project"
	serverURL := "https://server-url"
	tokenURI := "https://token-uri"
	saName := "test-service-account"
	privateKeyID := "test-private-key-id"
	privateKey := "fake-private-key"

	orgInfrastructureConfig := &client.OrgClusterConfig{
		OrgClusterURL: serverURL,
		CAData:        base64.StdEncoding.EncodeToString(caData),
	}

	serviceAccount := &auth.ServiceAccount{
		Name:         saName,
		Project:      projectName,
		TokenURI:     tokenURI,
		PrivateKeyID: privateKeyID,
		PrivateKey:   privateKey,
	}

	kubeconfigRaw, err := Raw(orgInfrastructureConfig, serviceAccount)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	//Parse to validate the Kubeconfig format
	got, err := clientcmd.Load([]byte(kubeconfigRaw))
	if err != nil {
		t.Errorf("failed to load config: %v", err)
	}

	saJSON, err := json.Marshal(serviceAccount)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	want := &api.Config{
		AuthInfos: map[string]*api.AuthInfo{
			saName: {
				Exec: &api.ExecConfig{
					APIVersion: "client.authentication.k8s.io/v1beta1",
					Args: []string{
						"--audience=" + serverURL,
						"--key-string=" + base64.StdEncoding.EncodeToString(saJSON),
						"--ca-cert=" + base64.RawStdEncoding.EncodeToString(caData),
					},
					Command:         "/gdch-sa-auth-plugin",
					InteractiveMode: api.IfAvailableExecInteractiveMode,
				},
				Extensions: map[string]runtime.Object{},
			},
		},
		Clusters: map[string]*api.Cluster{
			"cluster-name": {
				CertificateAuthorityData: caData,
				Extensions:               map[string]runtime.Object{},
				Server:                   serverURL,
			},
		},
		Contexts: map[string]*api.Context{
			fmt.Sprintf("%s@cluster-name", saName): {
				AuthInfo:   saName,
				Cluster:    "cluster-name",
				Extensions: map[string]runtime.Object{},
			},
		},
		CurrentContext: fmt.Sprintf("%s@cluster-name", saName),
		Extensions:     map[string]runtime.Object{},
		Preferences: api.Preferences{
			Extensions: nil,
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected diff (-want +got):\n%s", diff)
	}
}
