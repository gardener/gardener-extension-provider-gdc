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

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
)

// Raw returns a kubeconfig string of a service account against an org cluster of GDCH.
func Raw(
	config *client.OrgClusterConfig,
	serviceAccount *auth.ServiceAccount,
) (string, error) {
	cluster := api.NewCluster()
	cluster.Server = config.OrgClusterURL
	var err error
	cluster.CertificateAuthorityData, err = base64.StdEncoding.DecodeString(config.CAData)
	if err != nil {
		return "", fmt.Errorf("failed to decode caData: %w", err)
	}

	serviceAccountName := serviceAccount.Name
	clusterName := "cluster-name"
	context := api.NewContext()
	context.Cluster = clusterName
	context.AuthInfo = serviceAccountName
	contextName := fmt.Sprintf("%s@%s", serviceAccountName, clusterName)

	serviceAccountJson, err := json.Marshal(serviceAccount)
	if err != nil {
		return "", fmt.Errorf("failed to decode serviceAccount: %w", err)
	}

	encodedprivateKey := base64.StdEncoding.EncodeToString(serviceAccountJson)
	authInfo := api.NewAuthInfo()
	authInfo.Exec = &api.ExecConfig{
		APIVersion: "client.authentication.k8s.io/v1beta1",
		Command:    "/gdch-sa-auth-plugin",
		Args: []string{
			fmt.Sprintf("--audience=%s", config.OrgClusterURL),
			fmt.Sprintf("--key-string=%s", encodedprivateKey),
			fmt.Sprintf("--ca-cert=%s", config.CAData),
		},
	}

	kubeconfig := api.NewConfig()
	kubeconfig.Clusters[clusterName] = cluster
	kubeconfig.Contexts[contextName] = context
	kubeconfig.AuthInfos[serviceAccount.Name] = authInfo
	kubeconfig.CurrentContext = contextName
	kubeconfigBytes, err := clientcmd.Write(*kubeconfig)
	if err != nil {
		return "", err
	}

	return string(kubeconfigBytes), nil
}
