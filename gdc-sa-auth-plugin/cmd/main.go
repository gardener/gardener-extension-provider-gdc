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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"golang.org/x/oauth2"

	klog "k8s.io/klog/v2"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/iam/keys"
)

const (
	audienceFlag  = "audience"
	keyFileFlag   = "key-file"
	keyStringFlag = "key-string"
	caCertFlag    = "ca-cert"
)

func main() {
	var audience string
	var keyFile string
	var keyString string
	var caCert string

	cmd := &cobra.Command{
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(audience) == 0 {
				return fmt.Errorf("'--%s' must be specified", audienceFlag)
			}
			if len(keyFile) == 0 && len(keyString) == 0 {
				return fmt.Errorf("'--%s' or '--%s' must be specified", keyFileFlag, keyStringFlag)
			}
			if len(keyFile) > 0 && len(keyString) > 0 {
				return fmt.Errorf("'--%s' and '--%s' can not be specified at the same time", keyFileFlag, keyStringFlag)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			jc, err := readJSONCredentials(keyFile, keyString)
			if err != nil {
				klog.Fatal(err)
			}

			if len(caCert) > 0 && len(jc.CaCertPath) > 0 {
				klog.Fatalf("'--%s' and CaCertPath field in the privateKeyJson can not be specified at the same time", caCertFlag)
			}

			var opts []auth.Option
			if jc.CaCertPath != "" {
				caDataBytes, err := readFile(jc.CaCertPath)
				if err != nil {
					klog.Fatal(err)
				}
				opts = append(opts, auth.WithCACert(caDataBytes))
			} else if caCert != "" {
				caCertBytes, err := base64.StdEncoding.DecodeString(caCert)
				if err != nil {
					klog.Fatal(err)
				}
				opts = append(opts, auth.WithCACert(caCertBytes))
			}

			sa := &auth.ServiceAccount{
				Name:         jc.Name,
				PrivateKey:   jc.PrivateKey,
				PrivateKeyID: jc.PrivateKeyID,
				Project:      jc.Project,
				TokenURI:     jc.TokenURI,
			}

			stsTS := auth.NewSTSTokenSource(audience, sa, opts...)
			cw := &credentialWriter{
				tokenSource: stsTS,
				writer:      os.Stdout,
			}

			// Write the credential to STDOUT which will be consumed by kubectl:
			// https://github.com/kubernetes/enhancements/blob/master/keps/sig-auth/541-external-credential-providers/README.md#provider-output-format
			if err := cw.Write(); err != nil {
				klog.Fatalf("Failed to write credential to STDOUT: %v", err)
			}
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&audience, audienceFlag, "", "Audience")
	flags.StringVar(&keyFile, keyFileFlag, "", "Path to the priavte key file")
	flags.StringVar(&keyString, keyStringFlag, "", "Base64 encoding of the private key")
	flags.StringVar(&caCert, caCertFlag, "", "Base64 encoding of Certificate Authority Data")

	if err := cmd.Execute(); err != nil {
		klog.Fatal(err)
	}
}

type credentialWriter struct {
	tokenSource oauth2.TokenSource
	writer      io.Writer
}

func (cw *credentialWriter) Write() error {
	token, err := cw.tokenSource.Token()
	if err != nil {
		return fmt.Errorf("get a token: %w", err)
	}

	expiry := &metav1.Time{Time: token.Expiry}
	ec := v1beta1.ExecCredential{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "client.authentication.k8s.io/v1beta1",
			Kind:       "ExecCredential",
		},
		Spec: v1beta1.ExecCredentialSpec{
			// stdin is not passed to the exec plugin
			Interactive: false,
		},
		Status: &v1beta1.ExecCredentialStatus{
			ExpirationTimestamp: expiry,
			Token:               token.AccessToken,
		},
	}

	jsonBytes, err := json.MarshalIndent(ec, "", "  ")
	if err != nil {
		klog.Fatal("json.MarshalIndent(): %w", err)
	}
	fmt.Fprintf(cw.writer, "%s\n", jsonBytes)

	return nil
}

func readFile(path string) ([]byte, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file %q: %w", path, err)
	}
	return bytes, nil
}

func readJSONCredentials(path string, keyString string) (*keys.JSONCredentials, error) {
	var bytes []byte
	var err error
	if len(path) == 0 {
		bytes, err = base64.StdEncoding.DecodeString(keyString)
		if err != nil {
			return nil, fmt.Errorf("base64 decode a keyString: %w", err)
		}
	} else {
		bytes, err = readFile(path)
		if err != nil {
			return nil, err
		}
	}

	jc := &keys.JSONCredentials{}
	if err := json.Unmarshal(bytes, jc); err != nil {
		return nil, fmt.Errorf("unmarshal file content to 'JSONCredentials': %w", err)
	}

	return jc, nil
}
