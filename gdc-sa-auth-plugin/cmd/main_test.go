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
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/oauth2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCredentialWriter(t *testing.T) {
	token := "fake-token"
	expiry := time.Now().Add(5 * time.Second)
	mts := &mockTokenSource{
		expiry: expiry,
		token:  token,
	}

	bb := &bytes.Buffer{}
	w := io.Writer(bb)

	cw := &credentialWriter{
		tokenSource: mts,
		writer:      w,
	}

	if err := cw.Write(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ts, err := metav1.Time{Time: expiry}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal JSON: %s", err)
	}
	expected := fmt.Sprintf(`{
  "kind": "ExecCredential",
  "apiVersion": "client.authentication.k8s.io/v1beta1",
  "spec": {
    "interactive": false
  },
  "status": {
    "expirationTimestamp": %s,
    "token": "fake-token"
  }
}
`, ts)

	if diff := cmp.Diff(expected, bb.String()); diff != "" {
		t.Errorf("unexpected output (-want +got):\n%s", diff)
	}
}

type mockTokenSource struct {
	expiry time.Time
	token  string
}

func (m *mockTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{
		AccessToken: m.token,
		Expiry:      m.expiry,
	}, nil
}
