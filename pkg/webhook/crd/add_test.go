// Copyright 2026 Google LLC
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

package crd

import (
	"testing"

	"github.com/Masterminds/semver/v3"
)

func TestGetMutatorForVersion(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		expectedNil bool
	}{
		{
			name:        "version < 1.32",
			version:     "1.30.0",
			expectedNil: false,
		},
		{
			name:        "GKE version < 1.32 with build metadata",
			version:     "1.30.12-gke.300",
			expectedNil: false,
		},
		{
			name:        "version == 1.32.0",
			version:     "1.32.0",
			expectedNil: true,
		},
		{
			name:        "GKE version 1.32 with build metadata",
			version:     "1.32.0-gke.300",
			expectedNil: true,
		},
		{
			name:        "version > 1.32",
			version:     "1.33.0",
			expectedNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, err := semver.NewVersion(tc.version)
			if err != nil {
				t.Fatalf("failed to parse version %s: %v", tc.version, err)
			}

			m := getMutatorForVersion(v)

			isNil := m == nil
			if isNil != tc.expectedNil {
				t.Errorf("Expected isNil to be %t, but got %t", tc.expectedNil, isNil)
			}
		})
	}
}
