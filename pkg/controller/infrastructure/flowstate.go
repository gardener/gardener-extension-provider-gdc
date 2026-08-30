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

package infrastructure

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/v1alpha1"
)

// As we will use flow to manage the Infrastructure reconcillation, FlowState is used as
// the kind of infrastructurestatus.state.
const (
	// FlowStateKind is the kind used for the FlowState type.
	FlowStateKind = "FlowState"
)

var (
	// SchemeGroupVersion is the SchemeGroupVersion for use with the FlowState object.
	SchemeGroupVersion = v1alpha1.SchemeGroupVersion
)

// FlowState stores information about the infrastructure state for use with the FlowReconciler.
type FlowState struct {
	metav1.TypeMeta
	Data map[string]string `json:"data"`
}

// NewFlowState creates a new FlowState object.
func NewFlowState() *FlowState {
	return &FlowState{
		TypeMeta: metav1.TypeMeta{
			Kind:       FlowStateKind,
			APIVersion: SchemeGroupVersion.String(),
		},
		Data: map[string]string{},
	}
}

// ToJSON marshals state as JSON
func (f *FlowState) ToJSON() ([]byte, error) {
	return json.Marshal(f)
}
