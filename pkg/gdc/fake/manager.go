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

package fake

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FakeManager is a fake manager that embeds the ctrl.Manager interface.
// It is used for testing purposes and provides access to the given client and scheme.
type Manager struct {
	ctrl.Manager
	client client.Client
}

// NewManager return a manager w/ given client.
func NewManager(c client.Client) *Manager {
	return &Manager{client: c}
}

// GetClient returns the client instance.
func (m *Manager) GetClient() client.Client {
	return m.client
}

// GetScheme returns the Scheme associated with the client field.
func (m *Manager) GetScheme() *runtime.Scheme {
	return m.client.Scheme()
}

// GetRESTMapper returns the RESTMapper associated with the client field.
func (m *Manager) GetRESTMapper() meta.RESTMapper {
	return m.client.RESTMapper()
}

func (m *Manager) GetConfig() *rest.Config {
	return &rest.Config{}
}
