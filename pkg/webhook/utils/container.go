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

package utils

import (
	corev1 "k8s.io/api/core/v1"
)

// EnsureSELinuxSPCT ensures specific containers have privilage to access the host linux system with SELinux enabled
func EnsureSELinuxSPCT(c *corev1.Container) {
	if c.SecurityContext == nil {
		c.SecurityContext = &corev1.SecurityContext{}
	}
	if c.SecurityContext.SELinuxOptions == nil {
		c.SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{}
	}
	c.SecurityContext.SELinuxOptions.Type = "spc_t"
}
