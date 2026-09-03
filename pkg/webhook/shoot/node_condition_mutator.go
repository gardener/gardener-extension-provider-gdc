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

package shoot

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

const (
	noRouteCreated          = "NoRouteCreated"
	nodeCreatedWithoutRoute = "Node created without a route"
)

func (m *mutator) mutateNetworkUnavailableNodeCondition(_ context.Context, newObj *corev1.Node, oldObj *corev1.Node, logMutation func()) error {
	if newObj == nil || oldObj == nil {
		return nil
	}
	for i, c := range newObj.Status.Conditions {
		isNeworkUnavailable := c.Type == corev1.NodeNetworkUnavailable
		isNoRouteCreated := c.Reason == noRouteCreated && c.Message == nodeCreatedWithoutRoute
		if isNeworkUnavailable && c.Status == corev1.ConditionTrue && isNoRouteCreated {
			logMutation()
			for _, oldCondition := range oldObj.Status.Conditions {
				if oldCondition.Type == corev1.NodeNetworkUnavailable && oldCondition.Status == corev1.ConditionFalse {
					newObj.Status.Conditions[i] = oldCondition
					return nil
				}
			}
			// Did not find the condition in the old object => remove the condition
			newObj.Status.Conditions = append(newObj.Status.Conditions[:i], newObj.Status.Conditions[i+1:]...)
			return nil
		}
	}

	return nil
}
