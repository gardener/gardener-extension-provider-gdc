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

package shootservice

import (
	"context"
	"fmt"
	"net"
	"strings"

	extensionswebhook "github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	lbTypeAnnotationKey                  = "networking.gke.io/load-balancer-type"
	lbInternal                           = "internal"
	lbExternal                           = "external"
	internalLBSubnetAnnotationKey        = "networking.gke.io/load-balancer-subnet"
	externalLBIPAddressesAnnotationKey   = "networking.gke.io/load-balancer-ip-addresses"
	internalLBAllowProjectsAnnotationKey = "networking.gke.io/load-balancer-allow-projects"
)

type mutator struct {
	logger logr.Logger
}

// NewShootServiceWebhook creates a new webhook that validates and mutates(if needed) shoot cluster LoadBalancer services.
func NewShootServiceWebhook() extensionswebhook.Mutator {
	return &mutator{
		logger: log.Log.WithName("gdc-shoot-service-mutator"),
	}
}

func (m *mutator) Mutate(ctx context.Context, newObj, _ client.Object) error {
	service, ok := newObj.(*corev1.Service)
	if !ok {
		return fmt.Errorf("could not mutate: object is not of type corev1.Service")
	}

	// If the object does have a deletion timestamp, skip it
	if service.GetDeletionTimestamp() != nil {
		return nil
	}
	extensionswebhook.LogMutation(m.logger, service.Kind, service.Namespace, service.Name)

	if service.Spec.Type != corev1.ServiceTypeLoadBalancer {
		return nil
	}

	return validateService(service)
}

func validateService(svc *corev1.Service) error {
	internalVal, internalOk := svc.Annotations[internalLBSubnetAnnotationKey]
	externalVal, externalOk := svc.Annotations[externalLBIPAddressesAnnotationKey]
	svcType := svc.Annotations[lbTypeAnnotationKey]
	allowProjectsVal, allowProjectsOk := svc.Annotations[internalLBAllowProjectsAnnotationKey]

	if allowProjectsOk {
		if allowProjectsVal == "" {
			return fmt.Errorf("service %s/%s: annotation %q value cannot be empty",
				svc.Namespace, svc.Name, internalLBAllowProjectsAnnotationKey)
		}

		projects := strings.Split(allowProjectsVal, ",")
		projectMap := make(map[string]struct{})
		var cleanedProjects []string
		hasWildcard := false

		for _, p := range projects {
			trimmed := strings.TrimSpace(p)
			if trimmed == "" {
				return fmt.Errorf("service %s/%s: annotation %q contains empty project name",
					svc.Namespace, svc.Name, internalLBAllowProjectsAnnotationKey)
			}
			if trimmed == "*" {
				hasWildcard = true
				continue
			}

			if _, exists := projectMap[trimmed]; !exists {
				projectMap[trimmed] = struct{}{}
				cleanedProjects = append(cleanedProjects, trimmed)
				if errs := validation.IsDNS1035Label(trimmed); len(errs) > 0 {
					return fmt.Errorf("service %s/%s: annotation %q contains invalid project name %q: %v",
						svc.Namespace, svc.Name, internalLBAllowProjectsAnnotationKey, trimmed, errs)
				}
			}
		}

		newVal := strings.Join(cleanedProjects, ",")
		if hasWildcard {
			newVal = "*"
		}

		if newVal != allowProjectsVal {
			svc.Annotations[internalLBAllowProjectsAnnotationKey] = newVal
		}
	}

	switch svcType {
	case lbExternal, "":
		if allowProjectsOk {
			return fmt.Errorf("external LB service %s/%s must not have a load-balancer-allow-projects annotation %q",
				svc.Namespace, svc.Name, internalLBAllowProjectsAnnotationKey)
		}
		if internalOk {
			return fmt.Errorf("external LB service %s/%s must not have an internal subnet annotation %q",
				svc.Namespace, svc.Name, internalLBSubnetAnnotationKey)
		}
		if externalOk && net.ParseIP(externalVal) != nil {
			return fmt.Errorf("external LB service %s/%s: annotation %q value must be a subnet name, not an IP address: got %q",
				svc.Namespace, svc.Name, externalLBIPAddressesAnnotationKey, externalVal)
		}

	case lbInternal:
		if externalOk {
			return fmt.Errorf("internal LB service %s/%s: must not have an external IP annotation %q",
				svc.Namespace, svc.Name, externalLBIPAddressesAnnotationKey)
		}

		if internalOk && net.ParseIP(internalVal) != nil {
			return fmt.Errorf("internal LB service %s/%s: annotation %q value must be a subnet name, not an IP address: got %q",
				svc.Namespace, svc.Name, internalLBSubnetAnnotationKey, internalVal)
		}

	default:
		return fmt.Errorf("service %s/%s: unsupported LBType %q. Supported values are %q or %q",
			svc.Namespace, svc.Name, svcType, lbInternal, lbExternal)
	}

	return nil
}
