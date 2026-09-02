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
	"context"

	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"

	"github.com/gardener/gardener/extensions/pkg/controller/infrastructure"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
)

var (
	// DefaultAddOptions are the default AddOptions for AddToManager.
	DefaultAddOptions = AddOptions{}
)

// AddOptions are options to apply when adding the GDCH infrastructure controller to the manager.
type AddOptions struct {
	// Controller are the controller.Options.
	Controller controller.Options
	// IgnoreOperationAnnotation specifies whether to ignore the operation annotation or not.
	// please refer here for more info: https://github.com/gardener/gardener/blob/01fe7a739054a677ae299664195a72bdd7c3853b/extensions/pkg/predicate/default.go#L30
	IgnoreOperationAnnotation bool
	// ExtensionClasses defines the extension classes this extension is responsible for.
	ExtensionClasses []extensionsv1alpha1.ExtensionClass
}

// addToManagerWithOptions adds a controller with the given AddOptions to the given manager.
// The opts.Reconciler is being set with a newly instantiated actuator.
func addToManagerWithOptions(ctx context.Context, mgr manager.Manager, options AddOptions) error {
	return infrastructure.Add(mgr, infrastructure.AddArgs{
		Actuator:          NewActuator(mgr),
		ConfigValidator:   NewConfigValidator(mgr, log.Log),
		ControllerOptions: options.Controller,
		Predicates:        infrastructure.DefaultPredicates(ctx, mgr, options.IgnoreOperationAnnotation),
		Type:              gdc.Type,
		ExtensionClasses:  options.ExtensionClasses,
	})
}

// AddToManager adds a controller with the default AddOptions.
func AddToManager(ctx context.Context, mgr manager.Manager) error {
	return addToManagerWithOptions(ctx, mgr, DefaultAddOptions)
}
