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

package validator

import (
	"context"
	"fmt"
	"net"
	"reflect"

	"github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/gardener/gardener/pkg/api/core/helper"
	"github.com/gardener/gardener/pkg/apis/core"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	"github.com/gardener/gardener/pkg/utils/gardener"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/validation"
)

var (
	specPath                 = field.NewPath("spec")
	networkPath              = specPath.Child("networking")
	providerPath             = specPath.Child("provider")
	infrastructureConfigPath = providerPath.Child("infrastructureConfig")
	workersPath              = providerPath.Child("workers")
)

// shoot implements the webhookextension.Validator
// https://github.com/gardener/gardener/extensions/pkg/webhook/validator.go
type shoot struct {
	client         client.Client
	decoder        runtime.Decoder
	lenientDecoder runtime.Decoder
}

// NewShootValidator returns a new instance of a shoot validator.
func NewShootValidator(mgr manager.Manager) webhook.Validator {
	return &shoot{
		client:         mgr.GetClient(),
		decoder:        serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder(),
		lenientDecoder: serializer.NewCodecFactory(mgr.GetScheme()).UniversalDecoder(),
	}
}

// Validate validates the given shoot objects.
func (s *shoot) Validate(ctx context.Context, newObj, oldObj client.Object) error {
	shoot, ok := newObj.(*core.Shoot)
	if !ok {
		return fmt.Errorf("wrong object type %T", newObj)
	}

	// Skip if it's a workerless Shoot
	if helper.IsWorkerless(shoot) {
		return nil
	}

	if oldObj != nil {
		oldShoot, ok := oldObj.(*core.Shoot)
		if !ok {
			return fmt.Errorf("wrong object type %T for old object", oldObj)
		}
		return s.validateUpdate(ctx, oldShoot, shoot)
	}

	return s.validateCreate(ctx, shoot)
}

// getAllowedZonesFromCloudProfile fetches the set of allowed regions from the Cloud Profile.
func getAllowedZonesFromCloudProfile(cloudProfile *gardencorev1beta1.CloudProfile) map[string]bool {
	allowedZones := map[string]bool{}
	for _, region := range cloudProfile.Spec.Regions {
		for _, zone := range region.Zones {
			allowedZones[zone.Name] = true
		}
	}

	return allowedZones
}

func (s *shoot) validateContext(ctx context.Context, valContext *validationContext) field.ErrorList {
	allErrors := field.ErrorList{}
	cloudprofileZones := getAllowedZonesFromCloudProfile(valContext.cloudProfile)
	if valContext.shoot.Spec.Networking != nil {
		allErrors = append(allErrors, validation.ValidateNetworking(valContext.shoot.Spec.Networking, networkPath)...)
		allErrors = append(allErrors, validation.ValidateInfrastructureConfig(valContext.infrastructureConfig, cloudprofileZones, valContext.shoot.Spec.Networking.Nodes, infrastructureConfigPath)...)
		if err := s.validateNodeCIDRNotOverlap(ctx, valContext.infrastructureConfig, valContext.shoot); err != nil {
			allErrors = append(allErrors, err)
		}
	}
	allErrors = append(allErrors, validation.ValidateWorkers(valContext.shoot.Spec.Provider.Workers, cloudprofileZones, valContext.infrastructureConfig, workersPath)...)

	for i, worker := range valContext.shoot.Spec.Provider.Workers {
		if worker.ProviderConfig != nil {
			workerConfig, err := DecodeWorkerConfig(s.decoder, worker.ProviderConfig)
			if err != nil {
				allErrors = append(allErrors, field.Invalid(workersPath.Index(i).Child("providerConfig"), worker.ProviderConfig, fmt.Sprintf("unable to decode worker providerConfig: %v", err)))
			} else if workerConfig != nil {
				allErrors = append(allErrors, validation.ValidateWorkerConfig(workerConfig, workersPath.Index(i).Child("providerConfig"))...)
			}
		}
	}

	return allErrors
}

func (s *shoot) validateCreate(ctx context.Context, shoot *core.Shoot) error {
	validationContext, err := newValidationContext(ctx, s.decoder, s.client, shoot)
	if err != nil {
		return err
	}

	return s.validateContext(ctx, validationContext).ToAggregate()
}

func (s *shoot) validateUpdate(ctx context.Context, oldShoot, currentShoot *core.Shoot) error {
	oldValContext, err := newValidationContext(ctx, s.lenientDecoder, s.client, oldShoot)
	if err != nil {
		return err
	}

	currentValContext, err := newValidationContext(ctx, s.decoder, s.client, currentShoot)
	if err != nil {
		return err
	}

	if errList := s.validateContext(ctx, currentValContext); len(errList) > 0 {
		return errList.ToAggregate()
	}

	allErrors := field.ErrorList{}

	var (
		oldInfrastructureConfig, currentInfrastructureConfig = oldValContext.infrastructureConfig, currentValContext.infrastructureConfig
	)

	if !reflect.DeepEqual(oldInfrastructureConfig, currentInfrastructureConfig) {
		allErrors = append(allErrors, validation.ValidateInfrastructureConfigUpdate(oldInfrastructureConfig, currentInfrastructureConfig, infrastructureConfigPath)...)
	}

	// no rules enforced for worker update

	return allErrors.ToAggregate()
}

func (s *shoot) validateNodeCIDRNotOverlap(ctx context.Context, infrastructureConfig *gdc.InfrastructureConfig, shoot *core.Shoot) *field.Error {
	shootList := &gardencorev1beta1.ShootList{}
	if err := s.client.List(ctx, shootList); err != nil {
		return field.InternalError(infrastructureConfigPath.Child("nodecidr"), fmt.Errorf("error listing shoot CRs: %v", err))
	}

	_, currentNodeCIDR, err := net.ParseCIDR(infrastructureConfig.Networks.NodeCIDR)
	if err != nil {
		return field.Invalid(infrastructureConfigPath.Child("nodecidr"), currentNodeCIDR.String(), fmt.Sprintf("error parsing infraConfig NodeCIDR: %v", err.Error()))
	}
	shootName := shoot.Name
	for _, otherShoot := range shootList.Items {
		if shootName == otherShoot.Name {
			continue
		}
		// Skip shoots that are not in the same cloud profile (= not in the same organization)
		if shoot.Spec.CloudProfile.Name != otherShoot.Spec.CloudProfile.Name {
			continue
		}
		if otherShoot.Spec.Provider.InfrastructureConfig == nil {
			continue
		}
		otherInfraConfig, _ := DecodeInfrastructureConfig(s.decoder, otherShoot.Spec.Provider.InfrastructureConfig)
		if otherInfraConfig != nil {
			_, otherCIDR, _ := net.ParseCIDR(otherInfraConfig.Networks.NodeCIDR)
			if cidrOverlap(otherCIDR, currentNodeCIDR) {
				detail := fmt.Sprintf("NodeCIDR \"%s\" overlaps with the nodeCIDR \"%s\" of shoot with name \"%q\"", currentNodeCIDR.String(), otherCIDR.String(), otherShoot.Name)
				return field.Invalid(infrastructureConfigPath.Child("nodecidr"), currentNodeCIDR.String(), detail)
			}
		}
	}

	return nil
}

type validationContext struct {
	shoot                *core.Shoot
	infrastructureConfig *gdc.InfrastructureConfig
	cloudProfileConfig   *gdc.CloudProfileConfig
	cloudProfile         *gardencorev1beta1.CloudProfile
}

func newValidationContext(ctx context.Context, decoder runtime.Decoder, c client.Client, shoot *core.Shoot) (*validationContext, error) {
	if shoot.Spec.Provider.InfrastructureConfig == nil {
		return nil, field.Required(infrastructureConfigPath, "infrastructureConfig must be set for GDCH shoots")
	}
	infrastructureConfig, err := DecodeInfrastructureConfig(decoder, shoot.Spec.Provider.InfrastructureConfig)
	if err != nil {
		return nil, fmt.Errorf("error decoding infrastructureConfig: %v", err)
	}

	shootV1Beta1 := &gardencorev1beta1.Shoot{}
	if err := gardencorev1beta1.Convert_core_Shoot_To_v1beta1_Shoot(shoot, shootV1Beta1, nil); err != nil {
		return nil, err
	}

	cloudProfile, err := gardener.GetCloudProfile(ctx, c, shootV1Beta1)
	if err != nil {
		return nil, err
	}
	if cloudProfile == nil {
		return nil, fmt.Errorf("cloudprofile could not be found")
	}

	if cloudProfile.Spec.ProviderConfig == nil {
		return nil, fmt.Errorf("providerConfig is not given for cloud profile %q", cloudProfile.Name)
	}

	cloudProfileConfig, err := DecodeCloudProfileConfig(decoder, cloudProfile.Spec.ProviderConfig)
	if err != nil {
		return nil, fmt.Errorf("an error occurred while reading the cloud profile %q: %v", cloudProfile.Name, err)
	}

	return &validationContext{
		shoot:                shoot,
		infrastructureConfig: infrastructureConfig,
		cloudProfile:         cloudProfile,
		cloudProfileConfig:   cloudProfileConfig,
	}, nil
}

func cidrOverlap(cidr1, cidr2 *net.IPNet) bool {
	return cidr1 != nil && cidr2 != nil && (cidr1.Contains(cidr2.IP) || cidr2.Contains(cidr1.IP))
}
