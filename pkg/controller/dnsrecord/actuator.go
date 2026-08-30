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

package dnsrecord

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	"github.com/gardener/gardener/extensions/pkg/controller/dnsrecord"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	reconcilerutils "github.com/gardener/gardener/pkg/controllerutils/reconciler"

	globalnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/networking/v1"

	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/pkg/gdc"
)

const (
	// requeueAfterOnProviderError is a value for RequeueAfter to be returned on provider errors
	// in order to prevent quick retries that could quickly exhaust the account rate limits in case of e.g.
	// configuration issues.
	requeueAfterOnProviderError = 30 * time.Second
)

type actuator struct {
	decoder             runtime.Decoder
	client              client.Client
	getClientAndProject func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error)
}

// NewActuator creates a new dnsrecord.Actuator.
func NewActuator(mgr manager.Manager) dnsrecord.Actuator {
	decoder := serializer.NewCodecFactory(mgr.GetScheme(), serializer.EnableStrict).UniversalDecoder()
	return &actuator{
		decoder:             decoder,
		client:              mgr.GetClient(),
		getClientAndProject: getDNSClientAndProject,
	}
}

// Reconcile reconciles the DNSRecord.
func (a *actuator) Reconcile(ctx context.Context, log logr.Logger, dns *extensionsv1alpha1.DNSRecord, _ *extensionscontroller.Cluster) error {
	orgClusterCfg, err := gdc.GetGDCHConfigFromSecretReference(ctx, a.client, dns.Spec.SecretRef)
	if err != nil {
		return err
	}

	gdcclient, projectID, err := a.getClientAndProject(ctx, a.client, orgClusterCfg, dns.Spec.SecretRef, a.client.Scheme())
	if err != nil {
		return fmt.Errorf("failed to get gdcclient and project: %w", err)
	}
	// Determine DNS managed zone
	managedZone, err := getZone(ctx, log, dns, gdcclient, projectID)
	log.Info("Creating or updating DNS recordset", "managedZone", managedZone, "dnsName", dns.Spec.Name, "type", dns.Spec.RecordType, "rrdatas", dns.Spec.Values, "dnsrecord", gdc.ObjectName(dns))

	if err != nil {
		return fmt.Errorf("failed to get dns zone list for project %s: %w", projectID, err)
	}

	return createOrUpdateRRS(ctx, gdcclient, dns, managedZone, projectID)
}

// Delete deletes the DNSRecord.
func (a *actuator) Delete(ctx context.Context, log logr.Logger, dns *extensionsv1alpha1.DNSRecord, _ *extensionscontroller.Cluster) error {
	orgClusterCfg, err := gdc.GetGDCHConfigFromSecretReference(ctx, a.client, dns.Spec.SecretRef)
	if err != nil {
		return err
	}

	gdcclient, projectID, err := a.getClientAndProject(ctx, a.client, orgClusterCfg, dns.Spec.SecretRef, a.client.Scheme())
	if err != nil {
		return fmt.Errorf("failed to get gdcclient and project: %w", err)
	}

	log.Info("Delete DNS recordset", "dnsName", dns.Spec.Name, "type", dns.Spec.RecordType, "rrdatas", dns.Spec.Values, "dnsrecord", gdc.ObjectName(dns))
	dnsRecodSetName := GetDNSRecordSetName(string(dns.Spec.RecordType), dns.Spec.Name)
	return deleteRRS(ctx, gdcclient, dnsRecodSetName, projectID)
}

// ForceDelete deletes the DNSRecord.
func (a *actuator) ForceDelete(ctx context.Context, log logr.Logger, dns *extensionsv1alpha1.DNSRecord, cluster *extensionscontroller.Cluster) error {
	return a.Delete(ctx, log, dns, cluster)
}

// Restore restores the DNSRecord.
func (a *actuator) Restore(ctx context.Context, log logr.Logger, dns *extensionsv1alpha1.DNSRecord, cluster *extensionscontroller.Cluster) error {
	return nil
}

// Migrate migrates the DNSRecord.
func (a *actuator) Migrate(ctx context.Context, log logr.Logger, dns *extensionsv1alpha1.DNSRecord, cluster *extensionscontroller.Cluster) error {
	return a.Reconcile(ctx, log, dns, cluster)
}

func getZone(ctx context.Context, log logr.Logger, dns *extensionsv1alpha1.DNSRecord, gdcclient client.Client, project string) (string, error) {
	switch {
	case dns.Spec.Zone != nil && *dns.Spec.Zone != "":
		return *dns.Spec.Zone, nil
	case dns.Status.Zone != nil && *dns.Status.Zone != "":
		return *dns.Status.Zone, nil
	default:
		// The zone is not specified in the resource status or spec. Try to determine the zone by
		// getting all managed zones of the account and searching for the longest zone name that is a suffix of dns.spec.Name
		zones, err := getManagedDNSZones(ctx, gdcclient, project)
		if err != nil {
			return "", &reconcilerutils.RequeueAfterError{
				Cause:        fmt.Errorf("could not get DNS managed zones: %+v", err),
				RequeueAfter: requeueAfterOnProviderError,
			}
		}
		log.Info("Got DNS managed zones", "zones", zones, "dnsrecord", gdc.ObjectName(dns))
		zone := findZoneWithLongestSuffix(zones, dns.Spec.Name)
		if zone == "" {
			return "", fmt.Errorf("could not find DNS managed zone for name %s in zones %s", dns.Spec.Name, zones)
		}
		return zone, nil
	}
}

func getManagedDNSZones(ctx context.Context, gdcclient client.Client, project string) (map[string]string, error) {
	zones := make(map[string]string)

	managedZoneList := &globalnetworkingv1.ManagedDNSZoneList{}
	listOptions := client.InNamespace(project)
	if err := gdcclient.List(ctx, managedZoneList, listOptions); err != nil {
		return nil, fmt.Errorf("failed to list ManagedDNSZone in project %q: %v", project, err)
	}

	for _, zone := range managedZoneList.Items {
		dnsName := zone.Spec.DNSName
		zones[normalizeZoneName(zone.Name)] = dnsName
	}
	return zones, nil
}

// normalizeZoneName cleans up zone names by replacing escaped asterisks with literal
// ones and removing trailing dots
// Examples:
// zone1 := normalizeZoneName("\\052.example.com.")  // Output: *.example.com
// zone2 := normalizeZoneName("example.com.")     // Output: example.com
// zone3 := normalizeZoneName("example.com")      // Output: example.com
func normalizeZoneName(zoneName string) string {
	if strings.HasPrefix(zoneName, "\\052.") {
		zoneName = "*" + zoneName[4:]
	}
	if strings.HasSuffix(zoneName, ".") {
		return zoneName[:len(zoneName)-1]
	}
	return zoneName
}

// matchesDomain returns true if the given domain matches (is a subdomain) of the given name, false otherwise.
func matchesDomain(name, domain string) bool {
	// Check if 'name' is an exact match or ends with the extracted domain
	return name == domain || strings.HasSuffix(name, "."+domain)
}

// findZoneWithLongestSuffix returns longest zone from the given zones map that is matched by the given name.
// If the given name doesn't match any of the zone domains in the given zones map, an empty string is returned.
func findZoneWithLongestSuffix(zones map[string]string, name string) string {
	longestDNSDomain := ""
	zoneWithLongestDomainSuffix := ""
	for zoneID, dnsDomain := range zones {
		if matchesDomain(name, dnsDomain) && len(dnsDomain) > len(longestDNSDomain) {
			longestDNSDomain = dnsDomain
			zoneWithLongestDomainSuffix = zoneID
		}
	}

	// the DNS zone in the ResourceRecordSet spec is the name of the ManagedZone CR / zoneID
	// See the ResourceRecordSetSpec definition at
	// https://github.com/GoogleCloudPlatform/google-distributed-cloud-apis/blob/main/pkg/apis/public/global/networking/v1/resourcerecordsetreplica_type.go
	return zoneWithLongestDomainSuffix
}

func createOrUpdateRRS(ctx context.Context, gdcclient client.Client, dns *extensionsv1alpha1.DNSRecord, managedZone, projectID string) error {
	recordType := string(dns.Spec.RecordType)
	dnsRecodSetName := GetDNSRecordSetName(recordType, dns.Spec.Name)
	ttl := uint32(DNSDefaultTTL)

	recordSet := &globalnetworkingv1.ResourceRecordSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dnsRecodSetName,
			Namespace: projectID,
		},
	}

	resourceRecordSetSpec := globalnetworkingv1.ResourceRecordSetSpec{
		Name:       dns.Spec.Name,
		TTLSeconds: &ttl,
		Type:       recordType,
		RRData:     dns.Spec.Values,
		DNSZone:    managedZone,
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, gdcclient, recordSet, func() error {
		recordSet.Spec = resourceRecordSetSpec
		return nil
	}); err != nil {
		return &reconcilerutils.RequeueAfterError{
			Cause:        fmt.Errorf("could not create or update DNS recordset in managed zone %s with name %s, type %s, and rrdatas %v: %+v", managedZone, dns.Spec.Name, dns.Spec.RecordType, dns.Spec.Values, err),
			RequeueAfter: requeueAfterOnProviderError,
		}
	}
	return nil
}

func deleteRRS(ctx context.Context, gdcclient client.Client, dnsRecodSetName, projectID string) error {
	recordSet := &globalnetworkingv1.ResourceRecordSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dnsRecodSetName,
			Namespace: projectID,
		},
	}

	if err := gdcclient.Delete(ctx, recordSet); err != nil && !apierrors.IsNotFound(err) {
		return &reconcilerutils.RequeueAfterError{
			Cause:        fmt.Errorf("could not delete DNS recordset with name %s: in project %s: %+v", dnsRecodSetName, projectID, err),
			RequeueAfter: requeueAfterOnProviderError,
		}
	}

	return nil
}

func getDNSClientAndProject(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
	serviceAccount, err := gdc.GetServiceAccountFromSecretReference(ctx, c, sr)
	if err != nil {
		return nil, "", err
	}

	kubeclient, err := gdcclient.Get(orgClusterCfg, serviceAccount, scheme)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create kubeClient: %w", err)
	}

	return kubeclient, serviceAccount.Project, nil
}
