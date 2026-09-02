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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	extensionscontroller "github.com/gardener/gardener/extensions/pkg/controller"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"github.com/go-logr/logr"

	globalnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/networking/v1"

	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	gdcconstants "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/constants"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	gdcfake "github.com/gardener/gardener-extension-provider-gdc/pkg/gdc/fake"
)

func Test_actuator_ReconcileSuccess(t *testing.T) {
	type args struct {
		ctx     context.Context
		log     logr.Logger
		dns     *extensionsv1alpha1.DNSRecord
		cluster *extensionscontroller.Cluster
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(globalnetworkingv1.AddToScheme(scheme))

	if _, _, err := scheme.ObjectKinds(&globalnetworkingv1.ResourceRecordSet{}); err != nil {
		t.Fatalf("ResourceRecordSet not registered in scheme: %v", err)
	}

	if err := globalnetworkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add globalnetworkingv1 scheme: %v", err)
	}

	projectID := "prj"
	ttl := uint32(DNSDefaultTTL)
	shortManagedZone := &globalnetworkingv1.ManagedDNSZone{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "short-test-zone",
			Namespace: projectID,
		},
		Spec: globalnetworkingv1.ManagedDNSZoneSpec{
			DNSName: "test-zone",
		},
	}

	longManagedZone := &globalnetworkingv1.ManagedDNSZone{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "long-test-zone",
			Namespace: projectID,
		},
		Spec: globalnetworkingv1.ManagedDNSZoneSpec{
			DNSName: "gdc.test-zone",
		},
	}
	secretRef := corev1.SecretReference{Name: "my-secret", Namespace: "default"}
	providerSecret := createCloudProviderSecret(secretRef)
	fakeclient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shortManagedZone, longManagedZone, providerSecret).Build()
	rrs := &globalnetworkingv1.ResourceRecordSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "a-test-domain.gdc.test-zone",
			Namespace: projectID,
		},
		Spec: globalnetworkingv1.ResourceRecordSetSpec{
			Name:       "test-domain.gdc.test-zone",
			TTLSeconds: &ttl,
			Type:       "A",
			RRData:     []string{"1.2.3.4"},
			DNSZone:    "long-test-zone",
		},
	}
	fakeclientWithRRS := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rrs, providerSecret).Build()

	tests := []struct {
		name   string
		args   args
		client client.Client
		want   *globalnetworkingv1.ResourceRecordSet
	}{
		{
			name:   "create record set from zone using spec",
			client: fakeclient,
			args: args{
				ctx: context.Background(),
				log: logr.Logger{},
				dns: &extensionsv1alpha1.DNSRecord{
					Spec: extensionsv1alpha1.DNSRecordSpec{
						Name:       "_test-domain.gdc.test-zone",
						Zone:       ptr.To("test-zone"),
						SecretRef:  secretRef,
						RecordType: extensionsv1alpha1.DNSRecordTypeA,
						Values:     []string{"1.2.3.4"},
					},
				},
			},
			want: &globalnetworkingv1.ResourceRecordSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "a--test-domain.gdc.test-zone",
					Namespace: projectID,
				},
				Spec: globalnetworkingv1.ResourceRecordSetSpec{
					Name:       "_test-domain.gdc.test-zone",
					TTLSeconds: &ttl,
					Type:       "A",
					RRData:     []string{"1.2.3.4"},
					DNSZone:    "test-zone",
				},
			},
		},
		{
			name:   "create record set by getting zone from listing all DNSManagedZone",
			client: fakeclient,
			args: args{
				ctx: context.Background(),
				log: logr.Logger{},
				dns: &extensionsv1alpha1.DNSRecord{
					Spec: extensionsv1alpha1.DNSRecordSpec{
						Name:       "test-domain.gdc.test-zone",
						SecretRef:  secretRef,
						RecordType: extensionsv1alpha1.DNSRecordTypeA,
						Values:     []string{"1.2.3.4"},
					},
				},
			},
			want: &globalnetworkingv1.ResourceRecordSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "a-test-domain.gdc.test-zone",
					Namespace: projectID,
				},
				Spec: globalnetworkingv1.ResourceRecordSetSpec{
					Name:       "test-domain.gdc.test-zone",
					TTLSeconds: &ttl,
					Type:       "A",
					RRData:     []string{"1.2.3.4"},
					DNSZone:    "long-test-zone",
				},
			},
		},
		{
			name:   "update DNS record set",
			client: fakeclientWithRRS,
			args: args{
				ctx: context.Background(),
				log: logr.Logger{},
				dns: &extensionsv1alpha1.DNSRecord{
					Spec: extensionsv1alpha1.DNSRecordSpec{
						Name:       "test-domain.gdc.test-zone",
						RecordType: extensionsv1alpha1.DNSRecordTypeA,
						SecretRef:  secretRef,
						Values:     []string{"5.6.7.8"},
						Zone:       ptr.To("test-zone"),
					},
					Status: extensionsv1alpha1.DNSRecordStatus{
						Zone: ptr.To("test-zone"),
					},
				},
			},
			want: &globalnetworkingv1.ResourceRecordSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "a-test-domain.gdc.test-zone",
					Namespace: projectID,
				},
				Spec: globalnetworkingv1.ResourceRecordSetSpec{
					Name:       "test-domain.gdc.test-zone",
					TTLSeconds: &ttl,
					Type:       "A",
					RRData:     []string{"5.6.7.8"},
					DNSZone:    "test-zone",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := gdcfake.NewManager(tt.client)
			act := NewActuator(m)
			a := act.(*actuator)
			a.getClientAndProject = func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return tt.client, projectID, nil
			}
			if err := a.Reconcile(tt.args.ctx, tt.args.log, tt.args.dns, tt.args.cluster); err != nil {
				t.Fatalf("actuator.Reconcile() error = %v", err)
			}
			got := &globalnetworkingv1.ResourceRecordSet{}
			if err := tt.client.Get(context.Background(), client.ObjectKeyFromObject(tt.want), got); err != nil {
				t.Fatalf("actuator.Reconcile() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got, cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion", "UID", "CreationTimestamp")); diff != "" {
				t.Fatalf("Unexpected result (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_actuator_ReconcileErrors(t *testing.T) {
	type args struct {
		ctx context.Context
		log logr.Logger
		dns *extensionsv1alpha1.DNSRecord
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(globalnetworkingv1.AddToScheme(scheme))
	secretRef := corev1.SecretReference{Name: "my-secret", Namespace: "default"}
	providerSecret := createCloudProviderSecret(secretRef)
	fakeclient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(providerSecret).Build()

	projectID := "prj"
	zoneConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dnssuffix",
			Namespace: "gpc-system",
		},
		Data: map[string]string{
			"dnsSuffix": "org-1.zone1.google.gdc.test",
		},
	}

	fakeclientWithZone := fake.NewClientBuilder().WithScheme(scheme).WithObjects(providerSecret, zoneConfig).Build()
	tests := []struct {
		name       string
		args       args
		client     client.Client
		wantErrMsg string
	}{
		{
			name:   "failed to find zone",
			client: fakeclientWithZone,
			args: args{
				ctx: nil,
				log: logr.Logger{},
				dns: &extensionsv1alpha1.DNSRecord{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-domain",
						Namespace: "test-namespace",
					},
					Spec: extensionsv1alpha1.DNSRecordSpec{
						// name is a must have field.
						Name:       "test-domain.gdc.test-zone",
						RecordType: extensionsv1alpha1.DNSRecordTypeA,
						Values:     []string{"1.2.3.4"},
						SecretRef:  secretRef,
					},
				},
			},
			wantErrMsg: "could not find DNS managed zone for name",
		},
		{
			name:   "secret is not set",
			client: fakeclient,
			args: args{
				ctx: nil,
				log: logr.Logger{},
				dns: &extensionsv1alpha1.DNSRecord{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-domain",
						Namespace: "test-namespace",
					},
					Spec: extensionsv1alpha1.DNSRecordSpec{
						// name is a must have field.
						Name:       "test-domain.gdc.test-zone",
						RecordType: extensionsv1alpha1.DNSRecordTypeA,
						Values:     []string{"1.2.3.4"},
					},
				},
			},
			wantErrMsg: "failed to fetch secret",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := gdcfake.NewManager(tt.client)
			act := NewActuator(m)
			a := act.(*actuator)
			a.getClientAndProject = func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return tt.client, projectID, nil
			}
			err := a.Reconcile(tt.args.ctx, tt.args.log, tt.args.dns, nil)
			if err == nil {
				t.Fatal("actuator.Reconcile() wants error but got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Fatalf("actuator.Reconcile() error = %v, wantErrMsg %v", err.Error(), tt.wantErrMsg)
			}
		})
	}
}

func Test_actuator_Delete(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(globalnetworkingv1.AddToScheme(scheme))
	secretRef := corev1.SecretReference{Name: "my-secret", Namespace: "default"}
	providerSecret := createCloudProviderSecret(secretRef)
	fakeclient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(providerSecret).Build()
	ttl := uint32(DNSDefaultTTL)
	projectID := "prj"
	rrs := &globalnetworkingv1.ResourceRecordSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "a-test-domain.gdc.test-zone",
			Namespace: projectID,
		},
		Spec: globalnetworkingv1.ResourceRecordSetSpec{
			Name:       "test-domain.gdc.test-zone",
			TTLSeconds: &ttl,
			Type:       "A",
			RRData:     []string{"1.2.3.4"},
			DNSZone:    "test-zone",
		},
	}
	fakeclientWithRRS := fake.NewClientBuilder().WithScheme(scheme).WithObjects(providerSecret, rrs).Build()
	type args struct {
		ctx context.Context
		log logr.Logger
		dns *extensionsv1alpha1.DNSRecord
	}
	tests := []struct {
		name   string
		client client.Client
		args   args
	}{
		{
			name:   "delete record set by name",
			client: fakeclientWithRRS,
			args: args{
				ctx: context.Background(),
				log: logr.Logger{},
				dns: &extensionsv1alpha1.DNSRecord{
					Spec: extensionsv1alpha1.DNSRecordSpec{
						Name:       "test-domain.gdc.test-zone",
						RecordType: extensionsv1alpha1.DNSRecordTypeA,
						SecretRef:  secretRef,
						Values:     []string{"1.2.3.4"},
					},
				},
			},
		},
		{
			name:   "delete non-existent record",
			client: fakeclient,
			args: args{
				ctx: context.Background(),
				log: logr.Logger{},
				dns: &extensionsv1alpha1.DNSRecord{
					Spec: extensionsv1alpha1.DNSRecordSpec{
						Name:       "dns-name",
						RecordType: extensionsv1alpha1.DNSRecordTypeA,
						SecretRef:  secretRef,
						Values:     []string{"1.2.3.4"},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := gdcfake.NewManager(tt.client)
			act := NewActuator(m)
			a := act.(*actuator)
			a.getClientAndProject = func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return tt.client, projectID, nil
			}
			if err := a.Delete(tt.args.ctx, tt.args.log, tt.args.dns, nil); err != nil {
				t.Fatalf("actuator.Delete() error = %v", err)
			}
			rrslist := &globalnetworkingv1.ResourceRecordSetList{}
			if err := tt.client.List(context.Background(), rrslist); err != nil {
				t.Fatalf("resourcerecordset list() error = %v", err)
			}
			if len(rrslist.Items) != 0 {
				t.Errorf("expect empty recordset list, but got %v", len(rrslist.Items))
			}
		})
	}
}

func createCloudProviderSecret(secretRef corev1.SecretReference) *corev1.Secret {
	cfg := &gdcclient.OrgClusterConfig{
		OrgClusterURL: "test-url-secret",
		CAData:        "test-ca-data",
	}
	cfgJSON, _ := json.Marshal(cfg)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretRef.Name,
			Namespace: secretRef.Namespace,
		},
		Data: map[string][]byte{
			gdcconstants.GDCHConfigJSONField: cfgJSON,
		},
	}
}

func Test_actuator_DeleteErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(globalnetworkingv1.AddToScheme(scheme))
	secretRef := corev1.SecretReference{Name: "my-secret", Namespace: "default"}
	providerSecret := createCloudProviderSecret(secretRef)
	fakeclient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(providerSecret).Build()

	projectID := "prj"

	type args struct {
		ctx context.Context
		log logr.Logger
		dns *extensionsv1alpha1.DNSRecord
	}
	tests := []struct {
		name       string
		client     client.Client
		clientErr  error
		args       args
		wantErrMsg string
	}{
		{
			name: "unable to create client",
			args: args{
				ctx: context.Background(),
				log: logr.Logger{},
				dns: &extensionsv1alpha1.DNSRecord{
					Spec: extensionsv1alpha1.DNSRecordSpec{
						Name:      "dns-name",
						SecretRef: secretRef,
					},
				},
			},
			client:     fakeclient,
			clientErr:  errors.New("cannot get client"),
			wantErrMsg: "failed to get gdcclient and project",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := gdcfake.NewManager(tt.client)
			act := NewActuator(m)
			a := act.(*actuator)
			a.getClientAndProject = func(ctx context.Context, c client.Client, orgClusterCfg *gdcclient.OrgClusterConfig, sr corev1.SecretReference, scheme *runtime.Scheme) (client.Client, string, error) {
				return tt.client, projectID, tt.clientErr
			}
			fake.NewClientBuilder().WithScheme(scheme).Build()
			err := a.Delete(tt.args.ctx, tt.args.log, tt.args.dns, nil)
			if err == nil {
				t.Fatal("actuator.Delete() wants error but got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Fatalf("actuator.Delete() error = %v, wantErrMsg %v", err.Error(), tt.wantErrMsg)
			}
		})
	}
}
