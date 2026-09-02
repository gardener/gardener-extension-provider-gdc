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
	"testing"
)

func TestGetDNSRecordSetName(t *testing.T) {
	tests := []struct {
		name       string
		recordType string
		recordName string
		want       string
	}{
		{
			name:       "valid record name",
			recordType: "A",
			recordName: "mydns.org",
			want:       "a-mydns.org",
		},
		{
			name:       "valid wildcard record name",
			recordType: "TXT",
			recordName: "*.wildcard.org",
			want:       "txt-wildcard.org",
		},
		{
			name:       "invalid wildcard record name",
			recordType: "TXT",
			recordName: "wildcard.*.org",
			want:       "txt-wildcard.-.org",
		},
		{
			name:       "record name with underscore",
			recordType: "TXT",
			recordName: "_acme-challenge.mydns.org",
			want:       "txt--acme-challenge.mydns.org",
		},
		{
			name:       "uppercase characters converted to lowercase",
			recordType: "A",
			recordName: "MYDNS.ORG",
			want:       "a-mydns.org",
		},
		{
			name:       "multiple consecutive invalid characters replaced with single hyphen",
			recordType: "A",
			recordName: "my$$$dns.org",
			want:       "a-my-dns.org",
		},
		{
			name:       "leading and trailing hyphens preserved in record name",
			recordType: "A",
			recordName: "-mydns-.org",
			want:       "a--mydns-.org",
		},
		{
			name:       "dots are preserved in domain name",
			recordType: "A",
			recordName: "sub.sub.mydns.org",
			want:       "a-sub.sub.mydns.org",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetDNSRecordSetName(tt.recordType, tt.recordName)
			if got != tt.want {
				t.Errorf("GetDNSRecordSetName() = %v, want %v", got, tt.want)
			}
		})
	}
}
