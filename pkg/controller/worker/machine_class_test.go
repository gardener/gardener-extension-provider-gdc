// Copyright 2026 Google LLC
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

package worker

import (
	"bytes"
	"encoding/base64"
	"io"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/gardener/gardener/pkg/chartrenderer"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/version"

	"github.com/gardener/gardener-extension-provider-gdc/charts"
)

func TestMachineClassHelmDrift(t *testing.T) {
	// 1. Read the Helm template from embedded FS
	path := "internal/machineclass/templates/machineclass.yaml"
	content, err := charts.InternalChart.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read machineclass.yaml from embedded charts: %v", err)
	}

	// 2. Find all used fields
	// We look for patterns like $machineClass.field or .Values.machineClasses.field
	// In machineclass.yaml, it uses range with $machineClass
	// {{- range $index, $machineClass := .Values.machineClasses }}
	//   name: {{ $machineClass.name }}
	// We extract what comes after $machineClass.
	re := regexp.MustCompile(`\$machineClass\.([a-zA-Z0-9_\.]+)`)
	matches := re.FindAllStringSubmatch(string(content), -1)

	usedFields := make(map[string]bool)
	for _, match := range matches {
		if len(match) > 1 {
			usedFields[match[1]] = true
		}
	}

	// 3. Inspect machineClass struct via reflection
	st := reflect.TypeOf(machineClass{})
	jsonToFieldName := make(map[string]string)
	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			// Extract tag name (ignore omitempty etc)
			parts := strings.Split(jsonTag, ",")
			jsonToFieldName[parts[0]] = field.Name
		} else {
			jsonToFieldName[strings.ToLower(field.Name)] = field.Name
		}
	}

	// 4. Verify used fields exist in Go struct
	var missingFields []string
	for field := range usedFields {
		// Field might be nested like nodeTemplate.instanceType
		parts := strings.Split(field, ".")
		topLevelField := parts[0]

		if _, ok := jsonToFieldName[topLevelField]; !ok {
			missingFields = append(missingFields, field)
		}
	}

	if len(missingFields) > 0 {
		t.Errorf("Found fields in machineclass.yaml that are missing in machine_class.go struct: %v", missingFields)
		t.Errorf("Please ensure machine_class.go is updated to match Helm template.")
	} else {
		t.Logf("All %d fields used in Helm template are supported by machine_class.go", len(usedFields))
	}
}

// Exercise the actual renderer and YAML document decoder, including payloads
// that absorb the template's trailing fields into a second MachineClass.
func TestMachineClassScalarQuoting(t *testing.T) {
	renderer := chartrenderer.NewWithServerVersion(&version.Info{GitVersion: "v1.35.0", Major: "1", Minor: "35"})
	for _, value := range []string{
		"tenant-project", "true", "123", "null", "", "value: injected", "quote\" and 'apostrophe'", "tab\tvalue",
		"project\n---\napiVersion: machine.sapcloud.io/v1alpha1\nkind: MachineClass\nmetadata:\n  name: injected\nproviderSpec:",
	} {
		t.Run(value, func(t *testing.T) {
			values := map[string]interface{}{"machineClasses": []map[string]interface{}{{
				"name": value, "project": value, "addressPoolName": value, "subnetName": value,
				"orgClusterURL": value, "registryURL": value, "caData": value, "machineType": value,
				"enableEgress": true,
				"nodeTemplate": map[string]interface{}{"capacity": map[string]string{"cpu": "2"}, "instanceType": value, "region": value, "zone": value},
				"disks":        []map[string]interface{}{{"image": value, "project": value, "sizeGb": 20, "boot": true}},
				"labels":       map[string]string{"test": value}, "annotations": map[string]string{"test": value},
				"resourceLabels":       map[string]string{"test": value},
				"secret":               map[string]string{"cloudConfig": value},
				"credentialsSecretRef": map[string]string{"name": value, "namespace": value},
			}}}
			rendered, err := renderer.RenderEmbeddedFS(charts.InternalChart, "internal/machineclass", "test", value, values)
			if err != nil {
				t.Fatal(err)
			}
			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(rendered.Manifest()), 4096)
			objects := map[string]map[string]interface{}{}
			count := 0
			for {
				var obj map[string]interface{}
				err := decoder.Decode(&obj)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(obj) == 0 {
					continue
				}
				count++
				kind, _ := obj["kind"].(string)
				objects[kind] = obj
			}
			if count != 2 || objects["Secret"] == nil || objects["MachineClass"] == nil {
				t.Fatalf("expected exactly one Secret and one MachineClass, got %d documents", count)
			}
			assertString := func(obj map[string]interface{}, want string, path ...string) {
				t.Helper()
				got, found, err := unstructured.NestedString(obj, path...)
				if err != nil || !found || got != want {
					t.Errorf("%v = %q, found = %v, error = %v; want %q", path, got, found, err, want)
				}
			}
			for _, obj := range objects {
				assertString(obj, value, "metadata", "name")
				assertString(obj, value, "metadata", "namespace")
			}
			mc := objects["MachineClass"]
			for _, name := range []string{"project", "addressPoolName", "subnetName", "orgClusterURL", "registryURL", "caData", "machineType"} {
				assertString(mc, value, "providerSpec", name)
			}
			for _, name := range []string{"instanceType", "region", "zone"} {
				assertString(mc, value, "nodeTemplate", name)
			}
			for _, ref := range []string{"secretRef", "credentialsSecretRef"} {
				for _, name := range []string{"name", "namespace"} {
					assertString(mc, value, ref, name)
				}
			}
			assertString(mc, value, "providerSpec", "labels", "test")
			assertString(mc, value, "providerSpec", "annotations", "test")
			assertString(objects["Secret"], value, "metadata", "labels", "test")
			assertString(objects["Secret"], base64.StdEncoding.EncodeToString([]byte(value)), "data", "userData")
			egress, found, err := unstructured.NestedBool(mc, "providerSpec", "enableEgress")
			if err != nil || !found || !egress {
				t.Fatalf("enableEgress must remain a boolean: %v", err)
			}
		})
	}
}

// The machine controller keeps the legacy egress label on new VMs only while
// enableEgress is not set, and removes it for both true and false. The template
// must therefore not turn "not set" into false.
func TestMachineClassEnableEgress(t *testing.T) {
	renderer := chartrenderer.NewWithServerVersion(&version.Info{GitVersion: "v1.35.0", Major: "1", Minor: "35"})
	for _, tt := range []struct {
		name string
		set  bool
		want bool
	}{
		{name: "not set"},
		{name: "true", set: true, want: true},
		{name: "false", set: true, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			machineClass := map[string]interface{}{
				"name": "class", "project": "project", "addressPoolName": "pool", "subnetName": "subnet",
				"orgClusterURL": "https://org.example", "registryURL": "registry.example:443", "caData": "Y2E=", "machineType": "type",
				"nodeTemplate":         map[string]interface{}{"capacity": map[string]string{"cpu": "2"}, "instanceType": "type", "region": "region", "zone": "zone"},
				"disks":                []map[string]interface{}{{"image": "image", "project": "project", "sizeGb": 20, "boot": true}},
				"labels":               map[string]string{"test": "label"},
				"annotations":          map[string]string{"test": "annotation"},
				"resourceLabels":       map[string]string{"test": "label"},
				"secret":               map[string]string{"cloudConfig": "config"},
				"credentialsSecretRef": map[string]string{"name": "secret", "namespace": "namespace"},
			}
			if tt.set {
				machineClass["enableEgress"] = tt.want
			}
			values := map[string]interface{}{"machineClasses": []map[string]interface{}{machineClass}}
			rendered, err := renderer.RenderEmbeddedFS(charts.InternalChart, "internal/machineclass", "test", "namespace", values)
			if err != nil {
				t.Fatal(err)
			}
			var mc map[string]interface{}
			decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(rendered.Manifest()), 4096)
			for {
				var obj map[string]interface{}
				err := decoder.Decode(&obj)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if kind, _ := obj["kind"].(string); kind == "MachineClass" {
					mc = obj
				}
			}
			if mc == nil {
				t.Fatal("no MachineClass was rendered")
			}
			value, found, err := unstructured.NestedFieldNoCopy(mc, "providerSpec", "enableEgress")
			if err != nil {
				t.Fatal(err)
			}
			if !tt.set {
				if found && value != nil {
					t.Fatalf("enableEgress = %v, want it to stay unset", value)
				}
				return
			}
			if got, ok := value.(bool); !found || !ok || got != tt.want {
				t.Fatalf("enableEgress = %v (found = %v), want %v", value, found, tt.want)
			}
		})
	}
}
