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
	"reflect"
	"regexp"
	"strings"
	"testing"

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
