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

package crd

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/gardener/gardener/extensions/pkg/webhook"
	"github.com/go-logr/logr"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type mutator struct {
	logger logr.Logger
}

// NewCRDMutator returns a new CRD mutator.
func NewCRDMutator() webhook.Mutator {
	return &mutator{
		logger: log.Log.WithName("crd-mutator"),
	}
}

func (m *mutator) Mutate(ctx context.Context, newObj, _ client.Object) error {
	crd, ok := newObj.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		return fmt.Errorf("expected *apiextensionsv1.CustomResourceDefinition but got %T", newObj)
	}

	m.logger.Info("Mutating CRD", "name", crd.GetName())

	// Convert to JSON to easily search and replace in validation rules
	jsonData, err := json.Marshal(crd)
	if err != nil {
		return fmt.Errorf("failed to marshal CRD to JSON: %w", err)
	}

	jsonStr := string(jsonData)

	// List of CEL reserved keywords and tokens to escape
	keywords := []string{
		"true", "false", "null", "in",
		"as", "break", "const", "continue", "else",
		"for", "function", "if", "import", "let", "loop",
		"package", "namespace", "return", "var", "void", "while",
	}

	mutated := false
	for _, kw := range keywords {
		// Match literal dot followed by keyword, ensuring it's a field access
		// e.g., self.namespace -> self.__namespace__
		// We use \b to ensure word boundary after the keyword.
		re := regexp.MustCompile(fmt.Sprintf(`\.%s\b`, kw))
		if re.MatchString(jsonStr) {
			jsonStr = re.ReplaceAllString(jsonStr, fmt.Sprintf(".__%s__", kw))
			mutated = true
		}
	}

	if mutated {
		m.logger.Info("CRD mutated to escape CEL keywords", "name", crd.GetName())
		if err := json.Unmarshal([]byte(jsonStr), crd); err != nil {
			return fmt.Errorf("failed to unmarshal mutated JSON back to CRD: %w", err)
		}
	}

	return nil
}
