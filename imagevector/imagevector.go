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

package imagevector

import (
	_ "embed"

	"github.com/gardener/gardener/pkg/utils/imagevector"
	"k8s.io/apimachinery/pkg/util/runtime"
)

// ImagesYAML contains the content of the images.yaml file
//
//go:embed images.yaml
var imagesYAML string
var imageVector imagevector.ImageVector

const (
	// OverrideEnv is the name of the image vector override environment variable.
	overrideEnv = "IMAGEVECTOR_OVERWRITE"
)

func init() {
	var (
		caBundle *imagevector.CABundle
		err      error
	)

	imageVector, caBundle, err = imagevector.Read([]byte(imagesYAML))
	runtime.Must(err)

	imageVector, _, err = imagevector.WithEnvOverride(imageVector, caBundle, overrideEnv)
	runtime.Must(err)
}

// ImageVector is the image vector that contains all the needed images.
func ImageVector() imagevector.ImageVector {
	return imageVector
}
