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

package s3

import (
	"context"
	"fmt"
	"time"
)

type MockObject struct {
	Versions []*MockObjectVersion
}

type MockObjectVersion struct {
	Data           []byte
	IsLatest       bool
	LastModified   *time.Time
	VersionID      *string
	IsDeleteMarker bool
}

type MockBucket struct {
	Objects map[string]*MockObject
}

type MockS3ClientConfig struct {
	Buckets                        map[string]*MockBucket
	DeleteObjectFunc               func(input DeleteObjectInput) (*DeleteObjectOutput, error)
	DeleteExpiredDeleteMarkersFunc func(ctx context.Context, bucketFQN string) error
}

type mockClient struct {
	buckets map[string]*MockBucket

	// Assignable function fields for customizing mock behavior
	DeleteObjectFunc               func(input DeleteObjectInput) (*DeleteObjectOutput, error)
	DeleteExpiredDeleteMarkersFunc func(ctx context.Context, bucketFQN string) error
}

func (m *mockClient) ListObjectsV2Pages(bucketFQN string) ([]string, error) {
	bucket, ok := m.buckets[bucketFQN]
	if !ok {
		return nil, fmt.Errorf("no such bucket %q", bucketFQN)
	}
	var objects []string
	for k, obj := range bucket.Objects {
		if obj == nil || len(obj.Versions) == 0 {
			objects = append(objects, k)
			continue
		}
		latestIsDeleteMarker := false
		for _, v := range obj.Versions {
			if v != nil && v.IsLatest && v.IsDeleteMarker {
				latestIsDeleteMarker = true
				break
			}
		}
		if !latestIsDeleteMarker {
			objects = append(objects, k)
		}
	}
	return objects, nil
}

func (m *mockClient) DeleteObject(input DeleteObjectInput) (*DeleteObjectOutput, error) {
	// Call the custom function if provided
	if m.DeleteObjectFunc != nil {
		return m.DeleteObjectFunc(input)
	}

	bucket, ok := m.buckets[input.BucketFqn]
	if !ok {
		return nil, fmt.Errorf("no such bucket %q", input.BucketFqn)
	}

	delete(bucket.Objects, input.ObjectKey)
	return nil, nil
}

func (m *mockClient) DeleteExpiredDeleteMarkers(ctx context.Context, bucketFQN string) error {
	if m.DeleteExpiredDeleteMarkersFunc != nil {
		return m.DeleteExpiredDeleteMarkersFunc(ctx, bucketFQN)
	}
	if m.buckets == nil {
		return nil
	}
	bucket, ok := m.buckets[bucketFQN]
	if !ok {
		return fmt.Errorf("no such bucket %q", bucketFQN)
	}
	for key, obj := range bucket.Objects {
		if obj == nil || len(obj.Versions) == 0 {
			continue
		}
		hasObjectVersion := false
		hasDeleteMarker := false
		for _, v := range obj.Versions {
			if v == nil {
				continue
			}
			if v.IsDeleteMarker {
				hasDeleteMarker = true
			} else {
				hasObjectVersion = true
			}
		}
		if hasDeleteMarker && !hasObjectVersion {
			delete(bucket.Objects, key)
		}
	}
	return nil
}

func (m *mockClient) GetObject(
	input GetObjectInput,
	opts ...GetObjectOption,
) (*GetObjectOutput, error) {
	bucket, ok := m.buckets[input.BucketFqn]
	if !ok {
		return nil, fmt.Errorf("no such bucket %q", input.BucketFqn)
	}
	if len(input.ObjectKey) == 0 {
		return nil, fmt.Errorf("object key must be non-empty")
	}
	_, ok = bucket.Objects[input.ObjectKey]
	if !ok {
		return nil, fmt.Errorf("object %q does not exist", input.ObjectKey)
	}
	return nil, nil
}

func (m *mockClient) UploadObject(input UploadObjectInput) (*UploadObjectOutput, error) {
	return nil, nil
}

func CreateMockS3Client(config MockS3ClientConfig) Client {
	return &mockClient{
		buckets:                        config.Buckets,
		DeleteObjectFunc:               config.DeleteObjectFunc,
		DeleteExpiredDeleteMarkersFunc: config.DeleteExpiredDeleteMarkersFunc,
	}
}
