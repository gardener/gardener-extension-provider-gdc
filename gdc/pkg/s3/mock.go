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
	"strings"
	"time"
)

type MockObject struct {
	Versions []*MockObjectVersion
}

type MockObjectVersion struct {
	Data         []byte
	IsLatest     bool
	LastModified *time.Time
	VersionID    *string
}

type MockBucket struct {
	Objects map[string]*MockObject
}

type MockS3ClientConfig struct {
	Buckets                            map[string]*MockBucket
	DeleteObjectFunc                   func(context.Context, DeleteObjectInput) (*DeleteObjectOutput, error)
	DeleteObjectVersionsWithPrefixFunc func(context.Context, string, string) error
}

type mockClient struct {
	buckets map[string]*MockBucket

	// Assignable function field for customizing DeleteObject behavior
	DeleteObjectFunc                   func(context.Context, DeleteObjectInput) (*DeleteObjectOutput, error)
	DeleteObjectVersionsWithPrefixFunc func(context.Context, string, string) error
}

func (m *mockClient) ListObjectsV2Pages(_ context.Context, bucketFQN string) ([]string, error) {
	bucket, ok := m.buckets[bucketFQN]
	if !ok {
		return nil, fmt.Errorf("no such bucket %q", bucketFQN)
	}
	var objects []string
	for k := range bucket.Objects {
		objects = append(objects, k)
	}
	return objects, nil
}

func (m *mockClient) ListObjectVersionsPages(_ context.Context, bucketFQN string) ([]ObjectVersion, error) {
	bucket, ok := m.buckets[bucketFQN]
	if !ok {
		return nil, fmt.Errorf("no such bucket %q", bucketFQN)
	}

	var objectVersions []ObjectVersion
	for objectKey, object := range bucket.Objects {
		for _, version := range object.Versions {
			objectVersions = append(objectVersions, ObjectVersion{
				ObjectKey: objectKey,
				VersionID: version.VersionID,
			})
		}
	}
	return objectVersions, nil
}

func (m *mockClient) DeleteObjectVersionsWithPrefix(ctx context.Context, bucketFQN, prefix string) error {
	if m.DeleteObjectVersionsWithPrefixFunc != nil {
		return m.DeleteObjectVersionsWithPrefixFunc(ctx, bucketFQN, prefix)
	}

	versions, err := m.ListObjectVersionsPages(ctx, bucketFQN)
	if err != nil {
		return err
	}
	for _, version := range versions {
		if !strings.HasPrefix(version.ObjectKey, prefix) {
			continue
		}
		if _, err := m.DeleteObject(ctx, DeleteObjectInput{
			BucketFqn: bucketFQN,
			ObjectKey: version.ObjectKey,
			VersionId: version.VersionID,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockClient) DeleteObject(ctx context.Context, input DeleteObjectInput) (*DeleteObjectOutput, error) {
	// Call the custom function if provided
	if m.DeleteObjectFunc != nil {
		return m.DeleteObjectFunc(ctx, input)
	}

	bucket, ok := m.buckets[input.BucketFqn]
	if !ok {
		return nil, fmt.Errorf("no such bucket %q", input.BucketFqn)
	}

	object, ok := bucket.Objects[input.ObjectKey]
	if !ok {
		return nil, nil
	}
	if input.VersionId == nil {
		delete(bucket.Objects, input.ObjectKey)
		return nil, nil
	}

	remainingVersions := object.Versions[:0]
	for _, version := range object.Versions {
		if version.VersionID == nil || *version.VersionID != *input.VersionId {
			remainingVersions = append(remainingVersions, version)
		}
	}
	object.Versions = remainingVersions
	if len(object.Versions) == 0 {
		delete(bucket.Objects, input.ObjectKey)
	}
	return nil, nil
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
		buckets:                            config.Buckets,
		DeleteObjectFunc:                   config.DeleteObjectFunc,
		DeleteObjectVersionsWithPrefixFunc: config.DeleteObjectVersionsWithPrefixFunc,
	}
}
