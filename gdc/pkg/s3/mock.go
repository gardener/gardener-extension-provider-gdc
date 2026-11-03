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
	"sort"
	"strings"
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
	DeleteExpiredDeleteMarkersFunc func(ctx context.Context, bucketFQN string, opts DeleteExpiredDeleteMarkersOptions) (*DeleteExpiredDeleteMarkersResult, error)
}

type mockClient struct {
	buckets map[string]*MockBucket

	// Assignable function fields for customizing mock behavior
	DeleteObjectFunc               func(input DeleteObjectInput) (*DeleteObjectOutput, error)
	DeleteExpiredDeleteMarkersFunc func(ctx context.Context, bucketFQN string, opts DeleteExpiredDeleteMarkersOptions) (*DeleteExpiredDeleteMarkersResult, error)
}

func (m *mockClient) DeleteObjectsWithPrefix(ctx context.Context, bucketFQN, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("object deletion requires a non-empty prefix")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	bucket, ok := m.buckets[bucketFQN]
	if !ok {
		return fmt.Errorf("no such bucket %q", bucketFQN)
	}
	for k, obj := range bucket.Objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		// Keys whose latest version is already a delete marker are not "current"
		// objects and would not be returned by a ListObjectsV2 listing.
		if isLatestVersionDeleteMarker(obj) {
			continue
		}
		if _, err := m.DeleteObject(ctx, DeleteObjectInput{BucketFqn: bucketFQN, ObjectKey: k}); err != nil {
			return err
		}
	}
	return nil
}

// isLatestVersionDeleteMarker reports whether the mock object's current version is a delete marker.
func isLatestVersionDeleteMarker(obj *MockObject) bool {
	if obj == nil {
		return false
	}
	for _, v := range obj.Versions {
		if v != nil && v.IsLatest && v.IsDeleteMarker {
			return true
		}
	}
	return false
}

func (m *mockClient) DeleteObject(ctx context.Context, input DeleteObjectInput) (*DeleteObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

// DeleteExpiredDeleteMarkers mirrors the real implementation on the in-memory bucket: keys are visited in
// sorted order, a key whose versions are all delete markers is removed, and opts.Prefix / opts.KeyMarker /
// opts.MaxPages (one key per "page" to keep tests small) bound the sweep.
func (m *mockClient) DeleteExpiredDeleteMarkers(ctx context.Context, bucketFQN string, opts DeleteExpiredDeleteMarkersOptions) (*DeleteExpiredDeleteMarkersResult, error) {
	if m.DeleteExpiredDeleteMarkersFunc != nil {
		return m.DeleteExpiredDeleteMarkersFunc(ctx, bucketFQN, opts)
	}
	result := &DeleteExpiredDeleteMarkersResult{}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if m.buckets == nil {
		result.Completed = true
		return result, nil
	}
	bucket, ok := m.buckets[bucketFQN]
	if !ok {
		return nil, fmt.Errorf("no such bucket %q", bucketFQN)
	}
	keys := make([]string, 0, len(bucket.Objects))
	for key := range bucket.Objects {
		if strings.HasPrefix(key, opts.Prefix) && key > opts.KeyMarker {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for i, key := range keys {
		if err := ctx.Err(); err != nil {
			if i > 0 {
				result.NextKeyMarker = keys[i-1]
			}
			return result, err
		}
		if opts.MaxPages > 0 && result.ScannedPages >= opts.MaxPages {
			result.NextKeyMarker = keys[i-1]
			return result, nil
		}
		result.ScannedPages++
		obj := bucket.Objects[key]
		if obj == nil || len(obj.Versions) == 0 {
			continue
		}
		hasObjectVersion, markers := false, 0
		for _, v := range obj.Versions {
			if v == nil {
				continue
			}
			if v.IsDeleteMarker {
				markers++
			} else {
				hasObjectVersion = true
			}
		}
		if markers > 0 && !hasObjectVersion {
			delete(bucket.Objects, key)
			result.DeletedMarkers += markers
		}
	}
	result.Completed = true
	return result, nil
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
