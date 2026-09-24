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
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type fakeS3API struct {
	s3iface.S3API
	listObjectVersionsPagesWithContext func(aws.Context, *awss3.ListObjectVersionsInput, func(*awss3.ListObjectVersionsOutput, bool) bool, ...request.Option) error
	deleteObjectWithContext            func(aws.Context, *awss3.DeleteObjectInput, ...request.Option) (*awss3.DeleteObjectOutput, error)
	deleteObjectsWithContext           func(aws.Context, *awss3.DeleteObjectsInput, ...request.Option) (*awss3.DeleteObjectsOutput, error)
}

func (f *fakeS3API) ListObjectVersionsPagesWithContext(ctx aws.Context, input *awss3.ListObjectVersionsInput, callback func(*awss3.ListObjectVersionsOutput, bool) bool, opts ...request.Option) error {
	return f.listObjectVersionsPagesWithContext(ctx, input, callback, opts...)
}

func (f *fakeS3API) DeleteObjectWithContext(ctx aws.Context, input *awss3.DeleteObjectInput, opts ...request.Option) (*awss3.DeleteObjectOutput, error) {
	return f.deleteObjectWithContext(ctx, input, opts...)
}

func (f *fakeS3API) DeleteObjectsWithContext(ctx aws.Context, input *awss3.DeleteObjectsInput, opts ...request.Option) (*awss3.DeleteObjectsOutput, error) {
	return f.deleteObjectsWithContext(ctx, input, opts...)
}

func TestDeleteObjectPropagatesContext(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	api := &fakeS3API{
		deleteObjectWithContext: func(callCtx aws.Context, _ *awss3.DeleteObjectInput, _ ...request.Option) (*awss3.DeleteObjectOutput, error) {
			if callCtx.Value(contextKey{}) != "value" {
				t.Error("context was not propagated to DeleteObject")
			}
			return &awss3.DeleteObjectOutput{}, nil
		},
	}

	if _, err := (&s3Client{s3API: api}).DeleteObject(ctx, DeleteObjectInput{BucketFqn: "bucket", ObjectKey: "object"}); err != nil {
		t.Fatalf("DeleteObject() returned error: %v", err)
	}
}

func TestDeleteObjectVersionsWithPrefixStreamsPagesAndPropagatesContext(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	var deleted []string

	api := &fakeS3API{}
	api.deleteObjectsWithContext = func(callCtx aws.Context, input *awss3.DeleteObjectsInput, _ ...request.Option) (*awss3.DeleteObjectsOutput, error) {
		if callCtx.Value(contextKey{}) != "value" {
			t.Error("context was not propagated to DeleteObjects")
		}
		for _, object := range input.Delete.Objects {
			deleted = append(deleted, fmt.Sprintf("%s:%s", aws.StringValue(object.Key), aws.StringValue(object.VersionId)))
		}
		return &awss3.DeleteObjectsOutput{}, nil
	}
	api.listObjectVersionsPagesWithContext = func(callCtx aws.Context, input *awss3.ListObjectVersionsInput, callback func(*awss3.ListObjectVersionsOutput, bool) bool, _ ...request.Option) error {
		if callCtx.Value(contextKey{}) != "value" {
			t.Error("context was not propagated to ListObjectVersions")
		}
		if got := aws.StringValue(input.Prefix); got != "shoot/" {
			t.Fatalf("got prefix %q, want %q", got, "shoot/")
		}
		if got := aws.Int64Value(input.MaxKeys); got != 1000 {
			t.Fatalf("got max keys %d, want 1000", got)
		}
		if !callback(&awss3.ListObjectVersionsOutput{
			Versions:      []*awss3.ObjectVersion{{Key: aws.String("shoot/full"), VersionId: aws.String("v1")}},
			DeleteMarkers: []*awss3.DeleteMarkerEntry{{Key: aws.String("shoot/full"), VersionId: aws.String("m1")}},
		}, false) {
			return nil
		}
		if len(deleted) != 2 {
			t.Fatalf("first page was not deleted before requesting the next page")
		}
		callback(&awss3.ListObjectVersionsOutput{
			Versions: []*awss3.ObjectVersion{{Key: aws.String("shoot/incremental"), VersionId: aws.String("v2")}},
		}, true)
		return nil
	}

	client := &s3Client{s3API: api}
	if err := client.DeleteObjectVersionsWithPrefix(ctx, "bucket", "shoot/"); err != nil {
		t.Fatalf("DeleteObjectVersionsWithPrefix() returned error: %v", err)
	}
	want := []string{"shoot/full:v1", "shoot/full:m1", "shoot/incremental:v2"}
	if !reflect.DeepEqual(deleted, want) {
		t.Errorf("deleted versions = %v, want %v", deleted, want)
	}
}

func TestDeleteObjectVersionsWithPrefixStopsOnRetentionError(t *testing.T) {
	deleteCalls := 0
	api := &fakeS3API{}
	api.deleteObjectsWithContext = func(_ aws.Context, input *awss3.DeleteObjectsInput, _ ...request.Option) (*awss3.DeleteObjectsOutput, error) {
		deleteCalls++
		return &awss3.DeleteObjectsOutput{Errors: []*awss3.Error{{
			Code:      aws.String(ErrorCodeS3AccessDenied),
			Key:       input.Delete.Objects[0].Key,
			VersionId: input.Delete.Objects[0].VersionId,
			Message:   aws.String("object is still retained"),
		}}}, nil
	}
	api.listObjectVersionsPagesWithContext = func(_ aws.Context, _ *awss3.ListObjectVersionsInput, callback func(*awss3.ListObjectVersionsOutput, bool) bool, _ ...request.Option) error {
		if callback(&awss3.ListObjectVersionsOutput{Versions: []*awss3.ObjectVersion{
			{Key: aws.String("shoot/object"), VersionId: aws.String("v1")},
			{Key: aws.String("shoot/object"), VersionId: aws.String("v2")},
		}}, true) {
			t.Error("pagination callback should stop after a deletion error")
		}
		return nil
	}

	err := (&s3Client{s3API: api}).DeleteObjectVersionsWithPrefix(context.Background(), "bucket", "shoot/")
	if err == nil || !strings.Contains(err.Error(), ErrorCodeS3AccessDenied) {
		t.Fatalf("got error %v, want retention error", err)
	}
	if deleteCalls != 1 {
		t.Errorf("DeleteObjects called %d times, want 1", deleteCalls)
	}
}
