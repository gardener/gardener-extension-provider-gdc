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

package s3

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/google/go-cmp/cmp"
)

type fakeS3API struct {
	s3iface.S3API
	listObjectVersionsPagesFn func(aws.Context, *awss3.ListObjectVersionsInput, func(*awss3.ListObjectVersionsOutput, bool) bool, ...request.Option) error
	deleteObjectsFn           func(aws.Context, *awss3.DeleteObjectsInput, ...request.Option) (*awss3.DeleteObjectsOutput, error)
}

func (f *fakeS3API) ListObjectVersionsPagesWithContext(ctx aws.Context, input *awss3.ListObjectVersionsInput, fn func(*awss3.ListObjectVersionsOutput, bool) bool, opts ...request.Option) error {
	return f.listObjectVersionsPagesFn(ctx, input, fn, opts...)
}

func (f *fakeS3API) DeleteObjectsWithContext(ctx aws.Context, input *awss3.DeleteObjectsInput, opts ...request.Option) (*awss3.DeleteObjectsOutput, error) {
	return f.deleteObjectsFn(ctx, input, opts...)
}

func TestDeleteExpiredDeleteMarkers_CrossPageSafetyAndCleanup(t *testing.T) {
	var deleted []string
	api := &fakeS3API{
		listObjectVersionsPagesFn: func(_ aws.Context, _ *awss3.ListObjectVersionsInput, fn func(*awss3.ListObjectVersionsOutput, bool) bool, _ ...request.Option) error {
			// Page 1: "shoot-a/live-snap" has a DeleteMarker here, while its noncurrent ObjectVersion is on Page 2.
			// "shoot-b/expired-1" has two DeleteMarkers and zero ObjectVersions anywhere.
			cont := fn(&awss3.ListObjectVersionsOutput{
				DeleteMarkers: []*awss3.DeleteMarkerEntry{
					{Key: aws.String("shoot-a/live-snap"), VersionId: aws.String("dm-live")},
					{Key: aws.String("shoot-b/expired-1"), VersionId: aws.String("dm-exp-1")},
					{Key: aws.String("shoot-b/expired-1"), VersionId: aws.String("dm-exp-2")},
				},
			}, false)
			if !cont {
				return nil
			}
			// Page 2: noncurrent ObjectVersion for "shoot-a/live-snap", plus another expired DeleteMarker "shoot-c/expired-2".
			fn(&awss3.ListObjectVersionsOutput{
				Versions: []*awss3.ObjectVersion{
					{Key: aws.String("shoot-a/live-snap"), VersionId: aws.String("v-noncurrent")},
					{Key: aws.String("shoot-d/active"), VersionId: aws.String("v-current")},
				},
				DeleteMarkers: []*awss3.DeleteMarkerEntry{
					{Key: aws.String("shoot-c/expired-2"), VersionId: aws.String("dm-exp-3")},
				},
			}, true)
			return nil
		},
		deleteObjectsFn: func(_ aws.Context, input *awss3.DeleteObjectsInput, _ ...request.Option) (*awss3.DeleteObjectsOutput, error) {
			for _, obj := range input.Delete.Objects {
				deleted = append(deleted, fmt.Sprintf("%s@%s", aws.StringValue(obj.Key), aws.StringValue(obj.VersionId)))
			}
			return &awss3.DeleteObjectsOutput{}, nil
		},
	}

	cl := &s3Client{s3API: api}
	if err := cl.DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{
		"shoot-b/expired-1@dm-exp-1",
		"shoot-b/expired-1@dm-exp-2",
		"shoot-c/expired-2@dm-exp-3",
	}
	if diff := cmp.Diff(want, deleted); diff != "" {
		t.Fatalf("deleted markers mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteExpiredDeleteMarkers_BatchesOver1000AndSurfacesItemErrors(t *testing.T) {
	t.Run("batches more than 1000 expired delete markers", func(t *testing.T) {
		var batchSizes []int
		api := &fakeS3API{
			listObjectVersionsPagesFn: func(_ aws.Context, _ *awss3.ListObjectVersionsInput, fn func(*awss3.ListObjectVersionsOutput, bool) bool, _ ...request.Option) error {
				markers := make([]*awss3.DeleteMarkerEntry, 1205)
				for i := range markers {
					markers[i] = &awss3.DeleteMarkerEntry{
						Key:       aws.String(fmt.Sprintf("shoot/key-%04d", i)),
						VersionId: aws.String(fmt.Sprintf("dm-%04d", i)),
					}
				}
				fn(&awss3.ListObjectVersionsOutput{DeleteMarkers: markers}, true)
				return nil
			},
			deleteObjectsFn: func(_ aws.Context, input *awss3.DeleteObjectsInput, _ ...request.Option) (*awss3.DeleteObjectsOutput, error) {
				batchSizes = append(batchSizes, len(input.Delete.Objects))
				return &awss3.DeleteObjectsOutput{}, nil
			},
		}

		cl := &s3Client{s3API: api}
		if err := cl.DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if diff := cmp.Diff([]int{1000, 205}, batchSizes); diff != "" {
			t.Fatalf("batch sizes mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("returns error when DeleteObjects reports per-object failure", func(t *testing.T) {
		api := &fakeS3API{
			listObjectVersionsPagesFn: func(_ aws.Context, _ *awss3.ListObjectVersionsInput, fn func(*awss3.ListObjectVersionsOutput, bool) bool, _ ...request.Option) error {
				fn(&awss3.ListObjectVersionsOutput{
					DeleteMarkers: []*awss3.DeleteMarkerEntry{
						{Key: aws.String("shoot/key"), VersionId: aws.String("dm-1")},
					},
				}, true)
				return nil
			},
			deleteObjectsFn: func(_ aws.Context, _ *awss3.DeleteObjectsInput, _ ...request.Option) (*awss3.DeleteObjectsOutput, error) {
				return &awss3.DeleteObjectsOutput{
					Errors: []*awss3.Error{
						{
							Key:       aws.String("shoot/key"),
							VersionId: aws.String("dm-1"),
							Code:      aws.String("InternalError"),
							Message:   aws.String("transient backend failure"),
						},
					},
				}, nil
			},
		}

		cl := &s3Client{s3API: api}
		err := cl.DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn")
		if err == nil || !strings.Contains(err.Error(), "InternalError") {
			t.Fatalf("got error %v, want InternalError", err)
		}
	})
}
