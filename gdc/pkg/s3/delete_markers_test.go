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

// versionEntry models one row of a ListObjectVersions listing (sorted by key, then version).
type versionEntry struct {
	key, version string
	marker       bool
}

// pagedListing serves the given entries in pages of pageSize, honouring KeyMarker and Prefix like S3 does,
// and records the DeleteObjects calls it receives.
type pagedListing struct {
	entries  []versionEntry
	pageSize int
	listed   []string // key markers of each ListObjectVersions request
	deleted  []string // "key@version" in deletion order
	batches  []int
	deleteFn func(*awss3.DeleteObjectsInput) (*awss3.DeleteObjectsOutput, error)
}

func (p *pagedListing) api() *fakeS3API {
	return &fakeS3API{
		listObjectVersionsPagesFn: func(_ aws.Context, input *awss3.ListObjectVersionsInput, fn func(*awss3.ListObjectVersionsOutput, bool) bool, _ ...request.Option) error {
			p.listed = append(p.listed, aws.StringValue(input.KeyMarker))
			var filtered []versionEntry
			for _, e := range p.entries {
				if e.key > aws.StringValue(input.KeyMarker) && strings.HasPrefix(e.key, aws.StringValue(input.Prefix)) {
					filtered = append(filtered, e)
				}
			}
			for start := 0; start < len(filtered) || start == 0; start += p.pageSize {
				end := min(start+p.pageSize, len(filtered))
				out := &awss3.ListObjectVersionsOutput{}
				for _, e := range filtered[start:end] {
					if e.marker {
						out.DeleteMarkers = append(out.DeleteMarkers, &awss3.DeleteMarkerEntry{Key: aws.String(e.key), VersionId: aws.String(e.version)})
					} else {
						out.Versions = append(out.Versions, &awss3.ObjectVersion{Key: aws.String(e.key), VersionId: aws.String(e.version)})
					}
				}
				if !fn(out, end >= len(filtered)) || end >= len(filtered) {
					return nil
				}
			}
			return nil
		},
		deleteObjectsFn: func(_ aws.Context, input *awss3.DeleteObjectsInput, _ ...request.Option) (*awss3.DeleteObjectsOutput, error) {
			if p.deleteFn != nil {
				return p.deleteFn(input)
			}
			p.batches = append(p.batches, len(input.Delete.Objects))
			for _, obj := range input.Delete.Objects {
				if obj.VersionId == nil {
					return nil, fmt.Errorf("delete marker sweep must always delete by version id, got key %q without version", aws.StringValue(obj.Key))
				}
				p.deleted = append(p.deleted, fmt.Sprintf("%s@%s", aws.StringValue(obj.Key), aws.StringValue(obj.VersionId)))
			}
			return &awss3.DeleteObjectsOutput{}, nil
		},
	}
}

func TestDeleteExpiredDeleteMarkers_StreamsAcrossPageBoundaries(t *testing.T) {
	// Page size 3 forces "shoot-a/live-snap" (marker on page 1, retained version on page 2) and
	// "shoot-b/expired-1" (two markers split across pages 2 and 3) to be carried over page boundaries.
	listing := &pagedListing{pageSize: 3, entries: []versionEntry{
		{"shoot-a/live-snap", "dm-live", true},
		{"shoot-a/live-snap", "v-noncurrent", false},
		{"shoot-a/other", "dm-other", true}, // page 1 ends here
		{"shoot-b/expired-1", "dm-exp-1", true},
		{"shoot-b/expired-1", "dm-exp-2", true},
		{"shoot-c/expired-2", "dm-exp-3", true}, // page 2 ends here
		{"shoot-d/active", "v-current", false},
	}}

	result, err := (&s3Client{s3API: listing.api()}).DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn", DeleteExpiredDeleteMarkersOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Done() || result.ScannedPages != 3 || result.DeletedMarkers != 4 {
		t.Fatalf("unexpected result: %+v", result)
	}
	want := []string{"shoot-a/other@dm-other", "shoot-b/expired-1@dm-exp-1", "shoot-b/expired-1@dm-exp-2", "shoot-c/expired-2@dm-exp-3"}
	if diff := cmp.Diff(want, listing.deleted); diff != "" {
		t.Fatalf("deleted markers mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteExpiredDeleteMarkers_PageBudgetAndResume(t *testing.T) {
	var entries []versionEntry
	for i := range 7 {
		entries = append(entries, versionEntry{fmt.Sprintf("shoot/key-%d", i), fmt.Sprintf("dm-%d", i), true})
	}
	// key-3 still has a retained version and must survive every pass.
	entries = append(entries[:4], append([]versionEntry{{"shoot/key-3", "v-3", false}}, entries[4:]...)...)
	listing := &pagedListing{pageSize: 2, entries: entries}
	c := &s3Client{s3API: listing.api()}

	var keyMarker string
	var passes []DeleteExpiredDeleteMarkersResult
	for range 10 {
		result, err := c.DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn", DeleteExpiredDeleteMarkersOptions{KeyMarker: keyMarker, MaxPages: 2})
		if err != nil {
			t.Fatalf("pass %d: unexpected error: %v", len(passes), err)
		}
		passes = append(passes, *result)
		if result.ScannedPages > 2 {
			t.Fatalf("pass %d exceeded page budget: %+v", len(passes), result)
		}
		if result.Done() {
			break
		}
		if result.NextKeyMarker <= keyMarker {
			t.Fatalf("pass %d made no progress: %+v", len(passes), result)
		}
		keyMarker = result.NextKeyMarker
	}
	if last := passes[len(passes)-1]; !last.Done() {
		t.Fatalf("sweep did not complete within 10 passes: %+v", passes)
	}
	if len(passes) < 2 {
		t.Fatalf("expected the budget to split the sweep into multiple passes, got %d", len(passes))
	}

	var want []string
	for i := range 7 {
		if i != 3 {
			want = append(want, fmt.Sprintf("shoot/key-%d@dm-%d", i, i))
		}
	}
	if diff := cmp.Diff(want, listing.deleted); diff != "" {
		t.Fatalf("deleted markers mismatch (-want +got):\n%s", diff)
	}
	// Each resume must start after a fully processed key; the key open at the stop point is re-listed.
	for i := 1; i < len(listing.listed); i++ {
		if listing.listed[i] != passes[i-1].NextKeyMarker {
			t.Fatalf("pass %d listed from %q, want resume marker %q", i, listing.listed[i], passes[i-1].NextKeyMarker)
		}
	}
}

func TestDeleteExpiredDeleteMarkers_KeySpanningWholeBudgetIsNeverSplit(t *testing.T) {
	// "big" has 5 delete markers and a retained version spread over 6 single-entry pages; with a budget of 2
	// pages, no key closes within the budget. The sweep must keep reading until the key closes (when
	// "mid/expired" shows up on page 7) rather than stop without progress, must not delete big's markers (it
	// still has a version), and must then resume after "big" so that the remaining keys are cleaned up.
	entries := []versionEntry{
		{"big", "dm-1", true}, {"big", "dm-2", true}, {"big", "dm-3", true}, {"big", "dm-4", true}, {"big", "dm-5", true},
		{"big", "v-1", false},
		{"mid/expired", "dm-m", true},
		{"zzz/expired", "dm-z", true},
	}
	listing := &pagedListing{pageSize: 1, entries: entries}
	c := &s3Client{s3API: listing.api()}

	first, err := c.DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn", DeleteExpiredDeleteMarkersOptions{MaxPages: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first.Done() || first.NextKeyMarker != "big" || first.DeletedMarkers != 0 || first.ScannedPages != 7 {
		t.Fatalf("first pass should stop right after closing the oversized key without deleting anything: %+v", first)
	}
	second, err := c.DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn", DeleteExpiredDeleteMarkersOptions{KeyMarker: first.NextKeyMarker, MaxPages: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !second.Done() || second.DeletedMarkers != 2 {
		t.Fatalf("second pass should complete the sweep: %+v", second)
	}
	if diff := cmp.Diff([]string{"mid/expired@dm-m", "zzz/expired@dm-z"}, listing.deleted); diff != "" {
		t.Fatalf("deleted markers mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteExpiredDeleteMarkers_EmptyBucketCompletes(t *testing.T) {
	listing := &pagedListing{pageSize: 1000}
	result, err := (&s3Client{s3API: listing.api()}).DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn", DeleteExpiredDeleteMarkersOptions{MaxPages: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Done() || result.DeletedMarkers != 0 || result.NextKeyMarker != "" {
		t.Fatalf("unexpected result for empty bucket: %+v", result)
	}
}

func TestDeleteExpiredDeleteMarkers_PrefixScopesListing(t *testing.T) {
	listing := &pagedListing{pageSize: 1000, entries: []versionEntry{
		{"other/expired", "dm-o", true},
		{"shoot/expired", "dm-s", true},
	}}
	api := listing.api()
	var gotPrefix string
	inner := api.listObjectVersionsPagesFn
	api.listObjectVersionsPagesFn = func(ctx aws.Context, input *awss3.ListObjectVersionsInput, fn func(*awss3.ListObjectVersionsOutput, bool) bool, opts ...request.Option) error {
		gotPrefix = aws.StringValue(input.Prefix)
		return inner(ctx, input, fn, opts...)
	}
	result, err := (&s3Client{s3API: api}).DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn", DeleteExpiredDeleteMarkersOptions{Prefix: "shoot/"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPrefix != "shoot/" || result.DeletedMarkers != 1 {
		t.Fatalf("prefix=%q result=%+v", gotPrefix, result)
	}
	if diff := cmp.Diff([]string{"shoot/expired@dm-s"}, listing.deleted); diff != "" {
		t.Fatalf("deleted markers mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteExpiredDeleteMarkers_BatchesOver1000AndSurfacesItemErrors(t *testing.T) {
	t.Run("batches more than 1000 expired delete markers from a single page", func(t *testing.T) {
		var entries []versionEntry
		for i := range 1205 {
			entries = append(entries, versionEntry{fmt.Sprintf("shoot/key-%04d", i), fmt.Sprintf("dm-%04d", i), true})
		}
		listing := &pagedListing{pageSize: 2000, entries: entries}
		result, err := (&s3Client{s3API: listing.api()}).DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn", DeleteExpiredDeleteMarkersOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.DeletedMarkers != 1205 {
			t.Fatalf("unexpected result: %+v", result)
		}
		if diff := cmp.Diff([]int{1000, 205}, listing.batches); diff != "" {
			t.Fatalf("batch sizes mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("returns error when DeleteObjects reports per-object failure", func(t *testing.T) {
		listing := &pagedListing{pageSize: 1000, entries: []versionEntry{{"shoot/key", "dm-1", true}}}
		listing.deleteFn = func(*awss3.DeleteObjectsInput) (*awss3.DeleteObjectsOutput, error) {
			return &awss3.DeleteObjectsOutput{Errors: []*awss3.Error{{
				Key: aws.String("shoot/key"), VersionId: aws.String("dm-1"),
				Code: aws.String("InternalError"), Message: aws.String("transient backend failure"),
			}}}, nil
		}
		_, err := (&s3Client{s3API: listing.api()}).DeleteExpiredDeleteMarkers(context.Background(), "bucket-fqn", DeleteExpiredDeleteMarkersOptions{})
		if err == nil || !strings.Contains(err.Error(), "InternalError") {
			t.Fatalf("got error %v, want InternalError", err)
		}
	})

	t.Run("stops when the context is canceled", func(t *testing.T) {
		listing := &pagedListing{pageSize: 1, entries: []versionEntry{{"a", "dm", true}, {"b", "dm", true}}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := (&s3Client{s3API: listing.api()}).DeleteExpiredDeleteMarkers(ctx, "bucket-fqn", DeleteExpiredDeleteMarkersOptions{})
		if err == nil {
			t.Fatal("expected context cancellation error")
		}
		if len(listing.deleted) != 0 || result.NextKeyMarker != "" || result.Done() {
			t.Fatalf("no deletions or progress expected after immediate cancellation, got %v / %+v", listing.deleted, result)
		}
	})

	t.Run("reports resumable progress when canceled mid-sweep", func(t *testing.T) {
		listing := &pagedListing{pageSize: 1, entries: []versionEntry{{"a", "dm-a", true}, {"b", "dm-b", true}, {"c", "dm-c", true}, {"d", "dm-d", true}}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Cancel when the third page arrives: by then "a" has been closed and flushed (while processing page
		// 2) and "b" is still open, so the sweep must report "a" as the safe resume point.
		api := listing.api()
		inner := api.listObjectVersionsPagesFn
		api.listObjectVersionsPagesFn = func(ctx aws.Context, input *awss3.ListObjectVersionsInput, fn func(*awss3.ListObjectVersionsOutput, bool) bool, opts ...request.Option) error {
			pages := 0
			return inner(ctx, input, func(out *awss3.ListObjectVersionsOutput, last bool) bool {
				pages++
				if pages == 3 {
					cancel()
				}
				return fn(out, last)
			}, opts...)
		}
		result, err := (&s3Client{s3API: api}).DeleteExpiredDeleteMarkers(ctx, "bucket-fqn", DeleteExpiredDeleteMarkersOptions{})
		if err == nil {
			t.Fatal("expected context cancellation error")
		}
		if result.Done() || result.NextKeyMarker != "a" {
			t.Fatalf("expected resume marker after the last flushed key, got %+v", result)
		}
		if diff := cmp.Diff([]string{"a@dm-a"}, listing.deleted); diff != "" {
			t.Fatalf("deleted markers mismatch (-want +got):\n%s", diff)
		}
	})
}
