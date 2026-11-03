// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func testS3Client(t *testing.T, server *httptest.Server) Client {
	t.Helper()
	c, err := NewGDCHS3Client(&Config{AccessKeyId: "test", SecretAccessKey: "test", Region: "test",
		EndpointUrl: server.URL, CustomHttpClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Run through the real AWS SDK's pagination and XML protocol. The server models
// current objects becoming hidden by delete markers, while versions remain.
func TestDeleteObjectsWithPrefixPaginationAndRetry(t *testing.T) {
	for _, failedObjects := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("failedObjects=%d", failedObjects), func(t *testing.T) {
			var mu sync.Mutex
			current := map[string]bool{"shoot/a": true, "shoot/b": true, "shoot/c": true, "shoot/d": true, "other/a": true, "shoot-other/a": true}
			markers := map[string]int{}
			listCalls, deleteCalls := 0, 0
			failOnce := failedObjects > 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/xml")
				if r.URL.Path != "/bucket" {
					t.Errorf("unexpected bucket path: %s", r.URL.Path)
				}
				switch r.Method {
				case http.MethodGet:
					listCalls++
					q := r.URL.Query()
					if q.Get("prefix") != "shoot/" || q.Get("list-type") != "2" || q.Get("max-keys") != "1000" {
						t.Errorf("unexpected list parameters: %v", q)
					}
					if q.Get("continuation-token") != "" && deleteCalls == 0 {
						t.Error("listed next page before deleting the first page")
					}
					var keys []string
					for key, visible := range current {
						if visible && strings.HasPrefix(key, q.Get("prefix")) && key > q.Get("continuation-token") {
							keys = append(keys, key)
						}
					}
					sort.Strings(keys)
					truncated := len(keys) > 2
					if truncated {
						keys = keys[:2]
					}
					fmt.Fprintf(w, "<ListBucketResult><IsTruncated>%v</IsTruncated>", truncated)
					for _, key := range keys {
						fmt.Fprintf(w, "<Contents><Key>%s</Key></Contents>", key)
					}
					if truncated {
						fmt.Fprintf(w, "<NextContinuationToken>%s</NextContinuationToken>", keys[len(keys)-1])
					}
					fmt.Fprint(w, "</ListBucketResult>")
				case http.MethodPost:
					deleteCalls++
					if !r.URL.Query().Has("delete") {
						t.Error("expected multi-object delete")
					}
					var input struct {
						Objects []struct {
							Key       string
							VersionID *string
						} `xml:"Object"`
					}
					if err := xml.NewDecoder(r.Body).Decode(&input); err != nil {
						t.Error(err)
					}
					if len(input.Objects) == 0 || len(input.Objects) > 1000 {
						t.Errorf("unexpected batch size %d", len(input.Objects))
					}
					fmt.Fprint(w, "<DeleteResult>")
					for _, object := range input.Objects {
						if object.VersionID != nil || !strings.HasPrefix(object.Key, "shoot/") {
							t.Errorf("unexpected permanent or out-of-prefix deletion: %+v", object)
						}
						if failOnce && (object.Key == "shoot/b" || (failedObjects == 2 && object.Key == "shoot/a")) {
							fmt.Fprintf(w, "<Error><Key>%s</Key><Code>AccessDenied</Code><Message>injected failure</Message></Error>", object.Key)
							continue
						}
						current[object.Key] = false
						markers[object.Key]++
					}
					failOnce = false
					fmt.Fprint(w, "</DeleteResult>")
				default:
					t.Errorf("unexpected S3 request: %s", r.Method)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			c := testS3Client(t, server)
			err := c.DeleteObjectsWithPrefix(context.Background(), "bucket", "shoot/")
			if failedObjects > 0 {
				firstFailedKey := "shoot/b"
				if failedObjects == 2 {
					firstFailedKey = "shoot/a"
				}
				expectedError := fmt.Sprintf("%d objects failed to delete; first: %q: AccessDenied: injected failure", failedObjects, firstFailedKey)
				if err == nil || err.Error() != expectedError {
					t.Fatalf("expected per-object error, got %v", err)
				}
				mu.Lock()
				calls := listCalls
				mu.Unlock()
				if calls != 1 {
					t.Fatalf("pagination should stop on delete failure, got %d list calls", calls)
				}
				if err := c.DeleteObjectsWithPrefix(context.Background(), "bucket", "shoot/"); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			// Once the listing reflects the deletions, an empty-prefix retry must
			// not add more delete markers. This server models consistent listings.
			if err := c.DeleteObjectsWithPrefix(context.Background(), "bucket", "shoot/"); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			for key, visible := range current {
				if strings.HasPrefix(key, "shoot/") {
					if visible || markers[key] != 1 {
						t.Errorf("key %s: visible=%v markers=%d", key, visible, markers[key])
					}
				} else if !visible || markers[key] != 0 {
					t.Errorf("unrelated key %s was changed", key)
				}
			}
		})
	}
}

func TestS3DeletionCancellation(t *testing.T) {
	for _, phase := range []string{"list", "batch delete", "single delete"} {
		t.Run(phase, func(t *testing.T) {
			reached := make(chan struct{})
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if phase == "batch delete" && r.Method == http.MethodGet {
					fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>shoot/a</Key></Contents></ListBucketResult>")
					return
				}
				// Consume the POST body so the server can observe connection closure.
				_, _ = io.Copy(io.Discard, r.Body)
				close(reached)
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer func() { close(release); server.Close() }()
			c := testS3Client(t, server)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				if phase == "single delete" {
					_, err := c.DeleteObject(ctx, DeleteObjectInput{BucketFqn: "bucket", ObjectKey: "shoot/a"})
					result <- err
				} else {
					result <- c.DeleteObjectsWithPrefix(ctx, "bucket", "shoot/")
				}
			}()
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("S3 request did not reach server")
			}
			cancel()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("canceled S3 request succeeded")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("S3 request did not stop after cancellation")
			}
		})
	}
}

func TestDeleteObjectsWithPrefixRequiresPrefix(t *testing.T) {
	c := &s3Client{} // Must reject the prefix before making an API call.
	if err := c.DeleteObjectsWithPrefix(context.Background(), "bucket", ""); err == nil {
		t.Fatal("empty prefix accepted")
	}
}

func TestDeleteObjectsWithPrefixListErrors(t *testing.T) {
	for _, failure := range []string{"out-of-prefix object", "missing bucket"} {
		t.Run(failure, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("delete called after invalid listing")
				}
				if failure == "missing bucket" {
					w.WriteHeader(http.StatusNotFound)
					fmt.Fprint(w, "<Error><Code>NoSuchBucket</Code><Message>bucket gone</Message></Error>")
					return
				}
				fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>other/a</Key></Contents></ListBucketResult>")
			}))
			defer server.Close()
			err := testS3Client(t, server).DeleteObjectsWithPrefix(context.Background(), "bucket", "shoot/")
			if err == nil {
				t.Fatal("invalid listing succeeded")
			}
			if failure == "missing bucket" && !errors.Is(err, ErrorS3NoSuchBucket) {
				t.Fatalf("missing bucket error not preserved: %v", err)
			}
		})
	}
}
