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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	defaultS3TimeoutInSeconds = 60
	// maxDeleteBatchSize is the maximum number of object identifiers accepted by a single DeleteObjects call.
	maxDeleteBatchSize = 1000
)

type Config struct {
	AccessKeyId     string
	SecretAccessKey string
	EndpointUrl     string
	Region          string
	S3Certificate   []byte
	// +Optional
	CustomHttpClient *http.Client
}

type GetObjectInput struct {
	BucketFqn string
	ObjectKey string
	VersionId *string
}

type GetObjectOutput struct {
	Body      io.ReadCloser
	VersionId *string
}

type UploadObjectInput struct {
	BucketFqn string
	Key       string
	Reader    io.Reader
}

type UploadObjectOutput struct {
	// +Optional
	VersionId *string
}

type DeleteObjectInput struct {
	// Required
	BucketFqn string
	// Required
	ObjectKey string
	// +Optional
	VersionId *string
}

type DeleteObjectOutput struct {
	// +Optional
	DeleteMarker *bool
	// +Optional
	VersionId *string
}

type Client interface {
	DeleteObjectsWithPrefix(ctx context.Context, bucketFQN, prefix string) error
	DeleteObject(ctx context.Context, input DeleteObjectInput) (*DeleteObjectOutput, error)
	DeleteExpiredDeleteMarkers(ctx context.Context, bucketFQN string, opts DeleteExpiredDeleteMarkersOptions) (*DeleteExpiredDeleteMarkersResult, error)
	UploadObject(input UploadObjectInput) (*UploadObjectOutput, error)
	GetObject(input GetObjectInput, opts ...GetObjectOption) (*GetObjectOutput, error)
}

func NewGDCHS3Client(config *Config) (Client, error) {
	// The config needs to be validated since awssdk won't trigger these errors
	// since we fill them as blank if the values aren't set in the config.
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	if config.CustomHttpClient == nil {
		tr := &http.Transport{}
		caCertPool := x509.NewCertPool()
		if config.S3Certificate == nil {
			var err error
			caCertPool, err = x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("certificate pool was empty and failed to fall back to system cert pool")
			}
		}
		caCertPool.AppendCertsFromPEM(config.S3Certificate)
		tr.TLSClientConfig = &tls.Config{RootCAs: caCertPool}
		config.CustomHttpClient = &http.Client{
			Transport: tr,
			Timeout:   defaultS3TimeoutInSeconds * time.Second,
		}
	}

	session, err := session.NewSession(
		&aws.Config{
			Region:     aws.String(config.Region),
			Endpoint:   aws.String(config.EndpointUrl),
			HTTPClient: config.CustomHttpClient,
			Credentials: credentials.NewStaticCredentials(
				config.AccessKeyId,
				config.SecretAccessKey,
				"",
			),
			S3ForcePathStyle:     aws.Bool(true),
			S3Disable100Continue: aws.Bool(true),
		})
	if err != nil {
		return nil, err
	}

	_ = session

	return &s3Client{
		s3API:    s3.New(session),
		uploader: s3manager.NewUploader(session),
		Config:   *config,
	}, nil
}

type s3Client struct {
	s3API    s3iface.S3API
	uploader *s3manager.Uploader
	Config   Config
}

// DeleteObjectsWithPrefix deletes current objects one page at a time. Version IDs
// are deliberately omitted: versioned buckets create delete markers and retain
// noncurrent versions for Object Lock and lifecycle expiration.
// A canceled batch may still complete server-side. With eventually consistent
// listings, a retry can create duplicate delete markers without removing versions.
func (s3Client *s3Client) DeleteObjectsWithPrefix(ctx context.Context, bucketFQN, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("object deletion requires a non-empty prefix")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	input := &s3.ListObjectsV2Input{Bucket: &bucketFQN, Prefix: &prefix, MaxKeys: aws.Int64(1000)}
	var deleteErr error
	err := s3Client.s3API.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, _ bool) bool {
		if deleteErr = ctx.Err(); deleteErr != nil {
			return false
		}
		objects := make([]*s3.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			// Guard against an incompatible endpoint returning keys outside the prefix.
			if object.Key == nil || !strings.HasPrefix(*object.Key, prefix) {
				deleteErr = fmt.Errorf("listing returned object outside prefix %q", prefix)
				return false
			}
			objects = append(objects, &s3.ObjectIdentifier{Key: object.Key})
		}
		if len(objects) == 0 {
			return true
		}
		output, err := s3Client.s3API.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: &bucketFQN,
			Delete: &s3.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			deleteErr = err
			return false
		}
		if output != nil && len(output.Errors) > 0 {
			objectError := output.Errors[0]
			deleteErr = fmt.Errorf("%d objects failed to delete; first: %q: %s: %s",
				len(output.Errors), aws.StringValue(objectError.Key), aws.StringValue(objectError.Code), aws.StringValue(objectError.Message))
			return false
		}
		return true
	})
	if deleteErr != nil {
		return handleAWSError(deleteErr, bucketFQN)
	}
	return handleAWSError(err, bucketFQN)
}

func (s3Client *s3Client) DeleteObject(ctx context.Context, input DeleteObjectInput) (*DeleteObjectOutput, error) {
	s3Input := &s3.DeleteObjectInput{
		Bucket: &input.BucketFqn,
		Key:    &input.ObjectKey,
	}

	if input.VersionId != nil {
		s3Input.VersionId = input.VersionId
	}
	s3output, err := s3Client.s3API.DeleteObjectWithContext(ctx, s3Input)
	if err != nil {
		return nil, err
	}

	output := &DeleteObjectOutput{}
	if s3output.DeleteMarker != nil {
		output.DeleteMarker = s3output.DeleteMarker
	}
	if s3output.VersionId != nil {
		output.VersionId = s3output.VersionId
	}

	return output, nil
}

// DeleteExpiredDeleteMarkersOptions bounds and scopes a delete marker sweep so that a single call never
// has to iterate over an entire (potentially very large) bucket.
type DeleteExpiredDeleteMarkersOptions struct {
	// Prefix optionally restricts the sweep to keys starting with this prefix.
	Prefix string
	// KeyMarker resumes a previous sweep: listing starts after this key (exclusive).
	KeyMarker string
	// MaxPages bounds how many ListObjectVersions pages (up to 1000 entries each) are scanned per call.
	// Zero or negative means unbounded.
	MaxPages int
}

// DeleteExpiredDeleteMarkersResult reports the progress of a (possibly partial) delete marker sweep.
type DeleteExpiredDeleteMarkersResult struct {
	// DeletedMarkers is the number of expired delete markers removed by this call.
	DeletedMarkers int
	// ScannedPages is the number of ListObjectVersions pages consumed by this call.
	ScannedPages int
	// Completed is true when the sweep reached the end of the listing.
	Completed bool
	// NextKeyMarker is set when the sweep stopped before the end of the listing (Completed is false), either
	// because the page budget was exhausted or because the context was canceled after at least one key had
	// been fully processed and its markers deleted. Pass it as KeyMarker to a subsequent call to resume; it
	// always lies strictly after the KeyMarker this call started from, so successive calls make progress.
	NextKeyMarker string
}

// Done reports whether the sweep reached the end of the listing.
func (r *DeleteExpiredDeleteMarkersResult) Done() bool {
	return r != nil && r.Completed
}

// DeleteExpiredDeleteMarkers removes expired object delete markers, i.e. delete markers of keys that have no
// remaining current or noncurrent object version. Keys that still own any object version (for example
// soft-deleted backups within their noncurrent retention window) are never modified.
//
// The listing is processed as a stream: ListObjectVersions returns entries sorted by key with all versions and
// delete markers of a key adjacent, so a key can be judged as soon as the listing moves past it. Only the
// state of the key that is still "open" at a page boundary is carried over, which keeps memory usage
// independent of the bucket size. Deletions are issued per page in batches of up to 1000 identifiers.
//
// With opts.MaxPages > 0 the call stops once that many pages have been scanned and at least one key has been
// fully processed since the start of the call, and returns a NextKeyMarker from which a later call can resume;
// the key that was open at the stop point is re-listed in full on resume. A single key spanning more pages than
// the budget is therefore scanned to its end rather than being resumed from the same marker forever.
//
// When ctx is canceled mid-sweep the error is returned together with the partial result: NextKeyMarker then
// points after the last key whose markers were actually deleted, so callers can persist the progress made so far.
func (s3Client *s3Client) DeleteExpiredDeleteMarkers(ctx context.Context, bucketFQN string, opts DeleteExpiredDeleteMarkersOptions) (*DeleteExpiredDeleteMarkersResult, error) {
	result := &DeleteExpiredDeleteMarkersResult{}
	input := &s3.ListObjectVersionsInput{
		Bucket:  aws.String(bucketFQN),
		MaxKeys: aws.Int64(1000),
	}
	if opts.Prefix != "" {
		input.Prefix = aws.String(opts.Prefix)
	}
	if opts.KeyMarker != "" {
		input.KeyMarker = aws.String(opts.KeyMarker)
	}

	// State of the key that may continue on the next page.
	var (
		openKey        string
		openHasVersion bool
		openMarkers    []*s3.ObjectIdentifier
		// lastClosedKey is the greatest key that has been fully processed; used as the resume marker.
		lastClosedKey string
		// lastFlushedKey is the greatest fully processed key whose markers have been deleted; used as the
		// resume marker when the sweep is interrupted by an error.
		lastFlushedKey string
		pending        []*s3.ObjectIdentifier
		sweepErr       error
	)

	flush := func() error {
		for len(pending) > 0 {
			n := min(len(pending), maxDeleteBatchSize)
			output, err := s3Client.s3API.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketFQN),
				Delete: &s3.Delete{Objects: pending[:n], Quiet: aws.Bool(true)},
			})
			if err != nil {
				return err
			}
			if output != nil && len(output.Errors) > 0 {
				objErr := output.Errors[0]
				return fmt.Errorf("%d expired delete markers failed to delete; first: %q version %q: %s: %s",
					len(output.Errors), aws.StringValue(objErr.Key), aws.StringValue(objErr.VersionId),
					aws.StringValue(objErr.Code), aws.StringValue(objErr.Message))
			}
			result.DeletedMarkers += n
			pending = pending[n:]
		}
		lastFlushedKey = lastClosedKey
		return nil
	}

	closeKey := func(key string, hasVersion bool, markers []*s3.ObjectIdentifier) {
		if !hasVersion {
			pending = append(pending, markers...)
		}
		lastClosedKey = key
	}

	err := s3Client.s3API.ListObjectVersionsPagesWithContext(ctx, input, func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
		if sweepErr = ctx.Err(); sweepErr != nil {
			return false
		}
		result.ScannedPages++

		// Merge the versions and delete markers of this page per key, preserving key order.
		type keyState struct {
			hasVersion bool
			markers    []*s3.ObjectIdentifier
		}
		var orderedKeys []string
		states := map[string]*keyState{}
		get := func(key string) *keyState {
			st, ok := states[key]
			if !ok {
				st = &keyState{}
				states[key] = st
				orderedKeys = append(orderedKeys, key)
			}
			return st
		}
		for _, v := range page.Versions {
			if v != nil && v.Key != nil {
				get(*v.Key).hasVersion = true
			}
		}
		for _, dm := range page.DeleteMarkers {
			if dm != nil && dm.Key != nil && dm.VersionId != nil {
				st := get(*dm.Key)
				st.markers = append(st.markers, &s3.ObjectIdentifier{Key: dm.Key, VersionId: dm.VersionId})
			}
		}
		sort.Strings(orderedKeys)

		for i, key := range orderedKeys {
			st := states[key]
			if openKey != "" && key == openKey {
				// Continuation of the key left open by the previous page.
				openHasVersion = openHasVersion || st.hasVersion
				openMarkers = append(openMarkers, st.markers...)
			} else {
				if openKey != "" {
					closeKey(openKey, openHasVersion, openMarkers)
				}
				openKey, openHasVersion, openMarkers = key, st.hasVersion, st.markers
			}
			// Every key except the greatest one on this page is complete; the greatest key may continue
			// on the next page and therefore stays open.
			if i < len(orderedKeys)-1 || lastPage {
				closeKey(openKey, openHasVersion, openMarkers)
				openKey, openHasVersion, openMarkers = "", false, nil
			}
		}

		if sweepErr = flush(); sweepErr != nil {
			return false
		}
		if lastPage {
			result.Completed = true
			return false
		}
		// Budget exhausted and progress made: resume after the last fully processed key so that the key
		// still open is re-listed in full. Without a closed key (one key spanning every scanned page) keep
		// going, otherwise a resume from the same marker could never make progress.
		if opts.MaxPages > 0 && result.ScannedPages >= opts.MaxPages && lastClosedKey != "" {
			result.NextKeyMarker = lastClosedKey
			return false
		}
		return true
	})
	if sweepErr == nil {
		sweepErr = err
	}
	if sweepErr != nil {
		// Interrupted (context canceled, listing or deletion failure): expose the progress that is safe to
		// resume from, i.e. after the last key whose markers were actually deleted.
		if lastFlushedKey != "" {
			result.NextKeyMarker = lastFlushedKey
		}
		return result, handleAWSError(sweepErr, bucketFQN)
	}
	if result.NextKeyMarker == "" {
		// The listing ended (an empty bucket yields no page callback at all). Close a key left open in case
		// the SDK did not flag the last page; when the budget stopped the sweep, the open key is deliberately
		// left for the resumed call instead.
		if openKey != "" {
			closeKey(openKey, openHasVersion, openMarkers)
			if err := flush(); err != nil {
				if lastFlushedKey != "" {
					result.NextKeyMarker = lastFlushedKey
				}
				return result, handleAWSError(err, bucketFQN)
			}
		}
		result.Completed = true
	}
	return result, nil
}

// Uses PutObject or Multipart Upload awssdk API depending on file size
// default size to use Multipart is 5MB
func (s3Client *s3Client) UploadObject(
	input UploadObjectInput,
) (*UploadObjectOutput, error) {
	r := s3manager.UploadInput{
		Bucket: aws.String(input.BucketFqn),
		Key:    aws.String(input.Key),
		Body:   input.Reader,
	}
	result, err := s3Client.uploader.Upload(&r)
	if err != nil {
		return nil, err
	}
	output := &UploadObjectOutput{VersionId: result.VersionID}
	return output, err
}

type GetObjectOption func(*s3.GetObjectInput)

func (s3Client *s3Client) GetObject(
	input GetObjectInput,
	opts ...GetObjectOption,
) (*GetObjectOutput, error) {
	getObjectInput := s3.GetObjectInput{
		Bucket: &input.BucketFqn,
		Key:    &input.ObjectKey,
	}
	for _, opt := range opts {
		opt(&getObjectInput)
	}
	o, err := s3Client.s3API.GetObject(&getObjectInput)
	if aerr := handleAWSError(err, input.ObjectKey); aerr != nil {
		return &GetObjectOutput{}, aerr
	}
	return &GetObjectOutput{Body: o.Body}, err
}

var (
	errorMissingAccessKeyId     = errors.New("MissingAccessKeyId")
	errorMissingSecretAccessKey = errors.New("MissingSecretAccessKey")
	errorMissingEndpointURL     = errors.New("MissingEndpointURL")
	errorMissingRegion          = errors.New("MissingRegion")
)

func validateConfig(config *Config) error {
	if config.AccessKeyId == "" {
		return errorMissingAccessKeyId
	}
	if config.SecretAccessKey == "" {
		return errorMissingSecretAccessKey
	}
	if config.EndpointUrl == "" {
		return errorMissingEndpointURL
	}
	if config.Region == "" {
		return errorMissingRegion
	}
	return nil
}
