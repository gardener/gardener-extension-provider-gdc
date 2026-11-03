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
	ListObjectsV2Pages(bucketFQN string) ([]string, error)
	DeleteObject(input DeleteObjectInput) (*DeleteObjectOutput, error)
	DeleteExpiredDeleteMarkers(ctx context.Context, bucketFQN string) error
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

func (s3Client *s3Client) ListObjectsV2Pages(bucketFQN string) ([]string, error) {
	var objectKeys []string
	input := &s3.ListObjectsV2Input{Bucket: &bucketFQN}
	err := s3Client.s3API.ListObjectsV2Pages(
		input,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, o := range page.Contents {
				objectKeys = append(objectKeys, *o.Key)
			}
			// Continue iterating until there are no more pages.
			return true
		})
	if err != nil {
		return nil, err
	}

	return objectKeys, nil
}

func (s3Client *s3Client) DeleteObject(input DeleteObjectInput) (*DeleteObjectOutput, error) {
	s3Input := &s3.DeleteObjectInput{
		Bucket: &input.BucketFqn,
		Key:    &input.ObjectKey,
	}

	if input.VersionId != nil {
		s3Input.VersionId = input.VersionId
	}
	s3output, err := s3Client.s3API.DeleteObject(s3Input)
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

// DeleteExpiredDeleteMarkers lists all object versions and delete markers in the bucket and deletes
// only expired object delete markers — i.e., delete markers for keys that have zero remaining
// current or noncurrent object versions. Keys that still have any object version (such as soft-deleted
// backups within their noncurrentExpiration retention window) are never modified.
func (s3Client *s3Client) DeleteExpiredDeleteMarkers(ctx context.Context, bucketFQN string) error {
	keysWithVersions := make(map[string]struct{})
	deleteMarkersByKey := make(map[string][]string)

	input := &s3.ListObjectVersionsInput{
		Bucket:  aws.String(bucketFQN),
		MaxKeys: aws.Int64(1000),
	}
	err := s3Client.s3API.ListObjectVersionsPagesWithContext(
		ctx,
		input,
		func(page *s3.ListObjectVersionsOutput, _ bool) bool {
			for _, v := range page.Versions {
				if v != nil && v.Key != nil {
					keysWithVersions[*v.Key] = struct{}{}
				}
			}
			for _, dm := range page.DeleteMarkers {
				if dm != nil && dm.Key != nil && dm.VersionId != nil {
					deleteMarkersByKey[*dm.Key] = append(deleteMarkersByKey[*dm.Key], *dm.VersionId)
				}
			}
			return true
		},
	)
	if err != nil {
		return err
	}

	expiredKeys := make([]string, 0)
	for key := range deleteMarkersByKey {
		if _, hasVersion := keysWithVersions[key]; !hasVersion {
			expiredKeys = append(expiredKeys, key)
		}
	}
	if len(expiredKeys) == 0 {
		return nil
	}
	sort.Strings(expiredKeys)

	identifiers := make([]*s3.ObjectIdentifier, 0, len(expiredKeys))
	for _, key := range expiredKeys {
		for _, versionID := range deleteMarkersByKey[key] {
			identifiers = append(identifiers, &s3.ObjectIdentifier{
				Key:       aws.String(key),
				VersionId: aws.String(versionID),
			})
		}
	}

	const maxDeleteBatchSize = 1000
	for start := 0; start < len(identifiers); start += maxDeleteBatchSize {
		end := start + maxDeleteBatchSize
		if end > len(identifiers) {
			end = len(identifiers)
		}
		output, err := s3Client.s3API.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketFQN),
			Delete: &s3.Delete{
				Objects: identifiers[start:end],
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return err
		}
		if output != nil && len(output.Errors) > 0 {
			objErr := output.Errors[0]
			return fmt.Errorf("failed to delete expired delete marker %q version %q: %s: %s",
				aws.StringValue(objErr.Key),
				aws.StringValue(objErr.VersionId),
				aws.StringValue(objErr.Code),
				aws.StringValue(objErr.Message),
			)
		}
	}

	return nil
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
