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
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	// AWS specific error codes based on https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
	ErrorCodeS3AccessDenied                   = "AccessDenied"
	ErrorCodeS3NotFound                       = "NotFound"
	ErrorCodeS3BucketNoSuchBucket             = s3.ErrCodeNoSuchBucket
	ErrorCodeS3NoSuchBucketPolicy             = "NoSuchBucketPolicy"
	ErrorCodeS3NoSuchCORSConfiguration        = "NoSuchCORSConfiguration"
	ErrorCodeS3NoSuchTagSet                   = "NoSuchTagSet"
	ErrorCodeS3NoSuchLifecycleConfiguration   = "NoSuchLifecycleConfiguration"
	ErrorCodeS3BucketNotEmpty                 = "BucketNotEmpty"
	ErrorCodeS3IllegalVersioningConfiguration = "IllegalVersioningConfigurationException"
	ErrorCodeS3NoSuchUpload                   = s3.ErrCodeNoSuchUpload
	ErrorCodeS3NoSuchKey                      = s3.ErrCodeNoSuchKey
	ErrorCodeMethodNotAllowed                 = "MethodNotAllowed"
	// Ceph radosgw error
	ErrorCodeNoSuchTagSet = "NoSuchTagSetError"
)

var (
	// GDCH Specific Errors
	ErrorMissingAccessKeyId     = errors.New("MissingAccessKeyId")
	ErrorMissingSecretAccessKey = errors.New("MissingSecretAccessKey")
	ErrorMissingEndpointURL     = errors.New("MissingEndpointURL")
	ErrorMissingRegion          = errors.New("MissingRegion")
	ErrorMissingCertificate     = errors.New("MissingCertificate")

	// AWS specific errors for AWS error codes.
	ErrorS3AccessDenied                   = errors.New(ErrorCodeS3AccessDenied)
	ErrorS3NotFound                       = errors.New(ErrorCodeS3NotFound)
	ErrorS3NoSuchBucket                   = errors.New(ErrorCodeS3BucketNoSuchBucket)
	ErrorS3NoSuchBucketPolicy             = errors.New(ErrorCodeS3NoSuchBucketPolicy)
	ErrorS3NoSuchCORSConfiguration        = errors.New(ErrorCodeS3NoSuchCORSConfiguration)
	ErrorS3NoSuchTagSet                   = errors.New(ErrorCodeS3NoSuchTagSet)
	ErrorS3NoSuchLifecycleConfiguration   = errors.New(ErrorCodeS3NoSuchLifecycleConfiguration)
	ErrorS3BucketNotEmpty                 = errors.New(ErrorCodeS3BucketNotEmpty)
	ErrorS3IllegalVersioningConfiguration = errors.New(ErrorCodeS3IllegalVersioningConfiguration)
	ErrorS3NoSuchUpload                   = errors.New(ErrorCodeS3NoSuchUpload)
	ErrorS3NoSuchKey                      = errors.New(ErrorCodeS3NoSuchKey)
	ErrorS3MethodNotAllowed               = errors.New(ErrorCodeMethodNotAllowed)

	// Ceph radosgw error
	ErrorNoSuchTagSet = errors.New(ErrorCodeNoSuchTagSet)
)

// Special error handling for the specific aws error codes
// to give a more direct error message.
func handleAWSError(err error, input string) error {
	if err == nil {
		return nil
	}
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case ErrorCodeS3AccessDenied:
			return fmt.Errorf(
				"%w: 403 access denied for bucket %q",
				ErrorS3AccessDenied,
				input,
			)
		case ErrorCodeS3BucketNoSuchBucket:
			return fmt.Errorf(
				"%w: bucket %q does not exist",
				ErrorS3NoSuchBucket,
				input,
			)
		case ErrorCodeS3NoSuchBucketPolicy:
			return fmt.Errorf(
				"%w: bucket %q has no such bucket policy",
				ErrorS3NoSuchBucketPolicy,
				input,
			)
		case ErrorCodeS3NoSuchCORSConfiguration:
			return fmt.Errorf(
				"%w: bucket %q has no such CORS configuration",
				ErrorS3NoSuchCORSConfiguration,
				input,
			)
		case ErrorCodeS3NoSuchTagSet:
			return fmt.Errorf(
				"%w: bucket %q has no such Tag set",
				ErrorS3NoSuchTagSet,
				input,
			)
		case ErrorCodeS3NoSuchLifecycleConfiguration:
			return fmt.Errorf(
				"%w: bucket %q has no such lifecycle configuration",
				ErrorS3NoSuchLifecycleConfiguration,
				input,
			)
		case ErrorCodeS3BucketNotEmpty:
			return fmt.Errorf(
				"%w: The bucket %q that you tried to delete is not empty",
				ErrorS3BucketNotEmpty,
				input,
			)
		case ErrorCodeNoSuchTagSet:
			return fmt.Errorf(
				"%w: bucket %q has no such Tag set",
				ErrorNoSuchTagSet,
				input,
			)
		case ErrorCodeS3NotFound:
			return fmt.Errorf(
				"%w: resource %q does not exist",
				ErrorS3NotFound,
				input,
			)
		case ErrorCodeS3IllegalVersioningConfiguration:
			return fmt.Errorf(
				"%w: bucket versioning configuration is invalid: %v",
				ErrorS3IllegalVersioningConfiguration,
				input,
			)
		case ErrorCodeS3NoSuchUpload:
			return fmt.Errorf(
				"%w: upload %s does not exist",
				ErrorS3NoSuchUpload,
				input,
			)
		case ErrorCodeS3NoSuchKey:
			return fmt.Errorf(
				"%w: object %q does not exist",
				ErrorS3NoSuchKey,
				input,
			)
		case ErrorCodeMethodNotAllowed:
			return fmt.Errorf(
				"%w: method not allowed on object %q, likely because it was just deleted",
				ErrorS3MethodNotAllowed,
				input,
			)
		}
	}
	return err
}
