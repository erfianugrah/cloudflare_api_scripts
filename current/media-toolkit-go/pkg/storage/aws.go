package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
)

// AWSStorage implements Storage interface using AWS SDK
type AWSStorage struct {
	client *s3.Client
	config StorageConfig
	logger *zap.Logger
}

// NewAWSStorage creates a new AWS storage instance
func NewAWSStorage(storageConfig StorageConfig, logger *zap.Logger) (*AWSStorage, error) {
	if storageConfig.BatchSize == 0 {
		storageConfig.BatchSize = 1000
	}
	if storageConfig.Timeout == 0 {
		storageConfig.Timeout = 5 * time.Minute
	}
	if storageConfig.RetryAttempts == 0 {
		storageConfig.RetryAttempts = 3
	}
	if storageConfig.RetryDelay == 0 {
		storageConfig.RetryDelay = time.Second
	}

	// Load AWS configuration
	var awsConfig aws.Config
	var err error

	opts := []func(*config.LoadOptions) error{
		config.WithRegion(storageConfig.AWSRegion),
	}

	if storageConfig.AWSProfile != "" {
		opts = append(opts, config.WithSharedConfigProfile(storageConfig.AWSProfile))
	}

	awsConfig, err = config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, NewStorageError("aws_config", "", StorageBackendAWS, err)
	}

	// Override endpoint if specified (for S3-compatible services like R2)
	s3Options := []func(*s3.Options){}
	if storageConfig.AWSEndpoint != "" {
		s3Options = append(s3Options, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(storageConfig.AWSEndpoint)
			o.UsePathStyle = true // Required for custom endpoints
		})
	}

	client := s3.NewFromConfig(awsConfig, s3Options...)

	logger.Info("AWS storage initialized", 
		zap.String("region", storageConfig.AWSRegion),
		zap.String("profile", storageConfig.AWSProfile),
		zap.String("endpoint", storageConfig.AWSEndpoint))

	return &AWSStorage{
		client: client,
		config: storageConfig,
		logger: logger,
	}, nil
}

// ListObjects lists objects in the specified bucket and directory
func (a *AWSStorage) ListObjects(ctx context.Context, req ListRequest) ([]Object, error) {
	prefix := req.Directory
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(req.Bucket),
		Prefix: aws.String(prefix),
	}

	if req.Limit > 0 {
		input.MaxKeys = aws.Int32(int32(req.Limit))
	}

	a.logger.Debug("Listing S3 objects", 
		zap.String("bucket", req.Bucket),
		zap.String("prefix", prefix),
		zap.Int("limit", req.Limit))

	var objects []Object
	paginator := s3.NewListObjectsV2Paginator(a.client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, NewStorageError("list_objects", req.Bucket+"/"+prefix, StorageBackendAWS, err)
		}

		for _, item := range page.Contents {
			// Remove prefix from path to get relative path
			path := *item.Key
			if prefix != "" {
				path = strings.TrimPrefix(path, prefix)
			}

			// Skip directories (objects ending with /)
			if strings.HasSuffix(path, "/") {
				continue
			}

			// Filter by extensions if specified
			if len(req.Extensions) > 0 {
				ext := strings.ToLower(filepath.Ext(path))
				found := false
				for _, allowedExt := range req.Extensions {
					if strings.ToLower(allowedExt) == ext {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			obj := Object{
				Path: path,
				Size: *item.Size,
			}

			if item.LastModified != nil {
				obj.Modified = *item.LastModified
			}

			if item.ETag != nil {
				obj.ETag = strings.Trim(*item.ETag, `"`)
			}

			objects = append(objects, obj)
		}

		// Apply limit across all pages
		if req.Limit > 0 && len(objects) >= req.Limit {
			objects = objects[:req.Limit]
			break
		}
	}

	// Sort by size descending if sizes are available
	if len(objects) > 0 {
		for i := 0; i < len(objects)-1; i++ {
			for j := i + 1; j < len(objects); j++ {
				if objects[i].Size < objects[j].Size {
					objects[i], objects[j] = objects[j], objects[i]
				}
			}
		}
	}

	a.logger.Info("Listed S3 objects", 
		zap.Int("count", len(objects)),
		zap.String("bucket", req.Bucket))

	return objects, nil
}

// GetObjectSizes gets sizes of multiple objects (batch operation via HeadObject)
func (a *AWSStorage) GetObjectSizes(ctx context.Context, objects []string) (map[string]int64, error) {
	if len(objects) == 0 {
		return make(map[string]int64), nil
	}

	result := make(map[string]int64)

	// Process in batches to avoid overwhelming the API
	for i := 0; i < len(objects); i += a.config.BatchSize {
		end := i + a.config.BatchSize
		if end > len(objects) {
			end = len(objects)
		}

		batchResult, err := a.getObjectSizesBatch(ctx, objects[i:end])
		if err != nil {
			return nil, err
		}

		for path, size := range batchResult {
			result[path] = size
		}
	}

	a.logger.Info("Retrieved S3 object sizes", 
		zap.Int("requested", len(objects)),
		zap.Int("found", len(result)))

	return result, nil
}

// getObjectSizesBatch gets sizes for a batch of objects using concurrent HeadObject calls
func (a *AWSStorage) getObjectSizesBatch(ctx context.Context, objects []string) (map[string]int64, error) {
	result := make(map[string]int64)
	
	// Use a channel to limit concurrency
	semaphore := make(chan struct{}, 10) // Limit to 10 concurrent requests
	results := make(chan struct {
		path string
		size int64
		err  error
	}, len(objects))

	// Launch goroutines for each object
	for _, objPath := range objects {
		go func(path string) {
			semaphore <- struct{}{} // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore

			fullPath := path
			if a.config.Directory != "" {
				fullPath = a.config.Directory + "/" + path
			}

			input := &s3.HeadObjectInput{
				Bucket: aws.String(a.config.Bucket),
				Key:    aws.String(fullPath),
			}

			output, err := a.client.HeadObject(ctx, input)
			if err != nil {
				a.logger.Warn("Failed to get object metadata", 
					zap.String("path", path), 
					zap.Error(err))
				results <- struct {
					path string
					size int64
					err  error
				}{path, 0, err}
				return
			}

			results <- struct {
				path string
				size int64
				err  error
			}{path, *output.ContentLength, nil}
		}(objPath)
	}

	// Collect results
	for i := 0; i < len(objects); i++ {
		res := <-results
		if res.err == nil {
			result[res.path] = res.size
		}
	}

	return result, nil
}

// DownloadObject downloads an object from S3 to local path
func (a *AWSStorage) DownloadObject(ctx context.Context, remotePath, localPath string) error {
	// Ensure local directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return NewStorageError("download_object", localPath, StorageBackendAWS, err)
	}

	fullPath := remotePath
	if a.config.Directory != "" {
		fullPath = a.config.Directory + "/" + remotePath
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(a.config.Bucket),
		Key:    aws.String(fullPath),
	}

	a.logger.Debug("Downloading S3 object", 
		zap.String("bucket", a.config.Bucket),
		zap.String("key", fullPath),
		zap.String("local", localPath))

	output, err := a.client.GetObject(ctx, input)
	if err != nil {
		return NewStorageError("download_object", remotePath, StorageBackendAWS, err)
	}
	defer output.Body.Close()

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return NewStorageError("download_object", localPath, StorageBackendAWS, err)
	}
	defer file.Close()

	// Copy data
	_, err = io.Copy(file, output.Body)
	if err != nil {
		return NewStorageError("download_object", remotePath, StorageBackendAWS, err)
	}

	a.logger.Info("Downloaded S3 object", 
		zap.String("bucket", a.config.Bucket),
		zap.String("key", fullPath),
		zap.String("local", localPath))

	return nil
}

// UploadObject uploads a local file to S3
func (a *AWSStorage) UploadObject(ctx context.Context, localPath, remotePath string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return NewStorageError("upload_object", localPath, StorageBackendAWS, err)
	}
	defer file.Close()

	fullPath := remotePath
	if a.config.Directory != "" {
		fullPath = a.config.Directory + "/" + remotePath
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(a.config.Bucket),
		Key:    aws.String(fullPath),
		Body:   file,
	}

	a.logger.Debug("Uploading to S3", 
		zap.String("local", localPath),
		zap.String("bucket", a.config.Bucket),
		zap.String("key", fullPath))

	_, err = a.client.PutObject(ctx, input)
	if err != nil {
		return NewStorageError("upload_object", localPath, StorageBackendAWS, err)
	}

	a.logger.Info("Uploaded to S3", 
		zap.String("local", localPath),
		zap.String("bucket", a.config.Bucket),
		zap.String("key", fullPath))

	return nil
}

// ReplaceObject replaces an existing object with a new one
func (a *AWSStorage) ReplaceObject(ctx context.Context, remotePath, localPath string) error {
	// For S3, replace is the same as upload (PutObject overwrites by default)
	return a.UploadObject(ctx, localPath, remotePath)
}

// GetObjectMetadata gets metadata for a single object
func (a *AWSStorage) GetObjectMetadata(ctx context.Context, objectPath string) (*Object, error) {
	fullPath := objectPath
	if a.config.Directory != "" {
		fullPath = a.config.Directory + "/" + objectPath
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(a.config.Bucket),
		Key:    aws.String(fullPath),
	}

	output, err := a.client.HeadObject(ctx, input)
	if err != nil {
		return nil, NewStorageError("get_object_metadata", objectPath, StorageBackendAWS, err)
	}

	obj := &Object{
		Path: objectPath,
		Size: *output.ContentLength,
	}

	if output.LastModified != nil {
		obj.Modified = *output.LastModified
	}

	if output.ETag != nil {
		obj.ETag = strings.Trim(*output.ETag, `"`)
	}

	if output.Metadata != nil {
		obj.Metadata = make(map[string]string)
		for k, v := range output.Metadata {
			obj.Metadata[k] = v
		}
	}

	return obj, nil
}

// Close closes any resources used by the storage implementation
func (a *AWSStorage) Close() error {
	a.logger.Debug("Closing AWS storage")
	return nil
}