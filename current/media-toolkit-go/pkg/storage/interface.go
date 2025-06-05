package storage

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Storage defines the interface for storage operations
type Storage interface {
	// ListObjects lists objects in the specified bucket and directory
	ListObjects(ctx context.Context, req ListRequest) ([]Object, error)

	// GetObjectSizes gets sizes of multiple objects in batch
	GetObjectSizes(ctx context.Context, objects []string) (map[string]int64, error)

	// DownloadObject downloads an object from remote storage to local path
	DownloadObject(ctx context.Context, remotePath, localPath string) error

	// UploadObject uploads a local file to remote storage
	UploadObject(ctx context.Context, localPath, remotePath string) error

	// ReplaceObject replaces an existing object with a new one
	ReplaceObject(ctx context.Context, remotePath, localPath string) error

	// GetObjectMetadata gets metadata for a single object
	GetObjectMetadata(ctx context.Context, objectPath string) (*Object, error)

	// Close closes any resources used by the storage implementation
	Close() error
}

// ListRequest represents a request to list objects
type ListRequest struct {
	Bucket     string   `json:"bucket"`
	Directory  string   `json:"directory"`
	Extensions []string `json:"extensions,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	GetSizes   bool     `json:"get_sizes"`
}

// Object represents a storage object
type Object struct {
	Path     string            `json:"path"`
	Size     int64             `json:"size"`
	Modified time.Time         `json:"modified"`
	ETag     string            `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// StorageConfig represents configuration for storage backends
type StorageConfig struct {
	// Common settings
	Backend   StorageBackend `json:"backend"`
	Remote    string         `json:"remote"`
	Bucket    string         `json:"bucket"`
	Directory string         `json:"directory"`

	// AWS SDK specific settings
	AWSRegion   string `json:"aws_region,omitempty"`
	AWSProfile  string `json:"aws_profile,omitempty"`
	AWSEndpoint string `json:"aws_endpoint,omitempty"`

	// Rclone specific settings
	RcloneConfig string `json:"rclone_config,omitempty"`
	RcloneBinary string `json:"rclone_binary,omitempty"`

	// Performance settings
	BatchSize     int           `json:"batch_size"`
	Timeout       time.Duration `json:"timeout"`
	RetryAttempts int           `json:"retry_attempts"`
	RetryDelay    time.Duration `json:"retry_delay"`
}

// StorageBackend represents the type of storage backend
type StorageBackend string

const (
	StorageBackendRclone StorageBackend = "rclone"
	StorageBackendAWS    StorageBackend = "aws"
	StorageBackendLocal  StorageBackend = "local"
)

// String returns the string representation of StorageBackend
func (s StorageBackend) String() string {
	return string(s)
}

// StorageError represents a storage operation error
type StorageError struct {
	Operation string
	Path      string
	Backend   StorageBackend
	Err       error
}

// Error implements the error interface
func (e *StorageError) Error() string {
	return fmt.Sprintf("storage error [%s] during %s operation on %s: %v",
		e.Backend, e.Operation, e.Path, e.Err)
}

// Unwrap returns the underlying error
func (e *StorageError) Unwrap() error {
	return e.Err
}

// NewStorageError creates a new storage error
func NewStorageError(operation, path string, backend StorageBackend, err error) *StorageError {
	return &StorageError{
		Operation: operation,
		Path:      path,
		Backend:   backend,
		Err:       err,
	}
}

// StorageFactory creates storage instances based on configuration
type StorageFactory interface {
	CreateStorage(config StorageConfig) (Storage, error)
}

// NewStorage creates a new storage client based on configuration
func NewStorage(storageConfig *StorageConfig, logger *zap.Logger) (Storage, error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storage config cannot be nil")
	}

	// Create factory and storage
	factory := NewStorageFactory(logger)
	return factory.CreateStorage(*storageConfig)
}

// FileInfo represents file information (alias for Object for compatibility)
type FileInfo = Object
