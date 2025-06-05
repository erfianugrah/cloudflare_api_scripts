package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

// LocalStorage implements Storage interface for local filesystem
type LocalStorage struct {
	config   StorageConfig
	logger   *zap.Logger
	basePath string
}

// NewLocalStorage creates a new local storage instance
func NewLocalStorage(config StorageConfig, logger *zap.Logger) *LocalStorage {
	// For local storage, we use the bucket as the base directory
	basePath := config.Bucket
	if config.Directory != "" {
		basePath = filepath.Join(basePath, config.Directory)
	}

	return &LocalStorage{
		config:   config,
		logger:   logger,
		basePath: basePath,
	}
}

// ListObjects lists objects in the local directory
func (l *LocalStorage) ListObjects(ctx context.Context, req ListRequest) ([]Object, error) {
	searchPath := l.basePath
	if req.Directory != "" {
		searchPath = filepath.Join(l.basePath, req.Directory)
	}

	l.logger.Debug("Listing local files", zap.String("path", searchPath))

	var objects []Object

	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path
		relPath, err := filepath.Rel(l.basePath, path)
		if err != nil {
			return err
		}

		// Convert to forward slashes for consistency
		relPath = filepath.ToSlash(relPath)

		// Filter by extensions if specified
		if len(req.Extensions) > 0 {
			ext := strings.ToLower(filepath.Ext(relPath))
			found := false
			for _, allowedExt := range req.Extensions {
				if strings.ToLower(allowedExt) == ext {
					found = true
					break
				}
			}
			if !found {
				return nil
			}
		}

		obj := Object{
			Path:     relPath,
			Size:     info.Size(),
			Modified: info.ModTime(),
		}

		objects = append(objects, obj)

		// Apply limit during walk for efficiency
		if req.Limit > 0 && len(objects) >= req.Limit {
			return fmt.Errorf("limit reached") // Use error to stop walking
		}

		return nil
	})

	// Check if we stopped due to limit
	if err != nil && err.Error() == "limit reached" {
		err = nil
	}

	if err != nil {
		return nil, NewStorageError("list_objects", searchPath, StorageBackendLocal, err)
	}

	// Sort by size descending
	if len(objects) > 0 {
		for i := 0; i < len(objects)-1; i++ {
			for j := i + 1; j < len(objects); j++ {
				if objects[i].Size < objects[j].Size {
					objects[i], objects[j] = objects[j], objects[i]
				}
			}
		}
	}

	l.logger.Info("Listed local files",
		zap.Int("count", len(objects)),
		zap.String("path", searchPath))

	return objects, nil
}

// GetObjectSizes gets sizes of multiple objects
func (l *LocalStorage) GetObjectSizes(ctx context.Context, objects []string) (map[string]int64, error) {
	result := make(map[string]int64)

	for _, objPath := range objects {
		fullPath := filepath.Join(l.basePath, objPath)

		info, err := os.Stat(fullPath)
		if err != nil {
			l.logger.Warn("Failed to stat file",
				zap.String("path", objPath),
				zap.Error(err))
			continue
		}

		result[objPath] = info.Size()
	}

	l.logger.Info("Retrieved local file sizes",
		zap.Int("requested", len(objects)),
		zap.Int("found", len(result)))

	return result, nil
}

// DownloadObject copies a file from one local path to another
func (l *LocalStorage) DownloadObject(ctx context.Context, remotePath, localPath string) error {
	sourcePath := filepath.Join(l.basePath, remotePath)

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return NewStorageError("download_object", localPath, StorageBackendLocal, err)
	}

	l.logger.Debug("Copying local file",
		zap.String("source", sourcePath),
		zap.String("destination", localPath))

	// Open source file
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return NewStorageError("download_object", remotePath, StorageBackendLocal, err)
	}
	defer sourceFile.Close()

	// Create destination file
	destFile, err := os.Create(localPath)
	if err != nil {
		return NewStorageError("download_object", localPath, StorageBackendLocal, err)
	}
	defer destFile.Close()

	// Copy data
	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return NewStorageError("download_object", remotePath, StorageBackendLocal, err)
	}

	l.logger.Info("Copied local file",
		zap.String("source", sourcePath),
		zap.String("destination", localPath))

	return nil
}

// UploadObject copies a local file to the storage directory
func (l *LocalStorage) UploadObject(ctx context.Context, localPath, remotePath string) error {
	destPath := filepath.Join(l.basePath, remotePath)

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return NewStorageError("upload_object", destPath, StorageBackendLocal, err)
	}

	l.logger.Debug("Copying to local storage",
		zap.String("source", localPath),
		zap.String("destination", destPath))

	// Open source file
	sourceFile, err := os.Open(localPath)
	if err != nil {
		return NewStorageError("upload_object", localPath, StorageBackendLocal, err)
	}
	defer sourceFile.Close()

	// Create destination file
	destFile, err := os.Create(destPath)
	if err != nil {
		return NewStorageError("upload_object", destPath, StorageBackendLocal, err)
	}
	defer destFile.Close()

	// Copy data
	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return NewStorageError("upload_object", localPath, StorageBackendLocal, err)
	}

	l.logger.Info("Copied to local storage",
		zap.String("source", localPath),
		zap.String("destination", destPath))

	return nil
}

// ReplaceObject replaces an existing file
func (l *LocalStorage) ReplaceObject(ctx context.Context, remotePath, localPath string) error {
	// For local storage, replace is the same as upload
	return l.UploadObject(ctx, localPath, remotePath)
}

// GetObjectMetadata gets metadata for a single object
func (l *LocalStorage) GetObjectMetadata(ctx context.Context, objectPath string) (*Object, error) {
	fullPath := filepath.Join(l.basePath, objectPath)

	info, err := os.Stat(fullPath)
	if err != nil {
		return nil, NewStorageError("get_object_metadata", objectPath, StorageBackendLocal, err)
	}

	return &Object{
		Path:     objectPath,
		Size:     info.Size(),
		Modified: info.ModTime(),
		Metadata: map[string]string{
			"mode": info.Mode().String(),
		},
	}, nil
}

// Close closes any resources used by the storage implementation
func (l *LocalStorage) Close() error {
	l.logger.Debug("Closing local storage")
	return nil
}
