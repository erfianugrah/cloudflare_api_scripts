package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// RcloneStorage implements Storage interface using rclone
type RcloneStorage struct {
	config StorageConfig
	logger *zap.Logger
}

// NewRcloneStorage creates a new rclone storage instance
func NewRcloneStorage(config StorageConfig, logger *zap.Logger) *RcloneStorage {
	if config.RcloneBinary == "" {
		config.RcloneBinary = "rclone"
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1000
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Minute
	}
	if config.RetryAttempts == 0 {
		config.RetryAttempts = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = time.Second
	}

	return &RcloneStorage{
		config: config,
		logger: logger,
	}
}

// ListObjects lists objects in the specified bucket and directory
func (r *RcloneStorage) ListObjects(ctx context.Context, req ListRequest) ([]Object, error) {
	remotePath := fmt.Sprintf("%s:%s/%s", r.config.Remote, req.Bucket, req.Directory)
	remotePath = strings.TrimSuffix(remotePath, "/")

	var cmd *exec.Cmd
	if req.GetSizes {
		// Use rclone ls to get files with sizes
		cmd = exec.CommandContext(ctx, r.config.RcloneBinary, "ls", remotePath)
	} else {
		// Use rclone lsf for just filenames
		cmd = exec.CommandContext(ctx, r.config.RcloneBinary, "lsf", remotePath, "--recursive")
	}

	if r.config.RcloneConfig != "" {
		cmd.Args = append(cmd.Args, "--config", r.config.RcloneConfig)
	}

	r.logger.Info("Running rclone command",
		zap.String("command", strings.Join(cmd.Args, " ")),
		zap.String("remote_path", remotePath))

	output, err := cmd.Output()
	if err != nil {
		r.logger.Error("Rclone command failed",
			zap.String("command", strings.Join(cmd.Args, " ")),
			zap.Error(err))
		return nil, NewStorageError("list_objects", remotePath, StorageBackendRclone, err)
	}

	r.logger.Info("Rclone command completed",
		zap.String("command", strings.Join(cmd.Args, " ")),
		zap.Int("output_size", len(output)))

	var objects []Object
	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var obj Object
		if req.GetSizes {
			// Parse "rclone ls" output: "  12345 path/to/file.ext"
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				size, err := strconv.ParseInt(parts[0], 10, 64)
				if err != nil {
					r.logger.Warn("Failed to parse file size",
						zap.String("line", line),
						zap.Error(err))
					continue
				}
				obj.Size = size
				obj.Path = strings.Join(parts[1:], " ")
			}
		} else {
			// Parse "rclone lsf" output: just the filename
			obj.Path = line
		}

		// Filter by extensions if specified
		if len(req.Extensions) > 0 {
			ext := strings.ToLower(filepath.Ext(obj.Path))
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

		objects = append(objects, obj)
	}

	// Sort by size descending if sizes are available
	if req.GetSizes && len(objects) > 0 {
		for i := 0; i < len(objects)-1; i++ {
			for j := i + 1; j < len(objects); j++ {
				if objects[i].Size < objects[j].Size {
					objects[i], objects[j] = objects[j], objects[i]
				}
			}
		}
	}

	// Apply limit if specified
	if req.Limit > 0 && len(objects) > req.Limit {
		r.logger.Info("Limiting results",
			zap.Int("total_found", len(objects)),
			zap.Int("limit", req.Limit))
		objects = objects[:req.Limit]
	}

	r.logger.Info("Listed objects",
		zap.Int("count", len(objects)),
		zap.String("remote_path", remotePath))

	return objects, nil
}

// GetObjectSizes gets sizes of multiple objects using rclone lsjson for efficiency
func (r *RcloneStorage) GetObjectSizes(ctx context.Context, objects []string) (map[string]int64, error) {
	if len(objects) == 0 {
		return make(map[string]int64), nil
	}

	remotePath := fmt.Sprintf("%s:%s/%s", r.config.Remote, r.config.Bucket, r.config.Directory)
	remotePath = strings.TrimSuffix(remotePath, "/")

	result := make(map[string]int64)

	// Process in batches to avoid command line length limits
	for i := 0; i < len(objects); i += r.config.BatchSize {
		end := i + r.config.BatchSize
		if end > len(objects) {
			end = len(objects)
		}

		batchResult, err := r.getObjectSizesBatch(ctx, remotePath, objects[i:end])
		if err != nil {
			return nil, err
		}

		for path, size := range batchResult {
			result[path] = size
		}
	}

	r.logger.Info("Retrieved object sizes",
		zap.Int("requested", len(objects)),
		zap.Int("found", len(result)))

	return result, nil
}

// getObjectSizesBatch gets sizes for a batch of objects using rclone lsjson
func (r *RcloneStorage) getObjectSizesBatch(ctx context.Context, remotePath string, objects []string) (map[string]int64, error) {
	cmd := exec.CommandContext(ctx, r.config.RcloneBinary, "lsjson", remotePath,
		"--files-only", "--no-modtime", "--no-mimetype")

	if r.config.RcloneConfig != "" {
		cmd.Args = append(cmd.Args, "--config", r.config.RcloneConfig)
	}

	r.logger.Debug("Running rclone lsjson batch command",
		zap.String("command", strings.Join(cmd.Args, " ")),
		zap.Int("batch_size", len(objects)))

	output, err := cmd.Output()
	if err != nil {
		return nil, NewStorageError("get_object_sizes_batch", remotePath, StorageBackendRclone, err)
	}

	var fileInfos []struct {
		Name string `json:"Name"`
		Path string `json:"Path"`
		Size int64  `json:"Size"`
	}

	if err := json.Unmarshal(output, &fileInfos); err != nil {
		return nil, NewStorageError("get_object_sizes_batch", remotePath, StorageBackendRclone,
			fmt.Errorf("failed to parse JSON: %w", err))
	}

	// Create lookup map from rclone results
	sizeMap := make(map[string]int64)
	for _, info := range fileInfos {
		// Try both Name and Path fields
		if info.Path != "" {
			sizeMap[info.Path] = info.Size
		} else if info.Name != "" {
			sizeMap[info.Name] = info.Size
		}
	}

	// Match requested objects
	result := make(map[string]int64)
	for _, objPath := range objects {
		if size, found := sizeMap[objPath]; found {
			result[objPath] = size
		} else {
			// Try with directory prefix removed
			basename := filepath.Base(objPath)
			if size, found := sizeMap[basename]; found {
				result[objPath] = size
			} else {
				r.logger.Warn("Object not found in batch", zap.String("path", objPath))
			}
		}
	}

	return result, nil
}

// DownloadObject downloads an object from remote storage
func (r *RcloneStorage) DownloadObject(ctx context.Context, remotePath, localPath string) error {
	// Ensure local directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return NewStorageError("download_object", localPath, StorageBackendRclone, err)
	}

	cmd := exec.CommandContext(ctx, r.config.RcloneBinary, "copyto", remotePath, localPath)

	if r.config.RcloneConfig != "" {
		cmd.Args = append(cmd.Args, "--config", r.config.RcloneConfig)
	}

	r.logger.Debug("Downloading object",
		zap.String("remote", remotePath),
		zap.String("local", localPath))

	if err := cmd.Run(); err != nil {
		return NewStorageError("download_object", remotePath, StorageBackendRclone, err)
	}

	r.logger.Info("Downloaded object",
		zap.String("remote", remotePath),
		zap.String("local", localPath))

	return nil
}

// UploadObject uploads a local file to remote storage
func (r *RcloneStorage) UploadObject(ctx context.Context, localPath, remotePath string) error {
	cmd := exec.CommandContext(ctx, r.config.RcloneBinary, "copyto", localPath, remotePath)

	if r.config.RcloneConfig != "" {
		cmd.Args = append(cmd.Args, "--config", r.config.RcloneConfig)
	}

	r.logger.Debug("Uploading object",
		zap.String("local", localPath),
		zap.String("remote", remotePath))

	if err := cmd.Run(); err != nil {
		return NewStorageError("upload_object", localPath, StorageBackendRclone, err)
	}

	r.logger.Info("Uploaded object",
		zap.String("local", localPath),
		zap.String("remote", remotePath))

	return nil
}

// ReplaceObject replaces an existing object with a new one
func (r *RcloneStorage) ReplaceObject(ctx context.Context, remotePath, localPath string) error {
	// For rclone, replace is the same as upload (copyto overwrites by default)
	return r.UploadObject(ctx, localPath, remotePath)
}

// GetObjectMetadata gets metadata for a single object
func (r *RcloneStorage) GetObjectMetadata(ctx context.Context, objectPath string) (*Object, error) {
	remotePath := fmt.Sprintf("%s:%s/%s/%s", r.config.Remote, r.config.Bucket, r.config.Directory, objectPath)

	cmd := exec.CommandContext(ctx, r.config.RcloneBinary, "lsjson", filepath.Dir(remotePath))

	if r.config.RcloneConfig != "" {
		cmd.Args = append(cmd.Args, "--config", r.config.RcloneConfig)
	}

	output, err := cmd.Output()
	if err != nil {
		return nil, NewStorageError("get_object_metadata", objectPath, StorageBackendRclone, err)
	}

	var fileInfos []struct {
		Name     string    `json:"Name"`
		Path     string    `json:"Path"`
		Size     int64     `json:"Size"`
		ModTime  time.Time `json:"ModTime"`
		MimeType string    `json:"MimeType"`
	}

	if err := json.Unmarshal(output, &fileInfos); err != nil {
		return nil, NewStorageError("get_object_metadata", objectPath, StorageBackendRclone,
			fmt.Errorf("failed to parse JSON: %w", err))
	}

	filename := filepath.Base(objectPath)
	for _, info := range fileInfos {
		if info.Name == filename || info.Path == objectPath {
			return &Object{
				Path:     objectPath,
				Size:     info.Size,
				Modified: info.ModTime,
				Metadata: map[string]string{
					"mime_type": info.MimeType,
				},
			}, nil
		}
	}

	return nil, NewStorageError("get_object_metadata", objectPath, StorageBackendRclone,
		fmt.Errorf("object not found: %s", objectPath))
}

// Close closes any resources used by the storage implementation
func (r *RcloneStorage) Close() error {
	r.logger.Debug("Closing rclone storage")
	return nil
}
