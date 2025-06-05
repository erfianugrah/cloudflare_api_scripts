package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

// FileListCache provides caching for file listings between workflow stages
type FileListCache struct {
	logger   *zap.Logger
	cacheDir string
	ttl      time.Duration
}

// CachedFileList represents a cached file listing
type CachedFileList struct {
	Timestamp     time.Time  `json:"timestamp"`
	Remote        string     `json:"remote"`
	Bucket        string     `json:"bucket"`
	Directory     string     `json:"directory"`
	Extensions    []string   `json:"extensions,omitempty"`
	Files         []FileInfo `json:"files"`
	TotalCount    int        `json:"total_count"`
	FilteredCount int        `json:"filtered_count,omitempty"`
}

// NewFileListCache creates a new file list cache
func NewFileListCache(cacheDir string, ttl time.Duration, logger *zap.Logger) *FileListCache {
	if cacheDir == "" {
		cacheDir = "workflow-results"
	}

	// Ensure cache directory exists
	os.MkdirAll(cacheDir, 0755)

	return &FileListCache{
		logger:   logger,
		cacheDir: cacheDir,
		ttl:      ttl,
	}
}

// getCacheKey generates a unique cache key for the file listing
func (c *FileListCache) getCacheKey(remote, bucket, directory string, extensions []string) string {
	// Create a deterministic key based on the listing parameters
	key := fmt.Sprintf("%s_%s_%s", remote, bucket, directory)
	if len(extensions) > 0 {
		key += "_" + joinExtensions(extensions)
	}
	return key
}

// getCachePath returns the file path for a cache entry
func (c *FileListCache) getCachePath(key string) string {
	filename := fmt.Sprintf("file_list_cache_%s.json", key)
	return filepath.Join(c.cacheDir, filename)
}

// Get retrieves a cached file listing if it exists and is still valid
func (c *FileListCache) Get(remote, bucket, directory string, extensions []string) (*CachedFileList, bool) {
	key := c.getCacheKey(remote, bucket, directory, extensions)
	cachePath := c.getCachePath(key)

	// Check if cache file exists
	info, err := os.Stat(cachePath)
	if err != nil {
		if !os.IsNotExist(err) {
			c.logger.Warn("Failed to stat cache file", zap.String("path", cachePath), zap.Error(err))
		}
		return nil, false
	}

	// Check if cache is expired
	if time.Since(info.ModTime()) > c.ttl {
		c.logger.Debug("Cache entry expired",
			zap.String("key", key),
			zap.Duration("age", time.Since(info.ModTime())),
			zap.Duration("ttl", c.ttl))
		return nil, false
	}

	// Read cache file
	data, err := os.ReadFile(cachePath)
	if err != nil {
		c.logger.Warn("Failed to read cache file", zap.String("path", cachePath), zap.Error(err))
		return nil, false
	}

	// Unmarshal cache data
	var cached CachedFileList
	if err := json.Unmarshal(data, &cached); err != nil {
		c.logger.Warn("Failed to unmarshal cache data", zap.String("path", cachePath), zap.Error(err))
		return nil, false
	}

	// Verify cache is for the same parameters
	if cached.Remote != remote || cached.Bucket != bucket || cached.Directory != directory {
		c.logger.Warn("Cache entry parameters mismatch", zap.String("key", key))
		return nil, false
	}

	c.logger.Info("Using cached file listing",
		zap.String("key", key),
		zap.Int("file_count", len(cached.Files)),
		zap.Duration("age", time.Since(cached.Timestamp)))

	return &cached, true
}

// Set stores a file listing in the cache
func (c *FileListCache) Set(remote, bucket, directory string, extensions []string, files []FileInfo) error {
	key := c.getCacheKey(remote, bucket, directory, extensions)
	cachePath := c.getCachePath(key)

	// Create cache entry
	cached := CachedFileList{
		Timestamp:  time.Now(),
		Remote:     remote,
		Bucket:     bucket,
		Directory:  directory,
		Extensions: extensions,
		Files:      files,
		TotalCount: len(files),
	}

	// If extensions were specified, count how many files matched
	if len(extensions) > 0 {
		filteredCount := 0
		for _, file := range files {
			ext := filepath.Ext(file.Path)
			for _, allowedExt := range extensions {
				if ext == allowedExt {
					filteredCount++
					break
				}
			}
		}
		cached.FilteredCount = filteredCount
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(cached, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cache data: %w", err)
	}

	// Write to file
	if err := os.WriteFile(cachePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	c.logger.Info("Cached file listing",
		zap.String("key", key),
		zap.String("path", cachePath),
		zap.Int("file_count", len(files)))

	return nil
}

// Clear removes all cache entries
func (c *FileListCache) Clear() error {
	pattern := filepath.Join(c.cacheDir, "file_list_cache_*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list cache files: %w", err)
	}

	for _, file := range files {
		if err := os.Remove(file); err != nil {
			c.logger.Warn("Failed to remove cache file", zap.String("path", file), zap.Error(err))
		}
	}

	c.logger.Info("Cleared file list cache", zap.Int("files_removed", len(files)))
	return nil
}

// joinExtensions creates a string representation of extensions for the cache key
func joinExtensions(extensions []string) string {
	result := ""
	for i, ext := range extensions {
		if i > 0 {
			result += "_"
		}
		// Remove dots from extensions for cleaner filenames
		cleaned := ext
		if len(cleaned) > 0 && cleaned[0] == '.' {
			cleaned = cleaned[1:]
		}
		result += cleaned
	}
	return result
}
