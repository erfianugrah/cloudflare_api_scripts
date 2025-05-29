package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// EnsureDir creates a directory if it doesn't exist
func EnsureDir(path string) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}
	
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("path exists but is not a directory: %s", path)
		}
		return nil
	}
	
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	
	return err
}

// FileExists checks if a file exists
func FileExists(path string) bool {
	if path == "" {
		return false
	}
	
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// DirExists checks if a directory exists
func DirExists(path string) bool {
	if path == "" {
		return false
	}
	
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// GetFileSize returns the size of a file in bytes
func GetFileSize(path string) (int64, error) {
	if path == "" {
		return 0, fmt.Errorf("path cannot be empty")
	}
	
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	
	if info.IsDir() {
		return 0, fmt.Errorf("path is a directory, not a file: %s", path)
	}
	
	return info.Size(), nil
}

// GetFileExtension returns the file extension (with dot)
func GetFileExtension(path string) string {
	return strings.ToLower(filepath.Ext(path))
}

// GetFileNameWithoutExt returns filename without extension
func GetFileNameWithoutExt(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

// IsVideoFile checks if the file has a video extension
func IsVideoFile(path string) bool {
	videoExts := []string{
		".mp4", ".avi", ".mov", ".mkv", ".wmv", ".flv", 
		".webm", ".m4v", ".3gp", ".ogv", ".ts", ".mts",
	}
	
	ext := GetFileExtension(path)
	for _, videoExt := range videoExts {
		if ext == videoExt {
			return true
		}
	}
	return false
}

// IsImageFile checks if the file has an image extension
func IsImageFile(path string) bool {
	imageExts := []string{
		".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", 
		".webp", ".svg", ".ico", ".heic", ".heif",
	}
	
	ext := GetFileExtension(path)
	for _, imageExt := range imageExts {
		if ext == imageExt {
			return true
		}
	}
	return false
}

// JoinPath safely joins path components
func JoinPath(components ...string) string {
	if len(components) == 0 {
		return ""
	}
	
	// Filter out empty components
	filtered := make([]string, 0, len(components))
	for _, component := range components {
		if strings.TrimSpace(component) != "" {
			filtered = append(filtered, component)
		}
	}
	
	if len(filtered) == 0 {
		return ""
	}
	
	return filepath.Join(filtered...)
}

// MakeRelativePath makes a path relative to a base directory
func MakeRelativePath(path, basePath string) (string, error) {
	if path == "" || basePath == "" {
		return "", fmt.Errorf("path and basePath cannot be empty")
	}
	
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for %s: %w", path, err)
	}
	
	absBase, err := filepath.Abs(basePath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for %s: %w", basePath, err)
	}
	
	relPath, err := filepath.Rel(absBase, absPath)
	if err != nil {
		return "", fmt.Errorf("failed to make relative path: %w", err)
	}
	
	return relPath, nil
}

// CreateTempDir creates a temporary directory with a given prefix
func CreateTempDir(prefix string) (string, error) {
	return os.MkdirTemp("", prefix)
}

// CleanupTempDir removes a temporary directory and all its contents
func CleanupTempDir(path string) error {
	if path == "" {
		return nil
	}
	
	// Safety check - only remove directories under temp
	if !strings.Contains(path, os.TempDir()) {
		return fmt.Errorf("refusing to remove directory outside temp: %s", path)
	}
	
	return os.RemoveAll(path)
}