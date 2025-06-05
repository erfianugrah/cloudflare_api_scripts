package utils

import (
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"media-toolkit-go/pkg/storage"
)

// FilterConfig holds common filtering configuration
type FilterConfig struct {
	// Inclusion filters
	Extensions []string
	MediaType  string

	// Exclusion filters
	ExcludeExtensions  []string
	ExcludePatterns    []string
	ExcludeDirectories []string
	ExcludeMinSize     int
	ExcludeMaxSize     int
}

// AddFilteringFlags adds common filtering flags to a cobra command
func AddFilteringFlags(cmd *cobra.Command) {
	// Inclusion flags
	cmd.Flags().StringSlice("extensions", []string{}, "File extensions to filter by (e.g., .mp4,.jpg,.png)")
	cmd.Flags().String("media-type", "", "Media type preset: 'image', 'video', or 'all'")

	// Exclusion flags
	cmd.Flags().StringSlice("exclude-extensions", []string{}, "File extensions to exclude (e.g., .tmp,.log,.bak)")
	cmd.Flags().StringSlice("exclude-patterns", []string{}, "Patterns to exclude (supports wildcards: *thumb*, *.tmp)")
	cmd.Flags().StringSlice("exclude-directories", []string{}, "Directory paths to exclude (e.g., temp/, backup/)")
	cmd.Flags().Int("exclude-min-size", 0, "Exclude files smaller than this size in MiB")
	cmd.Flags().Int("exclude-max-size", 0, "Exclude files larger than this size in MiB")
}

// ApplyFileFilters filters a list of files based on the provided configuration
func ApplyFileFilters(files []storage.FileInfo, cfg *FilterConfig) []storage.FileInfo {
	// Determine extensions to include based on media type
	var includeExtensions []string
	if cfg.MediaType != "" {
		switch strings.ToLower(cfg.MediaType) {
		case "image":
			includeExtensions = []string{".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg"}
		case "video":
			includeExtensions = []string{".mp4", ".webm", ".mov", ".avi", ".mkv", ".m4v"}
		case "all", "auto":
			// No filtering by extension
			includeExtensions = nil
		}
	} else if len(cfg.Extensions) > 0 {
		includeExtensions = cfg.Extensions
	}

	// Apply filters
	filtered := make([]storage.FileInfo, 0, len(files))
	for _, file := range files {
		// Check exclusions first
		if ShouldExcludeFile(file, cfg) {
			continue
		}

		// Then check inclusions if specified
		if len(includeExtensions) > 0 {
			fileExt := strings.ToLower(filepath.Ext(file.Path))
			included := false
			for _, ext := range includeExtensions {
				if fileExt == strings.ToLower(ext) {
					included = true
					break
				}
			}
			if !included {
				continue
			}
		}

		filtered = append(filtered, file)
	}

	return filtered
}

// ShouldExcludeFile checks if a file should be excluded based on exclusion rules
func ShouldExcludeFile(file storage.FileInfo, cfg *FilterConfig) bool {
	filePath := file.Path

	// Check file size exclusions
	if cfg.ExcludeMinSize > 0 {
		minSizeBytes := int64(cfg.ExcludeMinSize) * 1024 * 1024 // Convert MiB to bytes
		if file.Size < minSizeBytes {
			return true
		}
	}

	if cfg.ExcludeMaxSize > 0 {
		maxSizeBytes := int64(cfg.ExcludeMaxSize) * 1024 * 1024 // Convert MiB to bytes
		if file.Size > maxSizeBytes {
			return true
		}
	}

	// Check excluded extensions
	if len(cfg.ExcludeExtensions) > 0 {
		fileExt := strings.ToLower(filepath.Ext(filePath))
		for _, ext := range cfg.ExcludeExtensions {
			if fileExt == strings.ToLower(ext) {
				return true
			}
		}
	}

	// Check excluded directories
	if len(cfg.ExcludeDirectories) > 0 {
		for _, dir := range cfg.ExcludeDirectories {
			// Normalize directory separator
			dir = strings.TrimSuffix(dir, "/")
			// Check if file path starts with excluded directory
			if strings.HasPrefix(filePath, dir+"/") || filePath == dir {
				return true
			}
		}
	}

	// Check excluded patterns
	if len(cfg.ExcludePatterns) > 0 {
		for _, pattern := range cfg.ExcludePatterns {
			if MatchPattern(filePath, pattern) {
				return true
			}
		}
	}

	return false
}

// MatchPattern performs simple wildcard matching (* matches any characters)
func MatchPattern(text, pattern string) bool {
	// Use filepath.Match for glob pattern matching
	matched, err := filepath.Match(pattern, text)
	if err != nil {
		// If pattern is invalid, try simple substring matching
		return strings.Contains(text, strings.ReplaceAll(pattern, "*", ""))
	}
	return matched
}

// GetMediaTypeExtensions returns file extensions for a given media type
func GetMediaTypeExtensions(mediaType string) []string {
	switch strings.ToLower(mediaType) {
	case "image":
		return []string{".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg"}
	case "video":
		return []string{".mp4", ".webm", ".mov", ".avi", ".mkv", ".m4v"}
	default:
		return nil
	}
}
