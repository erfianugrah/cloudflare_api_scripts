package utils

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// TrimExtension removes the file extension from a path
func TrimExtension(path string) string {
	ext := filepath.Ext(path)
	if ext == "" {
		return path
	}
	return strings.TrimSuffix(path, ext)
}

// SanitizeFilename removes invalid characters from a filename
func SanitizeFilename(filename string) string {
	// Remove invalid characters for most filesystems
	re := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1f]`)
	sanitized := re.ReplaceAllString(filename, "_")

	// Trim whitespace and dots from start/end
	sanitized = strings.Trim(sanitized, " .")

	// Ensure it's not empty
	if sanitized == "" {
		sanitized = "unnamed"
	}

	return sanitized
}

// FormatBytes formats bytes into human-readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp])
}

// FormatDuration formats duration in seconds to human-readable format
func FormatDuration(seconds float64) string {
	if seconds < 60 {
		return fmt.Sprintf("%.1fs", seconds)
	}

	minutes := int(seconds / 60)
	remainingSeconds := seconds - float64(minutes*60)

	if minutes < 60 {
		return fmt.Sprintf("%dm %.1fs", minutes, remainingSeconds)
	}

	hours := minutes / 60
	remainingMinutes := minutes % 60

	return fmt.Sprintf("%dh %dm %.1fs", hours, remainingMinutes, remainingSeconds)
}

// TruncateString truncates a string to maxLength with ellipsis
func TruncateString(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}

	if maxLength <= 3 {
		return s[:maxLength]
	}

	return s[:maxLength-3] + "..."
}

// ContainsAny checks if string contains any of the given substrings
func ContainsAny(s string, substrings []string) bool {
	for _, substr := range substrings {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

// RemoveEmpty removes empty strings from a slice
func RemoveEmpty(slice []string) []string {
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if strings.TrimSpace(s) != "" {
			result = append(result, s)
		}
	}
	return result
}

// SplitAndTrim splits a string and trims whitespace from each part
func SplitAndTrim(s, separator string) []string {
	parts := strings.Split(s, separator)
	result := make([]string, len(parts))
	for i, part := range parts {
		result[i] = strings.TrimSpace(part)
	}
	return result
}
