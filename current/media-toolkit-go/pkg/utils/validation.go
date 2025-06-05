package utils

import (
	"fmt"
	"net/url"
	"strings"
)

// ValidateURL validates if a string is a valid URL
func ValidateURL(urlStr string) error {
	if strings.TrimSpace(urlStr) == "" {
		return fmt.Errorf("URL cannot be empty")
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	if parsedURL.Scheme == "" {
		return fmt.Errorf("URL must have a scheme (http/https)")
	}

	if parsedURL.Host == "" {
		return fmt.Errorf("URL must have a host")
	}

	// Check for supported schemes
	supportedSchemes := []string{"http", "https"}
	validScheme := false
	for _, scheme := range supportedSchemes {
		if parsedURL.Scheme == scheme {
			validScheme = true
			break
		}
	}

	if !validScheme {
		return fmt.Errorf("unsupported URL scheme: %s (supported: %v)",
			parsedURL.Scheme, supportedSchemes)
	}

	return nil
}

// ValidateExtension checks if a file extension is in the allowed list
func ValidateExtension(ext string, allowed []string) bool {
	if ext == "" || len(allowed) == 0 {
		return false
	}

	// Normalize extension to lowercase and ensure it starts with dot
	normalizedExt := strings.ToLower(ext)
	if !strings.HasPrefix(normalizedExt, ".") {
		normalizedExt = "." + normalizedExt
	}

	for _, allowedExt := range allowed {
		normalizedAllowed := strings.ToLower(allowedExt)
		if !strings.HasPrefix(normalizedAllowed, ".") {
			normalizedAllowed = "." + normalizedAllowed
		}

		if normalizedExt == normalizedAllowed {
			return true
		}
	}

	return false
}

// ValidateFilePath checks if a file path is valid and safe
func ValidateFilePath(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("file path cannot be empty")
	}

	// Check for dangerous path traversal
	if strings.Contains(path, "..") {
		return fmt.Errorf("path traversal not allowed: %s", path)
	}

	// Check for null bytes (security issue)
	if strings.Contains(path, "\x00") {
		return fmt.Errorf("null bytes not allowed in path")
	}

	return nil
}

// ValidatePortRange checks if a port number is in valid range
func ValidatePortRange(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got: %d", port)
	}
	return nil
}

// ValidateNonEmpty checks if a string is not empty after trimming
func ValidateNonEmpty(value, fieldName string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s cannot be empty", fieldName)
	}
	return nil
}

// ValidatePositiveInt checks if an integer is positive
func ValidatePositiveInt(value int, fieldName string) error {
	if value <= 0 {
		return fmt.Errorf("%s must be positive, got: %d", fieldName, value)
	}
	return nil
}

// ValidatePositiveInt64 checks if an int64 is positive
func ValidatePositiveInt64(value int64, fieldName string) error {
	if value <= 0 {
		return fmt.Errorf("%s must be positive, got: %d", fieldName, value)
	}
	return nil
}

// ValidateRange checks if a value is within a specified range
func ValidateRange(value, min, max int, fieldName string) error {
	if value < min || value > max {
		return fmt.Errorf("%s must be between %d and %d, got: %d",
			fieldName, min, max, value)
	}
	return nil
}

// ValidateStringLength checks if a string length is within bounds
func ValidateStringLength(value string, minLen, maxLen int, fieldName string) error {
	length := len(value)
	if length < minLen {
		return fmt.Errorf("%s must be at least %d characters, got: %d",
			fieldName, minLen, length)
	}
	if length > maxLen {
		return fmt.Errorf("%s must be at most %d characters, got: %d",
			fieldName, maxLen, length)
	}
	return nil
}

// ValidateOneOf checks if a value is one of the allowed values
func ValidateOneOf(value string, allowed []string, fieldName string) error {
	for _, allowedValue := range allowed {
		if value == allowedValue {
			return nil
		}
	}

	return fmt.Errorf("%s must be one of %v, got: %s",
		fieldName, allowed, value)
}
