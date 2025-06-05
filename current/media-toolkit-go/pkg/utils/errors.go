package utils

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
)

// WrapError wraps an error with additional context
func WrapError(err error, msg string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", msg, err)
}

// WrapErrorf wraps an error with formatted context
func WrapErrorf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err)
}

// IsTimeoutError checks if an error is a timeout error
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for context timeout
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for network timeout
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}

	// Check for URL timeout
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Timeout()
	}

	// Check error message for timeout indicators
	errStr := strings.ToLower(err.Error())
	timeoutIndicators := []string{
		"timeout",
		"deadline exceeded",
		"context deadline exceeded",
		"i/o timeout",
		"connection timed out",
	}

	for _, indicator := range timeoutIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

// IsNetworkError checks if an error is a network-related error
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Check for URL errors
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}

	// Check for DNS errors
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	// Check error message for network indicators
	errStr := strings.ToLower(err.Error())
	networkIndicators := []string{
		"connection refused",
		"connection reset",
		"no such host",
		"network unreachable",
		"connection timed out",
		"dial tcp",
		"dial udp",
		"lookup failed",
		"no route to host",
	}

	for _, indicator := range networkIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

// IsFileNotFoundError checks if an error indicates a file not found
func IsFileNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Check for os.ErrNotExist
	if errors.Is(err, os.ErrNotExist) {
		return true
	}

	// Check for syscall errors
	if errors.Is(err, syscall.ENOENT) {
		return true
	}

	// Check error message
	errStr := strings.ToLower(err.Error())
	notFoundIndicators := []string{
		"no such file or directory",
		"file not found",
		"cannot find the file",
		"does not exist",
	}

	for _, indicator := range notFoundIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

// IsPermissionError checks if an error indicates a permission problem
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for permission errors
	if errors.Is(err, os.ErrPermission) {
		return true
	}

	// Check for syscall errors
	if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
		return true
	}

	// Check error message
	errStr := strings.ToLower(err.Error())
	permissionIndicators := []string{
		"permission denied",
		"access denied",
		"operation not permitted",
		"insufficient privileges",
	}

	for _, indicator := range permissionIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

// IsDiskFullError checks if an error indicates disk space issues
func IsDiskFullError(err error) bool {
	if err == nil {
		return false
	}

	// Check for syscall errors
	if errors.Is(err, syscall.ENOSPC) {
		return true
	}

	// Check error message
	errStr := strings.ToLower(err.Error())
	diskFullIndicators := []string{
		"no space left on device",
		"disk full",
		"insufficient disk space",
		"not enough space",
	}

	for _, indicator := range diskFullIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

// IsRetryableError checks if an error is likely to be retryable
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Timeout errors are usually retryable
	if IsTimeoutError(err) {
		return true
	}

	// Some network errors are retryable
	if IsNetworkError(err) {
		errStr := strings.ToLower(err.Error())
		retryableNetworkErrors := []string{
			"connection refused",
			"connection reset",
			"network unreachable",
			"temporary failure",
			"service unavailable",
		}

		for _, indicator := range retryableNetworkErrors {
			if strings.Contains(errStr, indicator) {
				return true
			}
		}
	}

	// Check for HTTP status code errors that are retryable
	errStr := strings.ToLower(err.Error())
	retryableHttpErrors := []string{
		"status 429", // Too Many Requests
		"status 500", // Internal Server Error
		"status 502", // Bad Gateway
		"status 503", // Service Unavailable
		"status 504", // Gateway Timeout
	}

	for _, indicator := range retryableHttpErrors {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

// CombineErrors combines multiple errors into a single error
func CombineErrors(errors []error) error {
	if len(errors) == 0 {
		return nil
	}

	// Filter out nil errors
	validErrors := make([]error, 0, len(errors))
	for _, err := range errors {
		if err != nil {
			validErrors = append(validErrors, err)
		}
	}

	if len(validErrors) == 0 {
		return nil
	}

	if len(validErrors) == 1 {
		return validErrors[0]
	}

	// Combine error messages
	var messages []string
	for _, err := range validErrors {
		messages = append(messages, err.Error())
	}

	return fmt.Errorf("multiple errors occurred: %s", strings.Join(messages, "; "))
}

// NewTimeoutError creates a new timeout error
func NewTimeoutError(operation string, timeout time.Duration) error {
	return fmt.Errorf("%s timed out after %v", operation, timeout)
}

// NewValidationError creates a new validation error
func NewValidationError(field, message string) error {
	return fmt.Errorf("validation error for %s: %s", field, message)
}

// NewConfigError creates a new configuration error
func NewConfigError(message string) error {
	return fmt.Errorf("configuration error: %s", message)
}
