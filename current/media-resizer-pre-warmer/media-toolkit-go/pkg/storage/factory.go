package storage

import (
	"fmt"

	"go.uber.org/zap"
)

// DefaultStorageFactory implements StorageFactory interface
type DefaultStorageFactory struct {
	logger *zap.Logger
}

// NewStorageFactory creates a new storage factory
func NewStorageFactory(logger *zap.Logger) *DefaultStorageFactory {
	return &DefaultStorageFactory{
		logger: logger,
	}
}

// CreateStorage creates a storage instance based on the configuration
func (f *DefaultStorageFactory) CreateStorage(config StorageConfig) (Storage, error) {
	switch config.Backend {
	case StorageBackendRclone:
		return NewRcloneStorage(config, f.logger), nil
	case StorageBackendAWS:
		return NewAWSStorage(config, f.logger)
	case StorageBackendLocal:
		return NewLocalStorage(config, f.logger), nil
	default:
		return nil, fmt.Errorf("unsupported storage backend: %s", config.Backend)
	}
}

// CreateStorageFromConfig creates storage from application config with intelligent backend selection
func CreateStorageFromConfig(appConfig interface{}, logger *zap.Logger) (Storage, error) {
	// This would need to be adapted based on your main config structure
	// For now, let's assume we have the necessary fields accessible
	
	// Default to rclone for compatibility, but allow override
	backend := StorageBackendRclone
	
	// You can add logic here to auto-detect the best backend
	// For example, check if AWS credentials are available and prefer AWS SDK
	// Or check environment variables to determine the backend
	
	storageConfig := StorageConfig{
		Backend:   backend,
		// These would be populated from your main config
		// Remote:    appConfig.Remote,
		// Bucket:    appConfig.Bucket,
		// Directory: appConfig.Directory,
	}
	
	factory := NewStorageFactory(logger)
	return factory.CreateStorage(storageConfig)
}

// Helper function to detect optimal storage backend
func DetectOptimalBackend(logger *zap.Logger) StorageBackend {
	// Check for AWS credentials
	// You could check environment variables, config files, etc.
	// For now, default to rclone for compatibility
	
	logger.Debug("Detecting optimal storage backend")
	
	// Check if rclone is available
	// if isRcloneAvailable() {
	//     return StorageBackendRclone
	// }
	
	// Check if AWS credentials are configured
	// if isAWSConfigured() {
	//     return StorageBackendAWS
	// }
	
	// Default to rclone
	logger.Info("Using rclone as default storage backend")
	return StorageBackendRclone
}