package config

import (
	"testing"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestMediaTypeFiltering(t *testing.T) {
	tests := []struct {
		name           string
		mediaType      string
		expectedType   MediaType
		expectedExts   int // expected number of extensions
	}{
		{
			name:         "Video media type",
			mediaType:    "video",
			expectedType: MediaTypeVideo,
			expectedExts: 6, // .mp4, .webm, .mov, .avi, .mkv, .m4v
		},
		{
			name:         "Image media type",
			mediaType:    "image",
			expectedType: MediaTypeImage,
			expectedExts: 7, // .jpg, .jpeg, .png, .webp, .gif, .bmp, .svg
		},
		{
			name:         "Auto media type",
			mediaType:    "auto",
			expectedType: MediaTypeAuto,
			expectedExts: 13, // Both image and video extensions
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset viper for clean test
			viper.Reset()
			SetDefaults()
			
			// Set media type and required fields for validation
			viper.Set("media-type", tt.mediaType)
			viper.Set("remote", "test-remote")
			viper.Set("bucket", "test-bucket")
			viper.Set("base-url", "https://example.com")
			
			// Load config
			cfg, err := LoadConfig()
			assert.NoError(t, err)
			assert.NotNil(t, cfg)
			
			// Check media type
			assert.Equal(t, tt.expectedType, cfg.MediaType)
			
			// Check extensions count
			totalExts := len(cfg.ImageExtensions) + len(cfg.VideoExtensions)
			assert.GreaterOrEqual(t, totalExts, tt.expectedExts)
		})
	}
}

func TestSingleExtensionOverride(t *testing.T) {
	// Reset viper
	viper.Reset()
	SetDefaults()
	
	// Set single extension and required fields
	viper.Set("extension", ".mp4")
	viper.Set("remote", "test-remote")
	viper.Set("bucket", "test-bucket")
	viper.Set("base-url", "https://example.com")
	
	cfg, err := LoadConfig()
	assert.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, ".mp4", cfg.Extension)
}

func TestGetSizeCategory(t *testing.T) {
	tests := []struct {
		name       string
		sizeBytes  int64
		expected   SizeCategory
	}{
		{
			name:      "Small file (10MB)",
			sizeBytes: 10 * 1024 * 1024,
			expected:  SizeCategorySmall,
		},
		{
			name:      "Medium file (100MB)",
			sizeBytes: 100 * 1024 * 1024,
			expected:  SizeCategoryMedium,
		},
		{
			name:      "Large file (500MB)",
			sizeBytes: 500 * 1024 * 1024,
			expected:  SizeCategoryLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			category := GetSizeCategory(tt.sizeBytes, 50, 200)
			assert.Equal(t, tt.expected, category)
		})
	}
}