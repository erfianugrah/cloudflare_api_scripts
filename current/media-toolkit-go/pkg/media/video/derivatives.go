package video

import (
	"fmt"
	"net/url"
	"strings"
)

// Derivative represents a video derivative configuration
type Derivative struct {
	Name       string            `json:"name"`
	Width      int               `json:"width"`
	Height     int               `json:"height"`
	URLFormat  string            `json:"url_format"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

// GenerateURL generates the URL for this derivative
func (d *Derivative) GenerateURL(baseURL, filePath string, urlFormat string) (string, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")

	switch urlFormat {
	case "imwidth":
		// Format: /{path}?imwidth={width}
		return fmt.Sprintf("%s/%s?imwidth=%d", baseURL, filePath, d.Width), nil

	case "derivative":
		// Format: /{path}/{derivative_name}/
		return fmt.Sprintf("%s/%s/%s/", baseURL, filePath, d.Name), nil

	case "query":
		// Format: /{path}?width={width}&height={height}
		params := url.Values{}
		params.Add("width", fmt.Sprintf("%d", d.Width))
		params.Add("height", fmt.Sprintf("%d", d.Height))

		// Add any additional parameters
		for key, value := range d.Parameters {
			params.Add(key, value)
		}

		return fmt.Sprintf("%s/%s?%s", baseURL, filePath, params.Encode()), nil

	default:
		// Default to imwidth format
		return fmt.Sprintf("%s/%s?imwidth=%d", baseURL, filePath, d.Width), nil
	}
}

// PredefinedDerivatives contains common video derivative configurations
var PredefinedDerivatives = map[string]*Derivative{
	"desktop": {
		Name:   "desktop",
		Width:  1920,
		Height: 1080,
	},
	"tablet": {
		Name:   "tablet",
		Width:  1280,
		Height: 720,
	},
	"mobile": {
		Name:   "mobile",
		Width:  854,
		Height: 640,
	},
	"mobile_small": {
		Name:   "mobile_small",
		Width:  640,
		Height: 480,
	},
	"mobile_tiny": {
		Name:   "mobile_tiny",
		Width:  480,
		Height: 360,
	},

	// Additional common resolutions
	"4k": {
		Name:   "4k",
		Width:  3840,
		Height: 2160,
	},
	"2k": {
		Name:   "2k",
		Width:  2560,
		Height: 1440,
	},
	"1080p": {
		Name:   "1080p",
		Width:  1920,
		Height: 1080,
	},
	"720p": {
		Name:   "720p",
		Width:  1280,
		Height: 720,
	},
	"480p": {
		Name:   "480p",
		Width:  854,
		Height: 480,
	},
	"360p": {
		Name:   "360p",
		Width:  640,
		Height: 360,
	},
	"240p": {
		Name:   "240p",
		Width:  426,
		Height: 240,
	},

	// Aspect ratio specific
	"widescreen": {
		Name:   "widescreen",
		Width:  1920,
		Height: 800,
	},
	"ultrawide": {
		Name:   "ultrawide",
		Width:  2560,
		Height: 1080,
	},
	"square": {
		Name:   "square",
		Width:  1080,
		Height: 1080,
	},
	"portrait": {
		Name:   "portrait",
		Width:  1080,
		Height: 1920,
	},

	// Social media optimized
	"instagram_feed": {
		Name:   "instagram_feed",
		Width:  1080,
		Height: 1080,
	},
	"instagram_story": {
		Name:   "instagram_story",
		Width:  1080,
		Height: 1920,
	},
	"youtube_thumbnail": {
		Name:   "youtube_thumbnail",
		Width:  1280,
		Height: 720,
	},
	"facebook_video": {
		Name:   "facebook_video",
		Width:  1280,
		Height: 720,
	},
	"twitter_video": {
		Name:   "twitter_video",
		Width:  1280,
		Height: 720,
	},

	// Bandwidth optimized
	"low_bandwidth": {
		Name:   "low_bandwidth",
		Width:  640,
		Height: 360,
		Parameters: map[string]string{
			"quality": "low",
			"bitrate": "500k",
		},
	},
	"medium_bandwidth": {
		Name:   "medium_bandwidth",
		Width:  1280,
		Height: 720,
		Parameters: map[string]string{
			"quality": "medium",
			"bitrate": "2M",
		},
	},
	"high_bandwidth": {
		Name:   "high_bandwidth",
		Width:  1920,
		Height: 1080,
		Parameters: map[string]string{
			"quality": "high",
			"bitrate": "5M",
		},
	},
}

// GetDerivatives returns derivatives to process based on derivative names
func GetDerivatives(derivativeNames []string) map[string]*Derivative {
	if len(derivativeNames) == 0 {
		// Return default derivatives
		return map[string]*Derivative{
			"desktop": PredefinedDerivatives["desktop"],
			"tablet":  PredefinedDerivatives["tablet"],
			"mobile":  PredefinedDerivatives["mobile"],
		}
	}

	result := make(map[string]*Derivative)
	for _, name := range derivativeNames {
		if derivative, exists := PredefinedDerivatives[name]; exists {
			result[name] = derivative
		} else {
			// Create a custom derivative with basic dimensions if not found
			// This allows for dynamic derivative creation
			result[name] = &Derivative{
				Name:   name,
				Width:  1280, // Default width
				Height: 720,  // Default height
			}
		}
	}
	return result
}

// GetDerivativeNames returns all available derivative names
func GetDerivativeNames() []string {
	names := make([]string, 0, len(PredefinedDerivatives))
	for name := range PredefinedDerivatives {
		names = append(names, name)
	}
	return names
}

// GetDerivativeDimensions returns the dimensions for a specific derivative
func GetDerivativeDimensions(derivativeName string) (width, height int) {
	if derivative, exists := PredefinedDerivatives[derivativeName]; exists {
		return derivative.Width, derivative.Height
	}

	// Default dimensions for unknown derivatives
	return 1280, 720
}
