package image

import (
	"fmt"
	"net/url"
	"strings"
)

// Variant represents an image transformation variant
type Variant struct {
	Name        string                 `json:"name"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	PathParams  []PathParam            `json:"path_params,omitempty"`
	URLTemplate string                 `json:"url_template,omitempty"`
}

// PathParam represents a path-based parameter like /_width=800/_format=webp/
type PathParam struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// GenerateURL generates the URL for this variant
func (v *Variant) GenerateURL(baseURL, filePath string) (string, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")
	
	if v.URLTemplate != "" {
		// Use custom URL template if provided
		return fmt.Sprintf(v.URLTemplate, baseURL, filePath), nil
	}
	
	if len(v.PathParams) > 0 {
		// Path-based parameters (e.g., /_width=800/_format=webp/image.jpg)
		pathPrefix := ""
		for _, param := range v.PathParams {
			pathPrefix += fmt.Sprintf("/_%s=%s", param.Key, param.Value)
		}
		return fmt.Sprintf("%s%s/%s", baseURL, pathPrefix, filePath), nil
	}
	
	if len(v.Parameters) > 0 {
		// Query parameter-based (e.g., ?width=800&height=600&fit=cover)
		params := url.Values{}
		for key, value := range v.Parameters {
			params.Add(key, fmt.Sprintf("%v", value))
		}
		return fmt.Sprintf("%s/%s?%s", baseURL, filePath, params.Encode()), nil
	}
	
	// No parameters (original)
	return fmt.Sprintf("%s/%s", baseURL, filePath), nil
}

// PredefinedVariants contains all predefined image variants
var PredefinedVariants = map[string]*Variant{
	// Basic query parameter variants
	"thumbnail": {
		Name: "thumbnail",
		Parameters: map[string]interface{}{
			"width": 150, "height": 150, "fit": "cover",
		},
	},
	"small": {
		Name: "small",
		Parameters: map[string]interface{}{
			"width": 400,
		},
	},
	"medium": {
		Name: "medium",
		Parameters: map[string]interface{}{
			"width": 800,
		},
	},
	"large": {
		Name: "large",
		Parameters: map[string]interface{}{
			"width": 1200,
		},
	},
	"xlarge": {
		Name: "xlarge",
		Parameters: map[string]interface{}{
			"width": 1920,
		},
	},
	"xxlarge": {
		Name: "xxlarge",
		Parameters: map[string]interface{}{
			"width": 2560,
		},
	},

	// Mobile-optimized sizes
	"mobile_small": {
		Name: "mobile_small",
		Parameters: map[string]interface{}{
			"width": 320,
		},
	},
	"mobile_medium": {
		Name: "mobile_medium",
		Parameters: map[string]interface{}{
			"width": 640,
		},
	},
	"mobile_large": {
		Name: "mobile_large",
		Parameters: map[string]interface{}{
			"width": 960,
		},
	},

	// Aspect ratio variants
	"square": {
		Name: "square",
		Parameters: map[string]interface{}{
			"width": 600, "height": 600, "fit": "cover",
		},
	},
	"landscape": {
		Name: "landscape",
		Parameters: map[string]interface{}{
			"width": 800, "height": 450, "fit": "cover",
		},
	},
	"portrait": {
		Name: "portrait",
		Parameters: map[string]interface{}{
			"width": 450, "height": 800, "fit": "cover",
		},
	},
	"banner": {
		Name: "banner",
		Parameters: map[string]interface{}{
			"width": 1200, "height": 300, "fit": "cover",
		},
	},
	"wide": {
		Name: "wide",
		Parameters: map[string]interface{}{
			"width": 1600, "height": 400, "fit": "cover",
		},
	},

	// Compact syntax variants (using r, p, f parameters)
	"compact_16_9": {
		Name: "compact_16_9",
		Parameters: map[string]interface{}{
			"r": "16:9", "f": "l",
		},
	},
	"compact_4_3": {
		Name: "compact_4_3",
		Parameters: map[string]interface{}{
			"r": "4:3", "f": "m",
		},
	},
	"compact_1_1": {
		Name: "compact_1_1",
		Parameters: map[string]interface{}{
			"r": "1:1", "f": "m",
		},
	},
	"compact_focal": {
		Name: "compact_focal",
		Parameters: map[string]interface{}{
			"r": "16:9", "p": "0.5,0.3", "f": "l",
		},
	},

	// Format conversion variants
	"webp": {
		Name: "webp",
		Parameters: map[string]interface{}{
			"format": "webp",
		},
	},
	"avif": {
		Name: "avif",
		Parameters: map[string]interface{}{
			"format": "avif",
		},
	},
	"jpeg": {
		Name: "jpeg",
		Parameters: map[string]interface{}{
			"format": "jpeg", "quality": 85,
		},
	},
	"png": {
		Name: "png",
		Parameters: map[string]interface{}{
			"format": "png",
		},
	},

	// Format with quality variants
	"webp_low": {
		Name: "webp_low",
		Parameters: map[string]interface{}{
			"format": "webp", "quality": 60,
		},
	},
	"webp_high": {
		Name: "webp_high",
		Parameters: map[string]interface{}{
			"format": "webp", "quality": 95,
		},
	},
	"avif_balanced": {
		Name: "avif_balanced",
		Parameters: map[string]interface{}{
			"format": "avif", "quality": 75,
		},
	},

	// Responsive variants with different formats
	"responsive_small_webp": {
		Name: "responsive_small_webp",
		Parameters: map[string]interface{}{
			"width": 400, "format": "webp",
		},
	},
	"responsive_medium_webp": {
		Name: "responsive_medium_webp",
		Parameters: map[string]interface{}{
			"width": 800, "format": "webp",
		},
	},
	"responsive_large_webp": {
		Name: "responsive_large_webp",
		Parameters: map[string]interface{}{
			"width": 1200, "format": "webp",
		},
	},
	"responsive_small_avif": {
		Name: "responsive_small_avif",
		Parameters: map[string]interface{}{
			"width": 400, "format": "avif",
		},
	},
	"responsive_medium_avif": {
		Name: "responsive_medium_avif",
		Parameters: map[string]interface{}{
			"width": 800, "format": "avif",
		},
	},
	"responsive_large_avif": {
		Name: "responsive_large_avif",
		Parameters: map[string]interface{}{
			"width": 1200, "format": "avif",
		},
	},

	// Path-based parameter variants
	"path_small": {
		Name: "path_small",
		PathParams: []PathParam{
			{Key: "width", Value: "400"},
		},
	},
	"path_medium": {
		Name: "path_medium",
		PathParams: []PathParam{
			{Key: "width", Value: "800"},
		},
	},
	"path_large": {
		Name: "path_large",
		PathParams: []PathParam{
			{Key: "width", Value: "1200"},
		},
	},
	"path_webp": {
		Name: "path_webp",
		PathParams: []PathParam{
			{Key: "format", Value: "webp"},
		},
	},
	"path_avif": {
		Name: "path_avif",
		PathParams: []PathParam{
			{Key: "format", Value: "avif"},
		},
	},
	"path_responsive": {
		Name: "path_responsive",
		PathParams: []PathParam{
			{Key: "width", Value: "800"},
			{Key: "format", Value: "webp"},
		},
	},
	"path_quality": {
		Name: "path_quality",
		PathParams: []PathParam{
			{Key: "width", Value: "1200"},
			{Key: "quality", Value: "90"},
		},
	},

	// Akamai Image Manager compatible variants
	"akamai_resize_small": {
		Name: "akamai_resize_small",
		Parameters: map[string]interface{}{
			"im": "resize=width:400,height:300,mode:fit",
		},
	},
	"akamai_resize_medium": {
		Name: "akamai_resize_medium",
		Parameters: map[string]interface{}{
			"im": "resize=width:800,height:600,mode:fit",
		},
	},
	"akamai_resize_large": {
		Name: "akamai_resize_large",
		Parameters: map[string]interface{}{
			"im": "resize=width:1200,height:900,mode:fit",
		},
	},
	"akamai_crop": {
		Name: "akamai_crop",
		Parameters: map[string]interface{}{
			"im": "resize=width:800,height:600,mode:crop",
		},
	},
	"akamai_stretch": {
		Name: "akamai_stretch",
		Parameters: map[string]interface{}{
			"im": "resize=width:800,height:600,mode:stretch",
		},
	},
	"akamai_quality": {
		Name: "akamai_quality",
		Parameters: map[string]interface{}{
			"im": "quality=80",
		},
	},
	"akamai_format": {
		Name: "akamai_format",
		Parameters: map[string]interface{}{
			"im": "format=webp",
		},
	},
	"akamai_combined": {
		Name: "akamai_combined",
		Parameters: map[string]interface{}{
			"im": "resize=width:600,height:400,mode:fit&quality=85&format=webp",
		},
	},

	// Smart crop variants
	"smart_square": {
		Name: "smart_square",
		Parameters: map[string]interface{}{
			"width": 600, "height": 600, "smart": "true",
		},
	},
	"smart_banner": {
		Name: "smart_banner",
		Parameters: map[string]interface{}{
			"width": 1200, "height": 300, "smart": "true",
		},
	},
	"smart_portrait": {
		Name: "smart_portrait",
		Parameters: map[string]interface{}{
			"width": 400, "height": 600, "smart": "true",
		},
	},
	"smart_landscape": {
		Name: "smart_landscape",
		Parameters: map[string]interface{}{
			"width": 800, "height": 600, "smart": "true",
		},
	},

	// Social media variants
	"og_image": {
		Name: "og_image",
		Parameters: map[string]interface{}{
			"width": 1200, "height": 630, "fit": "cover",
		},
	},
	"twitter_card": {
		Name: "twitter_card",
		Parameters: map[string]interface{}{
			"width": 800, "height": 418, "fit": "cover",
		},
	},
	"instagram_square": {
		Name: "instagram_square",
		Parameters: map[string]interface{}{
			"width": 1080, "height": 1080, "fit": "cover",
		},
	},
	"instagram_portrait": {
		Name: "instagram_portrait",
		Parameters: map[string]interface{}{
			"width": 1080, "height": 1350, "fit": "cover",
		},
	},
	"instagram_landscape": {
		Name: "instagram_landscape",
		Parameters: map[string]interface{}{
			"width": 1080, "height": 608, "fit": "cover",
		},
	},
	"facebook_cover": {
		Name: "facebook_cover",
		Parameters: map[string]interface{}{
			"width": 851, "height": 315, "fit": "cover",
		},
	},
	"linkedin_post": {
		Name: "linkedin_post",
		Parameters: map[string]interface{}{
			"width": 1200, "height": 627, "fit": "cover",
		},
	},

	// Quality variants
	"low_quality": {
		Name: "low_quality",
		Parameters: map[string]interface{}{
			"quality": 60,
		},
	},
	"medium_quality": {
		Name: "medium_quality",
		Parameters: map[string]interface{}{
			"quality": 75,
		},
	},
	"high_quality": {
		Name: "high_quality",
		Parameters: map[string]interface{}{
			"quality": 90,
		},
	},
	"max_quality": {
		Name: "max_quality",
		Parameters: map[string]interface{}{
			"quality": 100,
		},
	},

	// Fit mode variants
	"fit_contain": {
		Name: "fit_contain",
		Parameters: map[string]interface{}{
			"width": 800, "height": 600, "fit": "contain",
		},
	},
	"fit_cover": {
		Name: "fit_cover",
		Parameters: map[string]interface{}{
			"width": 800, "height": 600, "fit": "cover",
		},
	},
	"fit_pad": {
		Name: "fit_pad",
		Parameters: map[string]interface{}{
			"width": 800, "height": 600, "fit": "pad",
		},
	},
	"fit_scale_down": {
		Name: "fit_scale_down",
		Parameters: map[string]interface{}{
			"width": 800, "height": 600, "fit": "scale-down",
		},
	},

	// Effects and filters
	"blurred": {
		Name: "blurred",
		Parameters: map[string]interface{}{
			"blur": 10,
		},
	},
	"blurred_heavy": {
		Name: "blurred_heavy",
		Parameters: map[string]interface{}{
			"blur": 20,
		},
	},
	"grayscale": {
		Name: "grayscale",
		Parameters: map[string]interface{}{
			"grayscale": "true",
		},
	},
	"sharpen": {
		Name: "sharpen",
		Parameters: map[string]interface{}{
			"sharpen": 2,
		},
	},

	// Combination variants for testing
	"combo_webp_small": {
		Name: "combo_webp_small",
		Parameters: map[string]interface{}{
			"width": 400, "format": "webp", "quality": 80,
		},
	},
	"combo_avif_medium": {
		Name: "combo_avif_medium",
		Parameters: map[string]interface{}{
			"width": 800, "format": "avif", "quality": 75,
		},
	},
	"combo_smart_webp": {
		Name: "combo_smart_webp",
		Parameters: map[string]interface{}{
			"width": 1200, "height": 630, "smart": "true", "format": "webp",
		},
	},
	"combo_effect_resize": {
		Name: "combo_effect_resize",
		Parameters: map[string]interface{}{
			"width": 600, "blur": 5, "quality": 85,
		},
	},
}

// GetVariants returns variants to process based on variant names
func GetVariants(variantNames []string) map[string]*Variant {
	if len(variantNames) == 0 {
		return PredefinedVariants
	}

	result := make(map[string]*Variant)
	for _, name := range variantNames {
		if variant, exists := PredefinedVariants[name]; exists {
			result[name] = variant
		}
	}
	return result
}

// GetVariantNames returns all available variant names
func GetVariantNames() []string {
	names := make([]string, 0, len(PredefinedVariants))
	for name := range PredefinedVariants {
		names = append(names, name)
	}
	return names
}