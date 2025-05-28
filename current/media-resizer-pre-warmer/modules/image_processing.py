"""
Image processing module for media resizer pre-warmer.
Contains functions for image URL generation and processing based on image-resizer-2 patterns.
"""
import requests
import time
import logging
from urllib.parse import urlencode, quote

logger = logging.getLogger(__name__)

class ImageVariant:
    """Represents an image transformation variant."""
    
    def __init__(self, name, params=None, path_params=None):
        self.name = name
        self.params = params or {}
        self.path_params = path_params or []
    
    def generate_url(self, base_url, file_path):
        """Generate URL for this variant."""
        if self.path_params:
            # Path-based parameters (e.g., /_width=800/_format=webp/image.jpg)
            path_prefix = "/".join([f"_{k}={v}" for k, v in self.path_params])
            return f"{base_url.rstrip('/')}/{path_prefix}/{file_path}"
        elif self.params:
            # Query parameter-based (e.g., ?width=800&height=600&fit=cover)
            query_string = urlencode(self.params)
            return f"{base_url.rstrip('/')}/{file_path}?{query_string}"
        else:
            # No parameters (original)
            return f"{base_url.rstrip('/')}/{file_path}"

# Predefined image variants based on image-resizer-2 documentation
IMAGE_VARIANTS = {
    # Basic query parameter variants
    "thumbnail": ImageVariant("thumbnail", {"width": 150, "height": 150, "fit": "cover"}),
    "small": ImageVariant("small", {"width": 400}),
    "medium": ImageVariant("medium", {"width": 800}),
    "large": ImageVariant("large", {"width": 1200}),
    "xlarge": ImageVariant("xlarge", {"width": 1920}),
    "xxlarge": ImageVariant("xxlarge", {"width": 2560}),
    
    # Mobile-optimized sizes
    "mobile_small": ImageVariant("mobile_small", {"width": 320}),
    "mobile_medium": ImageVariant("mobile_medium", {"width": 640}),
    "mobile_large": ImageVariant("mobile_large", {"width": 960}),
    
    # Aspect ratio variants
    "square": ImageVariant("square", {"width": 600, "height": 600, "fit": "cover"}),
    "landscape": ImageVariant("landscape", {"width": 800, "height": 450, "fit": "cover"}),
    "portrait": ImageVariant("portrait", {"width": 450, "height": 800, "fit": "cover"}),
    "banner": ImageVariant("banner", {"width": 1200, "height": 300, "fit": "cover"}),
    "wide": ImageVariant("wide", {"width": 1600, "height": 400, "fit": "cover"}),
    
    # Compact syntax variants (using r, p, f parameters)
    "compact_16_9": ImageVariant("compact_16_9", {"r": "16:9", "f": "l"}),
    "compact_4_3": ImageVariant("compact_4_3", {"r": "4:3", "f": "m"}),
    "compact_1_1": ImageVariant("compact_1_1", {"r": "1:1", "f": "m"}),
    "compact_focal": ImageVariant("compact_focal", {"r": "16:9", "p": "0.5,0.3", "f": "l"}),
    
    # Format conversion variants
    "webp": ImageVariant("webp", {"format": "webp"}),
    "avif": ImageVariant("avif", {"format": "avif"}),
    "jpeg": ImageVariant("jpeg", {"format": "jpeg", "quality": 85}),
    "png": ImageVariant("png", {"format": "png"}),
    
    # Format with quality variants
    "webp_low": ImageVariant("webp_low", {"format": "webp", "quality": 60}),
    "webp_high": ImageVariant("webp_high", {"format": "webp", "quality": 95}),
    "avif_balanced": ImageVariant("avif_balanced", {"format": "avif", "quality": 75}),
    
    # Responsive variants with different formats
    "responsive_small_webp": ImageVariant("responsive_small_webp", {"width": 400, "format": "webp"}),
    "responsive_medium_webp": ImageVariant("responsive_medium_webp", {"width": 800, "format": "webp"}),
    "responsive_large_webp": ImageVariant("responsive_large_webp", {"width": 1200, "format": "webp"}),
    "responsive_small_avif": ImageVariant("responsive_small_avif", {"width": 400, "format": "avif"}),
    "responsive_medium_avif": ImageVariant("responsive_medium_avif", {"width": 800, "format": "avif"}),
    "responsive_large_avif": ImageVariant("responsive_large_avif", {"width": 1200, "format": "avif"}),
    
    # Path-based parameter variants
    "path_small": ImageVariant("path_small", path_params=[("width", 400)]),
    "path_medium": ImageVariant("path_medium", path_params=[("width", 800)]),
    "path_large": ImageVariant("path_large", path_params=[("width", 1200)]),
    "path_webp": ImageVariant("path_webp", path_params=[("format", "webp")]),
    "path_avif": ImageVariant("path_avif", path_params=[("format", "avif")]),
    "path_responsive": ImageVariant("path_responsive", path_params=[("width", 800), ("format", "webp")]),
    "path_quality": ImageVariant("path_quality", path_params=[("width", 1200), ("quality", 90)]),
    
    # Akamai Image Manager compatible variants
    "akamai_resize_small": ImageVariant("akamai_resize_small", {"im": "resize=width:400,height:300,mode:fit"}),
    "akamai_resize_medium": ImageVariant("akamai_resize_medium", {"im": "resize=width:800,height:600,mode:fit"}),
    "akamai_resize_large": ImageVariant("akamai_resize_large", {"im": "resize=width:1200,height:900,mode:fit"}),
    "akamai_crop": ImageVariant("akamai_crop", {"im": "resize=width:800,height:600,mode:crop"}),
    "akamai_stretch": ImageVariant("akamai_stretch", {"im": "resize=width:800,height:600,mode:stretch"}),
    "akamai_quality": ImageVariant("akamai_quality", {"im": "quality=80"}),
    "akamai_format": ImageVariant("akamai_format", {"im": "format=webp"}),
    "akamai_combined": ImageVariant("akamai_combined", {"im": "resize=width:600,height:400,mode:fit&quality=85&format=webp"}),
    
    # Smart crop variants
    "smart_square": ImageVariant("smart_square", {"width": 600, "height": 600, "smart": "true"}),
    "smart_banner": ImageVariant("smart_banner", {"width": 1200, "height": 300, "smart": "true"}),
    "smart_portrait": ImageVariant("smart_portrait", {"width": 400, "height": 600, "smart": "true"}),
    "smart_landscape": ImageVariant("smart_landscape", {"width": 800, "height": 600, "smart": "true"}),
    
    # Social media variants
    "og_image": ImageVariant("og_image", {"width": 1200, "height": 630, "fit": "cover"}),
    "twitter_card": ImageVariant("twitter_card", {"width": 800, "height": 418, "fit": "cover"}),
    "instagram_square": ImageVariant("instagram_square", {"width": 1080, "height": 1080, "fit": "cover"}),
    "instagram_portrait": ImageVariant("instagram_portrait", {"width": 1080, "height": 1350, "fit": "cover"}),
    "instagram_landscape": ImageVariant("instagram_landscape", {"width": 1080, "height": 608, "fit": "cover"}),
    "facebook_cover": ImageVariant("facebook_cover", {"width": 851, "height": 315, "fit": "cover"}),
    "linkedin_post": ImageVariant("linkedin_post", {"width": 1200, "height": 627, "fit": "cover"}),
    
    # Quality variants
    "low_quality": ImageVariant("low_quality", {"quality": 60}),
    "medium_quality": ImageVariant("medium_quality", {"quality": 75}),
    "high_quality": ImageVariant("high_quality", {"quality": 90}),
    "max_quality": ImageVariant("max_quality", {"quality": 100}),
    
    # Fit mode variants
    "fit_contain": ImageVariant("fit_contain", {"width": 800, "height": 600, "fit": "contain"}),
    "fit_cover": ImageVariant("fit_cover", {"width": 800, "height": 600, "fit": "cover"}),
    "fit_pad": ImageVariant("fit_pad", {"width": 800, "height": 600, "fit": "pad"}),
    "fit_scale_down": ImageVariant("fit_scale_down", {"width": 800, "height": 600, "fit": "scale-down"}),
    
    # Effects and filters
    "blurred": ImageVariant("blurred", {"blur": 10}),
    "blurred_heavy": ImageVariant("blurred_heavy", {"blur": 20}),
    "grayscale": ImageVariant("grayscale", {"grayscale": "true"}),
    "sharpen": ImageVariant("sharpen", {"sharpen": 2}),
    
    # Combination variants for testing
    "combo_webp_small": ImageVariant("combo_webp_small", {"width": 400, "format": "webp", "quality": 80}),
    "combo_avif_medium": ImageVariant("combo_avif_medium", {"width": 800, "format": "avif", "quality": 75}),
    "combo_smart_webp": ImageVariant("combo_smart_webp", {"width": 1200, "height": 630, "smart": "true", "format": "webp"}),
    "combo_effect_resize": ImageVariant("combo_effect_resize", {"width": 600, "blur": 5, "quality": 85}),
}

def get_image_variants(variant_names=None):
    """
    Get image variants to process.
    
    Args:
        variant_names: List of variant names or None for all
        
    Returns:
        Dictionary of variants
    """
    if variant_names is None:
        return IMAGE_VARIANTS
    
    return {name: IMAGE_VARIANTS[name] for name in variant_names if name in IMAGE_VARIANTS}

def process_image_variant(obj_data, variant, base_url, timeout=30, retry_attempts=2, 
                         connection_close_delay=0, logger=None, use_head_request=False):
    """
    Process a single image variant.
    
    Args:
        obj_data: Image file metadata
        variant: ImageVariant instance
        base_url: Base URL for the image service
        timeout: Request timeout
        retry_attempts: Number of retries
        connection_close_delay: Delay before closing connection
        logger: Logger instance
        use_head_request: Whether to use HEAD requests
        
    Returns:
        Dictionary with processing results
    """
    result = {
        'variant_name': variant.name,
        'status': 'unknown',
        'status_code': None,
        'time_to_first_byte': None,
        'total_time': None,
        'response_size_bytes': None,
        'content_type': None,
        'retries': 0,
        'start_time': time.time(),
    }
    
    # Generate URL for this variant
    url = variant.generate_url(base_url, obj_data.path)
    result['url'] = url
    
    if logger:
        logger.debug(f"Processing image variant '{variant.name}': {url}")
    
    # Attempt request with retries
    for attempt in range(retry_attempts + 1):
        if attempt > 0:
            if logger:
                logger.debug(f"Retry {attempt}/{retry_attempts} for {url}")
            result['retries'] = attempt
            time.sleep(2 * attempt)
        
        try:
            # Try HEAD request first if enabled
            if use_head_request:
                try:
                    response = requests.head(url, timeout=timeout, allow_redirects=True)
                    
                    if response.status_code < 400:
                        result['status_code'] = response.status_code
                        result['time_to_first_byte'] = response.elapsed.total_seconds()
                        result['total_time'] = result['time_to_first_byte']
                        result['status'] = 'success'
                        result['method_used'] = 'HEAD'
                        
                        # Get content info from headers
                        if 'Content-Length' in response.headers:
                            result['response_size_bytes'] = int(response.headers['Content-Length'])
                        if 'Content-Type' in response.headers:
                            result['content_type'] = response.headers['Content-Type']
                        
                        if logger:
                            logger.debug(f"HEAD request successful for {url}")
                        break
                        
                except Exception as e:
                    if logger:
                        logger.debug(f"HEAD request failed, falling back to GET: {str(e)}")
            
            # GET request
            with requests.get(url, stream=True, timeout=timeout) as response:
                result['status_code'] = response.status_code
                
                if response.status_code >= 400:
                    result['status'] = 'error'
                    result['error_type'] = 'http_error'
                    
                    if response.status_code < 500:
                        break  # Client error, don't retry
                else:
                    # Success
                    ttfb = response.elapsed.total_seconds()
                    result['time_to_first_byte'] = ttfb
                    result['method_used'] = 'GET'
                    result['content_type'] = response.headers.get('Content-Type', '')
                    
                    # Stream content to get size
                    download_start = time.time()
                    content_size = 0
                    
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            content_size += len(chunk)
                    
                    download_time = time.time() - download_start
                    
                    result['total_time'] = ttfb + download_time
                    result['response_size_bytes'] = content_size
                    result['status'] = 'success'
                    
                    # Calculate size reduction if we have original size
                    if hasattr(obj_data, 'size_bytes') and obj_data.size_bytes > 0 and content_size > 0:
                        result['original_size_bytes'] = obj_data.size_bytes
                        size_diff = obj_data.size_bytes - content_size
                        reduction_percent = (size_diff / obj_data.size_bytes) * 100
                        result['size_reduction_bytes'] = size_diff
                        result['size_reduction_percent'] = reduction_percent
                    
                    if connection_close_delay > 0:
                        time.sleep(connection_close_delay)
                    
                    break
                    
        except requests.exceptions.Timeout:
            result['status'] = 'timeout'
            result['error_type'] = 'timeout'
            
        except requests.exceptions.ConnectionError as e:
            result['status'] = 'connection_error'
            result['error_type'] = 'connection_error'
            result['error_details'] = str(e)
            
        except Exception as e:
            result['status'] = 'exception'
            result['error_type'] = 'unknown'
            result['error_details'] = str(e)
    
    result['end_time'] = time.time()
    result['duration'] = result['end_time'] - result['start_time']
    
    return result

def process_image_object(obj_data, base_url, variants, timeout=30, retry_attempts=2,
                        connection_close_delay=0, logger=None, use_head_request=False):
    """
    Process an image object with all specified variants.
    
    Args:
        obj_data: Image file metadata
        base_url: Base URL for the image service
        variants: List of variant names to process
        timeout: Request timeout
        retry_attempts: Number of retries
        connection_close_delay: Delay before closing connection
        logger: Logger instance
        use_head_request: Whether to use HEAD requests
        
    Returns:
        Dictionary with variant results
    """
    # Mark start of processing
    if hasattr(obj_data, 'start_processing'):
        obj_data.start_processing()
    
    results = {}
    image_variants = get_image_variants(variants)
    
    for variant_name, variant in image_variants.items():
        if hasattr(obj_data, 'start_derivative_processing'):
            obj_data.start_derivative_processing(variant_name)
            
        results[variant_name] = process_image_variant(
            obj_data, variant, base_url, timeout, retry_attempts,
            connection_close_delay, logger, use_head_request
        )
        
        if hasattr(obj_data, 'complete_derivative_processing'):
            obj_data.complete_derivative_processing(variant_name)
    
    # Mark completion
    if hasattr(obj_data, 'complete_processing'):
        obj_data.complete_processing()
    
    return results