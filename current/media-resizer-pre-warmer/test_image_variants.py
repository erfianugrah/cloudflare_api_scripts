#!/usr/bin/env python3
"""Test script to verify image variant URL generation."""

from modules.image_processing import IMAGE_VARIANTS, ImageVariant

# Test URL generation for different variant types
test_base_url = "https://cdn.example.com"
test_file_path = "images/test.jpg"

print("Testing Image Variant URL Generation")
print("=" * 50)
print(f"Base URL: {test_base_url}")
print(f"File Path: {test_file_path}")
print("=" * 50)

# Test a few key variants
test_variants = [
    "thumbnail",
    "webp",
    "akamai_resize_small",
    "akamai_quality",
    "path_webp",
    "smart_square",
    "og_image"
]

for variant_name in test_variants:
    if variant_name in IMAGE_VARIANTS:
        variant = IMAGE_VARIANTS[variant_name]
        url = variant.generate_url(test_base_url, test_file_path)
        print(f"\n{variant_name}:")
        print(f"  URL: {url}")
        if variant.params:
            print(f"  Params: {variant.params}")
        if variant.path_params:
            print(f"  Path params: {variant.path_params}")

# Test Akamai encoding specifically
print("\n" + "=" * 50)
print("Akamai Image Manager URL Encoding Test:")
print("=" * 50)

akamai_variants = [k for k in IMAGE_VARIANTS.keys() if k.startswith('akamai_')]
for variant_name in akamai_variants:
    variant = IMAGE_VARIANTS[variant_name]
    url = variant.generate_url(test_base_url, test_file_path)
    print(f"\n{variant_name}:")
    print(f"  Full URL: {url}")
    # Extract the query part
    if '?' in url:
        query_part = url.split('?', 1)[1]
        print(f"  Query string: {query_part}")