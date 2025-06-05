#!/bin/bash

# Test the prewarm command with improved queue handling

echo "Testing prewarm with improved queue handling..."
echo "Configuration:"
echo "- Workers: 1000 (to reduce queue pressure)"
echo "- Queue multiplier: 10.0 (increased from default 3.0)"
echo "- Timeout: 180 seconds"
echo ""

./bin/toolkit prewarm \
    --remote ikea-ingkadam \
    --bucket ingka-dam-im-4-video-prod \
    --directory m \
    --base-url https://cdn.erfi.dev/m/ \
    --extensions .mp4,.mov \
    --media-type video \
    --derivatives desktop,tablet,mobile \
    --workers 1000 \
    --queue-multiplier 10.0 \
    --timeout 180 \
    --verbose

echo ""
echo "Test completed. Check the logs above for any 'queue full' errors."