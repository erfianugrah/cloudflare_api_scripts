#!/bin/bash
# Script to run the modular media-resizer-pre-warmer

# Ensure we're in the right directory
cd "$(dirname "$0")"

# Check for required packages
if ! python3 -c "import requests, numpy, tabulate" 2>/dev/null; then
  echo "Installing required packages..."
  pip3 install -r requirements.txt
fi

# Pass all arguments to the main.py script
PYTHONPATH=. python3 main.py "$@"