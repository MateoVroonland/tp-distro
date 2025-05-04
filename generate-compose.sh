#!/bin/bash

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found" >&2
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install required packages if not already installed
if ! python -c "import yaml" 2>/dev/null || ! python -c "import dotenv" 2>/dev/null; then
    echo "Installing required packages..."
    pip install pyyaml==6.0.1 python-dotenv==1.0.0
fi

# Run the compose generator
python compose-generator.py
echo "Compose generated"

# Deactivate virtual environment
deactivate