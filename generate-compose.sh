#!/bin/bash

if [ ! -f ".env" ]; then
    echo "Error: .env file not found" >&2
    exit 1
fi

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

if ! python -c "import yaml" 2>/dev/null || ! python -c "import dotenv" 2>/dev/null; then
    echo "Installing required packages..."
    pip install pyyaml==6.0.1 python-dotenv==1.0.0
fi

python compose-generator.py
echo "Compose generated"

deactivate