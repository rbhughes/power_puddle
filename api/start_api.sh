#!/bin/bash
echo "Starting Power Puddle API..."
cd "$(dirname "$0")/.."
python -m api.puddle_api
