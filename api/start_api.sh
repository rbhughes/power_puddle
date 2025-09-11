#!/bin/bash
echo "Starting Illinois Energy API..."
cd "$(dirname "$0")"
python energy_api.py
