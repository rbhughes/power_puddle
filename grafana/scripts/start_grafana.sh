#!/bin/bash
echo "Starting Grafana service..."
brew services start grafana

echo "Grafana started at http://localhost:3000"
# echo "Default login: admin/admin"
