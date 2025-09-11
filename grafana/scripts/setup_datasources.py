import requests
import json

GRAFANA_URL = "http://localhost:3000"
ADMIN_USER = "admin"
ADMIN_PASS = "n3utronST@R."

"""
Current Recommended Method (September 2025)
Install plugins through the Grafana UI (not CLI):

Open Grafana: http://localhost:3000

Navigate to: Administration → Plugins and data → Plugins

Search for: "JSON API" or "Infinity"

Click the plugin → Install
"""


def setup_api_datasource():
    """Add your Flask API as an Infinity data source"""
    datasource_config = {
        "name": "Illinois Energy API",
        "type": "yesoreyeram-infinity-datasource",
        "url": "http://localhost:5000",
        "access": "proxy",
        "isDefault": True,
        "jsonData": {"datasource_mode": "basic"},
    }

    response = requests.post(
        f"{GRAFANA_URL}/api/datasources",
        json=datasource_config,
        auth=(ADMIN_USER, ADMIN_PASS),
    )

    if response.status_code == 200:
        print("✅ Infinity datasource added successfully")
    else:
        print(f"❌ Failed: {response.text}")


if __name__ == "__main__":
    setup_api_datasource()

# from repo root:
# uv run python grafana/scripts/setup_datasources.py
