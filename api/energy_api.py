from flask import Flask
from flask_cors import CORS
import duckdb
import os

# Import endpoint blueprints
from .endpoints.carbon_plants import carbon_plants_bp
from .endpoints.mart_monthly import mart_monthly_bp
# from endpoints.fuel_mix import fuel_mix_bp
# from endpoints.datacenter_proximity import datacenter_bp

app = Flask(__name__)
CORS(app)  # Enable CORS for Grafana


# Database connection helper
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "../data/puddle.duckdb")
    return duckdb.connect(db_path)


app.register_blueprint(carbon_plants_bp, url_prefix="/api")
app.register_blueprint(mart_monthly_bp, url_prefix="/api")

# app.register_blueprint(fuel_mix_bp, url_prefix="/api")
# app.register_blueprint(datacenter_bp, url_prefix="/api")


@app.route("/api/health")
def health_check():
    return {"status": "healthy", "message": "Illinois Energy API"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5500, debug=True)
