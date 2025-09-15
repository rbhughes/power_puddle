from flask import Flask
from flask_cors import CORS
import duckdb
import os


from .endpoints.carbon_intensive_plants import carbon_intensive_plants_bp
from .endpoints.il_data_centers import il_data_centers_bp
from .endpoints.il_power_plants import il_power_plants_bp
from .endpoints.us_monthly_generation import us_monthly_generation_bp

app = Flask(__name__)
CORS(app)  # Enable CORS for Grafana


def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "../data/puddle.duckdb")
    return duckdb.connect(db_path)


app.register_blueprint(carbon_intensive_plants_bp, url_prefix="/api")
app.register_blueprint(il_data_centers_bp, url_prefix="/api")
app.register_blueprint(il_power_plants_bp, url_prefix="/api")
app.register_blueprint(us_monthly_generation_bp, url_prefix="/api")


@app.route("/api/health")
def health_check():
    return {"status": "healthy", "message": "Power Puddle API"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5500, debug=True)

# http://localhost:5500/api/carbon-intensive-plants
# http://localhost:5500/api/il-data-centers
# http://localhost:5500/api/il-power-plants
# http://localhost:5500/api/us-monthly-generation
# http://localhost:5500/api/il-monthly-generation
