from flask import Blueprint, jsonify
from ..energy_api import get_db_connection

carbon_plants_bp = Blueprint("carbon_plants", __name__)


@carbon_plants_bp.route("/carbon-plants")
def get_carbon_plants():
    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT 
                plant_name_eia,
                state,
                total_generation_mwh,
                avg_heat_rate,
                generation_rank
            FROM main_marts.dashboard_carbon_intensive_plants 
            ORDER BY total_generation_mwh DESC 
            LIMIT 20
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()
