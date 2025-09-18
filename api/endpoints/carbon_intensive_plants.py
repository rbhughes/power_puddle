from flask import Blueprint, jsonify

carbon_intensive_plants_bp = Blueprint("carbon_intensive_plants", __name__)


@carbon_intensive_plants_bp.route("/carbon-intensive-plants")
def get_carbon_plants():
    from ..puddle_api import (
        get_db_connection,
    )  # Import moved here to avoid circular import

    conn = get_db_connection()
    try:
        df = conn.execute("""
            select 
                plant_name_eia,
                state,
                total_generation_mwh,
                avg_heat_rate,
                generation_rank
            from main_marts.dashboard_carbon_intensive_plants 
            order by total_generation_mwh desc 
            limit 20
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()
