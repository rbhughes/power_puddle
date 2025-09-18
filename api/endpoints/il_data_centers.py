from flask import Blueprint, jsonify

il_data_centers_bp = Blueprint("il_data_centers", __name__, url_prefix="/api")


@il_data_centers_bp.route("/il-data-centers")
def get_illinois_data_centers():
    """Endpoint for Illinois data centers with coordinates"""

    from ..energy_api import get_db_connection

    conn = get_db_connection()

    try:
        df = conn.execute("""
            select 
                data_center_name,
                latitude as dc_latitude,
                longitude as dc_longitude 
            from main_marts.dim_data_center
            where latitude is not null 
              and longitude is not null
        """).df()

        return jsonify(df.to_dict("records"))

    finally:
        conn.close()
