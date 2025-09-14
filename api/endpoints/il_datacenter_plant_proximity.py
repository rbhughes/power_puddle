from flask import Blueprint, jsonify
from flask import Response


il_datacenter_plant_proximity_bp = Blueprint("il_datacenter_plant_proximity", __name__)


@il_datacenter_plant_proximity_bp.route("/il-datacenter-plant-proximity")
def get_il_datacenter_plant_proximity():
    """
    Endpoint for Illinois data center and power plant proximity analysis
    Returns complete dataset with coordinates for Grafana geomap
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            WITH recent_generation AS (
                SELECT
                    g.plant_id_eia,
                    SUM(g.net_generation_mwh) AS total_generation_last_year,
                    f.fuel_category AS primary_fuel_category,
                    ROW_NUMBER() OVER (PARTITION BY g.plant_id_eia ORDER BY SUM(g.net_generation_mwh) DESC) AS rn
                FROM main_marts.fact_generation g
                JOIN main_marts.dim_fuel f ON g.fuel_dim_key = f.fuel_dim_key
                JOIN main_marts.dim_time t ON g.report_date = t.report_date
                WHERE t.report_year = 2023 -- Most recent year
                GROUP BY 1, 3
            )
            SELECT
                dc.data_center_name,
                dc.address AS datacenter_address,
                dc.latitude AS datacenter_latitude,
                dc.longitude AS datacenter_longitude,
                p.plant_name_eia AS nearest_plant_name,
                p.county AS plant_county,
                p.latitude AS plant_latitude,
                p.longitude AS plant_longitude,
                dc.nearest_plant_distance_miles,
                -- Plant characteristics
                p.fuel_types_list AS available_fuel_types,
                p.plant_fuel_classification,
                -- Recent generation from nearest plant
                COALESCE(rg.total_generation_last_year, 0) AS nearest_plant_generation_mwh_last_year,
                COALESCE(rg.primary_fuel_category, 'No Recent Data') AS nearest_plant_primary_fuel,
                -- Carbon intensity of nearest power source
                CASE
                    WHEN COALESCE(rg.primary_fuel_category, '') IN ('Coal') THEN 'High Carbon'
                    WHEN COALESCE(rg.primary_fuel_category, '') IN ('Natural Gas', 'Petroleum') THEN 'Medium Carbon'
                    WHEN COALESCE(rg.primary_fuel_category, '') IN ('Nuclear', 'Wind', 'Solar', 'Hydro') THEN 'Low Carbon'
                    ELSE 'Unknown'
                END AS nearest_plant_carbon_profile
            FROM main_marts.dim_data_center dc
            LEFT JOIN main_marts.dim_plant p ON dc.nearest_plant_id_eia = p.plant_id_eia
            LEFT JOIN recent_generation rg ON p.plant_id_eia = rg.plant_id_eia AND rg.rn = 1
        """).df()

        # return jsonify(df.to_dict("records"))
        # avoids The truth value of an empty array is ambiguous. error
        json_str = df.to_json(orient="records", date_format="iso")
        return Response(json_str, mimetype="application/json")

    finally:
        conn.close()


@il_datacenter_plant_proximity_bp.route("/il-datacenters-only")
def get_il_datacenters_only():
    """
    Endpoint for data centers only - optimized for data center layer in Grafana geomap
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT
                dc.data_center_name,
                dc.address AS datacenter_address,
                dc.latitude AS datacenter_latitude,
                dc.longitude AS datacenter_longitude,
                dc.nearest_plant_distance_miles,
                CASE
                    WHEN p.plant_fuel_classification LIKE '%Coal%' THEN 'High Carbon'
                    WHEN p.plant_fuel_classification LIKE '%Gas%' THEN 'Medium Carbon'
                    WHEN p.plant_fuel_classification LIKE '%Nuclear%' OR p.plant_fuel_classification LIKE '%Wind%' 
                         OR p.plant_fuel_classification LIKE '%Solar%' THEN 'Low Carbon'
                    ELSE 'Unknown'
                END AS carbon_category
            FROM main_marts.dim_data_center dc
            LEFT JOIN main_marts.dim_plant p ON dc.nearest_plant_id_eia = p.plant_id_eia
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()


@il_datacenter_plant_proximity_bp.route("/il-plants-serving-datacenters")
def get_il_plants_serving_datacenters():
    """
    Endpoint for power plants that serve data centers - optimized for plant layer in Grafana geomap
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            WITH recent_generation AS (
                SELECT
                    g.plant_id_eia,
                    SUM(g.net_generation_mwh) AS total_generation_last_year,
                    f.fuel_category AS primary_fuel_category,
                    ROW_NUMBER() OVER (PARTITION BY g.plant_id_eia ORDER BY SUM(g.net_generation_mwh) DESC) AS rn
                FROM main_marts.fact_generation g
                JOIN main_marts.dim_fuel f ON g.fuel_dim_key = f.fuel_dim_key
                JOIN main_marts.dim_time t ON g.report_date = t.report_date
                WHERE t.report_year = 2023
                GROUP BY 1, 3
            ),
            plants_with_datacenters AS (
                SELECT DISTINCT dc.nearest_plant_id_eia
                FROM main_marts.dim_data_center dc
                WHERE dc.nearest_plant_id_eia IS NOT NULL
            )
            SELECT
                p.plant_name_eia,
                p.county AS plant_county,
                p.latitude AS plant_latitude,
                p.longitude AS plant_longitude,
                p.fuel_types_list AS available_fuel_types,
                p.plant_fuel_classification,
                COALESCE(rg.total_generation_last_year, 0) AS generation_mwh_last_year,
                COALESCE(rg.primary_fuel_category, 'No Recent Data') AS primary_fuel,
                -- Count of data centers served by this plant
                (SELECT COUNT(*) 
                 FROM main_marts.dim_data_center dc2 
                 WHERE dc2.nearest_plant_id_eia = p.plant_id_eia) AS datacenters_served,
                CASE
                    WHEN COALESCE(rg.primary_fuel_category, '') IN ('Coal') THEN 'High Carbon'
                    WHEN COALESCE(rg.primary_fuel_category, '') IN ('Natural Gas', 'Petroleum') THEN 'Medium Carbon'
                    WHEN COALESCE(rg.primary_fuel_category, '') IN ('Nuclear', 'Wind', 'Solar', 'Hydro') THEN 'Low Carbon'
                    ELSE 'Unknown'
                END AS carbon_profile
            FROM main_marts.dim_plant p
            JOIN plants_with_datacenters pdc ON p.plant_id_eia = pdc.nearest_plant_id_eia
            LEFT JOIN recent_generation rg ON p.plant_id_eia = rg.plant_id_eia AND rg.rn = 1
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()


@il_datacenter_plant_proximity_bp.route("/il-carbon-summary")
def get_il_carbon_summary():
    """
    Endpoint for carbon impact summary statistics
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            WITH carbon_analysis AS (
                SELECT
                    CASE
                        WHEN p.plant_fuel_classification LIKE '%Coal%' THEN 'High Carbon'
                        WHEN p.plant_fuel_classification LIKE '%Gas%' THEN 'Medium Carbon'
                        WHEN p.plant_fuel_classification LIKE '%Nuclear%' OR p.plant_fuel_classification LIKE '%Wind%' 
                             OR p.plant_fuel_classification LIKE '%Solar%' THEN 'Low Carbon'
                        ELSE 'Unknown'
                    END AS carbon_category,
                    COUNT(*) AS datacenter_count
                FROM main_marts.dim_data_center dc
                LEFT JOIN main_marts.dim_plant p ON dc.nearest_plant_id_eia = p.plant_id_eia
                GROUP BY 1
            )
            SELECT 
                carbon_category,
                datacenter_count,
                ROUND(datacenter_count * 100.0 / SUM(datacenter_count) OVER (), 2) AS percentage
            FROM carbon_analysis
            ORDER BY datacenter_count DESC
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()
