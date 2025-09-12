from flask import Blueprint, jsonify
import pandas as pd

mart_monthly_bp = Blueprint("mart_monthly", __name__)


@mart_monthly_bp.route("/monthly-generation-trends")
def get_monthly_generation_trends():
    """
    Endpoint for monthly generation trends line chart
    Returns time series data grouped by fuel category
    DOES NOT WORK --strftime('%Y-%m-%dT%H:%M:%S.000Z', report_date) as report_date,
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT 
                report_date,
                fuel_category,
                SUM(total_net_generation_mwh) as generation_mwh
            FROM main_marts.mart_monthly_generation_summary
            WHERE report_date >= '2020-01-01'  -- Last 5 years for performance
            GROUP BY report_date, fuel_category
            ORDER BY report_date, fuel_category
        """).df()

        # ISO date alone does not satisfy grafana. Still need transform
        df["report_date"] = pd.to_datetime(df["report_date"]).dt.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        return jsonify(df.to_dict("records"))
    finally:
        conn.close()


@mart_monthly_bp.route("/generation-by-state")
def get_generation_by_state():
    """
    Endpoint for state-level generation geomap/heatmap
    Returns latest 12 months aggregated by state
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT 
                state,
                SUM(total_net_generation_mwh) as total_generation_mwh,
                AVG(heat_rate_btu_per_kwh) as avg_heat_rate,
                COUNT(DISTINCT plant_id_eia) as plant_count
            FROM main_marts.mart_monthly_generation_summary
            WHERE report_date >= CURRENT_DATE - INTERVAL '12 months'
            AND state IS NOT NULL
            GROUP BY state
            ORDER BY total_generation_mwh DESC
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()


@mart_monthly_bp.route("/monthly-generation-by-fuel")
def get_monthly_generation_by_fuel():
    """
    Endpoint for monthly generation by fuel type stacked bar chart
    Returns data suitable for stacked visualization
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT 
                report_date,
                report_year,
                report_month,
                fuel_category,
                SUM(total_net_generation_mwh) as generation_mwh,
                SUM(total_fuel_consumed_mmbtu) as fuel_consumed_mmbtu
            FROM main_marts.mart_monthly_generation_summary
            WHERE report_date >= '2020-01-01'  -- Last 5 years
            AND fuel_category IS NOT NULL
            GROUP BY report_date, report_year, report_month, fuel_category
            ORDER BY report_date, fuel_category
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()


@mart_monthly_bp.route("/plant-efficiency-rankings")
def get_plant_efficiency_rankings():
    """
    Bonus endpoint for plant efficiency comparison
    Returns top and bottom performers by heat rate
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT 
                plant_name_eia,
                state,
                fuel_category,
                AVG(heat_rate_btu_per_kwh) as avg_heat_rate,
                SUM(total_net_generation_mwh) as total_generation_mwh,
                AVG(avg_fuel_mmbtu_per_unit) as avg_fuel_quality
            FROM main_marts.mart_monthly_generation_summary
            WHERE report_date >= CURRENT_DATE - INTERVAL '12 months'
            AND heat_rate_btu_per_kwh IS NOT NULL
            AND total_net_generation_mwh > 1000  -- Filter out very small plants
            GROUP BY plant_name_eia, state, fuel_category
            HAVING COUNT(*) >= 6  -- At least 6 months of data
            ORDER BY avg_heat_rate ASC
            LIMIT 50
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()
