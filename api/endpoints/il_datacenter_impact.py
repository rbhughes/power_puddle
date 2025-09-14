from flask import Blueprint, jsonify
import pandas as pd

il_datacenter_bp = Blueprint("il_datacenter_impact", __name__)


@il_datacenter_bp.route("/il-datacenter-impact")
def get_il_datacenter_impact():
    """
    Endpoint for Illinois data center impact analysis
    Returns time series data showing data center demand vs state generation
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            WITH illinois_generation AS (
                SELECT 
                    t.report_date,
                    t.report_year,
                    SUM(g.net_generation_mwh) AS total_state_generation_mwh,
                    SUM(
                        CASE WHEN f.fuel_category = 'Coal' 
                        THEN g.net_generation_mwh else 0 end)
                          AS coal_generation_mwh,
                    SUM(CASE WHEN f.carbon_intensity = 'High Carbon' 
                        THEN g.net_generation_mwh else 0 end)
                          AS high_carbon_generation_mwh
                FROM main_marts.fact_generation g
                JOIN main_marts.dim_plant p ON g.plant_id_eia = p.plant_id_eia
                JOIN main_marts.dim_fuel f ON g.fuel_dim_key = f.fuel_dim_key  
                JOIN main_marts.dim_time t ON g.report_date = t.report_date
                WHERE p.state = 'IL'
                GROUP BY t.report_date, t.report_year
            ),
            datacenter_metrics AS (
                SELECT 
                    COUNT(*) as total_data_centers,
                    COUNT(DISTINCT SUBSTRING(address, -5))
                          AS counties_with_datacenters,
                    -- Estimate 5-10 MW per datacenter (conservative estimate)
                    COUNT(*) * 7.5 AS estimated_datacenter_demand_mw
                FROM main_marts.dim_data_center
            )
            SELECT
                g.report_date,
                g.report_year,
                g.total_state_generation_mwh,
                g.coal_generation_mwh,
                g.high_carbon_generation_mwh,
                d.total_data_centers,
                d.counties_with_datacenters,
                d.estimated_datacenter_demand_mw,
                -- Calculate potential data center consumption (assume 80% capacity factor)
                (d.estimated_datacenter_demand_mw * 24 * 30 * 0.8)
                    AS estimated_monthly_datacenter_consumption_mwh,
                -- Percentage of state generation that could be used by data centers
                ((d.estimated_datacenter_demand_mw * 24 * 30 * 0.8) / 
                  g.total_state_generation_mwh * 100) AS datacenter_pct_of_state_generation
            FROM illinois_generation g
            CROSS JOIN datacenter_metrics d
            ORDER BY g.report_date
        """).df()

        df["report_date"] = pd.to_datetime(df["report_date"]).dt.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        return jsonify(df.to_dict("records"))
    finally:
        conn.close()


@il_datacenter_bp.route("/il-datacenter-summary")
def get_il_datacenter_summary():
    """
    Endpoint for summary statistics of Illinois data center impact
    Returns aggregate metrics and latest calculations
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            WITH latest_available AS (
                SELECT MAX(t.report_date) AS max_date
                FROM main_marts.fact_generation g
                JOIN main_marts.dim_plant p ON g.plant_id_eia = p.plant_id_eia
                JOIN main_marts.dim_time t ON g.report_date = t.report_date
                WHERE p.state = 'IL'
            ),
            latest_data AS (
                SELECT 
                    MAX(t.report_date) AS latest_month,
                    SUM(g.net_generation_mwh) AS recent_monthly_generation_mwh,
                    SUM(CASE WHEN f.fuel_category = 'Coal' THEN g.net_generation_mwh ELSE 0 END) AS recent_coal_generation_mwh
                FROM main_marts.fact_generation g
                JOIN main_marts.dim_plant p ON g.plant_id_eia = p.plant_id_eia
                JOIN main_marts.dim_fuel f ON g.fuel_dim_key = f.fuel_dim_key  
                JOIN main_marts.dim_time t ON g.report_date = t.report_date
                CROSS JOIN latest_available la
                WHERE p.state = 'IL'
                AND t.report_date = la.max_date  -- Get data from the latest available month
            ),
            datacenter_summary AS (
                SELECT 
                    COUNT(*) AS total_data_centers,
                    COUNT(DISTINCT substring(address, -5)) AS counties_with_datacenters,
                    COUNT(*) * 7.5 AS estimated_demand_mw
                FROM main_marts.dim_data_center
            )
            SELECT
                CAST(epoch(l.latest_month) AS BIGINT) * 1000 AS latest_month,
                l.recent_monthly_generation_mwh,
                l.recent_coal_generation_mwh,
                d.total_data_centers,
                d.counties_with_datacenters,
                d.estimated_demand_mw,
                (d.estimated_demand_mw * 24 * 30 * 0.8) AS estimated_monthly_consumption_mwh,
                ROUND(((d.estimated_demand_mw * 24 * 30 * 0.8) / l.recent_monthly_generation_mwh * 100), 2) AS pct_of_state_generation
            FROM latest_data l
            CROSS JOIN datacenter_summary d
        """).df()
        return jsonify(df.to_dict("records"))
    finally:
        conn.close()
