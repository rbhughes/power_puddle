from flask import Blueprint, jsonify
import pandas as pd

us_monthly_generation_bp = Blueprint("us_monthly_generation", __name__)


@us_monthly_generation_bp.route("/us-monthly-generation")
def get_us_monthly_generation_trends():
    """
    Endpoint for monthly generation trends for all US
    Returns time series data grouped by fuel category
    NOPE: --strftime('%Y-%m-%dT%H:%M:%S.000Z', report_date) as report_date,
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            select 
                report_date,
                fuel_category,
                sum(total_net_generation_mwh) as generation_mwh
            from main_marts.mart_monthly_generation_summary
            --where report_date >= '2020-01-01'  -- just return all
            group by report_date, fuel_category
            order by report_date, fuel_category
        """).df()

        # ISO date alone does not satisfy grafana/infinity.
        # Still need transform of report_date field in Grafana
        df["report_date"] = pd.to_datetime(df["report_date"]).dt.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        return jsonify(df.to_dict("records"))
    finally:
        conn.close()


@us_monthly_generation_bp.route("/il-monthly-generation")
def get_il_monthly_generation_trends():
    """
    Endpoint for monthly generation trends for IL
    Returns time series data grouped by fuel category
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()
    try:
        df = conn.execute("""
            select 
                report_date,
                fuel_category,
                sum(total_net_generation_mwh) as generation_mwh
            from main_marts.mart_monthly_generation_summary
            where state = 'IL'
            group by report_date, fuel_category
            order by report_date, fuel_category
        """).df()

        # ISO date alone does not satisfy grafana/infinity.
        # Still need transform of report_date field
        df["report_date"] = pd.to_datetime(df["report_date"]).dt.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        return jsonify(df.to_dict("records"))
    finally:
        conn.close()


# these below here are not implemented...


# @mart_monthly_bp.route("/generation-by-state")
# def get_generation_by_state():
#     """
#     Endpoint for state-level generation geomap/heatmap
#     Returns latest 12 months aggregated by state
#     """
#     from ..energy_api import get_db_connection

#     conn = get_db_connection()
#     try:
#         df = conn.execute("""
#             select
#                 state,
#                 sum(total_net_generation_mwh) as total_generation_mwh,
#                 avg(heat_rate_btu_per_kwh) as avg_heat_rate,
#                 count(distinct plant_id_eia) as plant_count
#             from main_marts.mart_monthly_generation_summary
#             where report_date >= current_date - interval '12 months'
#             and state is not null
#             group by state
#             order by total_generation_mwh desc
#         """).df()
#         return jsonify(df.to_dict("records"))
#     finally:
#         conn.close()


# @mart_monthly_bp.route("/monthly-generation-by-fuel")
# def get_monthly_generation_by_fuel():
#     """
#     Endpoint for monthly generation by fuel type stacked bar chart
#     Returns data suitable for stacked visualization
#     """
#     from ..energy_api import get_db_connection

#     conn = get_db_connection()
#     try:
#         df = conn.execute("""
#             select
#                 report_date,
#                 report_year,
#                 report_month,
#                 fuel_category,
#                 sum(total_net_generation_mwh) as generation_mwh,
#                 sum(total_fuel_consumed_mmbtu) as fuel_consumed_mmbtu
#             from main_marts.mart_monthly_generation_summary
#             where report_date >= '2020-01-01'  -- last 5 years
#             and fuel_category is not null
#             group by report_date, report_year, report_month, fuel_category
#             order by report_date, fuel_category
#         """).df()
#         return jsonify(df.to_dict("records"))
#     finally:
#         conn.close()


# @mart_monthly_bp.route("/plant-efficiency-rankings")
# def get_plant_efficiency_rankings():
#     """
#     Bonus endpoint for plant efficiency comparison
#     Returns top and bottom performers by heat rate
#     """
#     from ..energy_api import get_db_connection

#     conn = get_db_connection()
#     try:
#         df = conn.execute("""
#             select
#                 plant_name_eia,
#                 state,
#                 fuel_category,
#                 avg(heat_rate_btu_per_kwh) as avg_heat_rate,
#                 sum(total_net_generation_mwh) as total_generation_mwh,
#                 avg(avg_fuel_mmbtu_per_unit) as avg_fuel_quality
#             from main_marts.mart_monthly_generation_summary
#             where report_date >= current_date - interval '12 months'
#             and heat_rate_btu_per_kwh is not null
#             and total_net_generation_mwh > 1000  -- filter out very small plants
#             group by plant_name_eia, state, fuel_category
#             having count(*) >= 6  -- at least 6 months of data
#             order by avg_heat_rate asc
#             limit 50
#         """).df()
#         return jsonify(df.to_dict("records"))
#     finally:
#         conn.close()
