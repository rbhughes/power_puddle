from flask import Blueprint, jsonify

il_power_plants_bp = Blueprint("il_power_plants", __name__, url_prefix="/api")


@il_power_plants_bp.route("/il-power-plants")
def get_illinois_power_plants():
    """
    Endpoint for Illinois power plants with generation data
    Returns plant name, latitude, longitude, total generation, primary fuel
    """
    from ..energy_api import get_db_connection

    conn = get_db_connection()

    try:
        df = conn.execute("""
            with illinois_plants_generation as (
                select 
                    p.plant_name_eia,
                    p.latitude as pp_latitude,
                    p.longitude as pp_longitude,
                    sum(f.net_generation_mwh) as total_net_generation_mwh
                from main_marts.dim_plant p
                inner join main_marts.fact_generation f 
                    on p.plant_id_eia = f.plant_id_eia
                where p.latitude between 36.970298 and 42.508481
                  and p.longitude between -91.513079 and -87.019935
                  and p.latitude is not null
                  and p.longitude is not null
                  and f.net_generation_mwh > 0
                group by p.plant_name_eia, p.latitude, p.longitude
            ),
            plant_fuel_totals as (
                select
                    p.plant_name_eia,
                    f.fuel_dim_key,
                    sum(f.net_generation_mwh) as fuel_generation_mwh
                from main_marts.dim_plant p
                inner join main_marts.fact_generation f on p.plant_id_eia = f.plant_id_eia
                where p.latitude between 36.970298 and 42.508481
                  and p.longitude between -91.513079 and -87.019935
                  and p.latitude is not null
                  and p.longitude is not null
                  and f.net_generation_mwh > 0
                group by p.plant_name_eia, f.fuel_dim_key
            ),
            primary_fuel as (
                select plant_name_eia, fuel_dim_key
                from (
                    select
                        plant_name_eia,
                        fuel_dim_key,
                        fuel_generation_mwh,
                        row_number() over (partition by plant_name_eia order by fuel_generation_mwh desc) as rn
                    from plant_fuel_totals
                )
                where rn = 1
            )
            select
                g.*, 
                fuel.fuel_source_type as primary_fuel_source_type
            from illinois_plants_generation g
            left join primary_fuel pf on g.plant_name_eia = pf.plant_name_eia
            left join main_marts.dim_fuel fuel on pf.fuel_dim_key = fuel.fuel_dim_key
            order by g.total_net_generation_mwh desc
        """).df()

        return jsonify(df.to_dict("records"))

    finally:
        conn.close()
