import pandas as pd
import duckdb
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]

# https://catalystcoop-pudl.readthedocs.io/en/nightly/data_dictionaries/pudl_db.html


def init_nuke_db():
    plants_df = pd.read_parquet(
        "s3://pudl.catalyst.coop/nightly/core_eia__entity_plants.parquet",
        dtype_backend="pyarrow",
    )

    monthly_nuclear_df = pd.read_parquet(
        "s3://pudl.catalyst.coop/nightly/core_eia923__monthly_generation_fuel_nuclear.parquet",
        dtype_backend="pyarrow",
    )

    monthly_gen_df = pd.read_parquet(
        "s3://pudl.catalyst.coop/nightly/core_eia923__monthly_generation_fuel.parquet",
        dtype_backend="pyarrow",
    )

    pudl_db = project_root / "data" / "pudl.duckdb"

    con = duckdb.connect(pudl_db)

    con.register("plants_df", plants_df)
    con.execute("CREATE OR REPLACE TABLE plants AS SELECT * FROM plants_df")

    con.register("monthly_nuclear_df", monthly_nuclear_df)
    con.execute(
        "CREATE OR REPLACE TABLE monthly_nuclear AS SELECT * FROM monthly_nuclear_df"
    )

    con.register("monthly_gen_df", monthly_gen_df)
    con.execute(
        "CREATE OR REPLACE TABLE monthly_generation AS SELECT * FROM monthly_gen_df"
    )

    con.close()


init_nuke_db()


# from prefect import flow, task
# from myjob import collect_and_save

# @task
# def run_collect_save():
#     collect_and_save()

# @flow
# def pipeline():
#     run_collect_save()

# if __name__ == "__main__":
#     pipeline()
