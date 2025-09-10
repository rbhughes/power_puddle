import pandas as pd
import duckdb

# https://catalystcoop-pudl.readthedocs.io/en/nightly/data_dictionaries/pudl_db.html


def initialize_pudl_tables(db_path: str):
    plants_df = pd.read_parquet(
        "s3://pudl.catalyst.coop/nightly/core_eia__entity_plants.parquet",
        dtype_backend="pyarrow",
    )

    monthly_gen_nuc_df = pd.read_parquet(
        "s3://pudl.catalyst.coop/nightly/core_eia923__monthly_generation_fuel_nuclear.parquet",
        dtype_backend="pyarrow",
    )

    monthly_gen_df = pd.read_parquet(
        "s3://pudl.catalyst.coop/nightly/core_eia923__monthly_generation_fuel.parquet",
        dtype_backend="pyarrow",
    )

    con = duckdb.connect(db_path)

    con.register("plants_df", plants_df)
    con.execute("CREATE OR REPLACE TABLE plants AS SELECT * FROM plants_df")

    con.register("monthly_gen_nuc_df", monthly_gen_nuc_df)
    con.execute(
        "CREATE OR REPLACE TABLE monthly_gen_nuc AS SELECT * FROM monthly_gen_nuc_df"
    )

    con.register("monthly_gen_df", monthly_gen_df)
    con.execute("CREATE OR REPLACE TABLE monthly_gen AS SELECT * FROM monthly_gen_df")

    con.close()
