from pathlib import Path
from util import read_excel_tab, puddle_db
import duckdb

forecast_files = [
    "2016-load-report-data.xlsx",
    "2017-load-forecast-report-data.xlsx",
    "2018-load-forecast-report-data.xlsx",
    "2019-load-report-data.xlsx",
    "2020-load-report-data.xlsx",
    "2021-load-report-data.xlsx",
    "2022-load-report-data.xlsx",
    "2023-load-report-data.xlsx",
    # "2024-load-report-data.xlsx", # corrupt?
]


def add_pjm_forecasts_table(db_path: str):
    con = duckdb.connect(db_path)
    # Create the forecasts table if it doesn't exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            pk TEXT PRIMARY KEY,
            publication_year INTEGER,
            zone_name TEXT,
            year INTEGER,
            month INTEGER,
            peak_mw DOUBLE,
            energy_gwh DOUBLE
        )
    """)
    for f in forecast_files:
        try:
            excel_path = Path(__file__).resolve().parents[2] / f"data/pjm/{f}"
            print(f"Processing: {excel_path}")
            df = read_excel_tab(excel_path, "COMED")
            publication_year = int(f[:4])
            required_cols = {"ZONE_NAME", "YEAR", "MONTH", "PEAK_MW", "ENERGY_GWH"}
            if not required_cols.issubset(df.columns):
                print(f"Missing required columns in {f}")
                continue
            for _, row in df.iterrows():
                zone_name = str(row["ZONE_NAME"])
                year = int(row["YEAR"])
                month = int(row["MONTH"])
                peak_mw = float(row["PEAK_MW"])
                energy_gwh = float(row["ENERGY_GWH"])
                pk = f"{publication_year}-{zone_name}-{year}-{month}"
                con.execute(
                    """
                    INSERT INTO forecasts (pk, publication_year, zone_name, year, month, peak_mw, energy_gwh)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (pk) DO NOTHING
                """,
                    [pk, publication_year, zone_name, year, month, peak_mw, energy_gwh],
                )
        except Exception as e:
            print(f"Error processing file {f}: {e}")
            continue


add_pjm_forecasts_table(puddle_db)
