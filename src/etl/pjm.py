from pathlib import Path
from .util import read_excel_tab, puddle_db
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

resource_files = [
    "2018-2019-stage-1-resources-by-zone.xls",
    "2019-2020-stage-1-resources-by-zone.xls",
    "2020-2021-stage-1-resources-by-zone.xls",
    "2021-2022-stage-1-resources-by-zone.xlsx",
    "2022-2023-stage-1-resources-by-zone.xlsx",
    "2023-2024-stage-1-resources-by-zone.xlsx",
    "2024-2025-stage-1-resources-by-zone.xlsx",
    "2025-2026-stage-1-resources-by-zone.xlsx",
]


# def get_simplified_pjm_comed_plant_names():
#     pjm_resources = "2025-2026-rpm-existing-resource-list-post.xlsx"

#     xl = Path(__file__).resolve().parents[2] / f"data/pjm/{pjm_resources}"
#     df = read_excel_tab(xl, 0, skiprows=18)
#     comed_plants = df.loc[df["ZONENAME"] == "COMED", "RESOURCENAME"].tolist()

#     skipped_tokens = ["NUCLEAR", "SOLAR", "COAL"]

#     # String processing: split by whitespace, remove tokens under 3 characters, containing numbers, or in skipped_tokens
#     # Limit to max 2 tokens per plant, ensure uniqueness
#     processed_plants = set(
#         " ".join(
#             [
#                 token
#                 for token in plant.split()
#                 if len(token) >= 3
#                 and not any(char.isdigit() for char in token)
#                 and token not in skipped_tokens
#             ][:3]
#         )
#         for plant in comed_plants
#     )
#     return processed_plants


# def get_pjm_comed_plants():
#     pjm_resources = "2020-2021-stage-1-resources-by-zone.xls"
#     xl = Path(__file__).resolve().parents[2] / f"data/pjm/{pjm_resources}"
#     df = read_excel_tab(xl, sheet_name="COMED", skiprows=1)
#     print(df)


# def add_simplified_pjm_comed_plant_names_table(db_path: str):
#     con = duckdb.connect(db_path)
#     con.execute("""
#         DROP TABLE IF EXISTS pjm_comed_plants;
#         CREATE TABLE IF NOT EXISTS pjm_comed_plants (
#             plant_name TEXT PRIMARY KEY
#         )
#     """)
#     plant_names = get_simplified_pjm_comed_plant_names()
#     for name in plant_names:
#         con.execute(
#             """
#             INSERT INTO pjm_comed_plants (plant_name)
#             VALUES (?)
#             ON CONFLICT (plant_name) DO NOTHING
#         """,
#             [name],
#         )
#     con.close()


def add_pjm_forecasts_table(db_path: str):
    con = duckdb.connect(db_path)
    # Create the forecasts table if it doesn't exist
    con.execute("""
        DROP TABLE IF EXISTS pjm_forecasts;
        CREATE TABLE IF NOT EXISTS pjm_forecasts (
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
                    INSERT INTO pjm_forecasts (pk, publication_year, zone_name, year, month, peak_mw, energy_gwh)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (pk) DO NOTHING
                """,
                    [pk, publication_year, zone_name, year, month, peak_mw, energy_gwh],
                )
        except Exception as e:
            print(f"Error processing file {f}: {e}")
            continue


def simplify_resource_name(name, skipped_tokens=None):
    """
    Simplifies a resource name by:
    - Splitting by whitespace
    - Removing tokens under 3 characters
    - Removing tokens containing numbers
    - Removing tokens in skipped_tokens
    - Returning the first remaining token, or None if none remain
    - uppercased
    """
    if skipped_tokens is None:
        skipped_tokens = {"NUCLEAR", "SOLAR", "COAL"}
    tokens = [
        token
        for token in name.split()
        if len(token) >= 3
        and not any(char.isdigit() for char in token)
        and token not in skipped_tokens
    ]
    return tokens[0].upper() if tokens else None


def add_pjm_resources_table(db_path: str):
    con = duckdb.connect(db_path)
    # Collect all years from resource_files
    years = [f[:4] for f in resource_files]
    # Build CREATE TABLE statement with a column for each year (no primary key)
    columns_sql = ", ".join([f'"{year}" TEXT' for year in years])
    con.execute(f"""
        DROP TABLE IF EXISTS pjm_resources;
        CREATE TABLE pjm_resources (
            {columns_sql}
        );
    """)
    # Read each file and collect the "Historical Unit Name" values
    all_unit_names = []
    for f in resource_files:
        try:
            year = f[:4]
            excel_path = Path(__file__).resolve().parents[2] / f"data/pjm/{f}"
            df = read_excel_tab(excel_path, sheet_name="COMED", skiprows=1)
            # Get the 4th column, "Historical Unit Name"
            if df.shape[1] < 4:
                print(f"File {f} does not have enough columns.")
                continue
            # Simplify each resource name before storing
            unit_names = [
                simplify_resource_name(str(name))
                for name in df.iloc[:, 3].dropna().astype(str).tolist()
            ]
            # Remove None and duplicates while preserving order
            seen = set()
            simplified_names = []
            for n in unit_names:
                if n and n not in seen:
                    simplified_names.append(n)
                    seen.add(n)
            all_unit_names.append((year, simplified_names))
        except Exception as e:
            print(f"Error processing file {f}: {e}")
            continue
    # Find the max number of unit names across all years
    max_len = max(len(names) for _, names in all_unit_names) if all_unit_names else 0
    # Pad lists so all have the same length
    year_to_names = {
        year: names + [None] * (max_len - len(names)) for year, names in all_unit_names
    }
    # Insert rows into the table
    for i in range(max_len):
        row = [year_to_names.get(year, [None] * max_len)[i] for year in years]
        con.execute(
            f"INSERT INTO pjm_resources ({', '.join([f'"{year}"' for year in years])}) VALUES ({', '.join(['?' for _ in years])})",
            row,
        )
    con.close()
