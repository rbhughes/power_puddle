# from util import puddle_db
# from pathlib import Path
# from excel import read_excel_tab

# from pudl import initialize_pudl_tables
# from data_centers import (
#     scrape_dc_pages,
#     initialize_data_center_table,
#     update_dc_table_with_geocode,
# )


# initialize_pudl_tables(puddle_db)

# dc_data = scrape_dc_pages()

# initialize_data_center_table(puddle_db, dc_data)

# update_dc_table_with_geocode(puddle_db)


# excel_path = Path(__file__).resolve().parents[2] / "data/pjm/2016-load-report-data.xlsx"
# df = read_excel_tab(excel_path, "COMED")


# df = read_excel_tab("../data/pjm/2016-load-report-data.xlsx", "COMED")
# print(df)


# You can then compare this sum to the sum of ENERGY_GWH in the
# PJM Excel projections
# (remembering to convert GWh to MWh: 1 GWh = 1,000 MWh).

####################################################################
# just
# uv run src/etl/main.py
####################################################################

print("just kept this file around for testing dir layout and imports")
