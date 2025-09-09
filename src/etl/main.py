from util import puddle_db

from pudl import initialize_pudl_tables
from data_centers import (
    scrape_dc_pages,
    initialize_data_center_table,
    update_dc_table_with_geocode,
)


initialize_pudl_tables(puddle_db)

dc_data = scrape_dc_pages()

initialize_data_center_table(puddle_db, dc_data)

update_dc_table_with_geocode(puddle_db)
