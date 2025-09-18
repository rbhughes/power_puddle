from prefect import task, flow
from prefect.logging import get_run_logger
from src.etl.util import puddle_db, backup_table, load_table_from_backup
from src.etl.pudl import initialize_pudl_tables
from src.etl.data_centers import (
    scrape_dc_pages,
    initialize_data_center_table,
    update_dc_table_with_geocode,
)


@task(retries=3, retry_delay_seconds=60)
def init_pudl_tables_task():
    initialize_pudl_tables(puddle_db)


@task(retries=2, retry_delay_seconds=120)
def scrape_data_centers_task():
    try:
        dc_data = scrape_dc_pages()
        if not dc_data or len(dc_data) == 0:
            raise ValueError("No data centers scraped")
        initialize_data_center_table(puddle_db, dc_data)
        backup_file = backup_table("data_centers")
        return {
            "result": "fresh",
            "count": len(dc_data),
            "backup_file": str(backup_file),
        }
    except Exception as e:
        # Try loading from most recent backup on failure
        restored = load_table_from_backup("data_centers")
        if restored:
            return {"result": "backup", "error": str(e)}
        else:
            raise RuntimeError(f"Scraping and backup restore failed: {e}")


@task(retries=3, retry_delay_seconds=120)
def geocode_data_centers_task():
    update_dc_table_with_geocode(puddle_db)
    backup_table("data_centers")


@flow(name="Data Center ETL with Backup and Restore")
def etl_flow():
    logger = get_run_logger()
    logger.info("Initializing PUDL tables…")
    init_pudl_tables_task()
    logger.info("Scraping and loading data centers…")
    scrape_result = scrape_data_centers_task()
    logger.info(f"Scraping result: {scrape_result}")
    logger.info("Geocoding data centers…")
    geocode_data_centers_task()
    logger.info("ETL flow completed.")


if __name__ == "__main__":
    etl_flow()
