from pathlib import Path
from datetime import datetime
import pandas as pd
import duckdb


project_root = Path(__file__).resolve().parents[2]

data_root = project_root / "data"

puddle_db = data_root / "puddle.duckdb"


def backup_table(table_name: str):
    """Create timestamped parquet backup of table"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    backup_file = data_root / "bak" / f"{table_name}_backup_{timestamp}.parquet"

    con = duckdb.connect(puddle_db)
    try:
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()

        if result[0] > 0:  # result is tuple
            con.execute(f"COPY {table_name} TO '{backup_file}' (FORMAT PARQUET)")
            return backup_file
    finally:
        con.close()
    return None


def load_table_from_backup(table_name: str):
    """Load most recent backup if available"""
    backup_pattern = f"{table_name}_backup_"
    backup_files = [
        f
        for f in data_root.iterdir()
        if f.is_file() and f.name.startswith(backup_pattern)
    ]

    if not backup_files:
        return False

    latest_backup = sorted(backup_files)[-1]

    backup_path = latest_backup

    con = duckdb.connect(puddle_db)
    try:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{backup_path}')"
        )
        return True
    finally:
        con.close()


def read_excel_tab(file_path: str, sheet_name: str | int, **kwargs):
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if file_path.suffix.lower() == ".xlsx":
        engine = "openpyxl"
    elif file_path.suffix.lower() == ".xls":
        engine = "xlrd"

    df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine, **kwargs)
    return df
