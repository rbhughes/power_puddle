import pandas as pd
from pathlib import Path


def read_excel_tab(file_path: str, sheet_name: str):
    """Read a tab (sheet) from Excel, return dataframe."""

    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_excel(file_path, sheet_name=sheet_name)
    return df
