import pandas as pd
from pathlib import Path


def read_excel_tab(file_path: str, sheet_name: str):
    """
    Reads data from a specific tab (sheet) of an Excel (.xlsx) file.

    Args:
        file_path (str): Path to the Excel file.
        sheet_name (str): Name of the sheet/tab to read.

    Returns:
        pd.DataFrame: DataFrame containing the sheet's data.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_excel(file_path, sheet_name=sheet_name)
    return df


print("hello")
