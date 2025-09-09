from pathlib import Path


project_root = Path(__file__).resolve().parents[2]

puddle_db = project_root / "data" / "puddle.duckdb"
