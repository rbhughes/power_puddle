"""One-year-later refresh: rebuild the site's data artifacts with
fresh PUDL actuals and the 2024/2025/2026 PJM forecast vintages,
using the SAME transforms as the original September 2025 analysis
(dbt staging fuel_category mapping, the ComEd county filter, and the
Flask API's MASE method) so the update is comparable, not a rewrite.

The original data/puddle.duckdb is left untouched as the frozen
September 2025 baseline. Fresh inputs are cached under
data/refresh_2026/ (gitignored); outputs land in site/public/data/.

Run:
  uv run --no-project --with duckdb --with requests --with python-calamine \
      python src/scripts/build_site_data.py
"""
import json
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[2]
CACHE = ROOT / "data" / "refresh_2026"
OUT = ROOT / "site" / "public" / "data"
OLD_DB = ROOT / "data" / "puddle.duckdb"

PUDL = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/"
PUDL_FILES = [
    "core_eia__entity_plants.parquet",
    "core_eia923__monthly_generation_fuel.parquet",
    "core_eia923__monthly_generation_fuel_nuclear.parquet",
]
PJM = "https://www.pjm.com/-/media/DotCom/library/reports-notices/load-forecast/"
PJM_FILES = {  # publication year -> file (2016-2023 live in data/pjm/)
    2024: "2024-load-report-data.xlsx",
    2025: "2025-load-report-data.xlsx",
    2026: "2026-load-report-data.xlsx",
}

# dbt stg_monthly_gen fuel_category mapping, verbatim.
FUEL_CATEGORY = """
  case
    when fuel_type_code_pudl = 'coal' then 'Coal'
    when fuel_type_code_pudl = 'gas' then 'Natural Gas'
    when fuel_type_code_pudl = 'nuclear' then 'Nuclear'
    when fuel_type_code_pudl = 'oil' then 'Oil'
    when fuel_type_code_pudl = 'hydro' then 'Hydro'
    when fuel_type_code_pudl = 'wind' then 'Wind'
    when fuel_type_code_pudl = 'solar' then 'Solar'
    when fuel_type_code_pudl = 'geothermal' then 'Geothermal'
    when fuel_type_code_pudl = 'waste' then 'Biomass'
    when fuel_type_code_pudl = 'other' then 'Other'
    else 'Unknown'
  end
"""

# mart_actual_vs_forecast ComEd county filter, verbatim.
COMED_COUNTIES = [
    "Boone", "Bureau", "Carroll", "Cook", "DeKalb", "DuPage", "Ford",
    "Grundy", "Henry", "Iroquois", "Jo Daviess", "Kane", "Kankakee",
    "Kendall", "Lake", "LaSalle", "Lee", "Livingston", "Marshall",
    "McHenry", "Ogle", "Putnam", "Rock Island", "Stephenson", "Will",
    "Winnebago",
]


def fetch(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  fetching {url.rsplit('/', 1)[-1]}")
    with requests.get(url, stream=True, timeout=600,
                      headers={"User-Agent": "Mozilla/5.0"}) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    return dest


def load_pjm_vintages(con):
    """2016-2023 vintages from the original repo files; 2024-2026 fresh
    from pjm.com. Same COMED sheet, same columns. calamine, not
    openpyxl: PJM's 2024/2025 files carry document properties whose
    XML openpyxl rejects — the original project's "corrupt 2024 file"
    was this, not corruption."""
    from python_calamine import CalamineWorkbook

    rows = []
    old = {int(f.name[:4]): f for f in (ROOT / "data" / "pjm").glob("20*.xlsx")
           if "load" in f.name and "resources" not in f.name}
    fresh = {y: fetch(PJM + f, CACHE / f) for y, f in PJM_FILES.items()}
    for pub_year, path in sorted({**old, **fresh}.items()):
        try:
            wb = CalamineWorkbook.from_path(str(path))
        except Exception as e:
            print(f"  !! {path.name}: unreadable ({e}); skipped")
            continue
        if "COMED" not in wb.sheet_names:
            print(f"  !! {path.name}: no COMED sheet; skipped")
            continue
        data = wb.get_sheet_by_name("COMED").to_python()
        header = data[0]
        idx = {name: header.index(name) for name in
               ("ZONE_NAME", "YEAR", "MONTH", "ENERGY_GWH")}
        n = 0
        for row in data[1:]:
            if row[idx["ZONE_NAME"]] != "COMED":
                continue
            if row[idx["ENERGY_GWH"]] in (None, ""):
                continue
            rows.append((pub_year, int(float(row[idx["YEAR"]])),
                         int(float(row[idx["MONTH"]])),
                         float(row[idx["ENERGY_GWH"]])))
            n += 1
        print(f"  {path.name}: {n} COMED rows")
    con.execute("""create table pjm_forecasts (publication_year int,
        year int, month int, energy_gwh double)""")
    con.executemany("insert into pjm_forecasts values (?,?,?,?)", rows)


def mase(actual, forecast):
    """Flask endpoint's method: naive seasonality=1 over the
    overlapping non-null months."""
    n = len(actual)
    if n < 2:
        return None
    mae_naive = sum(abs(actual[i + 1] - actual[i])
                    for i in range(n - 1)) / (n - 1)
    mae_model = sum(abs(a - f) for a, f in zip(actual, forecast)) / n
    return None if mae_naive == 0 else mae_model / mae_naive


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    plants, gen, nuc = (fetch(PUDL + f, CACHE / f) for f in PUDL_FILES)
    con.execute(f"create view plants as select * from '{plants}'")
    con.execute(f"""
      create view combined as
      select g.plant_id_eia, g.report_date, {FUEL_CATEGORY} fuel_category,
             g.net_generation_mwh
      from '{gen}' g
      union all
      select n.plant_id_eia, n.report_date, 'Nuclear',
             n.net_generation_mwh
      from '{nuc}' n
    """)
    load_pjm_vintages(con)

    last, = con.execute("select max(report_date) from combined").fetchone()
    print(f"  PUDL actuals through {last}")

    # --- national + Illinois monthly generation mix (TWh) ---
    for name, where in [("us", "true"), ("il", "p.state = 'IL'")]:
        series = {}
        for cat, month, twh in con.execute(f"""
            select c.fuel_category, strftime(c.report_date, '%Y-%m'),
                   round(sum(c.net_generation_mwh) / 1e6, 3)
            from combined c join plants p using (plant_id_eia)
            where {where} and c.net_generation_mwh is not null
            group by 1, 2 order by 2""").fetchall():
            series.setdefault(cat, {})[month] = twh
        (OUT / f"{name}_generation.json").write_text(
            json.dumps(series, separators=(",", ":")))

    # --- ComEd actuals vs forecast vintages (GWh) ---
    actuals = dict(con.execute(f"""
        select strftime(c.report_date, '%Y-%m'),
               round(sum(c.net_generation_mwh) / 1e3, 1)
        from combined c join plants p using (plant_id_eia)
        where p.state = 'IL' and p.county in ({
            ",".join(f"'{c}'" for c in COMED_COUNTIES)})
        group by 1 order by 1""").fetchall())
    vintages = {}
    for pub, y, m, gwh in con.execute(
            "select * from pjm_forecasts order by 1, 2, 3").fetchall():
        vintages.setdefault(str(pub), {})[f"{y}-{m:02d}"] = round(gwh, 1)

    mase_scores = {}
    for pub, series in vintages.items():
        overlap = sorted(k for k in series if k in actuals
                         and k >= f"{pub}-01")
        a = [actuals[k] for k in overlap]
        f = [series[k] for k in overlap]
        s = mase(a, f)
        if s is not None:
            mase_scores[pub] = {"mase": round(s, 2), "months": len(a)}

    (OUT / "forecast.json").write_text(json.dumps({
        "actuals": actuals,
        "vintages": vintages,
        "mase": mase_scores,
        "actuals_through": str(last),
        "baseline_through": "2025-05",
    }, separators=(",", ":")))

    # --- map: IL plants (fresh PUDL) + frozen data centers (old db) ---
    il_plants = [
        {"id": r[0], "n": r[1], "lat": r[2], "lon": r[3],
         "fuel": r[4], "mwh": r[5]}
        for r in con.execute(f"""
          with pg as (
            select c.plant_id_eia, c.fuel_category,
                   sum(c.net_generation_mwh) mwh,
                   row_number() over (partition by c.plant_id_eia
                     order by sum(c.net_generation_mwh) desc) rn
            from combined c join plants p using (plant_id_eia)
            where p.state = 'IL'
            group by 1, 2)
          select p.plant_id_eia, p.plant_name_eia,
                 round(p.latitude, 5), round(p.longitude, 5),
                 pg.fuel_category, round(pg.mwh)
          from pg join plants p using (plant_id_eia)
          where pg.rn = 1 and pg.mwh > 0
            and p.latitude is not null
          order by pg.mwh desc""").fetchall()]
    old = duckdb.connect(str(OLD_DB), read_only=True)
    dcs = [{"n": r[0], "lat": r[1], "lon": r[2]} for r in old.execute(
        """select data_center_name, round(latitude,5), round(longitude,5)
           from main_marts.dim_data_center
           where latitude is not null""").fetchall()]
    (OUT / "map.json").write_text(json.dumps(
        {"plants": il_plants, "data_centers": dcs},
        separators=(",", ":")))

    print(f"  wrote {len(il_plants)} IL plants, {len(dcs)} data centers")
    print(f"  vintages: {sorted(vintages)}")
    print(f"  MASE: {json.dumps(mase_scores)}")


if __name__ == "__main__":
    main()
