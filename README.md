# Power Plants & Data Centers in Illinois – Data Engineering Workflow

## Executive Summary

[_Briefly describe project goals, tools, impact[11][7]._]

**Is the Illinois power grid prepared for projected data center growth**?

To answer this question, we examine

- historical electrical generation
- forecasts from the managing operator PJM
- the existing mix of power generation options

While there is much uncertainty (not limited to energy), we can make four broad observations:

1. Past electrical generation in Northern IL COMED zone shows a steady, modest rise.

2. PJM forecasts for 2025 and beyond do NOT show an expected "AI bump" and are relatively flat, suggesting a "wait and see" approach from operators.

3. Monitoring of data center energy use in IL will improve greatly with the passing of SB2181, which will mandate site-level energy and water use starting in 2026.

4. ???
<!-- 3. This "wait and see" approach may alarm some, but appears to be reasonable--IL's nuclear foundation and robust PJM interconnections could accommodate data center growth in IL until new (presumably sustainable) plants are brought online. -->

## Business Problem

[_Explain context, business relevance, and motivation[11][7]._]

The rapid growth of energy-hungry AI/ML dominates the news cycle. Google Trends shows a 5X increase in the term "[data center](https://trends.google.com/trends/explore?date=today%205-y&geo=US&q=data%20center&hl=en)" in the last 5 years. Common refrains include soaring energy bills and imminent grid collapse. [11,12] A robust power grid is essential for Illinois to attact investment and continue its role as a key infrastructure hub.

Illinois is an interesting case study for data center growth:

- IL has the 6th highest number of data centers in the US and is among the top 5 globally, with 222 as of Q3 2025. Most are concentrated near Chicago. [8]

- Data centers in Illinois consumed about 5.4% of the state’s total electricity use as of early 2025, a much higher percentage than national averages. [3]

- IL generates more electricity from nuclear energy than any other state, accounting for one-eighth of the nation's total nuclear power generation. [7]

The past, present and future contexts can help us learn about IL readiness:

**PAST**:

We have historical power generation data from [PUDL](https://catalystcoop-pudl.readthedocs.io/en/nightly/index.html#) (pronounced puddle), a data processing pipeline created by [Catalyst Cooperative](https://catalyst.coop/) that cleans, integrates, and standardizes some of the most widely used public energy datasets in the US. **_We have decades of monthly power generation data from PUDL for the US_**.

**PRESENT**:

Analysts are sounding the alarm today on grid readiness, cost increases for consumers and water usage [2,3,4]. Synapse predicts an 8% price hike soon. Governors in PJM stats are pushing for relief from data center-related price spikes [11]. **_The prevalence of news coverage alone suggests that there are real risks that need evaluation_**.

**FUTURE**:

[PJM](https://www.pjm.com/about-pjm) is the regional transmission operator that keeps the power grid running smoothly across several eastern US states, including the COMED (Commonwealth Edison) zone in Northern Illinois. PJM doesn’t make electricity; it manages who supplies power, keeps prices stable and prevents outages. Think of PJM as an "air traffic controller" of energy distribution. **_PJM publishes forecast data, and we have several years of forecasts we can evaluate for accuracy_**.

## Tech Stack Overview

Data Sources:

- [The Public Utility Data Liberation (PUDL) Project](https://catalyst.coop/pudl/) - open-source US electricity sector data
- [PJM](https://www.pjm.com/) - a regional coordinator of electricity in all or parts of 13 states and DC.

- [DATACENTERS.com](https://www.datacenters.com/) - searchable directory of US data centers
- [Nominatim](https://nominatim.org/) - open-source geocoding API service

Stack:

| tech                                  | usage                                |
| ------------------------------------- | ------------------------------------ |
| [DuckDB](https://duckdb.org/)         | fast, portable local database        |
| [Selenium](https://www.selenium.dev/) | browser automation                   |
| [dbt](https://www.getdbt.com/)        | data modeling, sharing via Flask API |
| [Prefect](https://www.prefect.io/)    | orchestration of database tables     |
| [Grafana](https://grafana.com/)       | presentation and analysis            |

## Installation & Quickstart

[_Step-by-step instructions to run the project[5]._]

#### PREREQUISITES:

**Python**: I used v3.13.7; anything from 3.10+ will probably work.

**Git**: 2.51 was used for this project. 2+ should work.

**Grafana**: homebrew: `brew install grafana` `brew services start grafana`

**uv**: (sort of optional, but highly recommended) `brew install uv` or `pipx install uv`

#### SETUP:

**1. Clone the repo**:

`git clone git@github.com:rbhughes/power_puddle.git`

**2. Install project dependencies**

`uv sync` _(or install libs in pyproject.toml if you prefer pip)_

**3. Setup dbt dependencies**

(within the project's dbt/ folder, `cd dbt`)
`uv run dbt deps`

**4. Run Prefect flows**

_If you don't care about the 150+ data center locations on the map (they're not part of the analysis) or you want to avoid Selenium, comment it out in `flows.etl_flows` before running._

`uv run --project . python -m flows.etl_flows`

**5. Build dbt models**

(again, from within dbt/)
`uv run dbt run`

To selectively (re)run a model after modifying

`uv run dbt run --select int_actual_vs_forecast`

**6. Start the Flask API for Grafana**

The port is set to 5500 by default. Change it in `api/puddle_api.py`.

`uv run --project . python -m api.puddle_api`

Test a route in your browser:
`http://localhost:5500/api/us-monthly-generation`

**7. Import visualizations into Grafana**

I'll leave this as an exercise for the reader. It is likely that some incompatibility in the JSON formats will have crept in for open source Grafana, but exports of the visualizations and dashboards in this repo are in `grafana/dashboards`.

## Architecture Diagram

[_Insert workflow diagram here[7]._]

## Workflow Steps

Most of these steps are orchestrated with Prefect. Refer to the [Prefect flows](flows/etl_flows.py) for details. We are using DuckDB for it's native parquet support and simplicity. \*_PostgreSQL was a close runner-up, but my dev environment was configured for an older version that I didn't want to break._

1. **Collect PJM data**

   The PJM site has rudimentary search and download capabilities and no API. The forecast files we need are available as either Excel .xls or .xlsx formats and have a somewhat inconsistent naming convention. This is historical data; just download them to the data [directory](data/pjm/).

   Note: The `<year>-load-report-data.xlsx` files contain forecast data for the publication year and several years into the future. We use this for COMED forecasts. The `*stage-1-resources-by-zone*` files were an attempt to link PJM's COMED power plant names to PUDL `plant_name_eia` but was deemed too inaccurate to use.

2. **Initialize PUDL tables**

   Load the parquet files from the PUDL distribution's S3 site into a local DuckDB database. These contain power plant id/location and monthly historical generation data for all states. Data on nuclear generation is managed separately; we merge them later.

   | PUDL                                        | DuckDB                      |
   | ------------------------------------------- | --------------------------- |
   | core_eia\_\_entity_plants                   | puddle.main.plants          |
   | core_eia\_\_monthly_generation_fuel_nuclear | puddle.main.monthly_gen_nuc |
   | core_eia\_\_monthly_generation_fuel         | puddle.main.monthly_gen     |

3. **Collect Data Center info**

   Use Selenium + BeautifulSoup to scrape data center names and street addresses from datacenters.com. This is fairly straightforward, but the site will rate-limit requests and has some tricky pagination. Once we have to addresses, use Nominative for geocoding to get latitude, longitude.

   Note that we don't use these data center locations for meaningful analysis. However, it is illuminating that nearly all 154 locations are in Northern IL and fall within the COMED zone. Southern IL looks like a different state.

4. **Run dbt workflows**

   **Staging**

   We need to merge the monthly generation from nuclear with monthly generation from other sources (solar, natural gas, oil, etc.). The main logic involves standardizing the `fuel_type_code_pudl` values as `fuel_category`.

   **Intermediate**

   Basically, just a union of `monthly_gen` and `monthly_gen_nuc` since nuclear MWh is reported separately by the US EIA.

   **Marts**

   | model                                 | purpose                                         |
   | ------------------------------------- | ----------------------------------------------- |
   | `dim_data_center.sql`                 | list from datacenters.com + lat/lon             |
   | `dim_fuel.sql`                        | standardize `fuel_type_code_pudl` values        |
   | `dim_plant.sql`                       | power plant metadata from PUDL                  |
   | `fact_generation.sql`                 | overkill, but used to group generation by plant |
   | `mart_actual_vs_forecast.sql`         | MWh from PUDL vs forecasts from PJM             |
   | `mart_monthly_generation_summary.sql` | all US (and IL) historical PUDL MWh             |

5. **Flask/Blueprint API**

   We're using Grafana for visualization and analysis. Grafana doesn't natively connect to DuckDB, but it can easily consume JSON from APIs. We define some endpoints that reference our dbt models and serve them via a local Flask app. This is ideal iterative local development and avoids the misery of intermediate .csv files or complexity of a local PostgreSQL instance.

   The followig routes are defined:

   | route                      | description                          |
   | -------------------------- | ------------------------------------ |
   | `/il-data-centers`         | data center names, lat/lon           |
   | `/il-power-plants`         | power plants with net MWh generation |
   | `/us-monthly-generation`   | US monthly generation time-series    |
   | `/il-monthly-generation`   | IL monthly generation time-series    |
   | `/actual-vs-forecast`      | PUDL generation vs PJM forecasts     |
   | `/actual-vs-forecast-mase` | Mean Absolute Scaled Error scores    |

6. **Define Grafana Visualizations**

   Grafana is running locally thanks to homebrew. Using the Flask routes described above we

## Results & Insights

[_Screenshots and business takeaways[11][7]._]

## Skills Demonstrated

[_List of relevant skills/tools, referenced by repo files[11][5]._]

## References, Endnots & Further Reading

[_Links for more context—energy sector specifics[7]._]

1 [Synapse: Forecasting Energy Demand and Policy Impacts in Illinois](https://www.synapse-energy.com/sites/default/files/A%20Snapshot%20of%20the%20Energy%20Landscape%20in%20Illinois_Synapse%20report%20for%20IMA%2024-134.pdf)

2 [Synapse: Risks of rapid data center load growth in IL (factsheet)](https://www.synapse-energy.com/sites/default/files/IL%20Data%20Center%20fact%20sheet_2025.04.30%2025-033.pdf)

3 [Synapse: Risks of rapid data center load growth in IL (slide deck)](https://www.synapse-energy.com/sites/default/files/IL%20Data%20Center%20results_2025.05.05%20Edits%20FINAL%2025-033.pdf)

4 [Synapse: Risks of rapid data center growth in PJM](https://www.sierraclub.org/sites/default/files/2025-03/pjmdatacentermodelingresults_dec2024.pdf)

5 [Synapse: A Snapshot of the Energy Landscape in Illinois](https://www.synapse-energy.com/sites/default/files/A%20Snapshot%20of%20the%20Energy%20Landscape%20in%20Illinois_Synapse%20report%20for%20IMA%2024-134.pdf)

6 [TGS: Illinois Basin’s Oil Revitalization and Data Center Growth](https://www.tgs.com/weekly-spotlight/09-08-2025) - interesting for E&P folks

7 [EIA: U.S. Energy Information Administration](https://www.eia.gov/state/analysis.php?sid=IL) - an excellent, dense one-page summary

8 [Dominion Energy VA and NC IRP](https://devirp.dominionenergy.com/img/feedback/media/irp-workshop-3-reliability-7-24-24.pdf)

9 [Brighlio: data centers in IL ](https://brightlio.com/data-centers-in-illinois/)

10 [PJM Critical Issue Fast Path](https://www.pjm.com/-/media/DotCom/about-pjm/who-we-are/public-disclosures/2025/20250808-pjm-board-letter-re-implementation-of-critical-issue-fast-path-process-for-large-load-additions.pdf)

11 [Reuters: Governors push PJM](https://www.reuters.com/business/energy/governors-push-more-sway-over-biggest-us-grid-power-bills-surge-2025-09-22/)

12 [IL SB2181 Gov Data Center Reporting](https://www.ilga.gov/Legislation/BillStatus?DocTypeID=SB&DocNum=2181&GAID=18&SessionID=114&LegID=161884)
