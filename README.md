# Power Plants & Data Centers in Illinois – Data Engineering Workflow

## Executive Summary

### Is the Illinois power grid prepared for projected data center growth?

To answer this question, we examine

- historical electrical generation from [PUDL](https://catalystcoop-pudl.readthedocs.io/en/nightly/index.html#)
- forecasts from the managing operator [PJM](https://www.pjm.com/about-pjm)
- the existing mix of power plants and Illinois government policies
- a brief survey of recent data center construction

Based on the analysis, we can make three broad observations:

<div style="border: 2px solid #f8a444; padding: 20px; border-radius: 5px; background:pink; font-weight: bold">

1. Past electrical generation since 2002 in the Northern Illinois ComEd zone shows a steady, modest rise.

2. Hyperscaler data center growth in the Midwest is not an illusion [16]. States and consumers are feeling the price increases [18].

3. PJM forecasts for 2025 and beyond do NOT show an expected "AI bump" and are relatively flat, suggesting a "wait and see" approach from operators.

4. The flat prediction is probably wrong. ComEd might rely on nuclear to offset high demand in the immediate future--maybe 2-5 years but, operators and governments should fast-track new renewable plants now.
</div>

## Business Problem

The rapid growth of energy-hungry AI/ML dominates the news cycle. Google Trends shows a 5X increase in the term "[data center](https://trends.google.com/trends/explore?date=today%205-y&geo=US&q=data%20center&hl=en)" in the last 5 years. Common refrains include soaring energy bills and imminent grid collapse.[7][10][11][12] A robust power grid is essential for Illinois to attract investment and continue its role as a key infrastructure hub.

Illinois is an interesting case study for data center growth:

- Illinois currently ranks as one of the top five data center markets globally, with Chicago specifically tied for the third-largest hub in the U.S. (222 sites in Q3 2025).[15]
- Data centers in Illinois consumed about 5.4% of the state’s total electricity use as of early 2025, a much higher percentage than national averages.[1]
- Illinois generates more electricity from nuclear energy than any other state, accounting for one-eighth of the nation's total nuclear power generation.[2]

The past, present and future contexts can help us learn about IL readiness:

**PAST:**  
We have historical power generation data from [PUDL](https://catalystcoop-pudl.readthedocs.io/en/nightly/index.html#) (pronounced puddle), a data processing pipeline created by [Catalyst Cooperative](https://catalyst.coop/) that cleans, integrates, and standardizes some of the most widely used public energy datasets in the US. **_We have decades of monthly power generation data from PUDL for the US._**

**PRESENT:**  
Analysts are sounding the alarm today on grid readiness, cost increases for consumers and water usage.[10][11][12] Synapse predicts an 8% price hike soon. Governors in PJM states are pushing for relief from data center-related price spikes.[7] **_The prevalence of news coverage alone suggests that there are real risks that need evaluation._**

**FUTURE:**  
[PJM](https://www.pjm.com/about-pjm) is the regional transmission operator that keeps the power grid running smoothly across several eastern US states, including the COMED (Commonwealth Edison) zone in Northern Illinois. PJM doesn’t make electricity; it manages who supplies power, keeps prices stable and prevents outages. Think of PJM as an "air traffic controller" of energy distribution. Monitoring of data center energy use in IL will improve greatly with the passing of SB2181, which will mandate site-level energy and water use starting in 2026.[5] **_PJM publishes forecast data, and we have several years of forecasts we can evaluate for accuracy._**

## Tech Stack Overview

<div style="border: 2px solid #f8a444; padding: 20px; border-radius: 5px;">

#### DATA SOURCES:

- [The Public Utility Data Liberation (PUDL) Project](https://catalyst.coop/pudl/) - open-source US electricity sector data
- [PJM](https://www.pjm.com/) - a regional coordinator of electricity in all or parts of 13 states and DC.

- [DATACENTERS.com](https://www.datacenters.com/) - searchable directory of US data centers
- [Nominatim](https://nominatim.org/) - open-source geocoding API service

#### STACK:

| tech                                  | usage                                                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| [DuckDB](https://duckdb.org/)         | fast, portable local database, optimized for analytical workflows in data science                      |
| [Selenium](https://www.selenium.dev/) | open-source automation framework for web browsers                                                      |
| [dbt](https://www.getdbt.com/)        | (data build tool) an open-source command-line tool to model objects in SQL                             |
| [Prefect](https://www.prefect.io/)    | a pythonic open-source framework for orchestrating, scheduling, and monitoring data workflows          |
| [Grafana](https://grafana.com/)       | open-source platform for creating interactive dashboards and visualizing metrics, logs, and other data |

</div>

## Installation & Quickstart

#### PREREQUISITES:

<div style="border: 2px solid #f8a444; padding: 20px; border-radius: 5px;">

**Python**: I used v3.13.7; anything from 3.10+ will probably work.

**Git**: 2.51 was used for this project. 2+ should work.

**Grafana**: homebrew: `brew install grafana` `brew services start grafana`

**uv**: (sort of optional, but highly recommended) `brew install uv` or `pipx install uv`

</div>

#### SETUP:

<div style="border: 2px solid #f8a444; padding: 20px; border-radius: 5px;">
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

</div>

## Architecture Diagram

[_Insert workflow diagram here._]

## Workflow Steps

Most of these steps are orchestrated with Prefect. Refer to the [Prefect flows](flows/etl_flows.py) for details. We are using DuckDB for it's native parquet support and simplicity. \*_PostgreSQL was a close runner-up, but my dev environment was configured for an older version that I didn't want to break._

1. **Collect PJM data**

   The PJM site has rudimentary search and download capabilities. The forecast files we need are available as either Excel .xls or .xlsx formats and have a somewhat inconsistent naming convention. This is historical data; just download them to the data [directory](data/pjm/).

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

#### IL Power Plants & Data Centers

![IL Power Plants & Data Centers](docs/il_power_plants_and_data_centers.png "IL Power Plants & Data Centers")

> The differences between the ComEd region in Northern Illinois and the Southern half of the state is striking. Large fossil-fuel plants (red) are mostly located in the South; nuclear power (purple) in the North. We can also see the clustering of data centers (yellow) near Chicago.

#### US Generation Mix

![US Generation Mix](docs/us_generation_mix.png "US Generation Mix")

> This busy time-series chart show monthly generation for the full US since 2002. Notable trends:
>
> - the gradual decline of Coal
> - the rise and dominance of Natural Gas
> - the general stability of Nuclear
> - slow growth of renewables, mostly Wind and Solar

#### IL Generation Mix

![IL Generation Mix](docs/il_generation_mix.png "IL Generation Mix")

> The strong nuclear capacity in Northern Illinois differentiates it from other geographic regions. This chart includes fossil-fuel power plants in Southern Illinois, but nuclear's importance is clear. The importance of nuclear is set to continue, as a long-standing moratorium on new nuclear facilities in Illinois was recently updated to permit small nuclear plants [14].
>
> Also note the relatively small contribution from Natural Gas in Illinois in contrast to the total US chart above. Illinois is a large consumer of natural gas and has many pipelines, but future commitments to sustainability would likely focus on new wind and solar rather natural gas power plants [7].

#### IL ComEd Actual (PUDL) vs Forecast (PJM)

![Actual vs Forecast](/docs/actual_vs_forecast_with_lr.png "IL Actual vs Forecast")

> This is perhaps the most surprising finding from comparing the PUDL and PJM datasets. The PUDL dataset (green) reflects actual, reported MWh electrical generation for all participating ComEd counties in Northern Illinois. The PJM dataset (purple) represents the latest\* published forecast for several years into the future.

<p>
<img src="docs/forecast_16-18.png" alt="PJM Forecast 2016-2018" style="width:49.8%; height:auto; display: inline-block;">
<img src="docs/forecast_20-23.png" alt="PJM Forecast 2020-2023" style="width:49.8%; height:auto; display: inline-block;">
</p>

> Forecasts from 2016 through 2018 (left) seem to accurately predict modest growth.
>
> Forecasts from 2019 through 2023 (right) depict steady-state. Not sure why.
>
> Despite all the jeremiads about the looming grid failures and price spikes caused by data centers, **the latest forecasts from the PJM show little or no projected increase in demand**. The forecast's linear regression looks nearly flat. The PJM acknowledges that their recent models may not accurately reflect the demands of hyperscale data centers, particularly in the ComEd zone [17]. It is unclear why not even modest growth is forecast for ComEd.
>
> _\* 2023 forecast from PJM, 2024 Excel file was corrupt (?!)_

I attempted to calculate a Mean Absolute Scaled Error to see if their forecast accuracy had measurably changed over the past few years, and it does suggest increasing error.

<table>
<tr>
<td>

| forecast year | MASE score |
| ------------- | ---------- |
| 2016          | 1.14       |
| 2017          | 1.03       |
| 2018          | 0.78       |
| 2019          | 0.69       |
| 2020          | 1.39       |
| 2021          | 1.27       |
| 2022          | 1.56       |
| 2023          | 1.93       |

</td>
<td>

<pre>
Mean Absolute Scaled Error (MASE) is a measure of forecast accuracy.
* A MASE of 0.7 = model’s error is 70% of the naïve error, good forecast.
* A MASE of 2.2 = error is more than twice as large as simply predicting
  the last actual for each step—poor forecast.
* Lower is better, ideally below 1.

MASE is a scale-free error metric. Both the numerator (average absolute
forecast error) and the denominator (average absolute naive error) are
affected proportionally by multiplicative changes in the actuals:
</pre>

</td>
</tr>
</table>

> [!CAUTION]
> For full accuracy, the calculation would need plant-by-plant data from PUDL for the ComEd zone to perfectly match PJM’s forecast inputs. This level of granularity is unavailable publicly. Attempts to "fuzzy match" plant names between PJM and PUDL were too inconsistent for reliable results. Additional complexity comes from interconnects, occasional non-PJM supply during high demand, new generators cycling on/off, and routine maintenance outages. The most robust method was to use all Northern Illinois counties associated with ComEd as the “actual” filter, leaving PJM forecasts unchanged.

## References, Endnots & Further Reading

1. [Brighlio: Illinois Data Centers Offer High Security, Efficiency](https://brightlio.com/data-centers-in-illinois/)  
   Summary: Overview of major Chicago-area data centers, highlighting their sustainable design, high security, and industry certifications for reliable and efficient operations.

2. [EIA: One-Page Illinois Energy Data Snapshot](https://www.eia.gov/state/analysis.php?sid=IL)  
   Summary: Comprehensive state energy profile including generation mix, electricity prices, top industries, and historical trends from the U.S. Energy Information Administration.

3. [IL General Assembly: Nuclear Reactors Law Lifts Small Plant Ban](https://ilga.gov/documents/legislation/publicacts/103/PDF/103-0569.pdf)  
   Summary: Details 2023 Illinois law partially lifting the ban on new nuclear plant construction, specifically permitting small modular reactors under 300 MW.

4. [Illinois Policy: Nuclear Growth Potential Despite Moratorium](https://www.illinoispolicy.org/nuclear-energy-gives-illinois-economic-power-if-it-will-allow-new-plants/)  
   Summary: Argues that Illinois can harness significant economic benefits from expanding nuclear energy if policy restrictions are further relaxed.

5. [IL SB2181: New Data Center Energy Reporting Requirement](https://www.ilga.gov/Legislation/BillStatus?DocTypeID=SB&DocNum=2181&GAID=18&SessionID=114&LegID=161884)  
   Summary: Illinois legislation mandating state agencies report on large data center power usage for improved grid planning.

6. [PJM: Board Fast-Tracks ‘Critical Issue’ Process for Data Centers](https://www.pjm.com/-/media/DotCom/about-pjm/who-we-are/public-disclosures/2025/20250808-pjm-board-letter-re-implementation-of-critical-issue-fast-path-process-for-large-load-additions.pdf)  
   Summary: PJM’s board launches an accelerated rulemaking track to address dramatic growth in data center electricity demand, targeting new reliability and interconnection standards.

7. [Reuters: Governors Demand Greater Say in PJM Grid](https://www.reuters.com/business/energy/governors-push-more-sway-over-biggest-us-grid-power-bills-surge-2025-09-22/)  
   Summary: More than a quarter of U.S. state governors seek more control over PJM, citing soaring electricity prices fueled by surging AI data center demand.

8. [SiteSelection: Illinois Remains a Top Data Center Location](https://siteselection.com/crunching-the-numbers/)  
   Summary: Analysis of Illinois’s national standing as a leading U.S. data center destination, with factors including incentives, infrastructure, workforce, and market activity.

9. [Small Nuclear Reactors Now Allowed in Illinois](https://ilga.gov/documents/legislation/publicacts/103/PDF/103-0569.pdf)  
   Summary: Law permitting new small modular nuclear reactors, ending Illinois’ total ban on new nuclear sites.

10. [Synapse: Illinois Data Center Growth Risks (Slide Deck)](https://www.synapse-energy.com/sites/default/files/IL%20Data%20Center%20results_2025.05.05%20Edits%20FINAL%2025-033.pdf)  
    Summary: Slides quantifying how rapid new data center load will boost Illinois and PJM grid demand, pushing total loads and requiring major new energy infrastructure by 2040.

11. [Synapse: Illinois Data Center Load Growth Risks (Fact Sheet)](https://www.synapse-energy.com/sites/default/files/IL%20Data%20Center%20fact%20sheet_2025.04.30%2025-033.pdf)  
    Summary: Brief describing a projected 30% increase in ComEd’s grid load from data centers by 2040, driving up residential bills 8.3% and boosting CO2 emissions by 64% in the region.

12. [Synapse: PJM Data Center Growth Raises Regional Bills](https://www.sierraclub.org/sites/default/files/2025-03/pjmdatacentermodelingresults_dec2024.pdf)  
    Summary: PJM-wide analysis finds large anticipated increases in load, peak demand, and emissions from rapid data center buildout, raising residential electricity bills.

13. [Synapse: State of Illinois Energy System—2025 Landscape](https://www.synapse-energy.com/sites/default/files/A%20Snapshot%20of%20the%20Energy%20Landscape%20in%20Illinois_Synapse%20report%20for%20IMA%2024-134.pdf)  
    Summary: Deep-dive report on Illinois energy trends, policies, decarbonization goals, and forecasted demand scenarios, including data center and electrification load impacts.

14. [TGS: Oil Revitalization and Data Center Expansion in Illinois Basin](https://www.tgs.com/weekly-spotlight/09-08-2025)  
    Summary: Outlines how massive regional oilfield redevelopment projects and hyperscale data center construction are reshaping the Illinois Basin’s economic and energy future, with integrated geoscience datasets supporting both.

15. [Brighlio: Illinois Rises as Colocation and Hosting Hub](https://brightlio.com/data-centers-in-illinois/)  
    Summary: Summarizes key players and facility features drawing major cloud and tech clients to Illinois’s data center market.

16. [Whitecase: Large Pipeline of Hyperscaler project in PJM](https://www.whitecase.com/insight-alert/grid-operators-propose-innovative-measures-manage-electricity-demand-data-centers)  
    Summary: Concrete growth through 2030 is now fully backed by utility/grid agreements, not projections or speculation

17. [PJM: Long Term Load Forecasts](https://www.pjm.com/-/media/DotCom/planning/res-adeq/load-forecast/2025-long-term-load-forecast-supplement.pdf)  
    Summary: Slow large-load reporting, model lag, and the difficulty of distinguishing “hyperscale” facilities from standard commercial uses makes modelling forecasts difficult

18. [UtilityDive: PA Governor threatens to leave PJM](https://www.utilitydive.com/news/governors-states-pjm-governance-conference-capacity/760842/)  
    Republican and Democratic governors of PJM Interconnection states on Monday threatened to pull out of the grid operator’s markets unless states are given a role in governing the organization.
