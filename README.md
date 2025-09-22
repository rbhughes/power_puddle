# Power Plants & Data Centers in Illinois – Data Engineering Workflow

## Executive Summary

Briefly describe project goals, tools, impact[11][7].

## Business Problem

Explain context, business relevance, and motivation[11][7].

Is the Illinois power grid prepared for projected data center growth?

PJM is a regional transmission operator that keeps the power grid running smoothly across several eastern US states. It doesn’t make electricity—PJM manages who supplies power, keeps prices stable, and prevents outages. The COMED zone is just the area served by Commonwealth Edison in northern Illinois; PJM handles the flow and market coordination for COMED like it does for all its regional zones. Think of PJM as an "air traffic controller" of energy distribution.

https://www.synapse-energy.com/sites/default/files/A%20Snapshot%20of%20the%20Energy%20Landscape%20in%20Illinois_Synapse%20report%20for%20IMA%2024-134.pdf

## Tech Stack Overview

Data Sources:

Most public sources for Illinois data center power usage are indirect, using reports, projections, and aggregate sector data. New legislation (SB2181, effective 2026) will soon require annual public reporting of site-level data center energy use for all operators in Illinois. Until then, we have:

- [The Public Utility Data Liberation (PUDL) Project](https://catalyst.coop/pudl/) - open-source US electricity sector data
- [PJM](https://www.pjm.com/) - a regional coordinator of electricity in all or parts of 13 states and DC.
- [Synapse Energy Economics Inc.](https://www.synapse-energy.com/)

  - [Forecasting Energy Demand and Policy Impacts in Illinois](https://www.synapse-energy.com/sites/default/files/A%20Snapshot%20of%20the%20Energy%20Landscape%20in%20Illinois_Synapse%20report%20for%20IMA%2024-134.pdf)

  - [Risks of rapid data center load growth in IL (factsheet)](https://www.synapse-energy.com/sites/default/files/IL%20Data%20Center%20fact%20sheet_2025.04.30%2025-033.pdf)
  - [Risks of rapid data center load growth in IL (slide deck)](https://www.synapse-energy.com/sites/default/files/IL%20Data%20Center%20results_2025.05.05%20Edits%20FINAL%2025-033.pdf)

  - [A Snapshot of the Energy Landscape in Illinois](https://www.synapse-energy.com/sites/default/files/A%20Snapshot%20of%20the%20Energy%20Landscape%20in%20Illinois_Synapse%20report%20for%20IMA%2024-134.pdf)

- [Illinois Basin’s Oil Revitalization and Data Center Growth](https://www.tgs.com/weekly-spotlight/09-08-2025) - interesting for E&P folks

- [U.S. Energy Information Administration](https://www.eia.gov/state/analysis.php?sid=IL) - a good, one-page summary with an enormous list of endnotes

---

While not crucial for energy predictions, we obtained a list of all data centers in Illinois and their latitude, longitude coordinates from:

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

Step-by-step instructions to run the project[5].

## Architecture Diagram

Insert workflow diagram here[7].

## Workflow Steps

- **Prefect:** Orchestration overview, link to code/notebook
- **Selenium:** Data ingestion details
- **DuckDB & dbt:** Storage, transformation logic
- **Grafana:** Visualization sample (with inline images/video)[11][7]

## Results & Insights

Screenshots and business takeaways[11][7].

## Skills Demonstrated

List of relevant skills/tools, referenced by repo files[11][5].

## Project Organization

Folder/file overview for repo navigation[7].

## References & Further Reading

Links for more context—energy sector specifics[7].
