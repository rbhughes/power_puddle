# Power Plants & Data Centers in Illinois – Data Engineering Workflow

## Executive Summary

Briefly describe project goals, tools, impact[11][7].

## Business Problem

Explain context, business relevance, and motivation[11][7].

Is the Illinois power grid prepared for Data Center growth?

Most public sources for Illinois data center power usage are indirect, using reports, projections, and aggregate sector data. However, new legislation (SB2181, effective 2026) will soon require annual public reporting of site-level data center energy use for all operators in Illinois.

https://www.synapse-energy.com/sites/default/files/A%20Snapshot%20of%20the%20Energy%20Landscape%20in%20Illinois_Synapse%20report%20for%20IMA%2024-134.pdf

## Tech Stack Overview

Data Sources:

- [The Public Utility Data Liberation (PUDL) Project](https://catalyst.coop/pudl/) - open-source US electricity sector data
- [synapse1](https://www.synapse-energy.com/forecasting-energy-demand-and-policy-impacts-illinois)
- [synapse2](https://www.synapse-energy.com/risks-rapid-data-center-load-growth-illinois)
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
