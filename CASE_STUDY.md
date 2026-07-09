# San Diego Police Stops: Search and Yield by Perceived Race

## Summary

This project is an end-to-end analytics engineering portfolio project using public San Diego police stop data from California’s Racial and Identity Profiling Act (RIPA) program. The project investigates whether search rates and yield rates differ by officer-perceived race after a person is stopped.

I built a full analytics workflow from scheduled cloud ingestion of public RIPA data through BigQuery raw source tables, layered dbt models, tested analytical marts, a Google Sheets publication layer, and a published Tableau Public dashboard.

The final dashboard focuses on search and yield patterns by perceived race, with additional views showing how those patterns vary over time and across stop reasons.

The project was designed to demonstrate practical analytics engineering skills: working with messy public data, building an automated data pipeline, defining a defensible analytical grain, creating layered dbt models, validating metric definitions, publishing dashboard-ready marts, designing a public-facing Tableau dashboard, and QA-checking the final outputs against reference data.

## What I Built

This project includes:

* A scheduled Python ingestion workflow using Google Cloud Run.
* Cloud storage of public RIPA CSV data in Google Cloud Storage.
* BigQuery raw source tables populated from public RIPA data.
* A dbt project that stages raw San Diego RIPA source tables and models them into intermediate, fact, and mart layers.
* A person-stop fact model with one row per person associated with a recorded stop.
* Intermediate models that summarize search flags, find flags, perceived race, and stop reason at the person-stop grain.
* Analytical marts for monthly search and yield metrics by perceived race and by perceived race plus stop reason.
* dbt documentation and tests for key model grains, accepted flag values, and rate ranges.
* An automated publication layer that outputs analytical mart data to Google Sheets for Tableau Public.
* A Tableau Public dashboard with two views: an overview of search/yield patterns by selected perceived race group, and a stop-reason analysis focused on where search-rate differences are concentrated.
* A GitHub repository with a project README, dashboard screenshots, source definitions, dbt model documentation, and reproducibility notes.

## Data, Grain, and Caveats

The project uses public San Diego RIPA police stop data. The source data includes separate raw tables for stops, perceived race, search basis, stop reason, stop result, contraband/evidence, force actions, non-force actions, disability, and property seizure details.

A central modeling decision was to use the **person-stop** as the analytical grain: one person associated with one recorded stop. This matters because some stops can involve multiple people, and many RIPA detail tables can contain multiple records per stop or per person. Modeling at the person-stop grain helps keep the core search and yield metrics interpretable.

Important caveats:

* Records represent person-stops, not unique people.
* Race is officer-perceived, not self-identified.
* Search and yield metrics are observational and do not establish causal explanations.
* Population-share context uses ACS/Census categories, which may not map exactly to RIPA perceived-race categories.

## Pipeline Architecture

This project uses an automated, layered analytics workflow:

```text
Public RIPA CSV data
→ scheduled Python ingestion via Google Cloud Run
→ file storage in Google Cloud Storage
→ BigQuery raw source tables
→ dbt source definitions
→ dbt staging models
→ dbt intermediate models
→ dbt fact and mart models
→ dbt tests and documentation
→ automated output of analytical mart data to Google Sheets
→ Tableau Public dashboard connected to Google Sheets
→ dashboard QA and metric validation
```

The goal was not just to create a one-time dashboard extract. The project was designed as a repeatable analytics pipeline: public source data is ingested through a scheduled cloud workflow, stored in Google Cloud Storage, loaded into BigQuery, transformed with dbt, published to Google Sheets, and consumed by Tableau Public.

Manual work still mattered where it should: model design, metric definition, dashboard design, validation queries, dashboard QA, and final review. The recurring data movement and transformation flow, however, was built as an automated pipeline rather than a purely manual reporting process.

## Modeling Approach

The dbt project follows a layered structure:

```text
BigQuery raw source tables
→ staging models
→ intermediate models
→ person-stop fact model
→ analytical marts
→ Google Sheets publication layer
→ Tableau Public dashboard
```

The staging layer standardizes raw RIPA source tables by renaming fields, preserving identifiers, and making the raw tables easier to work with downstream.

A major technical issue was that several source tables contained multiple rows per stop or person-stop. To avoid double-counting, I created intermediate models that reduced these tables to the person-stop grain before joining them into the final fact model. Search-basis records were collapsed into a single `had_search` flag, contraband/evidence records were collapsed into a single `had_find` flag, and multi-value race or stop-reason records were grouped into explicit summary categories rather than silently choosing one record.

The intermediate layer resolves several many-row source tables to the person-stop grain:

* `int_search_flag` aggregates search-basis records and creates a `had_search` flag.
* `int_find_flag` aggregates contraband/evidence records and creates a `had_find` flag.
* `int_race_summary` summarizes perceived-race records to one value per person-stop, grouping multi-race or conflicting records as `Multiple/ambiguous race`.
* `int_stop_reason_summary` summarizes stop-reason records to one value per person-stop, grouping multi-reason records as `Multiple stop reasons`.

The primary fact model, `fct_ripa_person_stop`, joins these intermediate models back to the staged stop records. It produces one row per person-stop with stop date, stop month, perceived race, search flag, find flag, demographic context, agency, city, and summarized stop reason.

The final dashboard uses two main mart models:

* `mart_search_yield_by_race_month`: monthly search and yield metrics by perceived race.
* `mart_search_yield_by_race_reason_month`: monthly search and yield metrics by perceived race and stop reason.

## Metric Definitions

The dashboard focuses on two primary rates:

* **Search rate** = searched persons / stopped persons
* **Yield rate** = searched and found persons / searched persons

The supporting counts are:

* **Stopped persons**: total person-stop records.
* **Searched persons**: person-stops with `had_search = 1`.
* **Searched and found persons**: person-stops with both `had_search = 1` and `had_find = 1`.

This distinction is important because yield rate is calculated only among searched persons, not among all stopped persons.

The metric definitions were carried through the dbt mart layer and into Tableau so that dashboard calculations could be checked against modeled data rather than treated as isolated workbook logic.

## Dashboard Design

The final Tableau Public dashboard has two main views.

The first view, **San Diego Police Stops - Overview**, provides a selected perceived-race group overview. It shows city population context, share of stopped persons, search rate, and yield rate, along with quarterly search-rate trends and overall search/yield comparisons across the four main perceived-race groups.

The second view, **San Diego Police Stop Reason Patterns**, focuses on where search-rate differences are concentrated. It includes selected-period race profiles, full-history sparklines, a stop-reason heatmap, and a focused comparison of traffic violation stops.

The dashboard was designed to keep the main analytical question visible while still making the metric definitions and caveats clear. In particular, the dashboard emphasizes that records are person-stops, race is officer-perceived, and yield rate is calculated among searched persons only.

## QA and Validation

I performed a dedicated QA pass before treating the Tableau dashboard as complete.

The QA process included:

* Reviewing Tableau calculated fields against the intended metric definitions.
* Checking worksheet structure, filters, parameters, and dashboard interactions.
* Verifying representative dashboard values against reference calculations.
* Confirming that rates could be recreated from the displayed raw counts.
* Fixing a denominator issue in the selected-race “share of stopped persons” KPI.
* Checking that selected-period filters affected the intended views.
* Running a final visual review of dashboard titles, caveats, labels, and tooltips.

One dashboard QA issue involved the selected-race “share of stopped persons” KPI. The original calculation used an incorrect denominator for the selected-period context. I corrected the denominator logic and rechecked the displayed value against reference calculations from the modeled mart data.

The dbt project was also validated through dbt Cloud. The final build completed successfully with all active models and tests passing:

```text
PASS=70 WARN=0 ERROR=0
```

## Reproducibility Notes

The repository can be reviewed without access to the original BigQuery environment. The README, dbt model files, dashboard screenshots, Tableau Public dashboard, and this case study are intended to show the final analytical output and modeling approach.

To run the dbt project independently, a user would need to create their own Google Cloud / BigQuery environment, load the public RIPA CSV files into raw source tables matching the project’s `sources.yml` definitions, and configure a local dbt profile with access to that dataset.

The project is therefore reproducible in structure and logic, but it is not designed as a one-click public demo environment with shared BigQuery credentials.

## What This Project Demonstrates

This project demonstrates a full analytics engineering workflow rather than a standalone dashboard. The final artifact depends on a complete chain of data ingestion, storage, raw table loading, dbt modeling, metric definition, testing, dashboard publication, dashboard design, and QA.

Key skills demonstrated:

* Building an automated analytics pipeline using Python, Google Cloud Run, Google Cloud Storage, and BigQuery.
* Building a layered dbt project with staging, intermediate, fact, and mart models.
* Managing data grain and avoiding double-counting in many-row source tables.
* Defining and documenting analytical metrics.
* Using dbt tests to validate model grain, accepted values, and metric ranges.
* Creating dbt documentation for intermediate, fact, and mart models.
* Publishing modeled analytical data to Google Sheets for Tableau Public consumption.
* Building a Tableau Public dashboard from modeled analytical data.
* Performing dashboard QA by checking calculations, denominators, filters, interactions, and reference values.
* Communicating caveats clearly for public-sector observational data.

## Links

* [Tableau Public Dashboard](https://public.tableau.com/app/profile/alan.ward2828/viz/RIPATableauPublic/SDPoliceStops-Overview)
* [GitHub Repository](https://github.com/NeonTheorist/ripa-police-data)
