# San Diego Police Stops: Search and Yield by Perceived Race

## Summary

This project is an end-to-end analytics engineering portfolio project using public San Diego police stop data from California’s Racial and Identity Profiling Act (RIPA) program. The project investigates whether search rates and yield rates differ by officer-perceived race after a person is stopped.

I built a full analytics workflow from raw public data through modeled analytical marts and a published Tableau dashboard. The final dashboard focuses on search and yield patterns by perceived race, with additional views showing how those patterns vary over time and across stop reasons.

The project was designed to demonstrate practical analytics engineering skills: working with messy public data, defining a defensible grain, building layered dbt models, creating tested marts, designing a public-facing dashboard, and validating that the dashboard metrics match the modeled data.

## What I Built

This project includes:

* A dbt project that stages raw San Diego RIPA source tables and models them into intermediate and mart layers.
* A person-stop fact model with one row per person associated with a recorded stop.
* Intermediate models that summarize search flags, find flags, perceived race, and stop reason at the person-stop grain.
* Analytical marts for monthly search and yield metrics by perceived race and by perceived race plus stop reason.
* dbt documentation and tests for key model grains, accepted flag values, and rate ranges.
* A Tableau Public dashboard with two views: an overview of search/yield patterns by selected perceived race group, and a stop-reason analysis focused on where search-rate differences are concentrated.
* A GitHub repository with a project README, dashboard screenshots, source definitions, dbt model documentation, and reproducibility notes.

## Data, Grain, and Caveats

The project uses public San Diego RIPA police stop data. The source data includes separate raw tables for stops, perceived race, search basis, stop reason, stop result, contraband/evidence, and related stop details.

A central modeling decision was to use the **person-stop** as the analytical grain: one person associated with one recorded stop. This matters because some stops can involve multiple people, and many RIPA detail tables can contain multiple records per stop or per person. Modeling at the person-stop grain helps keep the core search and yield metrics interpretable.

Important caveats:

* Records represent person-stops, not unique people.
* Race is officer-perceived, not self-identified.
* Search and yield metrics are observational and do not establish causal explanations.
* Population-share context uses ACS/Census categories, which may not map exactly to RIPA perceived-race categories.

## Modeling Approach

The dbt project follows a layered structure:

```text
public RIPA CSV data
→ Google Cloud / BigQuery raw source tables
→ staging models
→ intermediate models
→ person-stop fact model
→ analytical marts
→ Tableau dashboard
```

The staging layer standardizes raw RIPA source tables by renaming fields, preserving identifiers, and making the raw tables easier to work with downstream.

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

The dbt project was also validated through dbt Cloud. The final build completed successfully with all active models and tests passing:

```text
PASS=70 WARN=0 ERROR=0
```

## What This Project Demonstrates

This project demonstrates a full analytics engineering workflow rather than a standalone dashboard. The final artifact depends on a complete chain of data ingestion, modeling, metric definition, testing, dashboard design, and QA.

Key skills demonstrated:

* Building a layered dbt project with staging, intermediate, fact, and mart models.
* Managing data grain and avoiding double-counting in many-row source tables.
* Defining and documenting analytical metrics.
* Using `dbt_utils` tests to validate model grain and metric ranges.
* Creating dbt documentation for intermediate and mart models.
* Building a Tableau Public dashboard from modeled analytical data.
* Performing dashboard QA by checking calculations, denominators, filters, and interactions.
* Communicating caveats clearly for public-sector observational data.

## Links

* [Tableau Public Dashboard](https://public.tableau.com/app/profile/alan.ward2828/viz/RIPATableauPublic/SDPoliceStops-Overview)
* [GitHub Repository](https://github.com/NeonTheorist/ripa-police-data)
