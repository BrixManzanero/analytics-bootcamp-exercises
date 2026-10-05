# Analytics Solutions Bootcamp — My Exercises

This is my personal repository for the Analytics Solutions Bootcamp. It contains the lab exercises, case study work, and additional practice I've built while learning Data Engineering, Data Analysis, Data Science, Data Governance, and AI.

## Main Folders

1. `00_setup`: Configuration files and sample datasets used throughout the bootcamp.
   - `compose.yaml`: Docker compose file for PostgreSQL and Metabase services.
   - `requirements.txt`: Python package dependencies.
   - `setup_db.py`: Script that ingests sample data into PostgreSQL.
2. `01_slides`: PDF lecture slides used in class.
3. `02_lab`: Lab exercises per module — including extra work I built on my own (e.g. `dim_employee.sql`, Store demographics XML parsing).
4. `03_case_study`: Take-home case study instructions and my solutions.

## My Setup

I'm running a lightweight Postgres + Metabase stack (rather than the full Airflow/Spark/Iceberg stack) to fit an 8GB RAM machine:

- **WSL (Windows Subsystem for Linux)** — `wsl --install`
- **Docker Desktop**
- **Git** and **GitHub**
- **Python 3.12**
- **R** and **RStudio**
- **VS Code** with the SQLTools extension (PostgreSQL driver)

## Running the Services

1. Make sure Docker Desktop is running.
2. Run: `docker compose up -d`
3. Verify: `docker compose ps` — `postgres_db` and `metabase` should show "Up"/"healthy".
4. Access:
   - **PostgreSQL**: `localhost:5432` (db: `asb_oltp`, user: `db_user`)
   - **Metabase**: http://localhost:3000

## What I've Learned So Far

- Slowly Changing Dimension (SCD) Type 2 — tracking history with `valid_from`/`valid_to`/`is_current`
- XML parsing in PostgreSQL using `XMLTABLE`
- Dimensional modeling (star schema) — dimension and fact tables
- Debugging common SQL issues (syntax differences between Postgres and MySQL, duplicate column names)

## Progress

- [x] Module 3: Data Engineering — dimensional modeling exercises
- [ ] Module 3: Case Study (Wide World Importers pipeline)
- [ ] Module 4: Data Analysis & BI
- [ ] Module 5: Data Science
- [ ] Module 6: Data Governance (Databricks)
- [ ] Module 7: AI
