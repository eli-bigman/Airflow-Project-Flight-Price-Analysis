# Flight Price Analysis - Project Deliverables Report

## 1. Pipeline Architecture & Execution Flow

### System Overview

This project implements a **Modern Data Stack (MDS)** utilizing a containerized "Extract-Load-Transform" (ELT) architecture. The entire infrastructure is orchestrated by Apache Airflow and encapsulated within a Docker environment to ensure reproducibility.

**Core Components:**

- **Orchestration**: Apache Airflow (Scheduler, Webserver, Triggerer)
- **Staging Database**: MySQL (Stores raw, immutable data)
- **Data Warehouse**: PostgreSQL (Stores the Star Schema for analytics)
- **Visualization**: Metabase / DBGate (for data exploration)

### Execution Flow

The data flows linearly through the system, managed by the `flight_price_pipeline` DAG:

1.  **Ingestion ("Extract & Load")**:
    - The `load_csv_to_mysql_staging` task reads the raw CSV dataset (`Flight_Price_Dataset_of_Bangladesh.csv`).
    - It performs a robust incremental load, reading the file in chunks to optimize memory usage.
    - Data is hashed (MD5 of the row) to prevent duplicates and loaded into the `staging_flight_data.raw_flight_data` table in MySQL.

2.  **Transformation ("Transform")**:
    - The `transform_and_load_star_schema` task extracts the raw data from MySQL.
    - It applies data cleaning logic (string standardization, parsing "stops", handling currency).
    - It separates data into Dimensional attributes (`dim_airlines`, `dim_airports`, `dim_date`) and Fact metrics (`fact_flights`).
    - The transformed data is loaded into the `analytics` schema in PostgreSQL.

3.  **Validation**:
    - The `validate_row_counts` task compares record counts across the Source (CSV), Staging (MySQL), and Analytics (PostgreSQL) layers to ensure data integrity and detect any significant data loss.

---

## 2. Airflow DAG & Task Descriptions

**DAG Name**: `flight_price_pipeline`
**Schedule**: `@daily`
**Description**: An automated ELT pipeline that ingests flight data, processes it, and loads it into a Star Schema for analysis.

**Code Structure**:
The pipeline follows a **Modular Task Architecture** to separate orchestration from business logic:

- `dags/flight_pipeline_dag.py`: **Orchestrator**. Defines the DAG schedule and dependencies.
- `dags/tasks/ingestion.py`: Handles CSV reading and MySQL staging.
- `dags/tasks/transformation.py`: Handles data cleaning and Star Schema logic.
- `dags/tasks/validation.py`: Handles data quality checks.

The pipeline consists of the following sequential tasks:

### 1. Infrastructure Checks (Sensors)

- **`wait_for_mysql`** (`SqlSensor`):
  - **Purpose**: Validates that the MySQL Staging Database is online and ready to accept connections.
  - **Behavior**: Pokes the database every 10 seconds until it responds, preventing the pipeline from failing due to startup latency.

- **`wait_for_postgres`** (`SqlSensor`):
  - **Purpose**: Validates that the PostgreSQL Analytics Database is reachable.
  - **Behavior**: Ensures the destination data warehouse is ready before any processing begins.

### 2. Ingestion & Loading

- **`load_csv_to_mysql_staging`** (`PythonOperator`):
  - **Role**: The **Ingestion Engine**.
  - **Function**: Scans the `data/input/` directory for any new CSV files.
  - **Process**:
    1.  Reads each file entirely.
    2.  Loads data into MySQL `staging_flight_data.raw_flight_data`.
    3.  **Archives** the processed file to `data/archive/` to ensure it is not re-processed.
  - **Benefit**: This "Process & Archive" pattern ensures idempotency and provides a clear history of processed files.

### 3. Transformation

- **`transform_and_load_star_schema`** (`PythonOperator`):
  - **Role**: The **Core Logic Engine**.
  - **Function**: Extracts raw data from MySQL and transforms it into a dimensional model.
  - **Key Operations**:
    - **Standardization**: cleans text fields (e.g., stripping whitespace, capitalizing).
    - **Dimension Building**: separating attributes into `dim_airlines`, `dim_airports`, and `dim_date`.
    - **Fact Creation**: mapping foreign keys to build the `fact_flights` table.

### 4. Quality Assurance

- **`validate_row_counts`** (`PythonOperator`):
  - **Role**: The **Quality Gate**.
  - **Function**: Performs a sanity check by comparing the total record counts in the Source CSV, Staging Database, and Analytics Data Warehouse.
  - **Alerting**: Raises a failure if the data discrepancy exceeds 1%, ensuring no significant data loss occurred during the pipeline execution.

---

## 3. KPI Definitions & Computation Logic

The Star Schema enables the calculation of the following Key Performance Indicators (KPIs):

### 1. Average Base Fare

- **Definition**: The mean price of a flight ticket before taxes.
- **Logic**: `AVG(fact_flights.total_fare)` grouped by dimensions (Airline, Date).
- **Purpose**: To identify the most expensive and affordable carriers.

### 2. Total Bookings (Demand)

- **Definition**: The total count of flight segments or tickets sold.
- **Logic**: `COUNT(*)` from `fact_flights`.
- **Purpose**: To measure route popularity and carrier market share.

### 3. Seasonality Trends

- **Definition**: Categorization of travel dates into seasons (e.g., "Eid", "Winter Holidays", "Regular") to analyze price surges.
- **Logic**:
  - **Current Implementation**: Derived directly from the `Seasonality` column in the source CSV.
  - **Note**: This relies on the pre-labeled data source rather than a dynamic calendar lookup.

### 4. Route Popularity

- **Definition**: The most frequently flown paths between two cities.
- **Logic**: Count of flights grouped by `source_airport_id` and `destination_airport_id`.

---

## 4. Challenges Encountered & Resolutions

### Challenge 1: Incremental Data Loading

**Issue**: The dataset grows over time, and re-processing the entire file every day is inefficient and leads to duplicates.
**Initial Approach**: We tried using an "Offset" variable to track line numbers in a single growing file.
**Refined Resolution**: Switched to a **"Process & Archive"** pattern.

- New data arrives in `data/input/`.
- The pipeline processes the file and immediately moves it to `data/archive/`.
- This ensures that only _new_ data is ever processed, eliminating the need for complex offset tracking or hash-based deduplication.
