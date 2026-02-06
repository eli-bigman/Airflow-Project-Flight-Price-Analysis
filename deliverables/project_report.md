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
  - **Function**: Reads the raw CSV dataset in chunks. To handle efficient loads, it uses an _Offset_ mechanism to read only new lines added since the last run.
  - **Deduplication**: Generates a unique MD5 hash for every row and checks it against a reference table (`processed_hashes`) to ensure only unique data is eagerly loaded into `staging_flight_data.raw_flight_data` in MySQL.

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

### Challenge 1: Duplicate Data Handling

**Issue**: Re-running the pipeline resulted in duplicate entries in the database, skewing analytics.
**Root Cause**: The pipeline initially blindly appended data.
**Resolution**: Implemented a **Content-Addressable Hashing** strategy.

- We generate a unique MD5 hash for every row based on its content.
- Before inserting, we check a `processed_hashes` reference table.
- Only records with new, unseen hashes are ingested.

### Challenge 2: Incremental Loading of Large Files

**Issue**: Processing the entire dataset every time is inefficient and resource-heavy.
**Resolution**: Implemented an **Offset-based Reader**.

- The generic `flight_csv_offset` Airflow Variable tracks the last read line number.
- On the next run, the reader simply seeks to that offset and continues, processing only new appended lines.
