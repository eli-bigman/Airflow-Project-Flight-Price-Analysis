# Flight Price Analysis Pipeline

A robust, containerized **End-to-End ELT Pipeline** that ingests flight data, stages it in MySQL, and transforms it into a Star Schema in PostgreSQL for high-performance analytics. Orchestrated by **Apache Airflow**.

## 🚀 Project Overview

This project implements a **Modern Data Stack (MDS)** to analyze flight prices. It simulates a production-grade data engineering workflow where raw data is ingested, staged, and then transformed into a dimensional model suitable for Business Intelligence (BI) tools.

### Key Features

- **Containerized Architecture**: Fully Dockerized environment (Airflow, MySQL, PostgreSQL).
- **Automated orchestration**: Daily scheduled workflows using Airflow DAGs.
- **ELT Strategy**: "Extract-Load-Transform" pattern for flexibility and performance.
- **Dimensional Modeling**: Implementation of a Star Schema (Fact & Dimensions).
- **Infrastructure as Code**: Database schemas initialized via SQL scripts.

---

## 🏗️ Architecture

The pipeline follows a linear data flow orchestrated by the `flight_price_pipeline` DAG.

```mermaid
graph LR
    subgraph Source
        CSV[Flight_Price_Dataset.csv]
    end

    subgraph Orchestration [Apache Airflow]
        direction TB
        Task1[Task: load_csv_to_mysql_staging]
        Task2[Task: transform_and_load_star_schema]
    end

    subgraph Staging [Staging Layer]
        MySQL[(MySQL DB<br/>raw_flight_data)]
    end

    subgraph Analytics [Analytics Layer]
        Postgres[(PostgreSQL DB<br/>Star Schema)]
    end

    %% Data Flow
    CSV -->|Read & Clean| Task1
    Task1 -->|Load Raw Data| MySQL
    MySQL -->|Extract Raw| Task2
    Task2 -->|Transform & Load| Postgres

    %% Dependency
    Task1 -.->|Trigger| Task2

    style Source fill:#f9f9f9,stroke:#333,stroke-width:2px
    style Orchestration fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style Staging fill:#fff3e0,stroke:#e65100,stroke-width:2px
    style Analytics fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
```

### Pipeline Workflow (`flight_price_pipeline`)

The Airflow DAG consists of two primary PythonOperator tasks:

1.  **`load_csv_to_mysql_staging`**:
    - **Input**: `data/Flight_Price_Dataset_of_Bangladesh.csv`
    - **Action**: Reads CSV, standardizes column names (snake_case), and loads raw data into MySQL.
    - **Output**: `staging_flight_data.raw_flight_data` (MySQL).

2.  **`transform_and_load_star_schema`**:
    - **Input**: Raw data from MySQL.
    - **Action**: Extracts data, performs dimensional modeling (separating logic for Airlines, Airports, Dates), and creates the Fact table.
    - **Output**: `analytics` schema tables in PostgreSQL (`fact_flights`, `dim_airlines`, etc.).

---

## 💾 Database Schema (Star Schema)

The Analytics layer in PostgreSQL is designed as a **Star Schema** to optimize read performance for analytical queries.

```mermaid
erDiagram
    fact_flights {
        int flight_id PK
        int airline_id FK
        int source_airport_id FK
        int destination_airport_id FK
        date departure_date_id FK
        decimal total_fare
        decimal duration_hours
        int stopovers
    }

    dim_airlines {
        int airline_id PK
        varchar airline_name
    }

    dim_airports {
        int airport_id PK
        varchar airport_code
        varchar airport_name
    }

    dim_date {
        date date_id PK
        int year
        int month
        boolean is_weekend
        varchar seasonality
    }

    fact_flights }|..|| dim_airlines : "operated by"
    fact_flights }|..|| dim_airports : "departs from"
    fact_flights }|..|| dim_airports : "arrives at"
    fact_flights }|..|| dim_date : "flown on"
```

### Table Details

- **`fact_flights`**: Contains metrics like `total_fare`, `duration_hours`, and keys to dimensions.
- **`dim_airlines`**: Cleanup up list of airline names.
- **`dim_airports`**: Maps IATA codes (e.g., DAC) to full airport names.
- **`dim_date`**: Derived calendar attributes (Year, Month, Weekend flag) for time-series analysis.

---

## 🛠️ Technology Stack

| Component            | Technology                                                                                                               | Description                               |
| :------------------- | :----------------------------------------------------------------------------------------------------------------------- | :---------------------------------------- |
| **Orchestrator**     | ![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=Apache%20Airflow&logoColor=white) | Schedules and monitors the pipeline.      |
| **Language**         | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)                      | Core logic for ETL tasks (Polars/Pandas). |
| **Staging DB**       | ![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=flat-square&logo=mysql&logoColor=white)                         | Intermediate storage for raw data.        |
| **Data Warehouse**   | ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat-square&logo=postgresql&logoColor=white)          | Final storage for analytical tables.      |
| **Containerization** | ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)                      | Ensures consistent environments.          |

---

## 🏁 Getting Started

### Prerequisites

- **Docker Desktop** installed and running.
- **Git** installed.

### Installation

1.  **Clone the Repository**

    ```bash
    git clone https://github.com/your-username/airflow-flight-analysis.git
    cd airflow-flight-analysis
    ```

2.  **Environment Setup**
    Create a `.env` file in the root directory (or rename `.env.example`).

    ```bash
    cp .env.example .env
    ```

    _Ensure `AIRFLOW_UID` is set (usually 50000 on data sources)._

3.  **Launch Services**
    Initialize the database and start Airflow.

    ```bash
    docker-compose up -d --build
    ```

4.  **Access Airflow UI**
    - Wait for the containers to be healthy.
    - Open browser: [http://localhost:8080](http://localhost:8080)
    - **Username/Password**: `airflow` / `airflow`

### Running the Pipeline

1.  In the Airflow UI, find `flight_price_pipeline`.
2.  Toggle the DAG to **ON**.
3.  Click the **Trigger DAG** (Play button) to start a manual run.
4.  Monitor the `Graph` view to watch tasks execute.

---

## 📂 Project Structure

```bash
├── dags/
│   └── flight_pipeline_dag.py    # Main Airflow DAG definition
├── data/
│   └── ...csv                    # Source dataset
├── scripts/
│   ├── init_mysql.sql            # Staging table DDL
│   └── init_postgres.sql         # Analytics schema DDL
├── config/                       # Airflow configurations
├── docker-compose.yaml           # Container services definition
└── README.md                     # Project documentation
```
