# US DOT Flights Data Lakehouse

## Overview

A production-ready data lakehouse implementation processing **US Department of Transportation (DOT) On-Time Flight Performance** data using modern data engineering best practices. This project demonstrates a complete end-to-end pipeline with:

- **Medallion Architecture** (Bronze → Silver → Gold)
- **Delta Lake** for ACID transactions and time travel
- **Apache Airflow** for workflow orchestration
- **PySpark** for distributed data processing
- **Azure Data Lake Storage** for cloud storage
- **Great Expectations** for data quality validation
- **Star Schema** modeling for analytics

---

## Architecture

### Medallion Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                              │
│  US DOT BTS On-Time Performance (CSV files)                 │
│  Airport & Carrier Lookup Tables                            │
└─────────────────────────────────┬───────────────────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │    BRONZE LAYER           │
                    │  - Raw data ingestion     │
                    │  - Schema validation      │
                    │  - Metadata addition      │
                    │  - Delta format           │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │    SILVER LAYER           │
                    │  - Data cleaning          │
                    │  - Type conversion        │
                    │  - Business rules         │
                    │  - Enrichment             │
                    │  - Quality scoring        │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │     GOLD LAYER            │
                    │  ┌─────────────────────┐  │
                    │  │  Star Schema        │  │
                    │  │  - Fact: Flights    │  │
                    │  │  - Dim: Date        │  │
                    │  │  - Dim: Time        │  │
                    │  │  - Dim: Airline     │  │
                    │  │  - Dim: Airport     │  │
                    │  │  - Dim: Route       │  │
                    │  └─────────────────────┘  │
                    │  ┌─────────────────────┐  │
                    │  │  Aggregates         │  │
                    │  │  - Daily Airline    │  │
                    │  │  - Daily Airport    │  │
                    │  │  - Route Performance│  │
                    │  └─────────────────────┘  │
                    └───────────────────────────┘
```

### Data Flow

1. **Bronze Ingestion**: Direct load from Azure Data Lake → enriched with lookup tables → validated → saved as Delta
2. **Silver Transformation**: Clean nulls → validate business rules → enrich metrics → quality scoring
3. **Gold Marts**: Build dimensional model + performance aggregates for analytics

## Features

### Data Pipeline
- **Automated ETL**: End-to-end pipeline orchestrated with Airflow
- **Incremental Processing**: Delta Lake merge for efficient updates
- **Schema Evolution**: Automatic handling of schema changes
- **Data Quality Gates**: Great Expectations validation at each layer
- **Error Handling**: Comprehensive error handling and retry logic

### Data Quality
- **Completeness Checks**: Validate required fields
- **Validity Rules**: Airport codes, flight numbers, delay ranges
- **Business Rules**: Time constraints, cancellation logic
- **Quality Scoring**: Per-record data quality metrics

### Analytics Ready
- **Star Schema**: Optimized dimensional model
- **Pre-aggregated Metrics**: Daily airline/airport performance
- **Route Analysis**: Performance by route
- **Time Intelligence**: Date/time dimensions for temporal analysis

### Cloud Native
- **Azure Integration**: Direct read/write to Azure Data Lake
- **Scalable Storage**: Partitioned Delta tables
- **Authentication**: OAuth & Service Principal support

---

## Project Structure

```
us-dot-flights-lakehouse/
├── airflow/                          # Airflow orchestration
│   ├── dags/
│   │   └── lakehouse_etl_pipeline.py # Main ETL DAG
│   ├── docker-compose.yml            # Airflow containers
│   └── Dockerfile.airflow            # Custom Airflow image
│
├── configs/                          # Configuration modules
│   ├── azure_config.py               # Azure authentication & paths
│   ├── data_schema.py                # Data schema definitions
│   └── spark_config.py               # Spark configurations
│
├── spark_jobs/                       # PySpark processing jobs
│   ├── bronze_ingest/
│   │   └── download_and_ingest.py    # Bronze layer ingestion
│   ├── silver_transform/
│   │   ├── flights_silver_transform.py  # Silver transformation
│   │   ├── flight_data_cleaner.py    # Data cleaning logic
│   │   └── flight_data_enricher.py   # Data enrichment
│   ├── gold_marts/
│   │   ├── star_schema/              # Dimensional modeling
│   │   │   ├── build_all_dimensions.py
│   │   │   ├── build_fact_flights.py
│   │   │   └── dimensions/           # Individual dimension builders
│   │   └── aggregates/               # Pre-aggregated metrics
│   │       ├── build_all_aggregates.py
│   │       ├── daily_airline_performance.py
│   │       ├── daily_airport_performance.py
│   │       └── route_performance.py
│   └── validation/
│       ├── data_validator.py         # Great Expectations wrapper
│       ├── validate_bronze.py
│       └── validate_silver.py
│
├── expectations/                     # Data quality expectations
│   ├── bronze_expectations.py
│   ├── silver_expectations.py
│   ├── gold_expectations.py
│   └── great_expectations/           # GE configuration
│
├── notebooks/                        # Jupyter notebooks
│   ├── exploration/
│   │   └── data_exploration.ipynb    # EDA and insights
│   └── validation/
│
├── data/                             # Local data storage
│   ├── bronze/                       # Raw ingested data (Delta)
│   ├── silver/                       # Cleaned data (Delta)
│   ├── gold/                         # Analytics marts (Delta)
│   └── exported_aggregates/          # CSV exports
│
├── docs/
│   └── data_exploration_summary.md   # Data insights
│
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```

---

## Quick Start

### Prerequisites

- **Docker** & **Docker Compose** (for Airflow)
- **Python 3.12+**
- **Azure Storage Account** 

### 1. Clone Repository

```bash
git clone <repository-url>
cd us-dot-flights-lakehouse
```

### 2. Environment Setup

Create a `.env` file in the project root:

```bash
# Azure Configuration (optional - for cloud storage)
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_CONTAINER_NAME=flights-data
AZURE_STORAGE_ACCOUNT_KEY=your_key
# OR use Service Principal
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_secret
```

### 3. Install Python Dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Start Airflow (Optional)

```bash
cd airflow
docker-compose up -d
```

- **Airflow UI**: http://localhost:8081
- **Username/Password**: `admin` / `admin`

### 5. Run Pipeline Locally

```bash
# Bronze: Ingest raw data
python spark_jobs/bronze_ingest/download_and_ingest.py

# Silver: Transform and clean
python spark_jobs/silver_transform/flights_silver_transform.py

# Gold: Build dimensions
python spark_jobs/gold_marts/star_schema/build_all_dimensions.py

# Gold: Build fact table
python spark_jobs/gold_marts/star_schema/build_fact_flights.py

# Gold: Build aggregates
python spark_jobs/gold_marts/aggregates/build_all_aggregates.py
```

### 6. Access Jupyter Notebooks

```bash
cd notebooks/exploration
jupyter notebook data_exploration.ipynb
```

---

## Data Pipeline Stages

### Bronze Layer: Raw Ingestion

**Purpose**: Ingest raw data with minimal transformation

**Process**:
1. Load CSV files from Azure Data Lake
2. Enrich with airport/carrier lookup tables
3. Add metadata (ingestion timestamp, source file)
4. Validate schema
5. Write to Delta format with partitioning

**Output**: `data/bronze/flights/` (partitioned by date)

### Silver Layer: Cleansing & Enrichment

**Purpose**: Clean, validate, and enrich data for analytics

**Transformations**:

1. **Data Cleaning**:
   - Parse datetime fields (HHMM → proper timestamps)
   - Handle missing values (nulls for cancelled flights)
   - Standardize text fields (trim, uppercase codes)
   - Type conversions (double → boolean for flags)

2. **Business Rule Validation**:
   - Departure < Arrival time
   - Air time < 24 hours
   - Reasonable delay ranges (-60 to +300 min)
   - Distance > 0

3. **Enrichment**:
   - Delay categorizations (OnTime, Minor, Moderate, Severe)
   - Performance flags (is_delayed, is_ontime)
   - Route codes (ORIGIN→DEST)
   - Weekend indicators
   - Speed calculations (km/h)
   - Data quality scores per record

**Output**: `data/silver/` (partitioned by date + airline)

**Quality Checks**: 20+ Great Expectations validations

### Gold Layer: Analytics Marts

**Purpose**: Create optimized models for BI/analytics

#### Star Schema

**Fact Table**: `fact_flights`
- Flight-level grain
- Foreign keys to all dimensions
- Performance metrics (delays, cancellations)
- ~540K rows/month

**Dimensions**:
- `dim_date`: Date dimension (year, month, day, day of week, etc.)
- `dim_time`: Time dimension (hour, minute, period of day)
- `dim_airline`: Airline details (code, name)
- `dim_airport`: Airport details (code, name, city)
- `dim_route`: Route information (origin-destination pairs)

**Output**: `data/gold/star_schema/`

#### Pre-Aggregated Metrics

1. **Daily Airline Performance**:
   ```sql
   - Total flights per airline per day
   - Average delays
   - Cancellation rates
   - On-time performance %
   ```

2. **Daily Airport Performance**:
   ```sql
   - Departures/arrivals per airport per day
   - Average delays by airport
   - Busiest airports ranking
   ```

3. **Route Performance**:
   ```sql
   - Top routes by volume
   - Route-specific delays
   - Distance & duration stats
   ```

**Output**: `data/gold/aggregates/`

---

## Data Quality & Validation

### Great Expectations Integration

**Bronze Layer Checks**:
- Schema validation
- Required fields completeness
- Valid airport/airline codes
- Reasonable numeric ranges

**Silver Layer Checks**:
- Type correctness
- Business rule compliance
- Referential integrity
- Quality score thresholds

**Gold Layer Checks**:
- Dimension uniqueness
- Fact table referential integrity
- Aggregate accuracy

### Running Validations

```bash
# Validate all layers
python expectations/run_all_expectations.py

# Individual layer validation
python spark_jobs/validation/validate_bronze.py
python spark_jobs/validation/validate_silver.py
```


---

## Data Sources

- **Primary**: [US DOT BTS On-Time Performance](https://www.transtats.bts.gov/ONTIME/)
- **Lookup Tables**:
  - `L_AIRPORT_ID.csv` - Airport codes and names
  - `L_UNIQUE_CARRIERS.csv` - Airline codes and names

### Data Dictionary

See [data_exploration_summary.md](docs/data_exploration_summary.md) for detailed field descriptions and insights.