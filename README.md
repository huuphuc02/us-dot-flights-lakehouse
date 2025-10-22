# US DOT Flights Data Lakehouse

## Overview
End-to-end data pipeline processing US DOT On-Time Flights data using Delta Lake, Apache Airflow, and Azure Databricks.

## Architecture
- **Bronze**: Raw flight data ingestion
- **Silver**: Cleaned and enriched data
- **Gold**: Analytics-ready marts

## Quick Start
```bash
docker-compose up -d
# Access Airflow: http://localhost:8080
# Access Jupyter: http://localhost:8888
```

## Data Sources
- US DOT BTS On-Time Performance: https://www.transtats.bts.gov/ONTIME/