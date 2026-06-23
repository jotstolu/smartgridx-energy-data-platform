# SmartGridX Energy Data Platform

SmartGridX is a production-style data engineering project that simulates a modern energy company lakehouse platform.

The project uses:

- Apache Airflow for orchestration
- Databricks and PySpark for scalable processing
- Delta Lake for Bronze, Silver and Gold storage
- dbt for analytics engineering
- GitHub Actions for CI/CD
- Python for synthetic data generation
- Automated tests and data quality checks

## Architecture

The project follows a medallion architecture:

- Bronze: raw ingested data
- Silver: cleaned and validated data
- Gold: business-ready facts, dimensions and marts