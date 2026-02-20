# Real-Time Aviation Analytics: Medallion Architecture on Azure - Docker - Airflow
A robust end-to-end data engineering pipeline that ingests live flight data from the OpenSky API, processes it through a Medallion Architecture (Bronze, Silver, Gold) using PySpark, and visualizes global flight trends in Power BI.

# Architecture Overview
This project demonstrates a hybrid-cloud approach, leveraging Docker for local orchestration and Azure for scalable data processing and storage.

- Orchestration: Apache Airflow (running in Docker)
- Ingestion: Azure Data Factory (ADF)
- Security: Azure Key Vault (Service Principal authentication)
- Storage: Azure Data Lake Storage Gen2 (ADLS)
- Compute: PySpark (Azure/Local)
- Visualization: Power BI

# Data Flow (The Medallion Journey)
Bronze Layer: Raw Ingestion
    Process: ADF triggers an API call to OpenSky every 60 minutes.
    State: Raw JSON data is landed "as-is" to ensure a source of truth for potential reprocessing.

Silver Layer: Data Cleaning
    Process: PySpark jobs perform schema enforcement and data quality checks.
    Transformations: Renaming columns (e.g., lat/long to latitude/longitude), type casting, and filtering null coordinates.

Gold Layer: Business Analytics
    Process: Advanced analytical transformations for downstream reporting.
    Metrics: * Flight Phase Detection: Using Spark Window functions to determine if a plane is Climbing, Descending, or Cruising.
    Geofencing: Calculating distance-from-airport metrics to identify proximity status (e.g., Landing/Taking Off vs Passing Over).

# Key Features & Challenges
Dynamic Security: Implemented Azure Key Vault integration to securely manage secrets, ensuring no credentials are hardcoded in the PySpark scripts.

Window Logic: Solved complex state-tracking challenges using F.lag() to monitor altitude changes over time per aircraft.

Automated Scheduling: Configured an Airflow DAG with an end_date and catchup=False to manage Azure costs while maintaining a consistent data flow.

# Architecture diagram
![Architecture Diagram](assets/Process_flow.png)
