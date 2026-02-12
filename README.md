# Global_Aviation_Safety_and_Delay_Engine
A pipeline that ingests flight data and weather conditions to predict landing delays.

# Project: Cloud-Native Aviation Data Intelligence
End-to-End Azure Data Engineering & Predictive Analytics

# Project Overview
This project demonstrates a production-grade Medallion Architecture using the Azure ecosystem. It automates the ingestion, transformation, and predictive modeling of global flight data to forecast arrival delays based on real-time weather conditions.
Instead of a static script, this is a fully orchestrated system bridging the gap between raw data ingestion and "Model-as-a-Service" deployment.

# The Tech Stack
Orchestration: Azure Data Factory (ADF)
Processing: Databricks (PySpark / Spark SQL)
Storage: Azure Data Lake Storage (ADLS) Gen2 (Delta Lake format)
Environment: Docker (Local Dev & Model Serving)
Data Science: Scikit-Learn & MLflow
Security: Azure Key Vault (Secret Management)

# System Architecture
Ingestion (Bronze): ADF triggers a Python-based ingestion (packaged in Docker) to pull flight data from the OpenSky API into raw JSON format.
Transformation (Silver): A Databricks Spark job cleans the data, enforces schemas, and joins flight records with weather telemetry.
Aggregation (Gold): Cleaned data is aggregated into high-performance Delta Tables optimized for machine learning.
Intelligence: A Random Forest model is trained on Databricks to predict delays, tracked via MLflow, and deployed as a containerized FastAPI service.

# Engineering Highlights (The "Portfolio Flex")
Containerization: Used Docker to ensure the local development environment perfectly matches the cloud production environment.
ACID Transactions: Implemented Delta Lake to allow for time-travel, data versioning, and reliable upserts.
Scalability: Leveraged Databricks clusters to handle distributed PySpark processing, allowing the pipeline to scale to millions of rows.
CI/CD Ready: The repository includes JSON templates for ADF and Dockerfiles for the model API, making the entire stack reproducible.

# Key Outcomes
Automated Pipeline: Reduced manual data processing time by 100% through ADF scheduling.
Data Reliability: Implemented schema validation in the Silver layer to catch upstream API changes.
Production Deployment: Delivered a containerized ML model ready for integration with front-end applications.
