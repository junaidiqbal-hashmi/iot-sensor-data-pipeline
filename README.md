# iot-sensor-data-pipeline
## Project Overview
This project implements an end-to-end IoT data engineering pipeline using Python, PostgreSQL, Apache Airflow, Supabase, and Metabase. The pipeline ingests industrial IoT sensor data, transforms it using the Medallion Architecture (Bronze, Silver, Gold), orchestrates workflows using Apache Airflow, replicates analytical data to Supabase, and visualizes insights using Metabase dashboards.
## Problem Statement
Industrial IoT environments generate large volumes of sensor data continuously. Raw sensor data is difficult to analyze directly due to its size, noise, and lack of business context. The goal of this project is to design a scalable ELT pipeline that transforms raw sensor readings into business-ready analytical datasets for monitoring equipment health and operational performance.
## Project Objectives
- Build an end-to-end ELT pipeline
- Implement Medallion Architecture
- Orchestrate transformations with Apache Airflow
- Replicate analytical data to cloud storage
- Visualize business metrics using Metabase
- Containerize the entire platform using Docker
## Project Architecture Overview
![Architecture](images/architecture_diagram.png)
## Data Flow
```
Sensor Logs 
    ↓
Python Ingestion
    ↓
PostgreSQL Bronze
    ↓
Apache Airflow
    ↓
Silver Layer
    ↓
Gold Layer
    ↓
Supabase
    ↓
Metabase Dashboard
```
## Technology Stack Used
**Programming:** Python
**Database:** PostgreSQL
**Workflow:** Apache Airflow
**Cloud Database:** Supabase
**Visualization:** Metabase
**Containerization:** Docker
**Package Manager:** UV
**Data Processing:** Pandas
**SQL Engine:** PostgreSQL SQL