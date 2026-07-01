# iot-sensor-data-pipeline
## Project Overview
This project implements an end-to-end IoT data engineering pipeline using Python, PostgreSQL, Apache Airflow, Supabase, and Metabase. The pipeline ingests industrial IoT sensor data, transforms it using the Medallion Architecture (Bronze, Silver, Gold), orchestrates workflows using Apache Airflow, replicates analytical data to Supabase, and visualizes insights using Metabase dashboards.
# Problem Statement
Industrial IoT environments generate large volumes of sensor data continuously. Raw sensor data is difficult to analyze directly due to its size, noise, and lack of business context. The goal of this project is to design a scalable ELT pipeline that transforms raw sensor readings into business-ready analytical datasets for monitoring equipment health and operational performance.
