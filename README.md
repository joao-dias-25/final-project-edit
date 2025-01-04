# **Final Project - Data Engineering Course**

This repository contains all the components required for the final project of the Data Engineering course - Group 1. The project's goal is to build a real-time and batch data pipeline for supporting a dashboard displaying live positions and metrics for Carris buses in Lisbon, while also enriching the data with additional sources.

---

## **Overview**
![image](https://github.com/user-attachments/assets/889b0fb6-0e4d-4a2a-8b3a-17de9fff9204)

---

### **Key Components**
- **Real-Time Streaming Pipeline**: Processes live bus position data using Spark Structured Streaming.
- **Batch Pipeline**: Orchestrates data extraction, transformation, and loading (ETL) with Airflow.
- **Dimensional Model**: Builds a star schema in BigQuery for dashboard metrics using dbt.
- **Dashboard Metrics**: Defined based on real-time and historical data for analytical insights.

---
## **Google Cloud Storage Buckets**

1. **Streaming Data Bucket**:  
   Bucket containing JSON files for streaming data (updated every 30 seconds).  
   [Access here](https://console.cloud.google.com/storage/browser/edit-de-project-streaming-data;tab=objects?forceOnBucketsSortingFiltering=true&authuser=4&inv=1&invt=Abl2rw&project=data-eng-dev-437916&prefix=&forceOnObjectsSortingFiltering=false)

2. **Airflow DAGs**:  
   Bucket for storing DAGs and orchestration scripts.  
   [Access here](https://console.cloud.google.com/storage/browser/edit-de-project-airflow-dags;tab=objects?forceOnBucketsSortingFiltering=true&authuser=4&inv=1&invt=Abl2rw&project=data-eng-dev-437916&prefix=&forceOnObjectsSortingFiltering=false)

   Airflow interface.
   [Access here](http://edit-data-eng.duckdns.org/home)

3. **Producer Bucket**:  
   Bucket for additional data processing or temporary storage.  
   [Access here](https://console.cloud.google.com/storage/browser/edit-de-vm-mount;tab=objects?authuser=4&inv=1&invt=Abl2rw&project=data-eng-dev-437916&prefix=&forceOnObjectsSortingFiltering=false)

---

## **References**

- **Carris API**: [https://github.com/joaodcp/Carris-API](https://github.com/joaodcp/Carris-API)
- **Open Data - CM Lisboa**: [https://dados.cm-lisboa.pt/dataset](https://dados.cm-lisboa.pt/dataset)
- **Weather API**: [https://openweathermap.org/](https://openweathermap.org/)
- **Spark Streaming Examples**: [https://github.com/vaniamv/dataprocessing/tree/main/spark_streaming](https://github.com/vaniamv/dataprocessing/tree/main/spark_streaming)

---

## **Steps**
1. **Define Dashboard**
   - Establish metrics and create mockups for the dashboard..
2. **Dimensional Model**
   - Design the schema and identify data sources (Carris API, weather, etc.).
3. **Pipeline Development**
   - Implement Spark Streaming to process data from the GCP bucket containing 24-hour JSON data.
      - Ler GCP - **done**
      - Transforma json to parquet (?)
      - Escrever GCP - **WIP**
4. **Batch Processing**
   - Develop Airflow DAGs for batch processing and upload processed data to BigQuery.
      - Ler API - **done**
      - Endpoint Carris
         - Upload GCP - **done**
         - Converter txt para Json - **WIP**
      - Escrever GCP - **done**
      - Criar tabelas BigQuery 

---

## **Group 1**
- João
- Ana
- Diogo Martins
- Vânia
