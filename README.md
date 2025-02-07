# **Final Project - Data Engineering Course**

This repository contains all the components required for the final project of the Data Engineering course - Group 1. The project's goal is to build a real-time and batch data pipeline for supporting a dashboard displaying live positions and metrics for Carris buses in Lisbon, while also enriching the data with additional sources.

---

## **Overview**
![Untitled-2023-10-06-1235](https://github.com/user-attachments/assets/cd2313bc-1c6b-4e8a-babe-ca7eb581a9a1)


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

## **Project Steps**
1. **Define Dashboard**
   - Establish metrics and create mockups for the dashboard.
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
         - Converter para parquet - **WIP**
      - Escrever GCP - **done**
      - Criar tabelas BigQuery 

---


## **Technical Instructions**

## 1 - Data Ingestion

To be able to fetch all Carris API data, three different Ingestion scripts were developed.
 - 1_0_Raw_Ingestion.py: that's responsible to get API data from the following endpoints:
    ."alerts": "https://api.carrismetropolitana.pt/alerts",
    ."municipalities": "https://api.carrismetropolitana.pt/municipalities",
    ."stops": "https://api.carrismetropolitana.pt/stops",
    ."lines": "https://api.carrismetropolitana.pt/lines",
    ."routes": "https://api.carrismetropolitana.pt/routes",
    ."encm": "https://api.carrismetropolitana.pt/datasets/facilities/encm",
    ."schools": "https://api.carrismetropolitana.pt/datasets/facilities/schools",
    ."GTFS": "https://api.carrismetropolitana.pt/gtfs" (.txt files)
- 1_1_Raw_Ingestion_Patterns.py: fetch only patterns data from the pattern_id's available in the GTFS-trips.txt file
- 1_2_Raw_Ingestion_Shapes.py: fetch only shapes data from the shape_id's available in the GTFS-trips.txt file
- 1_3_Raw_Ingestion_Enrichment.py: fetch data from additional enrichment API's available in the Enrichment folder as json files.
- 1_4_Raw_Ingestion_Convert.py: Transform all LandingZone files in a tabular form as .parquet files stored in the LandingZone_Converted folder.

All API responses are stored in JSON and txt files in the edit-data-eng-project-group1/LandingZone folder.


### 1.1 - Data Ingestion Aux

Useful commands for Windows/WSL users:

### **Make sure it's all done in the same venv (using WSL)**

### **Create virtual environment: python3 -m venv venv**
    - Activate venv: source .venv/bin/activate

### **Execute script in WSl using python ReadAPI.py**

### **Install Google Cloud client package**
    - pip install google-cloud-storage

### **Install in WSL**
    - sudo snap install google-cloud-cli --classic
    - gcloud init

### **Command to GCP login**
    - gcloud auth application-default login


## 2 - Data Load (BigQuery)



## 3 - Data Transformation & Load (dbt)

Use as reference ./dbt/README.md for dbt Docker setup.
Runs the dbt model using the DAG "xxx" triggering GCP Run Cloud Job "zzz".


### 3.1 - Staging Layer

Layer with raw data transformation, connecting directely to the BigQuery source tables.
Include reference to dbt model documentation.


### 3.2 - Marts Layer

Layer with Detailed Data Model, with all the Dimension tables and only Fact_Vehicles_Trips_Det.
Uses as source tables references to the Staging Layer (e.g. DIM_Stops -> Select <fields> from ref{stg_Stops}).
Include reference to dbt model documentation.


### 3.3 - Reporting Layer

Layer with Aggregated Data Model, with all Dimension tables (except DIM_Stops and DIM_Weather) and only Fact_Vehicles_Trips_Agg.
Uses as source tables references to the Marts Layer (e.g. Fact_Vehicles_Trips_Agg -> Select <fields> from ref{Fact_Vehicles_Trips_Det}).
Include reference to dbt model documentation.


## 4 - Orchestration

Using Airflow DAG's scheduled to run Daily at 1AM everyday.
DAG's:
 - "yyyy.py": brief description

=======
## **Group 1**
- João
- Ana
- Diogo Martins
- Vânia

