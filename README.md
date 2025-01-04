# final-project-edit
Final Project of Data Engineering Course 

![image](https://github.com/user-attachments/assets/889b0fb6-0e4d-4a2a-8b3a-17de9fff9204)

Bucket de Google Cloud Storage edit-de-project-streaming-data: https://console.cloud.google.com/storage/browser/edit-de-project-streaming-data;tab=objects?forceOnBucketsSortingFiltering=true&authuser=4&inv=1&invt=Abl2rw&project=data-eng-dev-437916&prefix=&forceOnObjectsSortingFiltering=false

Bucket de DAGs edit-de-project-airflow-dags : https://console.cloud.google.com/storage/browser/edit-de-project-airflow-dags;tab=objects?forceOnBucketsSortingFiltering=true&authuser=4&inv=1&invt=Abl2rw&project=data-eng-dev-437916&prefix=&forceOnObjectsSortingFiltering=false

Bucket producer edit-de-vm-mount :https://console.cloud.google.com/storage/browser/edit-de-vm-mount;tab=objects?authuser=4&inv=1&invt=Abl2rw&project=data-eng-dev-437916&prefix=&forceOnObjectsSortingFiltering=false

Examples Spark Streaming: https://github.com/vaniamv/dataprocessing/tree/main/spark_streaming

Sources:
API Carris - https://github.com/joaodcp/Carris-API
Dados CM Lisboa - https://dados.cm-lisboa.pt/dataset
Weather API - https://openweathermap.org/

Steps:
- Defining dashboard (metrics, first draft, etc)
- Dimensioning model - sources (dates, carris, weather, etc)
- Read from 24h GCP bucket 


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
- 1_1_Raw_Ingestion_Patterns.py: fetchs only patterns data from the pattern_id's available in the GTFS-trips.txt file
- 1_2_Raw_Ingestion_Shapes.py: fetchs only shapes data from the shape_id's available in the GTFS-trips.txt file

All API responses are stored in JSON and txt files in the edit-data-eng-project-group1/LandingZone folder.


### 1.1 - Data Ingestion Aux

Useful commands for Windows/WSL users:

# Make sure it's all done in the same venv (using WSL)

# Create virtual environment: python3 -m venv venv
    - Activate venv: source .venv/bin/activate

# Execute script in WSl using python ReadAPI.py

# Install Google Cloud client package
    - pip install google-cloud-storage

# Install in WSL
    - sudo snap install google-cloud-cli --classic
    - gcloud init

# Command to GCP login
    - gcloud auth application-default login
