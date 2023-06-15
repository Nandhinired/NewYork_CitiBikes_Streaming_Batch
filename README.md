# NewYork_CitiBikes_Streaming_Batch
A data pipeline comprising Apache Nifi, docker, Apache Kafka, Spark Structured streaming, GCP.

## Description
The pipeline consumes real-time data from Citibikes using Apache NiFi, pushes it into Kafka, processes the data using Spark Structured Streaming,
writes the processed data into partitions in a data lake - Google Cloud Storage (GCS) bucket, creates models and tables in BigQuery using dbt, and generates reports in Looker Studio for tracking the BikesAvailability ratio per station.

## Pipeline Overview
The pipeline consists of the following components:

### Data Ingestion: 
Apache NiFi is used to consume real-time data from Citibikes GBFS feed(http://gbfs.citibikenyc.com/gbfs/gbfs.json) and pushing it into Kafka topic.

### Data Processing:
Spark Structured Streaming is used to process the data received from Kafka in real-time. The processing logic can be found in the Spark code.

### Data Storage: 
The processed data is written into partitions in a GCS bucket. Each partition represents a specific time period .

### Data Modeling: 
dbt (data build tool) is used to create models and tables in BigQuery based on the processed data. This step involves transforming and structuring the data for
efficient querying and analysis.

### Orchestration: 
Airflow scripts will run hourly basis to create external tables, those external tables would be acting as source for the dbt models, once the dbt scripts are executed, the external tables are deleted.

### Reporting: 
Looker Studio is used to create reports and visualizations based on the data in BigQuery. Reports are generated every hour to provide up-to-date insights.


## Pipeline
![Pipeline](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/f6628a07-3037-4e4b-92a0-605df370ddcc)


### Stream Data Flow
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/06405140-6fdd-4c0e-8ead-97705ad13398)




## Data Studio Reports

 Batch Report : https://lookerstudio.google.com/reporting/c78e0fb5-197a-4e93-bf68-294a835cc75b
 
 ![image](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/387d3b04-a14d-46e3-a134-cbdc144c4e66)
 
 Stream Report: https://lookerstudio.google.com/s/gCTWIyrIxAo
 
 ![image](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/0a87e15c-d8a0-481c-bf44-c83aff3aef20)



 ## DBT Modelling Used 
 
  Mapping of Temporary external tables
 ![external_table_dbt_model](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/d638bada-7276-438f-8d0d-c2a74fdf06f0)

 Batch Model 
 ![image](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/72b04d07-91a0-42e4-a957-6a4101ec6eb5)
 Stream Model
 ![Stream_dbt_model](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/7e4f93ed-7d53-4a2e-9d52-248c108e413a)


 ## Airflow Orchestration

 ### Stream Pipeline
 ![image](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/18a03386-d377-499c-8a1c-997731b3248e)


 ### Batch Pipeline
 ![image](https://github.com/Nandhinired/NewYork_CitiBikes_Batch_Streaming/assets/69593809/2fba67f2-fd01-4b6f-83c0-cac6fc2a7e69)


 
