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
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/b63d8931-968d-4c8c-9180-a621bdfbf1de)



### Stream Data Flow
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/2e1f8149-5fad-4829-831c-8fac8f55f79d)


### Batch Data Flow
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/a46f268d-b737-4760-a55b-2cd85a8e622a)




## Data Studio Reports

 Batch Report : https://lookerstudio.google.com/reporting/c78e0fb5-197a-4e93-bf68-294a835cc75b
 
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/751b7e6a-a455-46b1-9205-fb5ca558ba82)

 
 Stream Report: https://lookerstudio.google.com/s/gCTWIyrIxAo
 
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/478d80fa-012b-466c-a10d-bf3a66960c30)




 ## DBT Modelling Used 
 
  Mapping of Temporary external tables
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/f41c3e38-5bce-460f-8cfb-8748752ee986)


 ### Batch Model 
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/2ed78289-b05e-4290-9f3d-cbc61a229e26)

 ### Stream Model
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/e9117030-8cc5-402e-8e49-24e53dbae581)



 ## Airflow Orchestration

 ### Stream Pipeline
![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/619e3f21-af3e-4a22-ab25-6862d2740e5f)



 ### Batch Pipeline
 ![image](https://github.com/Nandhinired/NewYork_CitiBikes_Streaming_Batch/assets/69593809/a04a3d9b-c8c8-41e3-910a-3f8f29267f4c)



 
