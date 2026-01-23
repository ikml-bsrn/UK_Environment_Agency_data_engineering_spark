# UK Flood-Monitoring Data Engineering using Apache Spark (Component 2)

<img width="351" height="143" alt="environment_agency_logo" src="https://github.com/user-attachments/assets/c8b331fa-7ed9-4a1d-b82e-0d0d43435be6" />

This component focuses on building a modular data engineering pipeline for **processing raw flood-monitoring data with a complex, deeply nested JSON format**.

## Prerequesites

- Java SDK 21
- Python 3.19.3
- PySpark

## Methodology

**Figure 1**

_Data Engineering Architecture using PySpark_

<p align="center">
<img width="1178" height="482" alt="PySpark Pipeline" src="https://github.com/user-attachments/assets/bc1be703-eea3-4e5d-90f2-a1efeb6f70a1" />
</p>

Firstly, a SparkSession is initialised to ingest the JSON file into a Spark DataFrame. As illustrated in **Figure 2** below, the actual data exists in the ‘items’ array, whereas other sibling fields contain API metadata. To isolate the relevant records, I employed the PySpark explode() function to flatten the ‘items’ array, converting each element of the array into distinct rows in the DataFrame. This approach effectively normalises the data for further processing.

**Figure 2**

_Spark DataFrame Schema of a Measurements JSON File_

<p align="center">
<img width="466" height="574" alt="Spark Schema of the Raw JSON File" src="https://github.com/user-attachments/assets/0cc11fff-d5d7-4a67-80f1-62b978963bdb" />
</p>

Despite that, the initial flattening process reveals that ‘latestReading’ field **remains nested as a ‘StructType’** (as seen in **Figure 2**). To strictly normalise the data, I extracted the field (alongside its sub-fields) into a separate Spark DataFrame, establishing a relationship via the ‘measure’ key (later aliased as ‘measurementId’). This separation prepares the data for gold level and supports the Star Schema in **Figure 3**.

Subsequently, the pipeline executes required **transformation and cleaning processes**, including deduplication and null handling. As shown in **Figure 1**, a **custom validation function checks for malformed data**, such as list objects appearing in float columns, and **separates them into a quarantine DataFrame**. This implementation adheres to Data Engineering best practices as it maintains the data integrity of the DataFrame (silver layer) without discarding potentially valuable data. Additionally, it enables us to conduct further inspection and logic refinement. 

Lastly, **Figure 3** below illustrates the database schema which represents the gold layer of the medallion architecture. This layer enables analysts to query high-quality, structured data using simplified SQL joins to generate operational insights, such as retrieving past 10 measurement readings for a station in a specific town. 

 
**Figure 3** 

_Flood Monitoring Database Star Schema_

<img width="1006" height="448" alt="Database Schema" src="https://github.com/user-attachments/assets/5c0d5e3f-280e-4bab-a1dd-a2ea0330247a" />

## Analytical Demonstration using Spark SQL

The query below retrieves the latest readings from all measurements across all stations from the defined temporary views in PySpark.

<p align="center">
<img width="700" height="660" alt="spark sql demonstration" src="https://github.com/user-attachments/assets/19781813-5d37-4ca6-9a77-5dea03c1621a" />
</p>

## Setup

### Step 1: Clone the Repository

Run the following command to clone this repository.

        git clone https://github.com/ikml-bsrn/UK_Environment_Agency_data_engineering_spark.git

### Step 1: Install dependencies: 

Install the dependencies using the command below.

        pip install -r requirements.txt


## Execution

### Step 1. Walkthrough: 

Use the **pyspark_development.ipynb** Jupyter notebook to follow the step-by-step development process, from raw data ingestion to the final analytical demonstration.

### Step 2. Run the pipeline: 

To execute the full data pipeline, run the main script from your terminal:

    python main.py

Note: You should see progress bars indicating the processing of files and warnings for any malformed data being quarantined (as seen in the screenshot below).

<img width="676" height="292" alt="image" src="https://github.com/user-attachments/assets/4599bd83-2189-40a9-a73c-ec5755090782" />

