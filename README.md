# Instructions for Software Component 2

## Prerequesites

- Java SDK 21
- Python 3.19.3
- PySpark

## Setup

### Step 1: Install dependencies: 

Run the following command to insall PySpark.

        pip install -r requirements.txt

### Step 2: Check Raw Data Directory

Ensure your raw JSON files are placed in the "raw_data/" directory.

### Step 3: Check Python modules

Ensure the process_json_files.py file is located in the "modules/" directory, whereas the main.py file is in the root directory.

## Execution

### Step 1. Walkthrough: 

Use the **pyspark_development.ipynb** Jupyter notebook to follow the step-by-step development process, from raw data ingestion to the final analytical demonstration.

### Step 2. Run the pipeline: 

To execute the full data pipeline, run the main script from your terminal:

    python main.py

Note: You should see progress bars indicating the processing of files and warnings for any malformed data being quarantined (as seen in the screenshot below).

![alt text](image.png)