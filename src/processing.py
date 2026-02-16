def separate_malformed_data(df):
    """
    This function separates malformed data from a DataFrame based on a specific condition.
    
    Args:
        df: Spark DataFrame to be checked for malformed data.
        
    Returns:
        tuple: A tuple containing two DataFrames (clean_data_df, malformed_data_df)
    """
    from pyspark.sql.functions import col

    # Conditions configuration to identify malformed data
    is_malformed = None # Set to None
    for col_name in df.columns:
        condition = col(col_name).contains("[") # Check if the column contains '[' (indicating a string-list)
        is_malformed = condition if is_malformed is None else is_malformed | condition # Combine conditions with OR

    quarantine_df = df.filter(is_malformed)
    clean_df = df.filter(~is_malformed)

    return clean_df, quarantine_df
    
def transform_measurements_to_silver(
        spark, 
        raw_data_path, 
        clean_measurements_output_path,
        clean_readings_output_path,
        quarantine_measurements_output_path, 
        quarantine_readings_output_path
        ):   
    """
    This function reads the measurements JSON files from the specified folder path, processes them, and writes the output as Parquet files to the specified output path.
    
    Args:
        spark: SparkSession object
        raw_data_path (str): The path to the folder containing the raw JSON measurement files.

        clean_measurements_output_path (str): The path where the processed measurements Parquet files will be saved.
        clean_readings_output_path (str): The path where the processed readings Parquet files will be saved.
        
        quarantine_measurements_output_path (str): The path where the quarantined, malformed measurements Parquet files will be saved.
        quarantine_readings_output_path (str): The path where the quarantined, malformed readings Parquet files will be saved.
    """
    import os
    import logging
    from tqdm import tqdm

    from pyspark.sql.functions import col, explode, regexp_extract

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Iterate over JSON files in the specified file path
    for json_file in tqdm(os.listdir(raw_data_path)):
        try:
            
            # Check if the file is a JSON file
            if json_file.endswith('.json'):
                json_file_path = os.path.join(raw_data_path, json_file)
            else:
                logger.warning(f"Non-JSON file found! Skipping {json_file} ...")
                continue
                
            # Read the JSON file into a DataFrame
            df = spark.read.format('json').load(json_file_path)

            # --- Flatten the nested JSON structure ---
            # 1. Normalise the 'items' array and ignore 'context' and 'metadata' fields
            # 2. Normalise the measurements and readings df to separate Spark DataFrames
            
            measurements_df = df.select(explode("items").alias("item")) \
                .select(
                    col("item.@id").alias("measurementId"),
                    col("item.stationReference"),
                    col("item.parameterName").alias("parameter"),
                    col("item.period"),
                    col("item.qualifier"),
                    col("item.valueType"),
                    col("item.unitName")                    
                    )

            # Normalise latestReading struct to a separate Spark DataFrame
            readings_df = df.select(explode("items").alias("item")) \
                .select(
                    col("item.latestReading.@id").alias("readingId"),
                    col("item.latestReading.dateTime").alias("readingDatetime"),
                    col("item.latestReading.value").alias("readingValue"),
                    col("item.latestReading.measure").alias("measurementId")
                    )

            # 3. Clean data: handling duplicates, missing or malformed data
            measurements_df = measurements_df.dropDuplicates()
            readings_df = readings_df.dropDuplicates()

            # Note: We do not drop rows with null readingValue to retain all measurement records

            # Handle malformed data, and separate them into quarantine DataFrames
            measurements_df, quarantined_measurements_df = separate_malformed_data(measurements_df)
            readings_df, quarantined_readings_df = separate_malformed_data(readings_df)

            # 4. Data transformation
            # Extract IDs from the URL fields 

            measurements_df = measurements_df.withColumn(
                "measurementId", regexp_extract(col("measurementId"), r'measures/(.+)$', 1) # extract the measures ID from the full URL
            )
            readings_df = readings_df.withColumn(
                "readingId", regexp_extract(col("readingId"), r'readings/(.+)$', 1) # extract the readings ID from the full URL
            ).withColumn(
                "measurementId", regexp_extract(col("measurementId"), r'measures/(.+)$', 1) # extract the measures ID from the full URL
            )

            # Once malformed values are separated, cast data types appropriately
            readings_df = readings_df.withColumn(
                "readingDatetime", col("readingDatetime").cast("timestamp")
                ).withColumn(
                "readingValue", col("readingValue").cast("float")
                )

            # 4. Write the flattened, clean DataFrames to Parquet format (overwrite if existed)
            measurements_df.write.mode('overwrite').parquet(
                os.path.join(clean_measurements_output_path, json_file.replace('.json', '.parquet'))
                )
            readings_df.write.mode('overwrite').parquet(
                os.path.join(clean_readings_output_path, json_file.replace('.json', '.parquet'))
                )

            # 5. Check if quarantined DataFrames are not empty, if so, write them to quarantine paths
            if quarantined_measurements_df.count() > 0:
                logger.warning(f"Malformed data found in measurements for file {json_file}. Quarantining {quarantined_measurements_df.count()} records.")

                quarantined_measurements_df.write.mode('append').json(
                    os.path.join(quarantine_measurements_output_path, json_file.replace('.json', '_quarantined_measurements.json'))
                    )
            if quarantined_readings_df.count() > 0:
                logger.warning(f"Malformed data found in readings for file {json_file}. Quarantining {quarantined_readings_df.count()} records.")

                quarantined_readings_df.write.mode('append').json(
                    os.path.join(quarantine_readings_output_path, json_file.replace('.json', '_quarantined_readings.json'))
                    )
        
        except Exception as e:
            logger.error(f"Error processing file {json_file}: {e}")
            continue

def transform_station_to_silver(
        spark, 
        raw_data_path, 
        clean_station_output_path,
        quarantine_station_output_path
        ):
    """
    This function reads the station JSON files from the specified folder path, processes them, and writes the output as Parquet files to the specified output path.
    
    Args:
        spark: SparkSession object
        raw_data_path (str): The path to the folder containing the raw JSON station files.
        clean_station_output_path (str): The path where the processed station Parquet files will be saved.
        quarantine_station_output_path (str): The path where the quarantined, malformed station Parquet files will be saved.
    """
    import os
    import logging
    from tqdm import tqdm

    from pyspark.sql.functions import col, explode, regexp_extract

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Iterate over JSON files in the specified file path
    for json_file in tqdm(os.listdir(raw_data_path)):
        try:
            # Check if the file is a JSON file
            if json_file.endswith('.json'):
                json_file_path = os.path.join(raw_data_path, json_file)
            else:
                logger.warning(f"Non-JSON file found! Skipping {json_file} ...")
                continue
                
            # Read the JSON file into a DataFrame
            # logger.info(f" Processing file: {json_file}")
            stations_df = spark.read.format('json').load(json_file_path)

            # --- Flatten the nested JSON structure ---
            # 1. Select the 'items' array and ignore 'context' and 'metadata' fields
            flattened_station_df = stations_df.select(explode("items").alias("item")) \
                .select(
                    col("item.stationReference"),
                    col("item.label").alias("stationLabel"),
                    col("item.status"),
                    col("item.town"),
                    col("item.dateOpened"),
                    col("item.catchmentName"),
                    col("item.riverName"),
                    col("item.lat"),
                    col("item.long")
                    )
            
            # 2. Clean data: handling duplicates, missing or malformed data
            flattened_station_df = flattened_station_df.dropDuplicates()

            # Note: We do not automatically drop rows with null values to retain all station records
    
            # Handle malformed data, and separate them into quarantine DataFrames
            cleaned_station_df, quarantined_stations_df = separate_malformed_data(flattened_station_df)          

            # 3. Data transformation
            # Extract status from URL
            transformed_stations_df = cleaned_station_df.withColumn(
                "status", regexp_extract(col("status"), r'status([a-zA-Z]+)$', 1) # use PySpark regexp_extract function
            )

            # Cast data types appropriately
            transformed_stations_df = transformed_stations_df.withColumn(
                "dateOpened", col("dateOpened").cast("date")
                ).withColumn(
                "lat", col("lat").cast("float")
                ).withColumn(
                "long", col("long").cast("float")
                )

            # 3. Write the flattened, cleaned DataFrame to Parquet format
            transformed_stations_df.write.mode('overwrite').parquet(os.path.join(clean_station_output_path, json_file.replace('.json', '.parquet')))
            
            # 4. Check if quarantined DataFrame is not empty, if so, write it to quarantine path
            if quarantined_stations_df.count() > 0:
                logger.warning(f"Malformed data found in stations for file {json_file}. Quarantining {quarantined_stations_df.count()} records.")

                quarantined_stations_df.write.mode('append').json(
                    os.path.join(quarantine_station_output_path, json_file.replace('.json', '_quarantined_stations.json'))
                    )

        except Exception as e:
            logger.error(f"Error processing file {json_file}: {e}")
            continue

