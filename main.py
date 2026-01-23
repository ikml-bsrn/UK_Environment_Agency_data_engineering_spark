from modules.process_json_files import *

from pyspark.sql.session import SparkSession
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

    # Create Spark session
    logger.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("FlattenJSONFiles")\
        .master("local[*]")\
        .getOrCreate()

    # Process measurement files
    logger.info("Processing and cleaning JSON measurement files...")
    transform_measurements_to_silver(
        spark, 
        raw_data_path="raw_data/measurements_data", 
        clean_measurements_output_path="processed_data/measurements_data",
        clean_readings_output_path="processed_data/readings_data",
        quarantine_measurements_output_path="quarantine_data/measurements_data",
        quarantine_readings_output_path="quarantine_data/readings_data"
        )
    
    # Process station files
    logger.info("Processing and cleaning JSON station files...")
    transform_station_to_silver(
        spark, 
        raw_data_path="raw_data/station_data",
        clean_station_output_path="processed_data/station_data",
        quarantine_station_output_path="quarantine_data/station_data"
        )

    logger.info("Data processing complete.")

    # Stop Spark session
    logger.info("Stopping Spark session...")
    spark.stop()

if __name__ == "__main__":
    main()