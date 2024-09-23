# scripts/etl.py

import gc
import time
from dotenv import load_dotenv
import os
import sys
import logging
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.utils import AnalysisException
from .utils import broadcast_country_continent_mapping
from .utils import udf_get_continent
import shutil

# Retrieve environment variables
from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_OUTPUT_PATH,
    API_URL,
    SPARK_APP_NAME
)

# Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the current Python executable
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print(f"PYSPARK_PYTHON is set to: {os.environ['PYSPARK_PYTHON']}")
print(f"PYSPARK_DRIVER_PYTHON is set to: {os.environ['PYSPARK_DRIVER_PYTHON']}")
print(f"S3_OUTPUT_PATH: {S3_OUTPUT_PATH}, API_URL: {API_URL}, SPARK_APP_NAME: {SPARK_APP_NAME}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for detailed logs
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl.log")
    ]
)

logger = logging.getLogger(__name__)

def cleanup_spark_temp_dir(spark):
    temp_dir = spark.conf.get("spark.local.dir")
    try:
        time.sleep(5)  # Wait for 5 seconds before attempting cleanup
        shutil.rmtree(temp_dir)
        logger.info(f"Successfully deleted {temp_dir}")
    except Exception as e:
        logger.warning(f"Failed to delete {temp_dir}: {e}")

def main():
    try:
        # Validate environment variables
        if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_OUTPUT_PATH, API_URL, SPARK_APP_NAME]):
            logger.error("One or more environment variables are missing. Please check your .env file.")
            sys.exit(1)

        # # Initialize Spark Session
        # spark = SparkSession.builder \
        #     .appName(SPARK_APP_NAME) \
        #     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        #     .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        #     .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        #     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        #     .getOrCreate()

        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .config("spark.local.dir", "C:/Users/zaalg/Documents") \
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
            .config("spark.cleaner.periodicGC.interval", "1min") \
            .config("spark.cleaner.maxAttempts", 20) \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.cores.max", "4") \
            .getOrCreate()
        
        # spark.conf.set("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")

        logger.info("Spark session initialized.")

        # Set log level to DEBUG for detailed logs
        spark.sparkContext.setLogLevel("INFO")

        # Read the players data
        data_path = os.path.join('data', 'FIFA-18-Video-Game-Player-Stats.csv')
        if not os.path.exists(data_path):
            logger.error(f"Data file {data_path} does not exist.")
            sys.exit(1)

        players_df = spark.read.csv(data_path, header=True, inferSchema=True)
        logger.info("Players data read successfully.")
        logger.info("<" * 500 + f"Players DataFrame Schema:\n{players_df.head()}")

        # Broadcast country-continent mapping
        country_continent_broadcast = broadcast_country_continent_mapping(spark, API_URL)
        logger.info("Country-Continent mapping broadcasted.")

        # Define UDF to get continent
        get_continent_udf = udf_get_continent(country_continent_broadcast)

        # Enrich players data with continent information
        players_df = players_df.withColumn('Continent', get_continent_udf(col('Nationality')))
        logger.info("Players data enriched with continent information.")
        logger.info(">" * 500 + f"Enriched Players DataFrame Schema:\n{players_df.head()}")

        # Add an updated_at timestamp
        players_df = players_df.withColumn('updated_at', current_timestamp())

        # Repartition data by Continent
        players_df = players_df.repartition(col('Continent'))
        logger.info("Data repartitioned by Continent.")

        # Output path in S3
        output_path = S3_OUTPUT_PATH.strip()

        # Validate output_path
        if not output_path:
            logger.error("Output path is empty after stripping. Please check your S3_OUTPUT_PATH configuration.")
            sys.exit(1)

        logger.info(f"Output Path: {output_path}")

        # Check if the output path exists
        hadoop_conf = spark._jsc.hadoopConfiguration()
        path = spark._jvm.org.apache.hadoop.fs.Path(output_path)
        fs = path.getFileSystem(hadoop_conf)

        if fs.exists(path):
            # List Parquet files in the output directory
            parquet_files = fs.globStatus(spark._jvm.org.apache.hadoop.fs.Path(output_path + "/*.parquet"))
            if parquet_files:
                # Read existing data
                try:
                    existing_df = spark.read.parquet(output_path)
                    logger.info("Existing Parquet data read successfully.")
                    logger.debug(f"Existing DataFrame Schema:\n{existing_df.schema}")

                    # Merge new data with existing data
                    combined_df = existing_df.union(players_df).dropDuplicates(['Name'])
                    logger.info("Merged new data with existing data.")
                    logger.debug(f"Combined DataFrame Schema:\n{combined_df.schema}")
                except AnalysisException as e:
                    logger.error("Error reading existing Parquet data.")
                    logger.error(f"Error: {e}")
                    combined_df = players_df
            else:
                logger.info("Output directory exists but contains no Parquet files. Proceeding with new data.")
                combined_df = players_df
        else:
            logger.info("No existing data found. Proceeding with new data.")
            combined_df = players_df

        # Write data to S3 in Parquet format partitioned by Continent
        try:
            combined_df.write.mode('overwrite') \
                .partitionBy('Continent') \
                .option('compression', 'snappy') \
                .parquet(output_path)
            logger.info(f"Data written to S3 at {output_path} in Parquet format.")
        except Exception as write_e:
            logger.error(f"Failed to write data to S3: {write_e}")
            raise write_e

        logger.info("ETL process completed successfully.")

    except Exception as main_e:
        logger.error(f"ETL process failed: {main_e}")
    
    finally:
        # Stop the Spark session if it was initialized
        try:
            # gc.collect()
            print("*" * 500)
            cleanup_spark_temp_dir(spark)
            spark.stop()
            logger.info("Spark session stopped.")
        except Exception as e:
            logger.warning(f"Spark session was not running or failed to stop: {e}")

if __name__ == '__main__':
    main()
