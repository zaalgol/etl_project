import os
import sys
import logging
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.utils import AnalysisException

# Import utility functions
from scripts.utils import broadcast_country_continent_mapping, udf_get_continent

# Retrieve environment variables
from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_OUTPUT_PATH,
    API_URL,
    SPARK_APP_NAME
)

# Set PYSPARK environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl.log")
    ]
)

logger = logging.getLogger(__name__)

def cleanup_spark_temp_dir(spark):
    """
    Cleans up the Spark temporary directory.
    """
    temp_dir = spark.conf.get("spark.local.dir")
    try:
        for attempt in range(5):
            try:
                time.sleep(5)  # Wait before attempting
                shutil.rmtree(temp_dir)
                logger.info(f"Successfully deleted temporary Spark directory: {temp_dir}")
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt+1}: Failed to delete {temp_dir}: {e}")
                time.sleep(5)
        else:
            logger.error(f"Could not delete temporary Spark directory after multiple attempts: {temp_dir}")
    except Exception as e:
        logger.warning(f"Failed to delete temporary Spark directory {temp_dir}: {e}")

def initialize_spark():
    """
    Initializes and returns a Spark session with necessary configurations.
    """
    try:
        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .config("spark.local.dir", "C:/Users/zaalg/Documents") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.cores.max", "4") \
            .getOrCreate()
        
        logger.info("Spark session initialized.")
        spark.sparkContext.setLogLevel("INFO")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {e}")
        sys.exit(1)

def read_players_data(spark, data_path):
    """
    Reads the players CSV data into a DataFrame.
    """
    if not os.path.exists(data_path):
        logger.error(f"Data file {data_path} does not exist.")
        sys.exit(1)
    
    try:
        players_df = spark.read.csv(data_path, header=True, inferSchema=True)
        logger.info(f"Players data read successfully with {players_df.count()} rows.")
        return players_df
    except Exception as e:
        logger.error(f"Failed to read players data: {e}")
        sys.exit(1)

def enrich_players_data(spark, players_df, api_url):
    """
    Enriches the players DataFrame with continent information.
    """
    try:
        # Broadcast the country-continent mapping
        country_continent_broadcast = broadcast_country_continent_mapping(spark, api_url)
        if country_continent_broadcast is None:
            logger.error("Country-Continent mapping could not be broadcasted.")
            sys.exit(1)
        logger.info("Country-Continent mapping broadcasted.")
        
        # Define and apply the UDF
        get_continent_udf = udf_get_continent(country_continent_broadcast)
        enriched_df = players_df.withColumn('Continent', get_continent_udf(col('Nationality')))
        logger.info(f"Players data enriched with continent information. Rows after enrichment: {enriched_df.count()}")
        return enriched_df
    except Exception as e:
        logger.error(f"Failed to enrich players data: {e}")
        sys.exit(1)

def add_timestamp(df):
    """
    Adds an 'updated_at' timestamp column to the DataFrame.
    """
    try:
        df_with_timestamp = df.withColumn('updated_at', current_timestamp())
        logger.info(f"Added 'updated_at' timestamp to DataFrame. Rows with timestamp: {df_with_timestamp.count()}")
        return df_with_timestamp
    except Exception as e:
        logger.error(f"Failed to add timestamp: {e}")
        sys.exit(1)

def load_existing_partition_data(spark, output_path, continent):
    """
    Loads existing data from a specific partition (continent) if available.
    """
    try:
        partition_path = os.path.join(output_path, f"Continent={continent}")
        existing_df = spark.read.parquet(partition_path)
        logger.info(f"Existing data for {continent} read successfully with {existing_df.count()} rows.")
        return existing_df
    except AnalysisException:
        logger.info(f"No existing data found for {continent}. Proceeding with new data.")
        return None
    except Exception as e:
        logger.error(f"Error reading existing data for {continent}: {e}")
        sys.exit(1)

def merge_new_and_existing_data(existing_df, new_df, continent):
    """
    Merges new data with the existing data by avoiding duplication of rows based on 'Name'.
    """
    try:
        if existing_df is not None:
            dedup_columns = [col for col in existing_df.columns if col != 'updated_at']
            combined_df = existing_df.union(new_df).dropDuplicates(dedup_columns)
            logger.info(f"Merged new and existing data for {continent}. Total rows after merge: {combined_df.count()}")
        else:
            combined_df = new_df
            logger.info(f"No existing data for {continent}. Using new data with {new_df.count()} rows.")
        return combined_df
    except Exception as e:
        logger.error(f"Failed to merge data for {continent}: {e}")
        sys.exit(1)

def write_partition_data(df, output_path, continent):
    """
    Writes partitioned DataFrame to the output path for the specific continent.
    """
    try:
        partition_path = os.path.join(output_path, f"Continent={continent}")
        logger.info(f"Writing data for {continent} to {partition_path}. Number of partitions: {df.rdd.getNumPartitions()}")
        df.coalesce(1).write.mode('overwrite').parquet(partition_path)
        logger.info(f"Data for {continent} written successfully to {partition_path}.")
    except Exception as e:
        logger.error(f"Failed to write data for {continent}: {e}")
        sys.exit(1)

def main():
    try:
        # Validate environment variables
        if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_OUTPUT_PATH, API_URL, SPARK_APP_NAME]):
            logger.error("One or more environment variables are missing. Please check your .env file.")
            sys.exit(1)

        # Initialize Spark session
        spark = initialize_spark()

        # Read players data
        data_path = os.path.join('data', 'FIFA-18-Video-Game-Player-Stats.csv')
        players_df = read_players_data(spark, data_path)

        # Enrich data with continent information
        enriched_df = enrich_players_data(spark, players_df, API_URL)

        # Add timestamp
        df_with_timestamp = add_timestamp(enriched_df)

        # Define S3 output path
        output_path = S3_OUTPUT_PATH.strip()
        if not output_path:
            logger.error("Output path is empty after stripping. Please check your S3_OUTPUT_PATH configuration.")
            sys.exit(1)
        logger.info(f"Output Path: {output_path}")

        # Loop through each continent and update the data separately
        continents = df_with_timestamp.select('Continent').distinct().rdd.flatMap(lambda x: x).collect()

        for continent in continents:
            logger.info(f"Processing continent: {continent}")

            # Filter data for the current continent
            continent_df = df_with_timestamp.filter(col('Continent') == continent)

            # Load existing data for the current continent
            existing_df = load_existing_partition_data(spark, output_path, continent)

            # Merge new data with existing data
            combined_df = merge_new_and_existing_data(existing_df, continent_df, continent)

            # Write merged data back to S3
            write_partition_data(combined_df, output_path, continent)

        logger.info("ETL process completed successfully.")

    except Exception as main_e:
        logger.error(f"ETL process failed: {main_e}")
    
    finally:
        # Stop the Spark session if it was initialized
        try:
            cleanup_spark_temp_dir(spark)
            spark.stop()
            logger.info("Spark session stopped.")
        except Exception as e:
            logger.warning(f"Spark session was not running or failed to stop: {e}")

if __name__ == '__main__':
    main()
