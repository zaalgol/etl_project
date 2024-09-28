import os
import sys
import logging
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F

# Import utility functions
from scripts.utils import broadcast_country_continent_mapping, udf_get_continent

# Retrieve environment variables
from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_OUTPUT_PATH,
    API_URL,
    SPARK_APP_NAME,
    SPARK_LOCAL_DIR
)

# Set PYSPARK environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("etl.log")]
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

def set_spark_config():
    return SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .config("spark.local.dir", SPARK_LOCAL_DIR) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g")  \
        .config("spark.executor.cores", "4")  \
        .config("spark.cores.max", "16")  \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .config("spark.hadoop.fs.s3a.block.size", "104857600")  \
        .config("spark.hadoop.fs.s3a.fast.upload", "true")  \
        .config("spark.hadoop.fs.s3a.connection.maximum", "1000") \
        .config("spark.hadoop.fs.s3a.threads.max", "256") \
        .config("spark.sql.shuffle.partitions", "200")  \
        .config("spark.default.parallelism", "200")  \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  \
        .config("spark.shuffle.compress", "true")  \
        .config("spark.rdd.compress", "true")  \
        .config("spark.memory.fraction", "0.6")  \
        .config("spark.speculation", "true") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.initialExecutors", "4") \
        .config("spark.dynamicAllocation.minExecutors", "4") \
        .config("spark.dynamicAllocation.maxExecutors", "16") \
        .getOrCreate()

def initialize_spark():
    """
    Initializes and returns a Spark session with necessary configurations.
    """
    try:
        spark = set_spark_config()
        
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

def convert_value_salary_to_numeric(df):
    """
    Converts the 'Value' and 'Salary' columns to numeric values.
    """
    try:
        numeric_df = df.withColumn(
            "Value",
            F.when(col("Value").like('%M'), regexp_replace(col("Value"), '[€M]', '').cast("double") * 1000000)
             .when(col("Value").like('%K'), regexp_replace(col("Value"), '[€K]', '').cast("double") * 1000)
             .otherwise(regexp_replace(col("Value"), '[€]', '').cast("double"))
        ).withColumn(
            "Salary",
            F.when(col("Salary").like('%M'), regexp_replace(col("Salary"), '[€M]', '').cast("double") * 1000000)
             .when(col("Salary").like('%K'), regexp_replace(col("Salary"), '[€K]', '').cast("double") * 1000)
             .otherwise(regexp_replace(col("Salary"), '[€]', '').cast("double"))
        )
        logger.info("Converted 'Value' and 'Salary' columns to numeric values.")
        return numeric_df
    except Exception as e:
        logger.error(f"Failed to convert 'Value' and 'Salary' to numeric: {e}")
        sys.exit(1)

def deduplicate_data(df):
    """
    Deduplicates the DataFrame based on key columns (excluding 'updated_at').
    This ensures that rows with the same Name, Age, Nationality, Fifa Score, Club, etc., are considered duplicates.
    """
    try:
        dedup_columns = [col for col in df.columns if col != 'updated_at']
        deduplicated_df = df.dropDuplicates(dedup_columns)
        logger.info(f"Deduplicated data. Rows after deduplication: {deduplicated_df.count()}")
        return deduplicated_df
    except Exception as e:
        logger.error(f"Failed to deduplicate data: {e}")
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

def detect_changes(new_df, existing_df):
    """
    Detects new or updated rows by comparing the new and existing data
    based on all columns except 'updated_at'.
    """
    if existing_df is None:
        return new_df

    # Create a list of columns to compare (all except 'Club', 'updated_at')
    compare_columns = [col for col in new_df.columns if col not in ['Club', 'updated_at']]

    # Compare based on all columns except 'updated_at'
    updated_rows = new_df.alias('new').join(
        existing_df.alias('existing'),
        on=compare_columns,
        how='left_anti'  # Select rows in 'new_df' that are not in 'existing_df'
    )

    logger.info(f"Detected {updated_rows.count()} new/updated rows.")
    return updated_rows

def write_partition_data(df, output_path, continent):
    """
    Writes partitioned DataFrame to the output path for the specific continent.
    """
    try:
        partition_path = os.path.join(output_path, f"Continent={continent}")
        logger.info(f"Writing new data for {continent} to {partition_path}. Number of partitions: {df.rdd.getNumPartitions()}")
        df.write.mode('append').parquet(partition_path)
        logger.info(f"New data for {continent} written successfully to {partition_path}.")
    except Exception as e:
        logger.error(f"Failed to write new data for {continent}: {e}")
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
        data_path = os.path.join('data', 'FIFA-18-Video-Game-Player-Stats-10.csv')
        players_df = read_players_data(spark, data_path)

        # Enrich data with continent information
        enriched_df = enrich_players_data(spark, players_df, API_URL)

        # Convert 'Value' and 'Salary' columns to numeric
        numeric_df = convert_value_salary_to_numeric(enriched_df)

        # Add timestamp
        df_with_timestamp = add_timestamp(numeric_df)

        # Deduplicate the data
        deduplicated_df = deduplicate_data(df_with_timestamp)

        # Define S3 output path
        output_path = S3_OUTPUT_PATH.strip()
        if not output_path:
            logger.error("Output path is empty after stripping. Please check your S3_OUTPUT_PATH configuration.")
            sys.exit(1)
        logger.info(f"Output Path: {output_path}")

        # Loop through each continent and update the data separately
        continents = deduplicated_df.select('Continent').distinct().rdd.flatMap(lambda x: x).collect()

        for continent in continents:
            logger.info(f"Processing continent: {continent}")

            # Filter data for the current continent
            continent_df = deduplicated_df.filter(col('Continent') == continent)

            # Load existing data for the current continent
            existing_df = load_existing_partition_data(spark, output_path, continent)

            # Detect new or updated rows
            updated_rows = detect_changes(continent_df, existing_df)

            if updated_rows.count() > 0:
                # Write only new or updated data back to S3
                write_partition_data(updated_rows, output_path, continent)

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
