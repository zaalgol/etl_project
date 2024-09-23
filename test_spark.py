# test_spark.py

import sys
import os
from pyspark.sql import SparkSession

# Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the current Python executable
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print(f"PYSPARK_PYTHON is set to: {os.environ['PYSPARK_PYTHON']}")
print(f"PYSPARK_DRIVER_PYTHON is set to: {os.environ['PYSPARK_DRIVER_PYTHON']}")

def main():
    try:
        # Initialize Spark Session with or without hadoop-aws
        spark = SparkSession.builder \
            .appName("PySpark Test") \
            .getOrCreate()

        # Set log level to DEBUG for detailed logs
        spark.sparkContext.setLogLevel("DEBUG")

        # Create a simple DataFrame
        data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)

        # Show the DataFrame
        df.show()

        # Write DataFrame to local Parquet (optional)
        output_path = os.path.join(os.getcwd(), "output_parquet")
        df.write.mode('overwrite').parquet(output_path)
        print(f"Data written to {output_path} in Parquet format.")

        # Stop Spark session
        spark.stop()

        print("PySpark test completed successfully.")

    except Exception as e:
        print(f"PySpark test failed: {e}")

if __name__ == '__main__':
    pass
    main()
