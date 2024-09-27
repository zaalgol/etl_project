import requests
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

def broadcast_country_continent_mapping(spark, api_url):
    """
    Builds a broadcast variable containing the mapping of countries to continents.
    Fetches the mapping from the provided API URL.
    """
    try:
        response = requests.get(api_url)

        # Raise an exception if the request was not successful
        response.raise_for_status()
        
        country_continent = response.json()

    except requests.exceptions.RequestException as e:
        # Handle any errors that occur during the request
        print(f"Error fetching country-continent mapping: {e}")
        return None

    # Broadcast the mapping to all Spark nodes
    return spark.sparkContext.broadcast(country_continent)

def udf_get_continent(country_continent_broadcast):
    """
    Creates a UDF that fetches the continent from the broadcasted mapping.
    """
    def get_continent_from_broadcast(nationality):
        return country_continent_broadcast.value.get(nationality, 'Unknown')
    return udf(get_continent_from_broadcast, StringType())
