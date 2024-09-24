import requests
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

def get_continent(nationality, api_url):
    """
    Fetches the continent for a given nationality using the API.
    """
    try:
        response = requests.get(f"{api_url}{nationality}")
        if response.status_code == 200:
            data = response.json()
            return data['name']
        else:
            return 'Unknown'
    except Exception as e:
        print(f"Error fetching continent for {nationality}: {e}")
        return 'Unknown'

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
