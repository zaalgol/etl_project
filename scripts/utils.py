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
            return data['name']  # Assuming the API returns {'code': 'EU', 'name': 'Europe', ...}
        else:
            return 'Unknown'
    except Exception as e:
        print(f"Error fetching continent for {nationality}: {e}")
        return 'Unknown'

def broadcast_country_continent_mapping(spark, api_url):
    """
    Builds a broadcast variable containing the mapping of countries to continents.
    """
    # Fetch all countries from the API or database
    # For simplicity, we'll create a static mapping here
    country_continent = {
        'Portugal': 'Europe',
        'Argentina': 'South America',
        'Brazil': 'South America',
        'Uruguay': 'South America',
        'Germany': 'Europe',
        'Poland': 'Europe',
        'Spain': 'Europe',
        'Belgium': 'Europe',
        'Chile': 'South America',
        # Add more mappings as needed
    }
    return spark.sparkContext.broadcast(country_continent)

def udf_get_continent(country_continent_broadcast):
    """
    Creates a UDF that fetches the continent from the broadcasted mapping.
    """
    def get_continent_from_broadcast(nationality):
        return country_continent_broadcast.value.get(nationality, 'Unknown')
    return udf(get_continent_from_broadcast, StringType())
