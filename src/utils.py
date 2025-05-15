from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import names, geonamescache

# Get a unique list of names
def get_name_list(n=2000):
    name_set = set()
    while len(name_set) < n:
        name_set.add(names.get_first_name())
    return list(name_set)

# Get a unique list of cities
def get_city_list(min_len=7):
    gc = geonamescache.GeonamesCache()
    cities = gc.get_cities()
    city_names = [city['name'] for city in cities.values() if len(city['name']) >= min_len]
    return city_names

# UDF functions
def get_broadcast_udfs(spark, name_count=2000):
    name_list = get_name_list(name_count)
    city_list = get_city_list()

    # Broadcast the lists
    bc_names = spark.sparkContext.broadcast(name_list)
    bc_cities = spark.sparkContext.broadcast(city_list)

    # Define UDF functions
    def random_name(idx):
        return bc_names.value[int(idx) % len(bc_names.value)]

    def random_city(idx):
        return bc_cities.value[int(idx) % len(bc_cities.value)]

    # Convert UDFs to Spark UDFs
    random_name_udf = udf(random_name, StringType())
    random_city_udf = udf(random_city, StringType())

    return random_name_udf, random_city_udf