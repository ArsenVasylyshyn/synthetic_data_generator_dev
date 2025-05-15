from pyspark.sql.functions import expr, col, concat, lit, floor, rand, when
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf

from .utils import get_broadcast_udfs


# Generating a dataframe with random data
def generate_dataframe(spark, n_row = 1000, null_percent = 0.035):

    # Generates a random id
    df = spark.range(1, n_row + 1).toDF("id")

    # Get UDF functions
    random_name_udf, random_city_udf = get_broadcast_udfs(spark)
    # Indexes for selecting random names and cities
    df = df.withColumn("rand_index_name", (floor(rand() * n_row * 10)).cast(IntegerType()))
    df = df.withColumn("rand_index_city", (floor(rand() * n_row * 10)).cast(IntegerType()))


    # Use udf for random names and cities from broadcast lists
    df = df.withColumn("name", random_name_udf(col("rand_index_name")))
    df = df.withColumn("city", random_city_udf(col("rand_index_city")))

    # Generates a email address
    df = df.withColumn("domain", expr("CASE WHEN rand() > 0.5 THEN 'ru' ELSE 'com' END"))
    df = df.withColumn("email", concat(col("name"), lit("@example."), col("domain")))

    # Generates a age
    df = df.withColumn("age", expr("cast(floor(rand() * (85 - 18 + 1)) + 18 as int)"))
    # Generates a salary
    df = df.withColumn("salary", floor(expr("rand() * (250000 - 30000) + 30000")).cast("double"))
    # Generates registration date
    df = df.withColumn("registration_date", expr("date_sub(current_date(), age * 365 - cast(rand() * 365 as int))"))

    # Add null percent
    for col_name in ["name", "email", "city", "age", "salary", "registration_date"]:
        df = df.withColumn(
            col_name,
            when(rand() < null_percent, None)
            .otherwise(col(col_name))
        )

    df = df.drop("domain", "rand_index_name", "rand_index_city")
    return df
    
