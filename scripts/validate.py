from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("NullCheck").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

files = sorted([f for f in os.listdir("data") if f.endswith(".csv")], reverse=True)
df = spark.read.option("header", True).csv(f"data/{files[0]}")
total = df.count()

print(f"Checking file: {files[0]}")
for c in df.columns:
    nulls = df.filter(col(c).isNull()).count()
    ratio = nulls / total
    status = "⚠️" if ratio > 0.05 else "✅"
    print(f"{status} {c}: {ratio:.2%} NULL")

spark.stop()