from pyspark.sql import SparkSession
from datetime import datetime
from src.generate import generate_dataframe
import os, shutil

# Creating a Spark session
def get_spark():
    spark = SparkSession.builder \
        .appName("SyntheticDataGeneratorDEV") \
        .master("local[*]") \
        .config("spark.ui.showConsoleProgress", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Save to csv
def save_csv(df, output_dir="data/"):
    today_str = datetime.today().strftime('%Y-%m-%d')
    temp_path = os.path.join(output_dir, f"{today_str}-dev-temp")
    final_file = os.path.join(output_dir, f"{today_str}-dev.csv")

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

    # Search for CSV file
    for filename in os.listdir(temp_path):
        if filename.startswith("part-") and filename.endswith(".csv"):
            src_path = os.path.join(temp_path, filename)
            shutil.move(src_path, final_file)
            shutil.rmtree(temp_path)
            print(f"✅ CSV-file saved: {final_file}")
            return

    print("❌ File not found: part-*.csv")

if __name__ == "__main__":
    spark = get_spark()
    df = generate_dataframe(spark, n_row = 1000)
    save_csv(df)
    spark.stop()
