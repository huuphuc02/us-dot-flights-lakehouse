from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from spark_jobs.bronze_ingest.download_and_ingest import ingest_to_bronze_with_azure_sdk
from delta import configure_spark_with_delta_pip

def clean_flights_data(year: int, month: str):
    builder = (SparkSession.builder
                .appName("US_DOT_Flights_Azure_SDK_Ingestion")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.driver.memory", "4g"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    year = 2025
    month = 'January'
    bronze_output_path = ""
    try:
        df = ingest_to_bronze_with_azure_sdk(spark, year, month, bronze_output_path)
        print(f"Records in Bronze: {df.count():,}")
        
        from pyspark.sql.functions import min as spark_min, max as spark_max
        date_range = df.select(
            spark_min("FL_DATE").alias("min_date"),
            spark_max("FL_DATE").alias("max_date")
        ).collect()[0]
        print(f"Date range: {date_range['min_date']} to {date_range['max_date']}")
        
        # Show sample data
        print("Sample data:")
        df.show(5)
        
    except Exception as e:
        print(f"Error during Azure SDK ingestion: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_flights_data(2025, 'January')