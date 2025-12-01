"""
Test Bronze layer ingestion by reading directly from Azure Data Lake
"""
from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from spark_jobs.bronze_ingest.download_and_ingest import ingest_to_bronze_from_azure
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure

def test_azure_direct_ingestion():
    """Test Bronze layer ingestion reading directly from Azure Data Lake"""
    
    try:
        # Load Azure configuration
        config = load_azure_config_from_env()
        
        # Create Spark session with Azure support
        print("Creating Spark session with Azure Data Lake support...")
        spark = create_spark_session_with_azure(
            app_name="US_DOT_Flights_Azure_Direct_Ingestion",
            azure_config=config,
            master="local[*]",
            memory="4g"
        )
        
        bronze_output_path = "data/bronze/flights"
        
        # For direct Azure write 
        bronze_output_path = f"abfss://{config.container_name}@{config.storage_account_name}.dfs.core.windows.net/bronze/flights"
        
        print(f"Bronze output path: {bronze_output_path}")
        
        # Run ingestion - reads directly from Azure
        print("\nReading data directly from Azure Data Lake...")
        df = ingest_to_bronze_from_azure(spark, bronze_output_path)
        
        print("\n✓ Bronze ingestion completed successfully!")
        
        # Verify data was written
        print("\nVerifying data...")
        df_read = spark.read.format("delta").load(bronze_output_path)
        record_count = df_read.count()
        print(f"Records in Bronze: {record_count:,}")
        
        # Show date range
        from pyspark.sql.functions import min as spark_min, max as spark_max
        date_range = df_read.select(
            spark_min("FL_DATE").alias("min_date"),
            spark_max("FL_DATE").alias("max_date")
        ).collect()[0]
        print(f"Date range: {date_range['min_date']} to {date_range['max_date']}")
        
        # Show schema
        print("\nSchema:")
        df_read.printSchema()
        
        # Show sample data
        print("\nSample data:")
        df_read.select(
            "FL_DATE", "OP_UNIQUE_CARRIER", "CARRIER_NAME",
            "ORIGIN_AIRPORT_ID", "ORIGIN_AIRPORT_NAME",
            "DEST_AIRPORT_ID", "DEST_AIRPORT_NAME",
            "DEP_TIME", "ARR_TIME", "CANCELLED"
        ).show(5, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error during Azure direct ingestion: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if 'spark' in locals():
            spark.stop()
            print("\nSpark session stopped")

if __name__ == "__main__":
    success = test_azure_direct_ingestion()
    sys.exit(0 if success else 1)
