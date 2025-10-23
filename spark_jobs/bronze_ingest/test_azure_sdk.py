from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from spark_jobs.bronze_ingest.download_and_ingest import ingest_to_bronze_with_azure_sdk
from delta import configure_spark_with_delta_pip

def test_azure_sdk_ingestion():
    """Test the Bronze layer ingestion job with Azure SDK (no Hadoop filesystem)"""

    # Create basic Spark session (no Azure JARs needed)
    builder = (SparkSession.builder
                .appName("US_DOT_Flights_Azure_SDK_Ingestion")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.driver.memory", "4g"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Test parameters
    year = 2025
    month = 'January'
    # Use local path first, then we'll upload to Azure
    bronze_output_path = "data/bronze/flights"
    
    try:
        # Run ingestion with Azure SDK
        print("Testing with Azure Data Lake (SDK approach)...")
        ingest_to_bronze_with_azure_sdk(spark, year, month, bronze_output_path)
        print("Bronze ingestion completed successfully!")
        
        # Verify data was written
        df = spark.read.format("delta").load(bronze_output_path)
        print(f"Records in Bronze: {df.count():,}")
        
        # Show date range
        from pyspark.sql.functions import min as spark_min, max as spark_max
        date_range = df.select(
            spark_min("FL_DATE").alias("min_date"),
            spark_max("FL_DATE").alias("max_date")
        ).collect()[0]
        print(f"Date range: {date_range['min_date']} to {date_range['max_date']}")
        
        # Show sample data
        print("Sample data:")
        df.show(5)
        
        # Upload local Delta table to Azure
        print("Uploading Delta table to Azure Data Lake...")
        from spark_jobs.bronze_ingest.azure_downloader import AzureDataDownloader
        from configs.azure_config import load_azure_config_from_env
        config = load_azure_config_from_env()
        downloader = AzureDataDownloader(
            storage_account_name=config.storage_account_name,
            container_name=config.container_name,
            account_key=config.account_key,
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.client_secret
        )
        
        azure_delta_path = f"abfss://{config.container_name}@{config.storage_account_name}.dfs.core.windows.net/bronze/flights"
        success = downloader.upload_local_delta_to_azure(bronze_output_path, azure_delta_path)
        
        if success:
            print("✅ Successfully uploaded Delta table to Azure Data Lake!")
        else:
            print("❌ Failed to upload Delta table to Azure")
        
    except Exception as e:
        print(f"Error during Azure SDK ingestion: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up temp files if they exist
        try:
            from spark_jobs.bronze_ingest.download_and_ingest import AzureDataDownloader
            from configs.azure_config import load_azure_config_from_env
            config = load_azure_config_from_env()
            downloader = AzureDataDownloader(
                storage_account_name=config.storage_account_name,
                container_name=config.container_name,
                account_key=config.account_key,
                tenant_id=config.tenant_id,
                client_id=config.client_id,
                client_secret=config.client_secret
            )
            downloader.cleanup_temp_files()
        except:
            pass
        spark.stop()

if __name__ == "__main__":
    test_azure_sdk_ingestion()
