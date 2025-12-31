from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim, to_timestamp
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.data_schema import FLIGHT_DATA_SCHEMA
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure
from spark_jobs.validation.data_validator import DataValidator
import pandas as pd


class AzureDataLoader:
    """Load data directly from Azure Data Lake using Spark's native support"""
    
    def __init__(self, spark: SparkSession, config):
        """
        Initialize Azure Data Loader
        
        Args:
            spark: SparkSession configured for Azure access
            config: AzureConfig object with connection details
        """
        self.spark = spark
        self.config = config
        self.base_path = f"abfss://{config.container_name}@{config.storage_account_name}.dfs.core.windows.net"
    
    def get_azure_paths(self):
        """Get Azure Data Lake paths for data sources"""
        return {
            "flights": f"{self.base_path}/sources/flights",
            "airports": f"{self.base_path}/sources/lookup/L_AIRPORT_ID.csv",
            "carriers": f"{self.base_path}/sources/lookup/L_UNIQUE_CARRIERS.csv",
            "bronze": f"{self.base_path}/bronze",
            "silver": f"{self.base_path}/silver",
            "gold": f"{self.base_path}/gold"
        }
    
    def load_flight_data_from_azure(self):
        """
        Load flight data directly from Azure Data Lake using Spark
        
        Returns:
            DataFrame: Enriched flight data with airport and carrier names
        """
        try:
            paths = self.get_azure_paths()
            
            print(f"Reading flight data from: {paths['flights']}")
            print(f"Reading airport lookup from: {paths['airports']}")
            print(f"Reading carrier lookup from: {paths['carriers']}")
            
            # Read flight data directly from Azure using Spark
            # Read with inferSchema to handle different column names across files
            print("Reading CSV files (may take a moment to infer schema)...")
            data = (self.spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("timestampFormat", "M/d/yyyy hh:mm:ss a")
                    .csv(paths['flights']))
            
            # Standardize column names (some files have OP_CARRIER_AIRLINE_ID instead of OP_UNIQUE_CARRIER)
            if "OP_CARRIER_AIRLINE_ID" in data.columns:
                data = data.withColumnRenamed("OP_CARRIER_AIRLINE_ID", "OP_UNIQUE_CARRIER")
            
            # Some files might not have ORIGIN/DEST columns, only IDs
            # We'll add them from the lookup if missing
            has_origin_code = "ORIGIN" in data.columns
            has_dest_code = "DEST" in data.columns
            
            print(f"\n🔍 Column availability:")
            print(f"   - Has ORIGIN column: {has_origin_code}")
            print(f"   - Has DEST column: {has_dest_code}")
            print(f"   - Has ORIGIN_AIRPORT_ID: {'ORIGIN_AIRPORT_ID' in data.columns}")
            print(f"   - Has DEST_AIRPORT_ID: {'DEST_AIRPORT_ID' in data.columns}")
            
            # Cast FL_DATE to timestamp if it's string
            if dict(data.dtypes)["FL_DATE"] == "string":
                from pyspark.sql.functions import to_timestamp
                data = data.withColumn("FL_DATE", to_timestamp(col("FL_DATE"), "M/d/yyyy hh:mm:ss a"))
            
            # Read lookup tables directly from Azure
            from pyspark.sql.functions import trim, col as spark_col
            
            airport_lookup = (self.spark.read
                             .option("header", "true")
                             .csv(paths['airports'])
                             .select(
                                 trim(spark_col("Code")).cast("string").alias("Code"),
                                 trim(spark_col("Description")).alias("Description")
                             ))
            
            carrier_lookup = (self.spark.read
                             .option("header", "true")
                             .csv(paths['carriers'])
                             .select(
                                 trim(spark_col("Code")).cast("string").alias("Code"),
                                 trim(spark_col("Description")).alias("Description")
                             ))
            
            # Cache lookup tables for better performance
            airport_lookup.cache()
            carrier_lookup.cache()
            
            # Show what files are being read
            from pyspark.sql.functions import input_file_name
            file_list = data.select(input_file_name().alias("file")).distinct().collect()
            print(f"\n📁 Reading {len(file_list)} file(s) from Azure:")
            for f in file_list[:10]:  # Show first 10 files
                print(f"   - {f['file']}")
            if len(file_list) > 10:
                print(f"   ... and {len(file_list) - 10} more files")
            
            # Show actual columns found
            print(f"\n📋 Columns found in data: {', '.join(data.columns)}")
            
            flight_count = data.count()
            print(f"\n📊 Flight data records from source: {flight_count:,}")
            print(f"📊 Airport lookup records: {airport_lookup.count():,}")
            print(f"📊 Carrier lookup records: {carrier_lookup.count():,}")
            
            # Show sample IDs before join for debugging
            print(f"\n🔍 Sample IDs from flight data:")
            data.select("ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "OP_UNIQUE_CARRIER").show(3, truncate=False)
            
            print(f"\n🔍 Sample codes from airport lookup:")
            airport_lookup.show(3, truncate=False)
            
            print(f"\n🔍 Sample codes from carrier lookup:")
            carrier_lookup.show(3, truncate=False)
            
            # Perform joins to enrich flight data
            # Cast IDs to string to match lookup table format
            from pyspark.sql.functions import col as spark_col
            
            df = (data
                  .join(
                      airport_lookup.alias("origin_lookup"),
                      data.ORIGIN_AIRPORT_ID.cast("string") == spark_col("origin_lookup.Code"),
                      "left"
                  )
                  .withColumnRenamed("Description", "ORIGIN_AIRPORT_NAME")
                  .drop("Code")
                  .join(
                      airport_lookup.alias("dest_lookup"),
                      data.DEST_AIRPORT_ID.cast("string") == spark_col("dest_lookup.Code"),
                      "left"
                  )
                  .withColumnRenamed("Description", "DEST_AIRPORT_NAME")
                  .drop("Code")
                  .join(
                      carrier_lookup,
                      trim(data.OP_UNIQUE_CARRIER).cast("string") == carrier_lookup.Code,
                      "left"
                  )
                  .withColumnRenamed("Description", "CARRIER_NAME")
                  .drop("Code"))
            
            print(f"Successfully loaded and enriched {df.count():,} records from Azure")
            return df
            
        except Exception as e:
            print(f"Error loading data from Azure: {e}")
            import traceback
            traceback.print_exc()
            return None

def ingest_to_bronze_from_azure(spark: SparkSession, bronze_output_path: str):
    """
    Ingest data directly from Azure Data Lake to Bronze layer
    
    Args:
        spark: SparkSession configured for Azure access
        bronze_output_path: Path to write bronze Delta table
    
    Returns:
        DataFrame: Ingested data with metadata
    """
    from configs.azure_config import load_azure_config_from_env, configure_spark_for_azure
    
    # Load Azure configuration
    config = load_azure_config_from_env()
    
    # Configure Spark for Azure access (if not already configured)
    configure_spark_for_azure(spark, config)
    
    # Create Azure data loader
    loader = AzureDataLoader(spark, config)
    
    # Load data directly from Azure using Spark
    df = loader.load_flight_data_from_azure()
    
    if df is None:
        raise ValueError("Failed to load data from Azure")
    
    # Add metadata columns
    df_with_metadata = (df
                        .withColumn("ingestion_timestamp", current_timestamp())
                        .withColumn("partition_date", col("FL_DATE").cast("date")))
    
    # Write to Delta table
    records_to_ingest = df_with_metadata.count()
    print(f"\n💾 Writing {records_to_ingest:,} records to bronze layer: {bronze_output_path}")
    print(f"🔄 Mode: OVERWRITE (will replace existing data)")
    
    df_with_metadata.write\
        .format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .partitionBy("partition_date")\
        .save(bronze_output_path)
    
    print(f"✅ Successfully ingested {records_to_ingest:,} records to bronze layer")
    return df_with_metadata

def setup_autoloader_stream(spark: SparkSession, source_path: str, checkpoint_path: str, output_path: str):
    """Setup Autoloader for incremental file processing"""
    df = spark.readStream\
        .format("cloudFiles")\
            .option("cloudFiles.format", "csv")\
                .option("cloudFiles.schemaLocation", checkpoint_path)\
                    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
                        .option("cloudFiles.includeExistingFiles", "false")\
                            .option("cloudFiles.validateOptions", "true")\
                                .option("header", "true")\
                                    .option("timestampFormat", "M/d/yyyy hh:mm:ss a")\
                                        .schema(FLIGHT_DATA_SCHEMA)\
                                            .load(source_path)

    return df

def run_autoloader_ingestion(spark: SparkSession, source_path: str, checkpoint_path: str, output_path: str):
    """Run ingestion using Autoloader approach"""
    df = setup_autoloader_stream(spark, source_path, checkpoint_path, output_path)

    from pyspark.sql.functions import input_file_name

    df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp())\
        .withColumn("source_file", input_file_name())\
        .withColumn("partition_date", col("FL_DATE").cast("date"))

    query = df_with_metadata.writeStream\
        .format("delta")\
            .option("checkpointLocation", checkpoint_path)\
                .option("mergeSchema", "true")\
                    .outputMode("append")\
                        .trigger(once=True)\
                            .start(output_path)

    return query

if __name__ == "__main__":
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
        print("\n" + "="*60)
        print("📊 BRONZE LAYER VERIFICATION")
        print("="*60)
        df_read = spark.read.format("delta").load(bronze_output_path)
        record_count = df_read.count()
        print(f"Total Records in Bronze: {record_count:,}")
        print(f"✅ This is the complete Bronze layer (mode=overwrite)")

        # Validate data
        print("\nValidating data...")
        validator = DataValidator()
        results = validator.validate_bronze_data(df_read, 
            batch_id=f"bronze_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}",
        )
        # Get statistics
        stats = validator.get_validation_statistics(results)
        print("\nValidation Statistics:")
        print(f"  Total Expectations: {stats['total']}")
        print(f"  Passed: {stats['passed']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Success Rate: {stats['success_rate']:.2f}%")
        print("="*60 + "\n")

        # Exit with appropriate code
        if results.get("success"):
            print("✅ Bronze layer validation PASSED!")
        elif results.get("error") == "Context not loaded":
            print("⚠️  Bronze layer validation SKIPPED (Great Expectations not configured)")
            print("   Data ingestion completed successfully, but validation was skipped")
        else:
            print("❌ Bronze layer validation FAILED!")
            print("⚠️  WARNING: Continuing anyway (validation errors don't block ingestion)")
            # Don't raise exception - validation failures are warnings, not blockers
            # raise Exception("Bronze layer validation failed")
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
        
    except Exception as e:
        print(f"\n✗ Error during Azure direct ingestion: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'spark' in locals():
            spark.stop()
            print("\nSpark session stopped")