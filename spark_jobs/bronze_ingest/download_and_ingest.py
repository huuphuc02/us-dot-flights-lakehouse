from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from configs.data_schema import FLIGHT_DATA_SCHEMA


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
            data = (self.spark.read
                    .option("header", "true")
                    .option("timestampFormat", "M/d/yyyy hh:mm:ss a")
                    .schema(FLIGHT_DATA_SCHEMA)
                    .csv(paths['flights']))
            
            # Read lookup tables directly from Azure
            airport_lookup = (self.spark.read
                             .option("header", "true")
                             .csv(paths['airports']))
            
            carrier_lookup = (self.spark.read
                             .option("header", "true")
                             .csv(paths['carriers']))
            
            # Cache lookup tables for better performance
            airport_lookup.cache()
            carrier_lookup.cache()
            
            print(f"Flight data records: {data.count():,}")
            print(f"Airport lookup records: {airport_lookup.count():,}")
            print(f"Carrier lookup records: {carrier_lookup.count():,}")
            
            # Perform joins to enrich flight data
            df = (data
                  .join(airport_lookup, data.ORIGIN_AIRPORT_ID == airport_lookup.Code, "left")
                  .withColumnRenamed("Description", "ORIGIN_AIRPORT_NAME")
                  .drop("Code")
                  .join(airport_lookup, data.DEST_AIRPORT_ID == airport_lookup.Code, "left")
                  .withColumnRenamed("Description", "DEST_AIRPORT_NAME")
                  .drop("Code")
                  .join(carrier_lookup, data.OP_UNIQUE_CARRIER == carrier_lookup.Code, "left")
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
    print(f"Writing {df_with_metadata.count():,} records to bronze layer: {bronze_output_path}")
    
    df_with_metadata.write\
        .format("delta")\
        .mode("append")\
        .option("overwriteSchema", "true")\
        .partitionBy("partition_date")\
        .save(bronze_output_path)
    
    print(f"Successfully ingested data to bronze layer")
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
