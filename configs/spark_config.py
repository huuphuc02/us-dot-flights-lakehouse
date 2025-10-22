"""
Spark Configuration for US DOT Flights Data Lakehouse
Optimized settings for data exploration and processing
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os

def create_spark_session(app_name="US_DOT_Flights_Exploration", 
                        master="local[*]", 
                        memory="4g",
                        enable_delta=True):
    """
    Create and configure a Spark session for data exploration
    
    Args:
        app_name (str): Name of the Spark application
        master (str): Spark master URL
        memory (str): Driver memory allocation
        enable_delta (bool): Whether to enable Delta Lake support
    
    Returns:
        SparkSession: Configured Spark session
    """
    
    # Create Spark configuration
    conf = SparkConf()
    
    # Basic configuration
    conf.set("spark.app.name", app_name)
    conf.set("spark.master", master)
    conf.set("spark.driver.memory", memory)
    conf.set("spark.driver.maxResultSize", "2g")
    
    # Performance optimizations
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    # Delta Lake configuration
    if enable_delta:
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Memory management
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
    
    # Logging level (set to WARN to reduce verbosity)
    conf.set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
    
    # Create Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def get_data_paths(base_path="/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse"):
    """
    Get standardized data paths for the project
    
    Args:
        base_path (str): Base path of the project
    
    Returns:
        dict: Dictionary containing all data paths
    """
    return {
        "base": base_path, 
        "raw_data": os.path.join(base_path, "data"),
        "bronze": os.path.join(base_path, "data", "bronze"),
        "silver": os.path.join(base_path, "data", "silver"),
        "gold": os.path.join(base_path, "data", "gold"),
        "checkpoints": os.path.join(base_path, "data", "checkpoints"),
        "january_2025": os.path.join(base_path, "data", "T_ONTIME_FLIGHT_REPORTING_JANUARY_2025.csv"),
        "february_2025": os.path.join(base_path, "data", "T_ONTIME_REPORTING_FEBRUARY_2025.csv"),
        "march_2025": os.path.join(base_path, "data", "T_ONTIME_FLIGHT_REPORTING_MARCH_2025.csv"),
        "airport_lookup": os.path.join(base_path, "data", "L_AIRPORT_ID.csv"),
        "carrier_lookup": os.path.join(base_path, "data", "L_UNIQUE_CARRIERS.csv")
    }

def stop_spark_session(spark):
    """
    Properly stop Spark session and clean up resources
    
    Args:
        spark (SparkSession): Spark session to stop
    """
    if spark:
        spark.stop()
        print("Spark session stopped successfully")
