"""
Standalone Bronze Layer Validation Script
Validates bronze layer data quality using Great Expectations
"""
from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths
from spark_jobs.validation.data_validator import DataValidator

def validate_bronze_layer():
    """Validate bronze layer data"""
    
    print("="*60)
    print("Bronze Layer Data Validation")
    print("="*60 + "\n")
    
    # Load Azure configuration
    azure_config = load_azure_config_from_env()
    
    # Create Spark session
    spark = create_spark_session_with_azure(
        app_name="Validate_Bronze_Layer",
        azure_config=azure_config
    )
    
    # Get data paths
    paths = get_azure_data_paths(azure_config)
    
    # Load bronze data
    try:
        print(f"📂 Loading Bronze data from Azure...")
        print(f"   Path: {paths['bronze']}/flights")
        bronze_df = spark.read.format("delta").load(f"{paths['bronze']}/flights")
        print(f"✅ Loaded {bronze_df.count()} records\n")
    except Exception as e:
        print(f"❌ Failed to load from Azure: {e}")
        print("🔄 Falling back to local data...")
        bronze_df = spark.read.format("delta").load(
            "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/bronze/flights"
        )
        print(f"✅ Loaded {bronze_df.count()} records from local\n")
    
    # Initialize validator
    validator = DataValidator()
    
    # Run validation
    results = validator.validate_bronze_data(bronze_df, batch_id="bronze_latest")
    
    # Print statistics
    stats = validator.get_validation_statistics(results)
    print("\n" + "="*60)
    print("Validation Statistics:")
    print(f"  Total Expectations: {stats['total']}")
    print(f"  Passed: {stats['passed']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Success Rate: {stats['success_rate']:.2f}%")
    print("="*60 + "\n")
    
    # Exit with appropriate code
    if results.get("success"):
        print("✅ Bronze layer validation PASSED!")
        return 0
    else:
        print("❌ Bronze layer validation FAILED!")
        return 1

if __name__ == "__main__":
    exit_code = validate_bronze_layer()
    sys.exit(exit_code)

