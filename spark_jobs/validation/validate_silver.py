"""
Standalone Silver Layer Validation Script
Validates silver layer data quality using Great Expectations
"""
from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths
from spark_jobs.validation.data_validator import DataValidator

def validate_silver_layer():
    """Validate silver layer data"""
    
    print("="*60)
    print("Silver Layer Data Validation")
    print("="*60 + "\n")
    
    # Load Azure configuration
    azure_config = load_azure_config_from_env()
    
    # Create Spark session
    spark = create_spark_session_with_azure(
        app_name="Validate_Silver_Layer",
        azure_config=azure_config
    )
    
    # Get data paths
    paths = get_azure_data_paths(azure_config)
    
    # Load silver data
    try:
        print(f"📂 Loading Silver data from Azure...")
        print(f"   Path: {paths['silver']}")
        silver_df = spark.read.format("delta").load(paths['silver'])
        print(f"✅ Loaded {silver_df.count()} records\n")
    except Exception as e:
        print(f"❌ Failed to load from Azure: {e}")
        print("🔄 Falling back to local data...")
        silver_df = spark.read.format("delta").load(
            "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/silver"
        )
        print(f"✅ Loaded {silver_df.count()} records from local\n")
    
    # Initialize validator
    validator = DataValidator()
    
    # Run validation
    results = validator.validate_silver_data(silver_df, batch_id="silver_latest")
    
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
        print("✅ Silver layer validation PASSED!")
        return 0
    else:
        print("❌ Silver layer validation FAILED!")
        return 1

if __name__ == "__main__":
    exit_code = validate_silver_layer()
    sys.exit(exit_code)

