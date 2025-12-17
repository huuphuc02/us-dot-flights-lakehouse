import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.datasource.fluent import batch_request
from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths

def create_bronze_expectations(context, spark, azure_config = None):
    if azure_config:
        paths = get_azure_data_paths(azure_config)
        bronze_path = f"{paths['bronze']}/flights"
    else:
        bronze_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/bronze/flights"

    try:
        print(f"🔍 Attempting to load Bronze data from Azure...")
        print(f"   Path: {bronze_path}")
        bronze_df = spark.read.format("delta").load(bronze_path)
        print(f"✅ Successfully loaded from Azure! Row count: {bronze_df.count()}")
    except Exception as e:
        print(f"❌ Azure connection failed: {e}")
        print("🔄 Falling back to local data...")
        bronze_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/bronze/flights"
        bronze_df = spark.read.format("delta").load(bronze_path)
        print(f"✅ Loaded from local. Row count: {bronze_df.count()}")

    suite_name = 'bronze_flights_suite'
    context.add_or_update_expectation_suite(suite_name)

   # Create batch request
    batch_request = RuntimeBatchRequest(
        datasource_name="bronze_flights",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="flights",
        runtime_parameters={"batch_data": bronze_df},
        batch_identifiers={"batch_id": "bronze_validation"}
    )
    
    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    # Define Bronze layer expectations (basic data integrity)
    print("📋 Creating Bronze layer expectations...")
    
    # 1. Table should exist and have data
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    
    # 2. Required columns should exist
    required_columns = [
        "FL_DATE", "OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM",
        "ORIGIN", "DEST", "DEP_TIME", "ARR_TIME",
        "DEP_DELAY", "ARR_DELAY", "DISTANCE", "AIR_TIME"
    ]
    for col in required_columns:
        validator.expect_column_to_exist(col)

       # 3. Key columns should not be all null
    validator.expect_column_values_to_not_be_null("FL_DATE", mostly=0.95)
    validator.expect_column_values_to_not_be_null("OP_UNIQUE_CARRIER", mostly=0.99)
    validator.expect_column_values_to_not_be_null("ORIGIN", mostly=0.99)
    validator.expect_column_values_to_not_be_null("DEST", mostly=0.99)
    
    # 4. Data type validations
    validator.expect_column_values_to_be_of_type("OP_CARRIER_FL_NUM", "IntegerType")
    
    # 5. Airport codes should be between 3-5 characters
    # US DOT data has both 3-letter IATA codes (LAX, JFK) and 5-character codes for regional airports
    # validator.expect_column_value_lengths_to_be_between("ORIGIN", min_value=3, max_value=5, mostly=0.4)
    # validator.expect_column_value_lengths_to_be_between("DEST", min_value=3, max_value=5, mostly=0.4)
    
    # 6. Distance should be positive
    validator.expect_column_values_to_be_between("DISTANCE", min_value=0, max_value=20000, mostly=0.98)
    
    # Save suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✅ Created Bronze expectations suite: {suite_name}")
    print(f"   Total expectations: {len(validator.get_expectation_suite().expectations)}")
    
    return validator

if __name__ == "__main__":
    # Load Azure configuration from environment
    print("🔧 Loading Azure configuration...")
    azure_config = load_azure_config_from_env()
    
    # Create Spark session with Azure support
    print("⚙️ Creating Spark session with Azure Data Lake support...")
    spark = create_spark_session_with_azure(
        app_name="GX_Bronze_Expectations",
        azure_config=azure_config
    )
    
    # Initialize Great Expectations context
    project_root = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse"
    gx_dir = f"{project_root}/expectations/great_expectations"
    
    try:
        context = gx.get_context(context_root_dir=gx_dir)
        print(f"✅ Loaded existing Great Expectations context from {gx_dir}")
    except Exception as e:
        print(f"⚠️ Could not load context: {e}")
        print("💡 Please run init_great_expectations.py first!")
        sys.exit(1)
    
    # Setup datasources
    from init_great_expectations import setup_datasources
    setup_datasources(context)

    # Create Bronze expectations
    print("\n" + "="*60)
    print("Creating Bronze Layer Expectations")
    print("="*60 + "\n")
    
    validator = create_bronze_expectations(context, spark, azure_config)
    
    print("\n✅ Bronze expectations created successfully!")
    print(f"   Location: {gx_dir}")
    