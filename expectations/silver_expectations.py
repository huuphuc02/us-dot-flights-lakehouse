import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths

def create_silver_expectations(context, spark, azure_config=None):
    """Create expectations for Silver layer (cleaned and enriched data)"""
    
    # Get Silver data paths
    if azure_config:
        paths = get_azure_data_paths(azure_config)
        silver_path = paths['silver']
    else:
        silver_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/silver"
    
    # Try to load from Azure, fallback to local if it fails
    try:
        print(f"🔍 Attempting to load Silver data from Azure...")
        print(f"   Path: {silver_path}")
        silver_df = spark.read.format("delta").load(silver_path)
        print(f"✅ Successfully loaded from Azure! Row count: {silver_df.count()}")
    except Exception as e:
        print(f"❌ Azure connection failed: {e}")
        print("🔄 Falling back to local data...")
        silver_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/silver"
        silver_df = spark.read.format("delta").load(silver_path)
        print(f"✅ Loaded from local. Row count: {silver_df.count()}")
    
    # Create expectation suite
    suite_name = "silver_flights_suite"
    context.add_or_update_expectation_suite(suite_name)
    
    # Create batch request
    batch_request = RuntimeBatchRequest(
        datasource_name="silver_flights",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="flights",
        runtime_parameters={"batch_data": silver_df},
        batch_identifiers={"batch_id": "silver_validation"}
    )
    
    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    # Define Silver layer expectations (business logic and data quality)
    print("📋 Creating Silver layer expectations...")
    
    # 1. Table should exist and have data
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    
    # 2. All required columns should exist (from your silver schema)
    silver_columns = [
        "FLIGHT_DATE", "FLIGHT_NUMBER", "AIRLINE_CODE", "AIRLINE_NAME",
        "ORIGIN_AIRPORT_CODE", "DEST_AIRPORT_CODE", 
        "ORIGIN_AIRPORT_NAME", "DEST_AIRPORT_NAME",
        "ACTUAL_DEPARTURE_TIME", "ACTUAL_ARRIVAL_TIME",
        "PLANNED_DEPARTURE_TIME", "PLANNED_ARRIVAL_TIME",
        "DEPARTURE_DELAY", "ARRIVAL_DELAY",
        "DEPARTURE_DELAY_NEW", "ARRIVAL_DELAY_NEW",
        "DEPARTURE_DELAY_CATEGORY", "ARRIVAL_DELAY_CATEGORY",
        "IS_CANCELLED", "IS_DIVERTED", "IS_DELAYED", "IS_ONTIME",
        "DISTANCE_KM", "AIR_TIME_MINUTES", "AIR_TIME_HOURS",
        "SPEED_KM/H", "ROUTE_CODE", "ROUTE_NAME",
        "IS_WEEKEND", "DATA_QUALITY_SCORE"
    ]
    for col in silver_columns:
        validator.expect_column_to_exist(col)
    
    # 3. Critical fields should not be null
    validator.expect_column_values_to_not_be_null("FLIGHT_DATE", mostly=0.99)
    validator.expect_column_values_to_not_be_null("AIRLINE_CODE", mostly=0.99)
    validator.expect_column_values_to_not_be_null("AIRLINE_NAME", mostly=0.99)
    validator.expect_column_values_to_not_be_null("ORIGIN_AIRPORT_CODE", mostly=0.99)
    validator.expect_column_values_to_not_be_null("DEST_AIRPORT_CODE", mostly=0.99)
    validator.expect_column_values_to_not_be_null("ORIGIN_AIRPORT_NAME", mostly=0.99)
    validator.expect_column_values_to_not_be_null("DEST_AIRPORT_NAME", mostly=0.99)
    
    # 4. Business rule validations - Distance
    validator.expect_column_values_to_be_between(
        "DISTANCE_KM", 
        min_value=0, 
        max_value=20000,
        mostly=0.98
    )
    
    # 5. Business rule validations - Air time
    validator.expect_column_values_to_be_between(
        "AIR_TIME_MINUTES", 
        min_value=0, 
        max_value=1440,
        mostly=0.95
    )
    
    validator.expect_column_values_to_be_between(
        "AIR_TIME_HOURS", 
        min_value=0, 
        max_value=24,
        mostly=0.95
    )
    
    # 6. Business rule validations - Delays
    validator.expect_column_values_to_be_between(
        "DEPARTURE_DELAY", 
        min_value=-200, 
        max_value=2000,
        mostly=0.98
    )
    
    validator.expect_column_values_to_be_between(
        "ARRIVAL_DELAY", 
        min_value=-200, 
        max_value=2000,
        mostly=0.98
    )
    
    # 7. Boolean fields validation
    boolean_columns = ["IS_CANCELLED", "IS_DIVERTED", "IS_DELAYED", "IS_ONTIME", "IS_WEEKEND"]
    for bool_col in boolean_columns:
        validator.expect_column_values_to_be_in_set(
            bool_col, 
            [0, 1, True, False, None],
            mostly=0.99
        )
    
    # 8. Data quality score should be between 0 and 1
    validator.expect_column_values_to_be_between(
        "DATA_QUALITY_SCORE", 
        min_value=0, 
        max_value=1
    )
    
    # 9. Airport codes should be 3 characters
    validator.expect_column_value_lengths_to_equal(
        "ORIGIN_AIRPORT_CODE", 
        3, 
        mostly=0.99
    )
    validator.expect_column_value_lengths_to_equal(
        "DEST_AIRPORT_CODE", 
        3, 
        mostly=0.99
    )
    
    # 10. Airline codes should be 2-3 characters
    validator.expect_column_value_lengths_to_be_between(
        "AIRLINE_CODE", 
        min_value=2, 
        max_value=3,
        mostly=0.99
    )
    
    # 11. Speed validation (reasonable flight speeds in km/h)
    validator.expect_column_values_to_be_between(
        "SPEED_KM/H", 
        min_value=200,  # Very slow prop planes
        max_value=1200,  # Supersonic (allow for outliers)
        mostly=0.95
    )
    
    # 12. Delay categories should be in expected set
    expected_delay_categories = [
        "On Time", "Early", "Minor Delay", 
        "Moderate Delay", "Major Delay", "Severe Delay", None
    ]
    validator.expect_column_values_to_be_in_set(
        "DEPARTURE_DELAY_CATEGORY",
        expected_delay_categories,
        mostly=0.99
    )
    validator.expect_column_values_to_be_in_set(
        "ARRIVAL_DELAY_CATEGORY",
        expected_delay_categories,
        mostly=0.99
    )
    
    # 13. Route code format validation (should be like "ATL-DFW")
    validator.expect_column_value_lengths_to_be_between(
        "ROUTE_CODE",
        min_value=7,  # Minimum like "LAX-SFO"
        max_value=7,  # Maximum like "ATL-DFW"
        mostly=0.95
    )
    
    # Save suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✅ Created Silver expectations suite: {suite_name}")
    print(f"   Total expectations: {len(validator.get_expectation_suite().expectations)}")
    
    return validator

if __name__ == "__main__":
    # Load Azure configuration from environment
    print("🔧 Loading Azure configuration...")
    azure_config = load_azure_config_from_env()
    
    # Create Spark session with Azure support
    print("⚙️ Creating Spark session with Azure Data Lake support...")
    spark = create_spark_session_with_azure(
        app_name="GX_Silver_Expectations",
        azure_config=azure_config
    )
    
    # Initialize Great Expectations context
    project_root = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse"
    gx_dir = f"{project_root}/expectations"
    
    try:
        context = gx.get_context(project_root_dir=gx_dir)
        print(f"✅ Loaded existing Great Expectations context from {gx_dir}")
    except Exception as e:
        print(f"⚠️ Could not load context: {e}")
        print("💡 Please run init_great_expectations.py first!")
        sys.exit(1)
    
    # Create Silver expectations
    print("\n" + "="*60)
    print("Creating Silver Layer Expectations")
    print("="*60 + "\n")
    
    validator = create_silver_expectations(context, spark, azure_config)
    
    print("\n✅ Silver expectations created successfully!")
    print(f"   Location: {gx_dir}")

