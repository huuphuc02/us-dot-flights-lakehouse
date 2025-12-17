import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths

def create_dim_airline_expectations(context, spark, azure_config=None):
    """Create expectations for dim_airline dimension table"""
    
    # Get Gold data paths
    if azure_config:
        paths = get_azure_data_paths(azure_config)
        dim_path = f"{paths['gold']}/dimensions/dim_airline"
    else:
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_airline"
    
    # Try to load from Azure, fallback to local if it fails
    try:
        print(f"🔍 Loading dim_airline from Azure...")
        print(f"   Path: {dim_path}")
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Successfully loaded! Row count: {dim_df.count()}")
    except Exception as e:
        print(f"❌ Azure connection failed: {e}")
        print("🔄 Falling back to local data...")
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_airline"
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Loaded from local. Row count: {dim_df.count()}")
    
    suite_name = "gold_dim_airline_suite"
    context.add_or_update_expectation_suite(suite_name)
    
    batch_request = RuntimeBatchRequest(
        datasource_name="gold_dimensions",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="dim_airline",
        runtime_parameters={"batch_data": dim_df},
        batch_identifiers={"batch_id": "dim_airline_validation"}
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print("📋 Creating dim_airline expectations...")
    
    # 1. Table should have data
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    
    # 2. Required columns
    validator.expect_column_to_exist("AIRLINE_CODE")
    validator.expect_column_to_exist("AIRLINE_NAME")
    validator.expect_column_to_exist("created_at")
    validator.expect_column_to_exist("updated_at")
    
    # 3. Primary key should not be null and should be unique
    validator.expect_column_values_to_not_be_null("AIRLINE_CODE")
    validator.expect_column_values_to_be_unique("AIRLINE_CODE")
    
    # 4. Airline name should not be null
    validator.expect_column_values_to_not_be_null("AIRLINE_NAME")
    
    # 5. Airline codes should be 2-3 characters
    validator.expect_column_value_lengths_to_be_between("AIRLINE_CODE", min_value=2, max_value=3)
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✅ Created dim_airline expectations: {len(validator.get_expectation_suite().expectations)} expectations")
    
    return validator

def create_dim_airport_expectations(context, spark, azure_config=None):
    """Create expectations for dim_airport dimension table"""
    
    # Get Gold data paths
    if azure_config:
        paths = get_azure_data_paths(azure_config)
        dim_path = f"{paths['gold']}/dimensions/dim_airport"
    else:
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_airport"
    
    try:
        print(f"🔍 Loading dim_airport from Azure...")
        print(f"   Path: {dim_path}")
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Successfully loaded! Row count: {dim_df.count()}")
    except Exception as e:
        print(f"❌ Azure connection failed: {e}")
        print("🔄 Falling back to local data...")
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_airport"
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Loaded from local. Row count: {dim_df.count()}")
    
    suite_name = "gold_dim_airport_suite"
    context.add_or_update_expectation_suite(suite_name)
    
    batch_request = RuntimeBatchRequest(
        datasource_name="gold_dimensions",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="dim_airport",
        runtime_parameters={"batch_data": dim_df},
        batch_identifiers={"batch_id": "dim_airport_validation"}
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print("📋 Creating dim_airport expectations...")
    
    # 1. Table should have data
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    
    # 2. Required columns
    validator.expect_column_to_exist("AIRPORT_CODE")
    validator.expect_column_to_exist("AIRPORT_NAME")
    validator.expect_column_to_exist("created_at")
    validator.expect_column_to_exist("updated_at")
    
    # 3. Primary key should not be null and should be unique
    validator.expect_column_values_to_not_be_null("AIRPORT_CODE")
    validator.expect_column_values_to_be_unique("AIRPORT_CODE")
    
    # 4. Airport name should not be null
    validator.expect_column_values_to_not_be_null("AIRPORT_NAME")
    
    # 5. Airport codes should be exactly 3 characters
    validator.expect_column_value_lengths_to_equal("AIRPORT_CODE", 3)
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✅ Created dim_airport expectations: {len(validator.get_expectation_suite().expectations)} expectations")
    
    return validator

def create_dim_date_expectations(context, spark, azure_config=None):
    """Create expectations for dim_date dimension table"""
    
    # Get Gold data paths
    if azure_config:
        paths = get_azure_data_paths(azure_config)
        dim_path = f"{paths['gold']}/dimensions/dim_date"
    else:
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_date"
    
    try:
        print(f"🔍 Loading dim_date from Azure...")
        print(f"   Path: {dim_path}")
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Successfully loaded! Row count: {dim_df.count()}")
    except Exception as e:
        print(f"❌ Azure connection failed: {e}")
        print("🔄 Falling back to local data...")
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_date"
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Loaded from local. Row count: {dim_df.count()}")
    
    suite_name = "gold_dim_date_suite"
    context.add_or_update_expectation_suite(suite_name)
    
    batch_request = RuntimeBatchRequest(
        datasource_name="gold_dimensions",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="dim_date",
        runtime_parameters={"batch_data": dim_df},
        batch_identifiers={"batch_id": "dim_date_validation"}
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print("📋 Creating dim_date expectations...")
    
    # 1. Table should have data
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    
    # 2. Required columns
    validator.expect_column_to_exist("DATE_KEY")
    
    # 3. Primary key should not be null and should be unique
    validator.expect_column_values_to_not_be_null("DATE_KEY")
    validator.expect_column_values_to_be_unique("DATE_KEY")
    
    # 4. Date attributes validation (if they exist)
    date_columns = ["DAY", "MONTH", "YEAR", "DAY_OF_WEEK", "QUARTER"]
    for col in date_columns:
        try:
            validator.expect_column_to_exist(col)
        except:
            pass
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✅ Created dim_date expectations: {len(validator.get_expectation_suite().expectations)} expectations")
    
    return validator

def create_dim_time_expectations(context, spark, azure_config=None):
    """Create expectations for dim_time dimension table"""
    
    # Get Gold data paths
    if azure_config:
        paths = get_azure_data_paths(azure_config)
        dim_path = f"{paths['gold']}/dimensions/dim_time"
    else:
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_time"
    
    try:
        print(f"🔍 Loading dim_time from Azure...")
        print(f"   Path: {dim_path}")
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Successfully loaded! Row count: {dim_df.count()}")
    except Exception as e:
        print(f"❌ Azure connection failed: {e}")
        print("🔄 Falling back to local data...")
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_time"
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Loaded from local. Row count: {dim_df.count()}")
    
    suite_name = "gold_dim_time_suite"
    context.add_or_update_expectation_suite(suite_name)
    
    batch_request = RuntimeBatchRequest(
        datasource_name="gold_dimensions",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="dim_time",
        runtime_parameters={"batch_data": dim_df},
        batch_identifiers={"batch_id": "dim_time_validation"}
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print("📋 Creating dim_time expectations...")
    
    # 1. Table should have data (at least 1440 minutes in a day or 24 hours)
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    
    # 2. Required columns
    validator.expect_column_to_exist("TIME_KEY")
    
    # 3. Primary key should not be null and should be unique
    validator.expect_column_values_to_not_be_null("TIME_KEY")
    validator.expect_column_values_to_be_unique("TIME_KEY")
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✅ Created dim_time expectations: {len(validator.get_expectation_suite().expectations)} expectations")
    
    return validator

def create_dim_route_expectations(context, spark, azure_config=None):
    """Create expectations for dim_route dimension table"""
    
    # Get Gold data paths
    if azure_config:
        paths = get_azure_data_paths(azure_config)
        dim_path = f"{paths['gold']}/dimensions/dim_route"
    else:
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_route"
    
    try:
        print(f"🔍 Loading dim_route from Azure...")
        print(f"   Path: {dim_path}")
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Successfully loaded! Row count: {dim_df.count()}")
    except Exception as e:
        print(f"❌ Azure connection failed: {e}")
        print("🔄 Falling back to local data...")
        dim_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/dimensions/dim_route"
        dim_df = spark.read.format("delta").load(dim_path)
        print(f"✅ Loaded from local. Row count: {dim_df.count()}")
    
    suite_name = "gold_dim_route_suite"
    context.add_or_update_expectation_suite(suite_name)
    
    batch_request = RuntimeBatchRequest(
        datasource_name="gold_dimensions",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="dim_route",
        runtime_parameters={"batch_data": dim_df},
        batch_identifiers={"batch_id": "dim_route_validation"}
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print("📋 Creating dim_route expectations...")
    
    # 1. Table should have data
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    
    # 2. Required columns
    validator.expect_column_to_exist("ROUTE_CODE")
    validator.expect_column_to_exist("ROUTE_NAME")
    
    # 3. Primary key should not be null and should be unique
    validator.expect_column_values_to_not_be_null("ROUTE_CODE")
    validator.expect_column_values_to_be_unique("ROUTE_CODE")
    
    # 4. Route name should not be null
    validator.expect_column_values_to_not_be_null("ROUTE_NAME")
    
    # 5. Route code format validation (should be like "ATL-DFW")
    validator.expect_column_value_lengths_to_equal("ROUTE_CODE", 7, mostly=0.95)
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✅ Created dim_route expectations: {len(validator.get_expectation_suite().expectations)} expectations")
    
    return validator

def create_fact_flights_expectations(context, spark, azure_config=None):
    """Create expectations for fact_flights fact table"""
    
    # Get Gold data paths
    if azure_config:
        paths = get_azure_data_paths(azure_config)
        fact_path = f"{paths['gold']}/facts/fact_flights"
    else:
        fact_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/facts/fact_flights"
    
    try:
        print(f"🔍 Loading fact_flights from Azure...")
        print(f"   Path: {fact_path}")
        fact_df = spark.read.format("delta").load(fact_path)
        print(f"✅ Successfully loaded! Row count: {fact_df.count()}")
    except Exception as e:
        print(f"❌ Azure connection failed: {e}")
        print("🔄 Falling back to local data...")
        fact_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/gold/facts/fact_flights"
        fact_df = spark.read.format("delta").load(fact_path)
        print(f"✅ Loaded from local. Row count: {fact_df.count()}")
    
    suite_name = "gold_fact_flights_suite"
    context.add_or_update_expectation_suite(suite_name)
    
    batch_request = RuntimeBatchRequest(
        datasource_name="gold_facts",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="fact_flights",
        runtime_parameters={"batch_data": fact_df},
        batch_identifiers={"batch_id": "fact_flights_validation"}
    )
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print("📋 Creating fact_flights expectations...")
    
    # 1. Table should have data
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    
    # 2. Foreign key columns should exist
    fk_columns = [
        "DATE_KEY", "AIRLINE_CODE", "ORIGIN_AIRPORT_CODE", 
        "DEST_AIRPORT_CODE", "ROUTE_CODE"
    ]
    for col in fk_columns:
        validator.expect_column_to_exist(col)
    
    # 3. Measure columns should exist
    measure_columns = [
        "DEPARTURE_DELAY", "ARRIVAL_DELAY", "AIR_TIME_MINUTES",
        "DISTANCE_KM", "DATA_QUALITY_SCORE"
    ]
    for col in measure_columns:
        validator.expect_column_to_exist(col)
    
    # 4. Boolean flag columns should exist
    flag_columns = [
        "IS_CANCELLED", "IS_DIVERTED", "IS_DELAYED", "IS_ONTIME", "IS_WEEKEND"
    ]
    for col in flag_columns:
        validator.expect_column_to_exist(col)
    
    # 5. Foreign keys should mostly not be null (some joins may fail)
    validator.expect_column_values_to_not_be_null("DATE_KEY", mostly=0.95)
    validator.expect_column_values_to_not_be_null("AIRLINE_CODE", mostly=0.95)
    validator.expect_column_values_to_not_be_null("ORIGIN_AIRPORT_CODE", mostly=0.95)
    validator.expect_column_values_to_not_be_null("DEST_AIRPORT_CODE", mostly=0.95)
    
    # 6. Measures should be within reasonable ranges
    validator.expect_column_values_to_be_between("DISTANCE_KM", min_value=0, max_value=20000, mostly=0.98)
    validator.expect_column_values_to_be_between("AIR_TIME_MINUTES", min_value=0, max_value=1440, mostly=0.95)
    validator.expect_column_values_to_be_between("DATA_QUALITY_SCORE", min_value=0, max_value=1)
    
    # 7. Boolean flags should be 0 or 1
    for flag_col in flag_columns:
        validator.expect_column_values_to_be_in_set(flag_col, [0, 1, True, False, None], mostly=0.99)
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"✅ Created fact_flights expectations: {len(validator.get_expectation_suite().expectations)} expectations")
    
    return validator

if __name__ == "__main__":
    # Load Azure configuration from environment
    print("🔧 Loading Azure configuration...")
    azure_config = load_azure_config_from_env()
    
    # Create Spark session with Azure support
    print("⚙️ Creating Spark session with Azure Data Lake support...")
    spark = create_spark_session_with_azure(
        app_name="GX_Gold_Expectations",
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
    
    # Create Gold layer expectations
    print("\n" + "="*60)
    print("Creating Gold Layer Expectations - Dimensions")
    print("="*60 + "\n")
    
    create_dim_airline_expectations(context, spark, azure_config)
    print()
    create_dim_airport_expectations(context, spark, azure_config)
    print()
    create_dim_date_expectations(context, spark, azure_config)
    print()
    create_dim_time_expectations(context, spark, azure_config)
    print()
    create_dim_route_expectations(context, spark, azure_config)
    
    print("\n" + "="*60)
    print("Creating Gold Layer Expectations - Facts")
    print("="*60 + "\n")
    
    create_fact_flights_expectations(context, spark, azure_config)
    
    print("\n" + "="*60)
    print("✅ All Gold layer expectations created successfully!")
    print("="*60)
    print(f"\nLocation: {gx_dir}")

