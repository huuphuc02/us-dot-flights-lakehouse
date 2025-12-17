from pyspark.sql import SparkSession
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths
from spark_jobs.gold_marts.star_schema.facts.fact_flights import FactFlights
from spark_jobs.validation.data_validator import DataValidator

def build_and_validate_fact_flights(spark: SparkSession, paths: dict, enable_validation: bool = True):
    """Build and validate fact flights table"""
    
    print("="*60)
    print("Building Fact Flights Table")
    print("="*60 + "\n")
    
    # Load silver and dimensions
    print("Loading silver data...")
    silver_df = spark.read.format("delta").load(paths['silver'])
    
    print("Loading dimensions...")
    dim_date = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_date")
    dim_time = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_time")
    dim_airport = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_airport")
    dim_airline = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_airline")
    dim_route = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_route")
    
    # Build fact table
    print("\nBuilding fact table...")
    fact_builder = FactFlights(spark)
    fact_flights = fact_builder.build_fact_table(
        silver_df, dim_date, dim_time, dim_airport, dim_airline, dim_route
    )
    
    # Validate fact table
    if enable_validation:
        print("\n" + "="*60)
        print("Validating Fact Flights Table")
        print("="*60)
        
        validator = DataValidator()
        results = validator.validate_fact_table(fact_flights, fact_name="fact_flights")
        
        stats = validator.get_validation_statistics(results)
        print(f"\n📊 Fact Validation: {stats['passed']}/{stats['total']} checks passed ({stats['success_rate']:.1f}%)")
        
        if not results.get("success"):
            print("\n❌ ERROR: Fact table validation failed!")
            raise Exception("Fact table validation failed")
        
        print("\n✅ Fact table validation passed!")
    
    # Save fact table
    print("\nSaving fact table...")
    output_path = f"{paths['gold']}/facts/fact_flights"
    fact_flights.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)
    
    print(f"✅ Fact table saved to {output_path}")
    
    return fact_flights

if __name__ == "__main__":
    azure_config = load_azure_config_from_env()
    spark = create_spark_session_with_azure(
        app_name="US_DOT_Flights_Build_Fact_Flights",
        azure_config=azure_config
    )
    
    paths = get_azure_data_paths(azure_config)
    
    # Enable validation
    ENABLE_VALIDATION = False
    
    try:
        build_and_validate_fact_flights(spark, paths, enable_validation=ENABLE_VALIDATION)
        print("\n🎉 Fact table built and validated successfully!")
    except Exception as e:
        print(f"\n❌ Fact table building failed: {e}")
        import sys
        sys.exit(1)
    finally:
        spark.stop()