from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
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
    silver_count = silver_df.count()
    print(f"  → Silver records: {silver_count:,}")
    
    print("\nLoading dimensions (with caching for joins)...")
    # Cache small dimensions for broadcast joins
    dim_date = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_date").cache()
    print(f"  → dim_date: {dim_date.count():,} records (cached)")
    
    dim_time = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_time").cache()
    print(f"  → dim_time: {dim_time.count():,} records (cached)")
    
    dim_airport = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_airport").cache()
    print(f"  → dim_airport: {dim_airport.count():,} records (cached)")
    
    dim_airline = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_airline").cache()
    print(f"  → dim_airline: {dim_airline.count():,} records (cached)")
    
    dim_route = spark.read.format("delta").load(f"{paths['gold']}/dimensions/dim_route").cache()
    print(f"  → dim_route: {dim_route.count():,} records (cached)")
    
    # Build fact table
    print("\nBuilding fact table...")
    fact_builder = FactFlights(spark)
    fact_flights = fact_builder.build_fact_table(
        silver_df, dim_date, dim_time, dim_airport, dim_airline, dim_route
    )

    fact_count = fact_flights.count()
    print(f"\n✅ Fact table built: {fact_count:,} records")
    
    # Verify joins
    date_key_nulls = fact_flights.filter("DATE_KEY IS NULL").count()
    time_key_nulls = fact_flights.filter("ACTUAL_DEPARTURE_TIME_KEY IS NULL AND ACTUAL_DEPARTURE_TIME IS NOT NULL").count()
    
    print(f"\n📊 Join Quality:")
    print(f"  → DATE_KEY populated: {(fact_count-date_key_nulls)/fact_count*100:.1f}%")
    print(f"  → TIME_KEY populated: {(fact_count-time_key_nulls)/fact_count*100:.1f}%")
    
    print("\nSample fact_flights:")
    fact_flights.show(5, truncate=False)
    
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
    
    # Save fact table with partitioning for better query performance
    print("\nSaving fact table (partitioned by DATE_KEY)...")
    output_path = f"{paths['gold']}/facts/fact_flights"
    
    # Repartition before write for better file sizes
    num_partitions = max(200, silver_count // 100000)  # ~100k records per partition
    print(f"  → Repartitioning to {num_partitions} partitions...")
    
    fact_flights.repartition(num_partitions, "DATE_KEY") \
        .write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("DATE_KEY") \
        .save(output_path)
    
    print(f"✅ Fact table saved to {output_path}")
    
    # Unpersist cached dimensions
    dim_date.unpersist()
    dim_time.unpersist()
    dim_airport.unpersist()
    dim_airline.unpersist()
    dim_route.unpersist()
    
    return fact_flights

if __name__ == "__main__":
    azure_config = load_azure_config_from_env()
    spark = create_spark_session_with_azure(
        app_name="US_DOT_Flights_Build_Fact_Flights",
        azure_config=azure_config,
        master="local[*]",
        memory="8g"  # Increased memory for fact table build
    )
    
    # Enable broadcast joins for small dimensions
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
    # Enable adaptive query execution for better join strategy
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
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