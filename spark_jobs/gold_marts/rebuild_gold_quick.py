"""
Quick rebuild of gold layer with fixed DATE_KEY format
Option A: Rebuild dimensions and fact table only (keep existing silver data)
"""
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")

from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths
from spark_jobs.gold_marts.star_schema.dimensions.dim_date import DimDate
from spark_jobs.gold_marts.star_schema.dimensions.dim_time import DimTime
from spark_jobs.gold_marts.star_schema.dimensions.dim_airline import DimAirline
from spark_jobs.gold_marts.star_schema.dimensions.dim_airport import DimAirport
from spark_jobs.gold_marts.star_schema.dimensions.dim_route import DimRoute
from spark_jobs.gold_marts.star_schema.facts.fact_flights import FactFlights

def main():
    print("\n" + "="*70)
    print("QUICK REBUILD - GOLD LAYER (Option A)")
    print("="*70)
    print("Fixes applied:")
    print("  1. dim_date: INTEGER DATE_KEY format (yyyyMMdd)")
    print("  2. fact_flights: Fixed time joins (HH:MM → HHMM)")
    print("  3. Keep existing silver data (with numeric codes)")
    print("="*70 + "\n")
    
    # Setup
    azure_config = load_azure_config_from_env()
    spark = create_spark_session_with_azure(
        app_name="US_DOT_Quick_Rebuild_Gold",
        azure_config=azure_config
    )
    paths = get_azure_data_paths(azure_config)
    gold_path = paths['gold']
    
    # Step 1: Rebuild dim_date with correct format
    print("\n" + "="*70)
    print("STEP 1: Rebuilding dim_date with INTEGER DATE_KEY")
    print("="*70)
    
    dim_date_builder = DimDate(spark)
    dim_date = dim_date_builder.build_date_dimension("2020-01-01", "2030-12-31")
    
    print(f"\nSample of new dim_date:")
    dim_date.show(5, truncate=False)
    print(f"\nSchema:")
    dim_date.printSchema()
    
    dim_date_output = f"{gold_path}/dimensions/dim_date"
    dim_date_builder.save_date_dimension(dim_date, dim_date_output)
    print(f"✅ dim_date saved with INTEGER DATE_KEY format to: {dim_date_output}")
    
    # Step 2: Rebuild fact_flights
    print("\n" + "="*70)
    print("STEP 2: Rebuilding fact_flights")
    print("="*70)
    
    # Load silver
    print("Loading silver data...")
    silver_df = spark.read.format("delta").load(paths['silver'])
    print(f"  → Silver records: {silver_df.count():,}")
    
    # Load all dimensions
    print("Loading dimensions...")
    dim_time = spark.read.format("delta").load(f"{gold_path}/dimensions/dim_time")
    dim_airline = spark.read.format("delta").load(f"{gold_path}/dimensions/dim_airline")
    dim_airport = spark.read.format("delta").load(f"{gold_path}/dimensions/dim_airport")
    dim_route = spark.read.format("delta").load(f"{gold_path}/dimensions/dim_route")
    print(f"  → dim_date: {dim_date.count():,} records")
    print(f"  → dim_time: {dim_time.count():,} records")
    print(f"  → dim_airline: {dim_airline.count():,} records")
    print(f"  → dim_airport: {dim_airport.count():,} records")
    print(f"  → dim_route: {dim_route.count():,} records")
    
    # Build fact table
    print("\nBuilding fact_flights...")
    fact_builder = FactFlights(spark)
    fact_flights = fact_builder.build_fact_table(
        silver_df, dim_date, dim_time, dim_airport, dim_airline, dim_route
    )
    
    fact_count = fact_flights.count()
    print(f"  → Fact records: {fact_count:,}")
    
    print(f"\nSample of fact_flights:")
    fact_flights.show(5, truncate=False)
    
    # Check DATE_KEY population
    date_key_nulls = fact_flights.filter("DATE_KEY IS NULL").count()
    date_key_populated = fact_count - date_key_nulls
    print(f"\nDATE_KEY Check:")
    print(f"  → Populated: {date_key_populated:,} ({date_key_populated/fact_count*100:.1f}%)")
    print(f"  → NULL: {date_key_nulls:,} ({date_key_nulls/fact_count*100:.1f}%)")
    
    if date_key_nulls > fact_count * 0.5:
        print("\n⚠️  WARNING: More than 50% of DATE_KEY values are NULL!")
        print("   This suggests the join is still failing. Checking date formats...")
        
        # Debug info
        print("\nSilver FLIGHT_DATE sample:")
        silver_df.select("FLIGHT_DATE").show(5, truncate=False)
        
        print("\ndim_date DATE_KEY sample:")
        dim_date.select("DATE_KEY", "FULL_DATE").show(5, truncate=False)
    
    # Save fact table
    fact_output = f"{gold_path}/facts/fact_flights"
    print(f"\nSaving fact_flights to: {fact_output}")
    fact_flights.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(fact_output)
    
    print(f"✅ fact_flights rebuilt successfully!")
    
    # Summary
    print("\n" + "="*70)
    print("REBUILD COMPLETE")
    print("="*70)
    print(f"✅ dim_date: Rebuilt with INTEGER DATE_KEY")
    print(f"✅ fact_flights: {fact_count:,} records")
    print(f"   - DATE_KEY populated: {date_key_populated:,} ({date_key_populated/fact_count*100:.1f}%)")
    print("\nNext Steps:")
    print("  1. Run: python spark_jobs/gold_marts/aggregates/build_all_aggregates.py")
    print("  2. Run: python spark_jobs/gold_marts/aggregates/export_aggregates.py")
    print("="*70 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()

