from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths

# Import aggregate builders
from spark_jobs.gold_marts.aggregates.daily_airline_performance import DailyAirlinePerformance
from spark_jobs.gold_marts.aggregates.daily_airport_performance import DailyAirportPerformanceMart
from spark_jobs.gold_marts.aggregates.route_performance import RoutePerformanceMart


class AggregateBuilder:
    """Build all aggregate marts efficiently with single fact/dimension reads"""
    
    def __init__(self, spark: SparkSession, paths: dict, sample_output_dir: str = None):
        self.spark = spark
        self.paths = paths
        
        # Cache these once
        self.fact_flights = None
        self.dim_date = None
        self.dim_airline = None
        self.dim_airport = None
        self.dim_route = None
        
        # Sample output directory
        self.sample_output_dir = sample_output_dir or "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/samples"
        
        # Create sample output directory if it doesn't exist
        os.makedirs(self.sample_output_dir, exist_ok=True)
    
    def save_sample(self, df, table_name: str, num_rows: int = 10):
        """Save sample data to CSV for verification"""
        sample_file = f"{self.sample_output_dir}/{table_name}_sample.csv"
        print(f"  → Saving sample to: {sample_file}")
        
        # Save sample
        sample_df = df.limit(num_rows)
        sample_df.coalesce(1).write.format("csv") \
            .option("header", "true") \
            .mode("overwrite") \
            .save(sample_file)
        
        # Show sample in console
        print(f"\n  Sample data for {table_name}:")
        sample_df.show(num_rows, truncate=False)
    
    def load_data_sources(self):
        """Load fact table and dimensions once, cache for reuse"""
        print("=" * 50)
        print("Loading Fact Table and Dimensions")
        print("=" * 50)
        
        gold_path = self.paths['gold']
        
        # Load and cache fact table
        print("\nLoading fact_flights...")
        self.fact_flights = self.spark.read.format("delta") \
            .load(f"{gold_path}/facts/fact_flights") \
            .cache()
        fact_count = self.fact_flights.count()
        print(f"  → Cached {fact_count:,} fact records")
        if fact_count > 0:
            self.save_sample(self.fact_flights, "fact_flights", 10)
        else:
            print("  ⚠️  WARNING: fact_flights is EMPTY!")
        
        # Load dimensions (smaller, but cache for joins)
        print("\nLoading dim_date...")
        self.dim_date = self.spark.read.format("delta") \
            .load(f"{gold_path}/dimensions/dim_date").cache()
        date_count = self.dim_date.count()
        print(f"  → dim_date: {date_count:,} records")
        if date_count > 0:
            self.save_sample(self.dim_date, "dim_date", 5)
        else:
            print("  ⚠️  WARNING: dim_date is EMPTY!")
        
        print("\nLoading dim_airline...")
        self.dim_airline = self.spark.read.format("delta") \
            .load(f"{gold_path}/dimensions/dim_airline").cache()
        airline_count = self.dim_airline.count()
        print(f"  → dim_airline: {airline_count:,} records")
        if airline_count > 0:
            self.save_sample(self.dim_airline, "dim_airline", 5)
        else:
            print("  ⚠️  WARNING: dim_airline is EMPTY!")
        
        print("\nLoading dim_airport...")
        self.dim_airport = self.spark.read.format("delta") \
            .load(f"{gold_path}/dimensions/dim_airport").cache()
        airport_count = self.dim_airport.count()
        print(f"  → dim_airport: {airport_count:,} records")
        if airport_count > 0:
            self.save_sample(self.dim_airport, "dim_airport", 5)
        else:
            print("  ⚠️  WARNING: dim_airport is EMPTY!")
        
        print("\nLoading dim_route...")
        self.dim_route = self.spark.read.format("delta") \
            .load(f"{gold_path}/dimensions/dim_route").cache()
        route_count = self.dim_route.count()
        print(f"  → dim_route: {route_count:,} records")
        if route_count > 0:
            self.save_sample(self.dim_route, "dim_route", 5)
        else:
            print("  ⚠️  WARNING: dim_route is EMPTY!")
        
        print("\n" + "=" * 50)
        print("All data sources loaded and cached!")
        print(f"Samples saved to: {self.sample_output_dir}")
        print("=" * 50 + "\n")
    
    def build_daily_airline_performance(self):
        """Build daily airline performance mart"""
        print("Building daily_airline_performance...")
        
        builder = DailyAirlinePerformance(self.spark)
        mart = builder.build_from_fact(
            self.fact_flights, 
            self.dim_date, 
            self.dim_airline
        )
        
        output_path = f"{self.paths['gold']}/aggregates/daily_airline_performance"
        builder.save_mart(mart, output_path)
        print(f"  → Saved to {output_path}\n")
    
    def build_daily_airport_performance(self):
        """Build daily airport performance mart"""
        print("Building daily_airport_performance...")
        
        builder = DailyAirportPerformanceMart()
        mart = builder.build_from_fact(
            self.fact_flights,
            self.dim_date,
            self.dim_airport
        )
        
        output_path = f"{self.paths['gold']}/aggregates/daily_airport_performance"
        builder.save_mart(mart, output_path)
        print(f"  → Saved to {output_path}\n")
    
    def build_route_performance(self):
        """Build route performance mart"""
        print("Building route_performance...")
        
        builder = RoutePerformanceMart()
        mart = builder.build_from_fact(
            self.fact_flights,
            self.dim_date,
            self.dim_route,
            self.dim_airline
        )
        
        output_path = f"{self.paths['gold']}/aggregates/route_performance"
        builder.save_mart(mart, output_path)
        print(f"  → Saved to {output_path}\n")
    
    def build_all_aggregates(self):
        """Build all aggregate marts"""
        print("\n" + "=" * 50)
        print("Building All Aggregate Marts")
        print("=" * 50 + "\n")
        
        # Load data once
        self.load_data_sources()
        
        # Build each aggregate
        self.build_daily_airline_performance()
        self.build_daily_airport_performance()
        self.build_route_performance()
        
        # Cleanup
        self.cleanup()
        
        print("=" * 50)
        print("All Aggregate Marts Built Successfully!")
        print("=" * 50)
    
    def cleanup(self):
        """Unpersist cached data"""
        print("Cleaning up cached data...")
        if self.fact_flights:
            self.fact_flights.unpersist()
        if self.dim_date:
            self.dim_date.unpersist()
        if self.dim_airline:
            self.dim_airline.unpersist()
        if self.dim_airport:
            self.dim_airport.unpersist()
        if self.dim_route:
            self.dim_route.unpersist()


if __name__ == "__main__":
    azure_config = load_azure_config_from_env()
    spark = create_spark_session_with_azure(
        app_name="US_DOT_Flights_Build_All_Aggregates",
        azure_config=azure_config
    )
    
    paths = get_azure_data_paths(azure_config)
    
    builder = AggregateBuilder(spark, paths)
    builder.build_all_aggregates()
    
    spark.stop()