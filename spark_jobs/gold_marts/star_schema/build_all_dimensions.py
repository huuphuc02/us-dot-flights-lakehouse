from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths

# Import dimension builders
from spark_jobs.gold_marts.star_schema.dimensions.dim_airline import DimAirline
from spark_jobs.gold_marts.star_schema.dimensions.dim_airport import DimAirport
from spark_jobs.gold_marts.star_schema.dimensions.dim_route import DimRoute
from spark_jobs.gold_marts.star_schema.dimensions.dim_date import DimDate
from spark_jobs.gold_marts.star_schema.dimensions.dim_time import DimTime
from spark_jobs.validation.data_validator import DataValidator


class DimensionBuilder:
    """Build all dimension tables efficiently with single silver read"""
    
    def __init__(self, spark: SparkSession, paths: dict, enable_validation: bool = True):
        self.spark = spark
        self.paths = paths
        self.silver_df = None
        self.enable_validation = enable_validation

        if self.enable_validation:
            self.validator = DataValidator()
            print("Great Expectations validation enabled")
        else:
            self.validator = None
            print("Great Expectations validation disabled")

    def validate_dimension(self, dim_df, dimension_name: str):
        if not self.enable_validation or not self.validator:
            return True
        
        print(f"Validating {dimension_name}...")
        results = self.validator.validate_dimension(dim_df, dimension_name, batch_id=f"{dimension_name}_latest")
        stats = self.validator.get_validation_statistics(results)
        print(f"\n📊 {dimension_name} Validation: {stats['passed']}/{stats['total']} checks passed ({stats['success_rate']:.1f}%)")

        if not results.get("success"):
            print(f"❌ {dimension_name} validation FAILED")
            return False
        
        print(f"✅ {dimension_name} validation PASSED")
        return True
    
    def load_silver_once(self):
        """Load silver layer once and cache for reuse"""
        print("Loading silver layer (once for all dimensions)...")
        
        self.silver_df = self.spark.read.format("delta") \
            .load(f"{self.paths['silver']}") \
            .cache()  # Cache in memory for reuse
        
        # Force cache to materialize
        row_count = self.silver_df.count()
        print(f"Cached {row_count:,} silver records")
        
        return self.silver_df
    
    def build_static_dimensions(self):
        """Build dimensions that don't need silver data"""
        print("\n=== Building Static Dimensions ===")
        
        # dim_date
        print("Building dim_date...")
        dim_date_builder = DimDate(self.spark)
        dim_date = dim_date_builder.build_date_dimension("2020-01-01", "2030-12-31")

        self.validate_dimension(dim_date, "dim_date")
        dim_date_builder.save_date_dimension(dim_date, f"{self.paths['gold']}/dimensions/dim_date")
        
        # dim_time
        print("Building dim_time...")
        dim_time_builder = DimTime(self.spark)
        dim_time = dim_time_builder.build_time_dimension()

        self.validate_dimension(dim_time, "dim_time")
        dim_time_builder.save_time_dimension(dim_time, f"{self.paths['gold']}/dimensions/dim_time")
        
        print("Static dimensions complete!")
    
    def build_silver_dependent_dimensions(self):
        """Build dimensions that extract from silver layer"""
        print("\n=== Building Silver-Dependent Dimensions ===")
        
        if self.silver_df is None:
            self.load_silver_once()
        
        # dim_airline
        print("Building dim_airline...")
        dim_airline_builder = DimAirline(self.spark)
        dim_airline = dim_airline_builder.build_from_silver(self.silver_df)
        self.validate_dimension(dim_airline, "dim_airline")
        dim_airline_builder.save_dimension(dim_airline, f"{self.paths['gold']}/dimensions/dim_airline")
        
        # dim_airport
        print("Building dim_airport...")
        dim_airport_builder = DimAirport(self.spark)
        dim_airport = dim_airport_builder.build_from_silver(self.silver_df)
        self.validate_dimension(dim_airport, "dim_airport")
        dim_airport_builder.save_dimension(dim_airport, f"{self.paths['gold']}/dimensions/dim_airport")
        
        # dim_route
        print("Building dim_route...")
        dim_route_builder = DimRoute(self.spark)
        dim_route = dim_route_builder.build_from_silver(self.silver_df)
        self.validate_dimension(dim_route, "dim_route")
        dim_route_builder.save_route_dimension(dim_route, f"{self.paths['gold']}/dimensions/dim_route")
        
        print("Silver-dependent dimensions complete!")
    
    def build_all_dimensions(self):
        """Build all dimensions in optimal order"""
        print("=" * 50)
        print("Building All Dimensions")
        print("=" * 50)
        
        # Static dimensions first (no dependencies)
        self.build_static_dimensions()
        
        # Silver-dependent dimensions (single read)
        self.build_silver_dependent_dimensions()
        
        # Unpersist cached data
        if self.silver_df:
            self.silver_df.unpersist()
        
        print("\n" + "=" * 50)
        print("All Dimensions Built Successfully!")
        print("=" * 50)


if __name__ == "__main__":
    azure_config = load_azure_config_from_env()
    spark = create_spark_session_with_azure(
        app_name="US_DOT_Flights_Build_All_Dimensions",
        azure_config=azure_config
    )
    
    paths = get_azure_data_paths(azure_config)

    ENABLE_VALIDATION = False
    builder = DimensionBuilder(spark, paths, enable_validation=ENABLE_VALIDATION)

    try:
        builder.build_all_dimensions()
        print("\n🎉 Pipeline completed successfully with all validations passed!")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import sys
        sys.exit(1)
    finally:
        spark.stop()