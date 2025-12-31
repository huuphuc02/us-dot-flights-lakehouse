from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from pyspark.sql.functions import col
from spark_jobs.silver_transform.flight_data_cleaner import FlightDataCleaner
from spark_jobs.silver_transform.flight_data_enricher import FlightDataEnricher
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths
from delta import configure_spark_with_delta_pip
from spark_jobs.validation.data_validator import DataValidator

class FlightSilverTransformer:
    """Handles all Silver layer transformations for flight data"""

    def __init__(self, spark: SparkSession, enable_validation: bool = True):
        self.spark = spark
        self.cleaner = FlightDataCleaner(spark)
        self.enricher = FlightDataEnricher(spark)
        self.enable_validation = enable_validation

        if self.enable_validation:
            self.validator = DataValidator()
            print("Great Expectations validation enabled")
        else:
            self.validator = None
            print("Great Expectations validation disabled")

    def clean_flights_data(self, bronze_df):
        """Clean and transform flights data from Bronze layer to Silver layer"""

        print("Starting Silver layer data cleaning and transformation")

        # 1. Clean datetime fields first
        df_datetime_cleaned = self.cleaner.clean_datetime_fields(bronze_df)

        # 2. Handle missing values
        df_cleaned = self.cleaner.handle_missing_values(df_datetime_cleaned)

        # 3. Standardize data types
        df_typed = self.cleaner.standardize_data_types(df_cleaned)

        # 4. Clean and validate business rules
        df_validated = self.cleaner.validate_business_rules(df_typed)      

        # 5. Standardize text fields
        df_standardized = self.cleaner.standardize_text_fields(df_validated)

        return df_standardized

    def enrich_data(self, df):
        """Add calculated fields and business metrics"""
        print("Enriching data with calculated fields and business metrics")

        #1. Delay categorization
        df_delays = self.enricher.categorize_delays(df)

        #2. Performance metrics
        df_performance = self.enricher.calculate_performance_metrics(df_delays)

        #3. Route and operational metrics
        df_routes = self.enricher.add_operational_metrics(df_performance)

        return df_routes

    def create_final_silver_schema(self, df):
        print("Creating final silver schema")
        
        from pyspark.sql.functions import date_format

        df_final = df.select(
            # Core identifiers
            col("FL_DATE_PARSED").alias("FLIGHT_DATE"),
            col("OP_CARRIER_FL_NUM").alias("FLIGHT_NUMBER"),
            col("AIRLINE_CODE_CLEAN").alias("AIRLINE_CODE"),
            col("CARRIER_NAME_CLEAN").alias("AIRLINE_NAME"),
            col("ORIGIN_AIRPORT_CLEAN").alias("ORIGIN_AIRPORT_CODE"),
            col("DEST_AIRPORT_CLEAN").alias("DEST_AIRPORT_CODE"),
            col("ORIGIN_AIRPORT_NAME_CLEAN").alias("ORIGIN_AIRPORT_NAME"),
            col("DEST_AIRPORT_NAME_CLEAN").alias("DEST_AIRPORT_NAME"),

            col("DEP_TIME_PARSED").alias("ACTUAL_DEPARTURE_TIME"),
            col("ARR_TIME_PARSED").alias("ACTUAL_ARRIVAL_TIME"),
            col("CRS_DEP_TIME_PARSED").alias("PLANNED_DEPARTURE_TIME"),
            col("CRS_ARR_TIME_PARSED").alias("PLANNED_ARRIVAL_TIME"),

            # Delay metrics
            col("DEP_DELAY_CLEAN").alias("DEPARTURE_DELAY"),
            col("ARR_DELAY_CLEAN").alias("ARRIVAL_DELAY"),
            col("DEP_DELAY_NEW").alias("DEPARTURE_DELAY_NEW"),
            col("ARR_DELAY_NEW").alias("ARRIVAL_DELAY_NEW"),

            # Delay categories
            col("DEP_DELAY_CATEGORY").alias("DEPARTURE_DELAY_CATEGORY"),
            col("ARR_DELAY_CATEGORY").alias("ARRIVAL_DELAY_CATEGORY"),
            col("IS_DELAYED").alias("IS_DELAYED"),
            col("IS_ONTIME").alias("IS_ONTIME"),

            # Operation status
            col("CANCELLED_BOOL").alias("IS_CANCELLED"),
            col("DIVERTED_BOOL").alias("IS_DIVERTED"),

            # Flight details
            col("AIR_TIME_CLEAN").alias("AIR_TIME_MINUTES"),
            col("AIR_TIME_HOURS_CLEAN").alias("AIR_TIME_HOURS"),
            col("DISTANCE_KM").alias("DISTANCE_KM"),
            col("SPEED_KM_H").alias("SPEED_KM_H"),

            # Operational metrics
            col("ROUTE_CODE").alias("ROUTE_CODE"),
            col("ROUTE_NAME").alias("ROUTE_NAME"),
            col("IS_WEEKEND").alias("IS_WEEKEND"),

            # Data quality
            col("has_missing_times").alias("HAS_MISSING_TIMES"),
            col("has_missing_delays").alias("HAS_MISSING_DELAYS"),
            col("AIR_TIME_VALID").alias("AIR_TIME_VALID"),
            col("DISTANCE_VALID").alias("DISTANCE_VALID"),
            col("REASONABLE_DELAYS").alias("REASONABLE_DELAYS"),
            col("DATA_QUALITY_SCORE").alias("DATA_QUALITY_SCORE"),
        )

        print(f"Final silver schema created with {len(df_final.columns)} columns")

        return df_final

    def save_silver_data(self, df, output_path, partition_by=["FLIGHT_DATE", "AIRLINE_CODE"]):
        print(f"Saving silver data to delta table at {output_path}")

        df.write.format("delta").mode("overwrite").partitionBy(*partition_by).option("overwriteSchema", "true").save(output_path)

        print(f"Silver data saved to {output_path}")

        return df

    def validate_silver_output(self, df, batch_id: str = 'silver_output'):
        """Validate silver output data"""
        if not self.enable_validation or not self.validator:
            return True
        
        print(f"Validating silver output data (batch: {batch_id})...")
        results = self.validator.validate_silver_data(df, batch_id=batch_id)
        stats = self.validator.get_validation_statistics(results)
        print(f"\n📊 Silver Validation: {stats['passed']}/{stats['total']} checks passed ({stats['success_rate']:.1f}%)")
        return results.get("success", False)

    def run_silver_transformation(self, bronze_df, output_path, batch_id: str = 'silver_output'):
        print("Running silver transformation")

        # Clean and transform data
        df_cleaned = self.clean_flights_data(bronze_df)

        # Enrich data
        df_enriched = self.enrich_data(df_cleaned)

        # Create final silver schema
        df_final = self.create_final_silver_schema(df_enriched)

        if self.enable_validation:
            silver_valid = self.validate_silver_output(df_final, batch_id=batch_id)
            if not silver_valid:
                print(f"❌ Silver validation FAILED")
                raise Exception("Silver validation failed")
            print(f"✅ Silver validation PASSED")

        if output_path:
            self.save_silver_data(df_final, output_path)

        print("Silver transformation completed successfully")

        return df_final

if __name__ == "__main__":
    # Load Azure configuration
    azure_config = load_azure_config_from_env()
    
    # Create Spark session with Azure support
    spark = create_spark_session_with_azure(
        app_name="US_DOT_Flights_Silver_Transformation",
        azure_config=azure_config
    )

    ENABLE_VALIDATION = False
    transformer = FlightSilverTransformer(spark, enable_validation=ENABLE_VALIDATION)
    
    # Get Azure data paths
    paths = get_azure_data_paths(azure_config) 
    
    # Try to load from Azure, fallback to local if it fails
    try:
        print("Attempting to connect to Azure Data Lake...")
        bronze_df = spark.read.format("delta").load(f"{paths['bronze']}/flights")
        output_path = f"{paths['silver']}" 
        print(f"Successfully connected to Azure!")
        print(f"Bronze data path: {paths['bronze']}/flights")
        print(f"Output path: {output_path}")
    except Exception as e:
        print(f"Azure connection failed: {e}")
        print("Falling back to local data...")
        # Fallback to local data
        bronze_df = spark.read.format("delta").load("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/bronze/flights")
        output_path = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/silver"
    
    # Run transformation
    import pandas as pd
    batch_id = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        transformer.run_silver_transformation(bronze_df, output_path, batch_id=batch_id)
        print("\n🎉 Pipeline completed successfully with all validations passed!")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import sys
        sys.exit(1)