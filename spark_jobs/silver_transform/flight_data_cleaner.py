from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_timestamp, concat, lit, regexp_replace, upper, trim

class FlightDataCleaner:
    """Handles all data cleaning and transformation for flight data"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def handle_missing_values(self, df):
        print("Handling missing values")

        # For cancelled flights, null times are expected
        # For non-cancelled flights, we flag missing times
        df_handled = df.withColumn(
            "DEP_TIME_CLEAN",
            when(col("CANCELLED") == 1.0, None).otherwise(col("DEP_TIME"))
        ).withColumn(
            "ARR_TIME_CLEAN",
            when(col("CANCELLED") == 1.0, None).otherwise(col("ARR_TIME"))
        ).withColumn("AIR_TIME_HOURS_CLEAN"
        , when(col("CANCELLED") == 1.0, None).otherwise(col("AIR_TIME") / 60.0)).withColumn(
            "DEP_DELAY_CLEAN",
            when(col("CANCELLED") == 1.0, None).otherwise(col("DEP_DELAY"))
        ).withColumn(
            "ARR_DELAY_CLEAN",
            when(col("CANCELLED") == 1.0, None).otherwise(col("ARR_DELAY"))
        ).withColumn(
            "AIR_TIME_CLEAN",
            when(col("CANCELLED") == 1.0, None).otherwise(col("AIR_TIME"))
        )

        # Add data quality flags
        df_quality_flags = df_handled.withColumn(
            "has_missing_times",
            when(col("DEP_TIME_CLEAN").isNull() | col("ARR_TIME_CLEAN").isNull(), True).otherwise(False)
        ).withColumn(
            "has_missing_delays",
            when(col("DEP_DELAY_CLEAN").isNull() | col("ARR_DELAY_CLEAN").isNull(), True).otherwise(False)
        )

        return df_quality_flags

    def clean_datetime_fields(self, df):
        """Clean and standardize datetime fields to handle malformed data"""
        print("Cleaning datetime fields")
        
        from pyspark.sql.functions import regexp_replace, when, col, to_date
        
        # Clean FL_DATE field - convert to date type (removes time and timezone)
        df_cleaned = df.withColumn(
            "FL_DATE_PARSED",
            when(col("FL_DATE").isNull(), None)
            .otherwise(to_date(col("FL_DATE")))  # Use to_date instead of to_timestamp
        )
        
        return df_cleaned

    def standardize_data_types(self, df):

        print("Standardizing data types")
        
        from pyspark.sql.functions import date_format, date_add, expr

        # Convert time fields to proper timestamp type
        df_typed = df.withColumn(
            "DEP_TIME_PARSED",
            when(col("DEP_TIME_CLEAN").isNotNull(),
            regexp_replace(col("DEP_TIME_CLEAN"), "(\\d{2})(\\d{2})", "$1:$2")).otherwise(None)
        ).withColumn(
            "CRS_DEP_TIME_PARSED",
            when(col("CRS_DEP_TIME").isNotNull(),
            regexp_replace(col("CRS_DEP_TIME"), "(\\d{2})(\\d{2})", "$1:$2")).otherwise(None)
        )
        
        df_typed = df_typed.withColumn(
            "ARR_TIME_PARSED",
            when(col("ARR_TIME_CLEAN").isNotNull(),
            regexp_replace(col("ARR_TIME_CLEAN"), "(\\d{2})(\\d{2})", "$1:$2")).otherwise(None)
        ).withColumn(
            "CRS_ARR_TIME_PARSED",
            when(col("CRS_ARR_TIME").isNotNull(),
            regexp_replace(col("CRS_ARR_TIME"), "(\\d{2})(\\d{2})", "$1:$2")).otherwise(None)
        )
        
        df_typed = df_typed.withColumn(
            "DISTANCE_KM",
            when(col("DISTANCE").isNotNull(),
            col("DISTANCE") * 1.60934).otherwise(None)
        )

        # Convert boolean fields
        df_boolean = df_typed.withColumn(
            "CANCELLED_BOOL", col("CANCELLED") == 1.0
        ).withColumn(
            "DIVERTED_BOOL", col("DIVERTED") == 1.0
        )
        return df_boolean

    def validate_business_rules(self, df):
        print("Validating business rules")

        df_validated = df.withColumn(
            "AIR_TIME_VALID",
            when(col("AIR_TIME_CLEAN").isNotNull(),
            (col("AIR_TIME_CLEAN") > 0) & (col("AIR_TIME_CLEAN") < 1440)).otherwise(False) # 24 hours in minutes
        ).withColumn(
            "DISTANCE_VALID",
            when(col("DISTANCE").isNotNull(),
            (col("DISTANCE") > 0) & (col("DISTANCE_KM") < 20000)).otherwise(False) # 20000 km is a reasonable limit
        ).withColumn(
            "REASONABLE_DELAYS",
            when(col("DEP_DELAY_CLEAN").isNotNull() & col("ARR_DELAY_CLEAN").isNotNull(),
            (col("DEP_DELAY_CLEAN") > -60) & (col("DEP_DELAY_CLEAN") < 300) & (col("ARR_DELAY_CLEAN") > -60) & (col("ARR_DELAY_CLEAN") < 300)).otherwise(False)
        )

        df_quality = df_validated.withColumn(
            "DATA_QUALITY_SCORE",
            (when(col("has_missing_times"), 0).otherwise(1) +
            when(col("has_missing_delays"), 0).otherwise(1) +
            when(col("AIR_TIME_VALID"), 1).otherwise(0) +
            when(col("DISTANCE_VALID"), 1).otherwise(0) +
            when(col("REASONABLE_DELAYS"), 1).otherwise(0)) / 5.0
        )
        return df_quality

    def standardize_text_fields(self, df):
        print("Standardizing text fields")

        df_standardized = df.withColumn(
            "AIRLINE_CODE_CLEAN",
            upper(trim(col("OP_UNIQUE_CARRIER")))
        ).withColumn(
            "ORIGIN_AIRPORT_CLEAN",
            upper(trim(col("ORIGIN")))
        ).withColumn(
            "DEST_AIRPORT_CLEAN", 
            upper(trim(col("DEST")))
        ).withColumn(
            "CARRIER_NAME_CLEAN",
            trim(col("CARRIER_NAME"))
        ).withColumn(
            "ORIGIN_AIRPORT_NAME_CLEAN",
            trim(col("ORIGIN_AIRPORT_NAME"))
        ).withColumn(
            "DEST_AIRPORT_NAME_CLEAN",
            trim(col("DEST_AIRPORT_NAME"))
        )
        
        return df_standardized

