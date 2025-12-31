from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lpad, concat, lit
from pyspark.sql.dataframe import DataFrame

class DimTime:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_time_dimension(self):
        time_df = self.spark.range(0, 1440).select(
            col("id").alias("minutes_from_midnight")
        )
        
        dim_time = time_df.select(
            # Time key in HHMM format (0000-2359)
            (
                lpad((col("minutes_from_midnight") / 60).cast("int"), 2, "0") + 
                lpad((col("minutes_from_midnight") % 60).cast("int"), 2, "0")
            ).cast("int").alias("TIME_KEY"),
            
            # Hour (0-23)
            (col("minutes_from_midnight") / 60).cast("int").alias("HOUR_24"),
            
            # Hour (1-12) for 12-hour format
            when((col("minutes_from_midnight") / 60).cast("int") == 0, 12)
            .when((col("minutes_from_midnight") / 60).cast("int") <= 12, 
                  (col("minutes_from_midnight") / 60).cast("int"))
            .otherwise((col("minutes_from_midnight") / 60).cast("int") - 12)
            .alias("HOUR_12"),

                        # Minute (0-59)
            (col("minutes_from_midnight") % 60).cast("int").alias("MINUTE"),
            
            # Time string in HH:MM format
            concat(
                lpad((col("minutes_from_midnight") / 60).cast("int"), 2, "0"),
                lit(":"),
                lpad((col("minutes_from_midnight") % 60).cast("int"), 2, "0")
            ).alias("TIME_STRING"),
            
            # AM/PM indicator
            when((col("minutes_from_midnight") / 60).cast("int") < 12, "AM")
            .otherwise("PM")
            .alias("AM_PM"),
            
            # Time of day categories
            when((col("minutes_from_midnight") / 60).cast("int") < 6, "Night")
            .when((col("minutes_from_midnight") / 60).cast("int") < 12, "Morning")
            .when((col("minutes_from_midnight") / 60).cast("int") < 18, "Afternoon")
            .when((col("minutes_from_midnight") / 60).cast("int") < 22, "Evening")
            .otherwise("Night")
            .alias("TIME_OF_DAY"),
            
            # Business hours flag (9 AM - 5 PM)
            when(
                ((col("minutes_from_midnight") / 60).cast("int") >= 9) &
                ((col("minutes_from_midnight") / 60).cast("int") < 17),
                True
            ).otherwise(False)
            .alias("IS_BUSINESS_HOURS"),

                       # Peak travel hours flag (6-9 AM and 4-7 PM)
            when(
                (((col("minutes_from_midnight") / 60).cast("int") >= 6) & 
                 ((col("minutes_from_midnight") / 60).cast("int") < 9)) |
                (((col("minutes_from_midnight") / 60).cast("int") >= 16) & 
                 ((col("minutes_from_midnight") / 60).cast("int") < 19)),
                True
            ).otherwise(False)
            .alias("IS_PEAK_HOURS"),
            
            # Early morning flag (before 6 AM)
            when((col("minutes_from_midnight") / 60).cast("int") < 6, True)
            .otherwise(False)
            .alias("IS_EARLY_MORNING"),
            
            # Late night flag (after 10 PM)
            when((col("minutes_from_midnight") / 60).cast("int") >= 22, True)
            .otherwise(False)
            .alias("IS_LATE_NIGHT"),
            
            # Red-eye hours (10 PM - 6 AM)
            when(
                ((col("minutes_from_midnight") / 60).cast("int") >= 22) |
                ((col("minutes_from_midnight") / 60).cast("int") < 6),
                True
            ).otherwise(False)
            .alias("IS_RED_EYE")
        )
        
        return dim_time

    def save_dimension(self, dim_time: DataFrame, output_path: str):
        dim_time.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)

        print(f"Time dimension saved to {output_path}")
        return dim_time