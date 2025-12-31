from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_timestamp, concat, lit, regexp_replace, upper, trim, dayofweek
class FlightDataEnricher:
    """Handles all enrichment and calculation for flight data"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def categorize_delays(self, df):
        print("Categorizing delays")

        # Create delay categories
        df_categorized = df.withColumn(
            "DEP_DELAY_CATEGORY",
            when(col("DEP_DELAY_CLEAN").isNull(), "Unknown")
            .when(col("DEP_DELAY_CLEAN") < 0, "Early")
            .when(col("DEP_DELAY_CLEAN") == 0, "On time")
            .when(col("DEP_DELAY_CLEAN") <= 15, "Minor")
            .when(col("DEP_DELAY_CLEAN") <= 60, "Moderate")
            .when(col("DEP_DELAY_CLEAN") <= 180, "Significant")
            .otherwise("Severe")
        ).withColumn(
            "ARR_DELAY_CATEGORY",
            when(col("ARR_DELAY_CLEAN").isNull(), "Unknown")
            .when(col("ARR_DELAY_CLEAN") < 0, "Early")
            .when(col("ARR_DELAY_CLEAN") == 0, "On time")
            .when(col("ARR_DELAY_CLEAN") <= 15, "Minor")
            .when(col("ARR_DELAY_CLEAN") <= 60, "Moderate")
            .when(col("ARR_DELAY_CLEAN") <= 180, "Significant")
            .otherwise("Severe")
        ).withColumn(
            "IS_DELAYED",
            when(col("DEP_DELAY_CLEAN").isNull() | col("ARR_DELAY_CLEAN").isNull(), None)
            .when((col("DEP_DELAY_CLEAN") > 0) | (col("ARR_DELAY_CLEAN") > 0), True).otherwise(False)
        ).withColumn(
            "IS_ONTIME",
            when(col("DEP_DELAY_CLEAN").isNull() | col("ARR_DELAY_CLEAN").isNull(), None)
            .when((col("DEP_DELAY_CLEAN") <= 0) & (col("ARR_DELAY_CLEAN") <= 0), True)
            .otherwise(False)
        )
        return df_categorized

    def calculate_performance_metrics(self, df):
        print("Adding performance metrics")

        df_performance = df.withColumn(
            "SPEED_KM_H",
            when(col("DISTANCE_KM").isNotNull() & col("AIR_TIME_CLEAN").isNotNull(),
            col("DISTANCE_KM") / col("AIR_TIME_CLEAN") * 60).otherwise(None)
        )

        return df_performance

    def add_operational_metrics(self, df):
        print("Adding operational metrics")

        df_operational = df.withColumn(
            "ROUTE_CODE",
            concat(col("ORIGIN_AIRPORT_CLEAN"), lit("-"), col("DEST_AIRPORT_CLEAN"))
        ).withColumn(
            "ROUTE_NAME",
            concat(lit("from "), col("ORIGIN_AIRPORT_NAME_CLEAN"), lit(" to "), col("DEST_AIRPORT_NAME_CLEAN"))
        ).withColumn(
            "IS_WEEKEND",
            (dayofweek(col("FL_DATE_PARSED")) == 1) | (dayofweek(col("FL_DATE_PARSED")) == 7)
        )

        return df_operational