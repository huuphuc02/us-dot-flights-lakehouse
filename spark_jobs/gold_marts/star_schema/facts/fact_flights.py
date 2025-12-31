from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, expr, cast, date_format, regexp_replace, broadcast

from spark_jobs.gold_marts.star_schema.dimensions import dim_airline
class FactFlights:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_fact_table(self, silver_df, dim_date: DataFrame, dim_time: DataFrame, dim_airport: DataFrame, dim_airline: DataFrame, dim_route: DataFrame):
        # Pre-compute time keys in silver data to avoid 4 separate joins
        # This is much faster than joining 4 times with dim_time
        from pyspark.sql.functions import when
        
        print("  → Optimizing: Pre-computing time keys...")
        silver_with_time_keys = silver_df \
            .withColumn("ACTUAL_DEPARTURE_TIME_KEY",
                       when(col("ACTUAL_DEPARTURE_TIME").isNotNull(),
                            regexp_replace(col("ACTUAL_DEPARTURE_TIME"), ":", "").cast("int"))
                       .otherwise(None)) \
            .withColumn("ACTUAL_ARRIVAL_TIME_KEY",
                       when(col("ACTUAL_ARRIVAL_TIME").isNotNull(),
                            regexp_replace(col("ACTUAL_ARRIVAL_TIME"), ":", "").cast("int"))
                       .otherwise(None)) \
            .withColumn("PLANNED_DEPARTURE_TIME_KEY",
                       when(col("PLANNED_DEPARTURE_TIME").isNotNull(),
                            regexp_replace(col("PLANNED_DEPARTURE_TIME"), ":", "").cast("int"))
                       .otherwise(None)) \
            .withColumn("PLANNED_ARRIVAL_TIME_KEY",
                       when(col("PLANNED_ARRIVAL_TIME").isNotNull(),
                            regexp_replace(col("PLANNED_ARRIVAL_TIME"), ":", "").cast("int"))
                       .otherwise(None))
        
        # Now perform joins with broadcast hints for small dimensions
        print("  → Joining with dimensions (broadcast strategy)...")
        fact = silver_with_time_keys.alias("f")\
            .join(
                broadcast(dim_date.alias("dd")),
                date_format(col("f.FLIGHT_DATE"), "yyyyMMdd").cast("int") == col("dd.DATE_KEY"),
                "left"
            )\
            .join(
                broadcast(dim_airline.alias("da")),
                col("f.AIRLINE_CODE") == col("da.AIRLINE_CODE"),
                "left"
            )\
            .join(
                broadcast(dim_airport.alias("doa")),
                col("f.ORIGIN_AIRPORT_CODE") == col("doa.AIRPORT_CODE"),
                "left"
            )\
            .join(
                broadcast(dim_airport.alias("dda")),
                col("f.DEST_AIRPORT_CODE") == col("dda.AIRPORT_CODE"),
                "left"
            )\
            .join(
                broadcast(dim_route.alias("dr")),
                col("f.ROUTE_CODE") == col("dr.ROUTE_CODE"),
                "left"
            )
        print("  → Selecting final fact table columns...")
        fact_flights = fact.select(
            col("f.FLIGHT_NUMBER").alias("FLIGHT_NUMBER"),
            col("dd.DATE_KEY"),
            col("f.ACTUAL_DEPARTURE_TIME_KEY"),  # Already computed above
            col("f.ACTUAL_ARRIVAL_TIME_KEY"),     # Already computed above
            col("f.PLANNED_DEPARTURE_TIME_KEY"),  # Already computed above
            col("f.PLANNED_ARRIVAL_TIME_KEY"),    # Already computed above
            col("da.AIRLINE_CODE").alias("AIRLINE_CODE"),
            col("doa.AIRPORT_CODE").alias("ORIGIN_AIRPORT_CODE"),
            col("dda.AIRPORT_CODE").alias("DEST_AIRPORT_CODE"),
            col("dr.ROUTE_CODE").alias("ROUTE_CODE"),

            col("f.IS_WEEKEND"),
            col("f.IS_DIVERTED"),
            col("f.IS_CANCELLED"),
            col("f.IS_DELAYED"),
            col("f.IS_ONTIME"),

            col("f.DEPARTURE_DELAY"),
            col("f.ARRIVAL_DELAY"),
            col("f.DEPARTURE_DELAY_CATEGORY"),
            col("f.ARRIVAL_DELAY_CATEGORY"),
            col("f.AIR_TIME_MINUTES"),
            col("f.DISTANCE_KM"),
            col("f.SPEED_KM_H"),
            col("f.DATA_QUALITY_SCORE"),
        )

        print("  ✅ Fact table transformation complete")
        return fact_flights


            
        