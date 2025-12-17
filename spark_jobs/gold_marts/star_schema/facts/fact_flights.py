from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, expr, cast, date_format, regexp_replace

from spark_jobs.gold_marts.star_schema.dimensions import dim_airline
class FactFlights:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_fact_table(self, silver_df, dim_date: DataFrame, dim_time: DataFrame, dim_airport: DataFrame, dim_airline: DataFrame, dim_route: DataFrame):
        fact = silver_df.alias("f")\
            .join(
                dim_airline.alias("da"),
                col("f.AIRLINE_CODE") == col("da.AIRLINE_CODE"),
                "left"
            )\
            .join(
                dim_airport.alias("doa"),
                col("f.ORIGIN_AIRPORT_CODE") == col("doa.AIRPORT_CODE"),
                "left"
            )\
            .join(
                dim_airport.alias("dda"),
                col("f.DEST_AIRPORT_CODE") == col("dda.AIRPORT_CODE"),
                "left"
            )\
            .join(
                dim_date.alias("dd"),
                date_format(col("f.FLIGHT_DATE"), "yyyyMMdd").cast("int") == col("dd.DATE_KEY"),
                "left"
            )\
            .join(
                dim_time.alias("dadt"),
                regexp_replace(col("f.ACTUAL_DEPARTURE_TIME"), ":", "").cast("int") == col("dadt.TIME_KEY"),
                "left"
            )\
            .join(
                dim_time.alias("darrt"),
                regexp_replace(col("f.ACTUAL_ARRIVAL_TIME"), ":", "").cast("int") == col("darrt.TIME_KEY"),
                "left"
            )\
            .join(
                dim_time.alias("dcrsdt"),
                regexp_replace(col("f.PLANNED_DEPARTURE_TIME"), ":", "").cast("int") == col("dcrsdt.TIME_KEY"),
                "left"
            )\
            .join(
                dim_time.alias("dcrsart"),
                regexp_replace(col("f.PLANNED_ARRIVAL_TIME"), ":", "").cast("int") == col("dcrsdt.TIME_KEY"),
                "left"
            )\
            .join(
                dim_route.alias("dr"),
                col("f.ROUTE_CODE") == col("dr.ROUTE_CODE"),
                "left"
            )
        fact_flights = fact.select(
            col("f.FLIGHT_NUMBER").alias("FLIGHT_NUMBER"),
            col("dd.DATE_KEY"),
            col("dadt.TIME_KEY").alias("ACTUAL_DEPARTURE_TIME_KEY"),
            col("darrt.TIME_KEY").alias("ACTUAL_ARRIVAL_TIME_KEY"),
            col("dcrsdt.TIME_KEY").alias("PLANNED_DEPARTURE_TIME_KEY"),
            col("dcrsart.TIME_KEY").alias("PLANNED_ARRIVAL_TIME_KEY"),
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

        return fact_flights


            
        