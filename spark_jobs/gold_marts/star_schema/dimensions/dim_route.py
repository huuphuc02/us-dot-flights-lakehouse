from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, avg, count, when
from pyspark.sql.dataframe import DataFrame
class DimRoute:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_from_silver(self, silver_df):
        dim_route = silver_df.groupBy(
            "ROUTE_CODE",
            "ROUTE_NAME",
            "ORIGIN_AIRPORT_CODE",
            "ORIGIN_AIRPORT_NAME",
            "DEST_AIRPORT_CODE",
            "DEST_AIRPORT_NAME",
        ).agg(
            avg("DISTANCE_KM").alias("AVG_DISTANCE_KM"),
            avg("AIR_TIME_MINUTES").alias("AVG_AIR_TIME_MINUTES"),

            count("*").alias("TOTAL_FLIGHTS"),
        )

        dim_route = dim_route.select(
            col("ROUTE_CODE"),
            col("ROUTE_NAME"),
            col("ORIGIN_AIRPORT_CODE"),
            col("ORIGIN_AIRPORT_NAME"),
            col("DEST_AIRPORT_CODE"),
            col("DEST_AIRPORT_NAME"),
            col("AVG_DISTANCE_KM").cast("decimal(10, 2)").alias("DISTANCE_KM"),
            col("AVG_AIR_TIME_MINUTES").cast("decimal(10, 2)").alias("EXPECTED_AIR_TIME_MINUTES"),
            col("TOTAL_FLIGHTS"),
            # Route popularity indicator
            when(col("TOTAL_FLIGHTS") >= 1000, "Very Popular")
            .when(col("TOTAL_FLIGHTS") >= 500, "Popular")
            .when(col("TOTAL_FLIGHTS") >= 100, "Moderate")
            .otherwise("Low Frequency")
            .alias("ROUTE_POPULARITY"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )

        return dim_route

    def save_dimension(self, dim_route: DataFrame, output_path: str):
        dim_route.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
        print(f"Route dimension saved to {output_path}")
        return dim_route
        