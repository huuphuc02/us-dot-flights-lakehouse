from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count, avg, round as spark_round

class RoutePerformanceMart:
    """Route-level analytics (monthly aggregation)"""
    
    def build_from_fact(self, fact_flights, dim_date, dim_route, dim_airline):
        """Build route performance metrics"""
        
        route_perf = fact_flights.alias("f") \
            .join(dim_date.alias("dd"), col("f.DATE_KEY") == col("dd.DATE_KEY")) \
            .join(dim_route.alias("dr"), col("f.ROUTE_CODE") == col("dr.ROUTE_CODE")) \
            .join(dim_airline.alias("da"), col("f.AIRLINE_CODE") == col("da.AIRLINE_CODE")) \
            .groupBy(
                col("dd.YEAR"),
                col("dd.MONTH"),
                col("dr.ROUTE_CODE"),
                col("dr.ROUTE_NAME"),
                col("dr.ORIGIN_AIRPORT_CODE"),
                col("dr.DEST_AIRPORT_CODE"),
                col("da.AIRLINE_CODE"),
                col("da.AIRLINE_NAME")
            ).agg(
                count("*").alias("FLIGHT_FREQUENCY"),
                avg("f.DEPARTURE_DELAY").alias("AVG_DEPARTURE_DELAY"),
                avg("f.ARRIVAL_DELAY").alias("AVG_ARRIVAL_DELAY"),
                avg("f.AIR_TIME_MINUTES").alias("AVG_AIR_TIME"),
                sum(when(col("f.IS_CANCELLED"), 1).otherwise(0)).alias("CANCELLATIONS"),
                sum(when(col("f.IS_ONTIME"), 1).otherwise(0)).alias("ONTIME_FLIGHTS")
            ).select(
                "*",
                spark_round(
                    col("ONTIME_FLIGHTS") / (col("FLIGHT_FREQUENCY") - col("CANCELLATIONS")) * 100,
                    2
                ).alias("ONTIME_PERFORMANCE_PCT")
            )
        
        return route_perf
    
    def save_mart(self, mart_df, output_path):
        mart_df.write.format("delta").mode("overwrite")\
            .partitionBy("YEAR", "MONTH")\
            .option("overwriteSchema", "true").save(output_path)
        print(f"Route performance mart saved to {output_path}")
        return mart_df