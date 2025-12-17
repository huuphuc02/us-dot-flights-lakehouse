from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count, avg, round as spark_round, min, max

class DailyAirlinePerformance:
    """ Daily airline performance aggregate """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_from_fact(self, fact_flights, dim_date, dim_airline):
        fact_with_dims = fact_flights.alias("f")\
            .join(
                dim_date.alias("dd"),
                col("f.DATE_KEY") == col("dd.DATE_KEY"),
            )\
            .join(
                dim_airline.alias("da"),
                col("f.AIRLINE_CODE") == col("da.AIRLINE_CODE")
            )

        daily_perf = fact_with_dims.groupBy(
            col("dd.FULL_DATE").alias("FLIGHT_DATE"),
            col("dd.YEAR"),
            col("dd.MONTH"),
            col("dd.DAY_OF_WEEK_NAME"),
            col("dd.IS_WEEKEND"),
            col("da.AIRLINE_CODE"),
            col("da.AIRLINE_NAME"),
        ).agg(
            count("*").alias("TOTAL_FLIGHTS"),
            sum(when(col("f.IS_CANCELLED"), 1).otherwise(0)).alias("CANCELLED_FLIGHTS"),
            sum(when(col("f.IS_DIVERTED"), 1).otherwise(0)).alias("DIVERTED_FLIGHTS"),
            sum(when(col("f.IS_DELAYED"), 1).otherwise(0)).alias("DELAYED_FLIGHTS"),
            sum(when(col("f.IS_ONTIME"), 1).otherwise(0)).alias("ONTIME_FLIGHTS"),
            
            avg(when(~col("f.IS_CANCELLED"), col("f.DEPARTURE_DELAY"))).alias("AVG_DEPARTURE_DELAY"),
            avg(when(~col("f.IS_CANCELLED"), col("f.ARRIVAL_DELAY"))).alias("AVG_ARRIVAL_DELAY"),
            max(col("f.DEPARTURE_DELAY")).alias("MAX_DEPARTURE_DELAY"),
            max(col("f.ARRIVAL_DELAY")).alias("MAX_ARRIVAL_DELAY"),

            avg(col("f.AIR_TIME_MINUTES")).alias("AVG_AIR_TIME"),
            avg(col("f.DISTANCE_KM")).alias("AVG_DISTANCE"),
            avg(col("f.SPEED_KM_H")).alias("AVG_SPEED"),

            avg(col("f.DATA_QUALITY_SCORE")).alias("AVG_DATA_QUALITY_SCORE"),
        )

        daily_perf_with_kpis = daily_perf.select(
            "*",
            # Completion rate
            spark_round(
                (col("TOTAL_FLIGHTS") - col("CANCELLED_FLIGHTS")) / col("TOTAL_FLIGHTS") * 100,
                2
            ).alias("COMPLETION_RATE"),
            
            # Cancellation rate
            spark_round(
                (col("CANCELLED_FLIGHTS") / col("TOTAL_FLIGHTS")) * 100,
                2
            ).alias("CANCELLATION_RATE"),

            # On-time performance
            spark_round(
                (col("ONTIME_FLIGHTS") / (col("TOTAL_FLIGHTS") - col("CANCELLED_FLIGHTS"))) * 100,
                2
            ).alias("ON_TIME_PERFORMANCE"),

            # Delay rate
            spark_round(
                (col("DELAYED_FLIGHTS") / (col("TOTAL_FLIGHTS") - col("CANCELLED_FLIGHTS"))) * 100,
                2
            ).alias("DELAY_RATE")          
        )

        return daily_perf_with_kpis

    def save_mart(self, mart_df, output_path):
        mart_df.write.format("delta").mode("overwrite")\
            .partitionBy("YEAR", "MONTH")\
            .option("overwriteSchema", "true").save(output_path)
        print(f"Daily airline performance mart saved to {output_path}")
        return mart_df