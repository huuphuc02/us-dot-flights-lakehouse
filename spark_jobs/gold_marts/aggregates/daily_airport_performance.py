from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count, avg, lit

class DailyAirportPerformanceMart:
    """Pre-aggregated airport operations by day"""
    
    def build_from_fact(self, fact_flights, dim_date, dim_airport):
        """Build airport performance combining both departures and arrivals in one row"""
        
        # Departures aggregation
        departures = fact_flights.alias("f") \
            .join(dim_date.alias("dd"), col("f.DATE_KEY") == col("dd.DATE_KEY")) \
            .join(dim_airport.alias("da"), 
                  col("f.ORIGIN_AIRPORT_CODE") == col("da.AIRPORT_CODE")) \
            .groupBy(
                col("dd.FULL_DATE").alias("FLIGHT_DATE"),
                col("da.AIRPORT_CODE"),
                col("da.AIRPORT_NAME")
            ).agg(
                count("*").alias("TOTAL_DEPARTURES"),
                sum(when(col("f.IS_CANCELLED"), 1).otherwise(0)).alias("CANCELLED_DEPARTURES"),
                avg(when(~col("f.IS_CANCELLED"), col("f.DEPARTURE_DELAY"))).alias("AVG_DEPARTURE_DELAY"),
                sum(when(col("f.DEPARTURE_DELAY") > 0, 1).otherwise(0)).alias("DELAYED_DEPARTURES")
            )
        
        # Arrivals aggregation
        arrivals = fact_flights.alias("f") \
            .join(dim_date.alias("dd"), col("f.DATE_KEY") == col("dd.DATE_KEY")) \
            .join(dim_airport.alias("da"), 
                  col("f.DEST_AIRPORT_CODE") == col("da.AIRPORT_CODE")) \
            .groupBy(
                col("dd.FULL_DATE").alias("FLIGHT_DATE"),
                col("da.AIRPORT_CODE"),
                col("da.AIRPORT_NAME")
            ).agg(
                count("*").alias("TOTAL_ARRIVALS"),
                sum(when(col("f.IS_DIVERTED"), 1).otherwise(0)).alias("DIVERTED_ARRIVALS"),
                avg(when(~col("f.IS_CANCELLED"), col("f.ARRIVAL_DELAY"))).alias("AVG_ARRIVAL_DELAY"),
                sum(when(col("f.ARRIVAL_DELAY") > 0, 1).otherwise(0)).alias("DELAYED_ARRIVALS")
            )
        
        # Join departures and arrivals on (FLIGHT_DATE, AIRPORT_CODE, AIRPORT_NAME)
        # Use full outer join to capture airports that only have departures OR arrivals on a given day
        airport_perf = departures.alias("dep") \
            .join(
                arrivals.alias("arr"),
                on=[
                    col("dep.FLIGHT_DATE") == col("arr.FLIGHT_DATE"),
                    col("dep.AIRPORT_CODE") == col("arr.AIRPORT_CODE")
                ],
                how="outer"
            ) \
            .select(
                when(col("dep.FLIGHT_DATE").isNotNull(), col("dep.FLIGHT_DATE"))
                    .otherwise(col("arr.FLIGHT_DATE")).alias("FLIGHT_DATE"),
                when(col("dep.AIRPORT_CODE").isNotNull(), col("dep.AIRPORT_CODE"))
                    .otherwise(col("arr.AIRPORT_CODE")).alias("AIRPORT_CODE"),
                when(col("dep.AIRPORT_NAME").isNotNull(), col("dep.AIRPORT_NAME"))
                    .otherwise(col("arr.AIRPORT_NAME")).alias("AIRPORT_NAME"),
                col("dep.TOTAL_DEPARTURES"),
                col("dep.CANCELLED_DEPARTURES"),
                col("dep.AVG_DEPARTURE_DELAY"),
                col("dep.DELAYED_DEPARTURES"),
                col("arr.TOTAL_ARRIVALS"),
                col("arr.DIVERTED_ARRIVALS"),
                col("arr.AVG_ARRIVAL_DELAY"),
                col("arr.DELAYED_ARRIVALS")
            )
        
        return airport_perf

    def save_mart(self, mart_df, output_path):
        mart_df.write.format("delta").mode("overwrite")\
            .partitionBy("FLIGHT_DATE")\
            .option("overwriteSchema", "true").save(output_path)
        print(f"Daily airport performance mart saved to {output_path}")
        return mart_df