from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count, avg, lit

class DailyAirportPerformanceMart:
    """Pre-aggregated airport operations by day"""
    
    def build_from_fact(self, fact_flights, dim_date, dim_airport):
        """Build airport performance for both departures and arrivals"""
        
        # Departures
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
            ).withColumn("DIRECTION", lit("DEPARTURE"))
        
        # Arrivals
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
            ).withColumn("DIRECTION", lit("ARRIVAL"))
        
        # Combine both views
        airport_perf = departures.unionByName(arrivals, allowMissingColumns=True)
        
        return airport_perf

    def save_mart(self, mart_df, output_path):
        mart_df.write.format("delta").mode("overwrite")\
            .partitionBy("FLIGHT_DATE")\
            .option("overwriteSchema", "true").save(output_path)
        print(f"Daily airport performance mart saved to {output_path}")
        return mart_df