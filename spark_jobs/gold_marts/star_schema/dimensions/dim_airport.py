from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
class DimAirport:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_from_silver(self, silver_df):
        origin_airports = silver_df.select(
            col("ORIGIN_AIRPORT_CODE").alias("AIRPORT_CODE"), 
            col("ORIGIN_AIRPORT_NAME").alias("AIRPORT_NAME")).distinct()
        dest_airports = silver_df.select(
            col("DEST_AIRPORT_CODE").alias("AIRPORT_CODE"), 
            col("DEST_AIRPORT_NAME").alias("AIRPORT_NAME")).distinct()
        dim_airport = origin_airports.union(dest_airports).distinct()\
            .withColumn("created_at", current_timestamp())\
            .withColumn("updated_at", current_timestamp())
            
        return dim_airport
        
    def save_dimension(self, dim_df, output_path):
        dim_df.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .save(output_path)
        print(f"Dimension table saved to {output_path}")
        return dim_df