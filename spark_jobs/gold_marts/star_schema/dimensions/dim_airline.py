from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths
class DimAirline:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_from_silver(self, silver_df):
        dim_airline = silver_df.select(col("AIRLINE_CODE"), col("AIRLINE_NAME")).distinct()\
            .withColumn("created_at", current_timestamp())\
            .withColumn("updated_at", current_timestamp())
        return dim_airline

    def save_dimension(self, dim_df, output_path):
        dim_df.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .save(output_path)
        print(f"Dimension table saved to {output_path}")
        return dim_df
