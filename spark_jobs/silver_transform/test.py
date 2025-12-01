import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure, get_azure_data_paths
azure_config = load_azure_config_from_env()

spark = create_spark_session_with_azure(
    app_name="US_DOT_Flights_Silver_Transformation",
    azure_config=azure_config
)

paths = get_azure_data_paths(azure_config)
df_silver = spark.read.format("delta").load(f"{paths['silver']}")

# save 0.1% of the data to csv
df_silver.sample(0.001).write.csv("silver_data.csv", header=True, mode="overwrite")
print(f"Saved 0.1% of the data to csv")

# stop spark session
spark.stop()
