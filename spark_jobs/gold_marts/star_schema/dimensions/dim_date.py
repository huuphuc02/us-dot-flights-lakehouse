from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, expr, cast, col, year, quarter, month, weekofyear, dayofweek
from pyspark.sql.dataframe import DataFrame
class DimDate:
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def build_date_dimension(self, start_date = "2020-01-01", end_date = "2030-12-31"):

        date_df = self.spark.sql(f"""
            select explode(sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )) as full_date
        """)

        dim_date = date_df.select(
            date_format(col("full_date"), "yyyyMMdd").cast("int").alias("DATE_KEY"),
            col("full_date").alias("FULL_DATE"),
            year(col("full_date")).alias("YEAR"),
            quarter(col("full_date")).alias("QUARTER"),
            month(col("full_date")).alias("MONTH"),
            date_format(col("full_date"), "MMM").alias("MONTH_NAME"),
            weekofyear(col("full_date")).alias("WEEK_OF_YEAR"),
            expr("day(full_date)").alias("DAY_OF_MONTH"),
            dayofweek(col("full_date")).alias("DAY_OF_WEEK"),
            date_format(col("full_date"), "EEEE").alias("DAY_OF_WEEK_NAME"),
            expr("dayofweek(full_date) in (1, 7)").alias("IS_WEEKEND"),
        )
        

        return dim_date

    def save_dimension(self, dim_date: DataFrame, output_path: str):
        dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
        print(f"Date dimension saved to {output_path}")
        return dim_date
