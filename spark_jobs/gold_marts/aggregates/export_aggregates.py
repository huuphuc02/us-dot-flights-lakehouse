"""
Export aggregate tables from Azure Data Lake to local CSV files for inspection
"""
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure

def export_aggregates_to_csv():
    """Read aggregates from Azure and export to local CSV files"""
    
    # Load Azure configuration
    config = load_azure_config_from_env()
    
    # Create Spark session
    print("Creating Spark session with Azure Data Lake support...")
    spark = create_spark_session_with_azure(
        app_name="Export_Aggregates",
        azure_config=config,
        master="local[*]",
        memory="4g"
    )
    
    try:
        base_path = f"abfss://{config.container_name}@{config.storage_account_name}.dfs.core.windows.net/gold/aggregates"
        output_dir = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse/data/exported_aggregates"
        
        # List of aggregates to export
        aggregates = [
            "daily_airline_performance",
            "daily_airport_performance", 
            "route_performance",
            "monthly_trends"
        ]
        
        print(f"\n{'='*60}")
        print("EXPORTING AGGREGATES FROM AZURE TO LOCAL CSV")
        print(f"{'='*60}\n")
        
        for agg_name in aggregates:
            try:
                agg_path = f"{base_path}/{agg_name}"
                print(f"\n📊 Reading {agg_name}...")
                print(f"   From: {agg_path}")
                
                # Read the aggregate table
                df = spark.read.format("delta").load(agg_path)
                record_count = df.count()
                
                print(f"   Records: {record_count:,}")
                print(f"   Columns: {len(df.columns)}")
                
                # Show schema
                print(f"\n   Schema:")
                for field in df.schema.fields:
                    print(f"      - {field.name}: {field.dataType.simpleString()}")
                
                # Show sample data
                print(f"\n   Sample data (first 10 rows):")
                df.show(10, truncate=False)
                
                # Export to CSV
                output_path = f"{output_dir}/{agg_name}.csv"
                print(f"\n   💾 Exporting to: {output_path}")
                
                # Convert to single CSV file (coalesce to 1 partition)
                df.coalesce(1).write.format("csv")\
                    .option("header", "true")\
                    .mode("overwrite")\
                    .save(output_path)
                
                print(f"   ✅ Exported successfully!")
                
                # Show statistics
                print(f"\n   📈 Statistics:")
                df.describe().show()
                
            except Exception as e:
                print(f"   ⚠️  Could not export {agg_name}: {e}")
                continue
        
        print(f"\n{'='*60}")
        print("EXPORT SUMMARY")
        print(f"{'='*60}")
        print(f"✅ Aggregates exported to: {output_dir}")
        print(f"   You can find CSV files (part-*.csv) in each subdirectory")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\n❌ Error during export: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("\nSpark session stopped")

if __name__ == "__main__":
    export_aggregates_to_csv()

