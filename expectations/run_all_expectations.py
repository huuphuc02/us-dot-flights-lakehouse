"""
Run all Great Expectations validations for Bronze, Silver, and Gold layers
This script validates data quality across the entire lakehouse pipeline
"""
import great_expectations as gx
from pyspark.sql import SparkSession
import sys
sys.path.append("/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse")
from configs.azure_config import load_azure_config_from_env, create_spark_session_with_azure

# Import expectation functions
from bronze_expectations import create_bronze_expectations
from silver_expectations import create_silver_expectations
from gold_expectations import (
    create_dim_airline_expectations,
    create_dim_airport_expectations,
    create_dim_date_expectations,
    create_dim_time_expectations,
    create_dim_route_expectations,
    create_fact_flights_expectations
)

def print_section_header(title):
    """Print a formatted section header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")

def run_all_validations(context, spark, azure_config):
    """Run all validations across Bronze, Silver, and Gold layers"""
    
    results = {
        "bronze": None,
        "silver": None,
        "gold_dimensions": {},
        "gold_facts": {}
    }
    
    # Bronze Layer Validation
    try:
        print_section_header("BRONZE LAYER VALIDATION")
        results["bronze"] = create_bronze_expectations(context, spark, azure_config)
        print("✅ Bronze layer validation completed")
    except Exception as e:
        print(f"❌ Bronze layer validation failed: {e}")
        results["bronze"] = False
    
    # Silver Layer Validation
    try:
        print_section_header("SILVER LAYER VALIDATION")
        results["silver"] = create_silver_expectations(context, spark, azure_config)
        print("✅ Silver layer validation completed")
    except Exception as e:
        print(f"❌ Silver layer validation failed: {e}")
        results["silver"] = False
    
    # Gold Layer - Dimensions Validation
    print_section_header("GOLD LAYER - DIMENSIONS VALIDATION")
    
    dimensions = [
        ("dim_airline", create_dim_airline_expectations, "Airline Dimension"),
        ("dim_airport", create_dim_airport_expectations, "Airport Dimension"),
        ("dim_date", create_dim_date_expectations, "Date Dimension"),
        ("dim_time", create_dim_time_expectations, "Time Dimension"),
        ("dim_route", create_dim_route_expectations, "Route Dimension")
    ]
    
    for dim_name, dim_func, display_name in dimensions:
        try:
            print(f"\n📊 Validating {display_name}...")
            results["gold_dimensions"][dim_name] = dim_func(context, spark, azure_config)
            print(f"✅ {display_name} validation completed")
        except Exception as e:
            print(f"❌ {display_name} validation failed: {e}")
            results["gold_dimensions"][dim_name] = False
    
    # Gold Layer - Facts Validation
    try:
        print_section_header("GOLD LAYER - FACTS VALIDATION")
        results["gold_facts"]["fact_flights"] = create_fact_flights_expectations(context, spark, azure_config)
        print("✅ Fact table validation completed")
    except Exception as e:
        print(f"❌ Fact table validation failed: {e}")
        results["gold_facts"]["fact_flights"] = False
    
    return results

def print_summary(results):
    """Print a summary of all validation results"""
    print_section_header("VALIDATION SUMMARY")
    
    total = 0
    passed = 0
    
    # Bronze
    if results["bronze"]:
        print("✅ Bronze Layer: PASSED")
        total += 1
        passed += 1
    elif results["bronze"] is False:
        print("❌ Bronze Layer: FAILED")
        total += 1
    else:
        print("⚠️  Bronze Layer: NOT RUN")
    
    # Silver
    if results["silver"]:
        print("✅ Silver Layer: PASSED")
        total += 1
        passed += 1
    elif results["silver"] is False:
        print("❌ Silver Layer: FAILED")
        total += 1
    else:
        print("⚠️  Silver Layer: NOT RUN")
    
    # Gold Dimensions
    print("\nGold Layer - Dimensions:")
    for dim_name, result in results["gold_dimensions"].items():
        if result:
            print(f"  ✅ {dim_name}: PASSED")
            total += 1
            passed += 1
        elif result is False:
            print(f"  ❌ {dim_name}: FAILED")
            total += 1
        else:
            print(f"  ⚠️  {dim_name}: NOT RUN")
    
    # Gold Facts
    print("\nGold Layer - Facts:")
    for fact_name, result in results["gold_facts"].items():
        if result:
            print(f"  ✅ {fact_name}: PASSED")
            total += 1
            passed += 1
        elif result is False:
            print(f"  ❌ {fact_name}: FAILED")
            total += 1
        else:
            print(f"  ⚠️  {fact_name}: NOT RUN")
    
    print(f"\n{'='*70}")
    print(f"TOTAL: {passed}/{total} validations passed")
    print(f"{'='*70}\n")
    
    return passed == total

if __name__ == "__main__":
    print("="*70)
    print("  US DOT Flights Lakehouse - Data Quality Validation")
    print("  Powered by Great Expectations")
    print("="*70)
    
    # Load Azure configuration
    print("\n🔧 Loading Azure configuration...")
    azure_config = load_azure_config_from_env()
    
    # Create Spark session with Azure support
    print("⚙️  Creating Spark session with Azure Data Lake support...")
    spark = create_spark_session_with_azure(
        app_name="GX_All_Validations",
        azure_config=azure_config
    )
    
    # Initialize Great Expectations context
    project_root = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse"
    gx_dir = f"{project_root}/expectations"
    
    try:
        context = gx.get_context(project_root_dir=gx_dir)
        print(f"✅ Loaded Great Expectations context from {gx_dir}")
    except Exception as e:
        print(f"❌ Could not load Great Expectations context: {e}")
        print("💡 Please run init_great_expectations.py first!")
        sys.exit(1)
    
    # Run all validations
    results = run_all_validations(context, spark, azure_config)
    
    # Print summary
    all_passed = print_summary(results)
    
    # Exit with appropriate code
    if all_passed:
        print("🎉 All validations passed successfully!")
        sys.exit(0)
    else:
        print("⚠️  Some validations failed. Please review the results above.")
        sys.exit(1)

