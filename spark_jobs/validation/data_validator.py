"""
Data Validator Module
Validates data using Great Expectations within Spark jobs
"""
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import DataFrame
import sys
import os

class DataValidator:
    """Validates data using Great Expectations"""
    
    def __init__(self, project_root: str = None):
        """
        Initialize DataValidator
        
        Args:
            project_root: Path to project root directory
        """
        if project_root is None:
            project_root = "/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse"
        
        self.project_root = project_root
        # GX context is in expectations/great_expectations subdirectory
        self.gx_dir = f"{project_root}/expectations/great_expectations"
        
        try:
            self.context = gx.get_context(context_root_dir=self.gx_dir)
            print(f"✅ Loaded Great Expectations context from {self.gx_dir}")
        except Exception as e:
            print(f"⚠️ Could not load Great Expectations context: {e}")
            print(f"💡 Please run: python {project_root}/expectations/init_great_expectations.py")
            self.context = None
    
    def validate_bronze_data(self, df: DataFrame, batch_id: str = "bronze_validation"):
        """
        Validate bronze layer data
        
        Args:
            df: PySpark DataFrame to validate
            batch_id: Identifier for this batch
            
        Returns:
            dict: Validation results
        """
        if not self.context:
            print("❌ Great Expectations context not loaded. Skipping validation.")
            return {"success": False, "error": "Context not loaded"}
        
        print(f"🔍 Validating Bronze data (batch: {batch_id})...")
        
        try:
            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name="bronze_flights",
                data_connector_name="default_runtime_data_connector",
                data_asset_name="flights",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"batch_id": batch_id}
            )
            
            # Get validator directly (don't use checkpoint for runtime data)
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name="bronze_flights_suite"
            )
            
            # Run validation
            results = validator.validate()
            
            if results["success"]:
                print("✅ Bronze validation PASSED")
            else:
                print("❌ Bronze validation FAILED")
                self._print_validation_results(results)
            
            return results
        except Exception as e:
            print(f"❌ Bronze validation error: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def validate_silver_data(self, df: DataFrame, batch_id: str = "silver_validation"):
        """
        Validate silver layer data
        
        Args:
            df: PySpark DataFrame to validate
            batch_id: Identifier for this batch
            
        Returns:
            dict: Validation results
        """
        if not self.context:
            print("❌ Great Expectations context not loaded. Skipping validation.")
            return {"success": False, "error": "Context not loaded"}
        
        print(f"🔍 Validating Silver data (batch: {batch_id})...")
        
        try:
            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name="silver_flights",
                data_connector_name="default_runtime_data_connector",
                data_asset_name="flights",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"batch_id": batch_id}
            )
            
            # Get validator directly (don't use checkpoint for runtime data)
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name="silver_flights_suite"
            )
            
            # Run validation
            results = validator.validate()
            
            if results["success"]:
                print("✅ Silver validation PASSED")
            else:
                print("❌ Silver validation FAILED")
                self._print_validation_results(results)
            
            return results
        except Exception as e:
            print(f"❌ Silver validation error: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def validate_dimension(self, df: DataFrame, dimension_name: str, batch_id: str = None):
        """
        Validate a dimension table in the Gold layer
        
        Args:
            df: PySpark DataFrame to validate
            dimension_name: Name of the dimension (e.g., 'dim_airline', 'dim_airport')
            batch_id: Identifier for this batch
            
        Returns:
            dict: Validation results
        """
        if not self.context:
            print("❌ Great Expectations context not loaded. Skipping validation.")
            return {"success": False, "error": "Context not loaded"}
        
        if batch_id is None:
            batch_id = f"{dimension_name}_validation"
        
        print(f"🔍 Validating {dimension_name} (batch: {batch_id})...")
        
        batch_request = RuntimeBatchRequest(
            datasource_name="gold_dimensions",
            data_connector_name="default_runtime_data_connector",
            data_asset_name=dimension_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": batch_id}
        )
        
        checkpoint_config = {
            "name": f"{dimension_name}_checkpoint_{batch_id}",
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": f"gold_{dimension_name}_suite"
                }
            ]
        }
        
        try:
            checkpoint = self.context.add_or_update_checkpoint(**checkpoint_config)
            results = checkpoint.run()
            
            if results["success"]:
                print(f"✅ {dimension_name} validation PASSED")
            else:
                print(f"❌ {dimension_name} validation FAILED")
                self._print_validation_results(results)
            
            return results
        except Exception as e:
            print(f"❌ {dimension_name} validation error: {e}")
            return {"success": False, "error": str(e)}
    
    def validate_fact_table(self, df: DataFrame, fact_name: str = "fact_flights", batch_id: str = None):
        """
        Validate a fact table in the Gold layer
        
        Args:
            df: PySpark DataFrame to validate
            fact_name: Name of the fact table (e.g., 'fact_flights')
            batch_id: Identifier for this batch
            
        Returns:
            dict: Validation results
        """
        if not self.context:
            print("❌ Great Expectations context not loaded. Skipping validation.")
            return {"success": False, "error": "Context not loaded"}
        
        if batch_id is None:
            batch_id = f"{fact_name}_validation"
        
        print(f"🔍 Validating {fact_name} (batch: {batch_id})...")
        
        batch_request = RuntimeBatchRequest(
            datasource_name="gold_facts",
            data_connector_name="default_runtime_data_connector",
            data_asset_name=fact_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": batch_id}
        )
        
        checkpoint_config = {
            "name": f"{fact_name}_checkpoint_{batch_id}",
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": f"gold_{fact_name}_suite"
                }
            ]
        }
        
        try:
            checkpoint = self.context.add_or_update_checkpoint(**checkpoint_config)
            results = checkpoint.run()
            
            if results["success"]:
                print(f"✅ {fact_name} validation PASSED")
            else:
                print(f"❌ {fact_name} validation FAILED")
                self._print_validation_results(results)
            
            return results
        except Exception as e:
            print(f"❌ {fact_name} validation error: {e}")
            return {"success": False, "error": str(e)}
    
    def _print_validation_results(self, results):
        """Print detailed validation results"""
        print("\n" + "-"*60)
        print("Failed Expectations:")
        print("-"*60)
        
        # Handle both checkpoint results and validator results
        if hasattr(results, 'results'):
            # Direct validator results (list of expectation results)
            expectations = results.results
        elif hasattr(results, 'run_results'):
            # Checkpoint results (nested structure)
            expectations = []
            for run_result in results.run_results.values():
                validation_result = run_result.get("validation_result", {})
                expectations.extend(validation_result.get("results", []))
        else:
            print("   No results to display")
            return
        
        failed_count = 0
        for exp_result in expectations:
            if not exp_result.get("success", True):
                failed_count += 1
                exp_type = exp_result.get("expectation_config", {}).get("expectation_type", "Unknown")
                kwargs = exp_result.get("expectation_config", {}).get("kwargs", {})
                column = kwargs.get("column", "N/A")
                
                print(f"\n❌ {exp_type}")
                print(f"   Column: {column}")
                
                # Print specific failure details
                result_details = exp_result.get("result", {})
                if "observed_value" in result_details:
                    print(f"   Observed: {result_details['observed_value']}")
                if "element_count" in result_details:
                    print(f"   Element count: {result_details['element_count']}")
                if "unexpected_count" in result_details:
                    print(f"   Unexpected count: {result_details['unexpected_count']}")
                if "unexpected_percent" in result_details:
                    print(f"   Unexpected %: {result_details['unexpected_percent']:.2f}%")
        
        if failed_count == 0:
            print("   No failed expectations!")
        
        print("-"*60 + "\n")
    
    def get_validation_statistics(self, results):
        """
        Extract statistics from validation results
        
        Args:
            results: Validation results dictionary
            
        Returns:
            dict: Statistics including total, passed, failed expectations
        """
        # Handle error cases
        if isinstance(results, dict) and not results.get("success") and "error" in results:
            return {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "success_rate": 0.0
            }
        
        total = 0
        passed = 0
        
        # Handle both checkpoint results and validator results
        if hasattr(results, 'results'):
            # Direct validator results (list of expectation results)
            expectations = results.results
        elif hasattr(results, 'run_results'):
            # Checkpoint results (nested structure)
            expectations = []
            for run_result in results.run_results.values():
                validation_result = run_result.get("validation_result", {})
                expectations.extend(validation_result.get("results", []))
        else:
            return {
                "total": 0,
                "passed": 0,
                "failed": 0,
                "success_rate": 0.0
            }
        
        for exp_result in expectations:
            total += 1
            if exp_result.get("success", False):
                passed += 1
        
        failed = total - passed
        success_rate = (passed / total * 100) if total > 0 else 0.0
        
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "success_rate": success_rate
        }

