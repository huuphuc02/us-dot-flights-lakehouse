import great_expectations as gx
from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import BatchRequest
import os

def init_great_expectations():
    """Initialize Great Expectations"""
    project_root = '/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse'
    expectations_dir = os.path.join(project_root, 'expectations')
    gx_dir = os.path.join(expectations_dir, 'great_expectations')
    
    # Ensure the directory exists
    os.makedirs(expectations_dir, exist_ok=True)
    
    try:
        # Try to get existing context from the gx_dir
        context = gx.get_context(context_root_dir=gx_dir)
        print(f'✅ Loading existing Great Expectations context from {gx_dir}')
    except Exception as e:
        print(f"Creating new Great Expectations context in {expectations_dir}...")
        print(f"Error was: {e}")
        
        # Initialize GX in the expectations directory
        import subprocess
        original_dir = os.getcwd()
        os.chdir(expectations_dir)
        
        try:
            # Run great_expectations init (non-interactive)
            result = subprocess.run(
                ["great_expectations", "init"],
                input="\n\n",  # Answer prompts with defaults
                capture_output=True,
                text=True
            )
            print(result.stdout)
            
            # Now get the context - GX creates a great_expectations subdirectory
            context = gx.get_context(context_root_dir=gx_dir)
            print("✅ Great Expectations initialized successfully")
        finally:
            os.chdir(original_dir)
    
    return context

def setup_datasources(context):
    project_root = '/Users/phuchuu/Desktop/Data engineering/Personal Projects/us-dot-flights-lakehouse'

    bronze_datasource_config = {
        "name": "bronze_flights",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        }
    }

    silver_datasource_config = {
        "name": "silver_flights",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        }
    }

    gold_dimensions_datasource_config = {
        "name": "gold_dimensions",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        }
    }

    gold_facts_datasource_config = {
        "name": "gold_facts",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        }
    }

    # Add all datasources
    datasources = [
        (bronze_datasource_config, "Bronze"),
        (silver_datasource_config, "Silver"),
        (gold_dimensions_datasource_config, "Gold Dimensions"),
        (gold_facts_datasource_config, "Gold Facts")
    ]

    for datasource_config, name in datasources:
        try:
            context.add_datasource(**datasource_config)
            print(f"✅ {name} datasource added successfully")
        except Exception as e:
            print(f"⚠️ {name} datasource already exists or error: {e}")

    return context

if __name__ == "__main__":
    context = init_great_expectations()
    setup_datasources(context)