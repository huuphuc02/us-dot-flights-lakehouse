"""
Azure Data Lake Configuration
Handles authentication and connection to Azure Storage
"""

import os
from dataclasses import dataclass
from typing import Optional
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@dataclass
class AzureConfig:
    """Azure Data Lake configuration"""
    storage_account_name: str
    container_name: str
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    account_key: Optional[str] = None
    
    @property
    def account_url(self) -> str:
        return f"https://{self.storage_account_name}.dfs.core.windows.net"
    
    @property
    def container_url(self) -> str:
        return f"{self.account_url}/{self.container_name}"

def load_azure_config_from_env() -> AzureConfig:
    """Load Azure configuration from environment variables"""
    return AzureConfig(
        storage_account_name=os.getenv("AZURE_STORAGE_ACCOUNT_NAME", ""),
        container_name=os.getenv("AZURE_CONTAINER_NAME", "flights-data"),
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        account_key=os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    )

def get_azure_credential(config: AzureConfig):
    """Get Azure credential based on available configuration"""
    if config.client_id and config.client_secret and config.tenant_id:
        return ClientSecretCredential(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.client_secret
        )
    else:
        return DefaultAzureCredential()

def configure_spark_for_azure(spark: SparkSession, config: AzureConfig):
    """Configure Spark session for Azure Data Lake access"""
    
    # Set Azure storage account
    spark.conf.set(f"fs.azure.account.name.{config.storage_account_name}.dfs.core.windows.net", config.storage_account_name)
    
    # Configure authentication
    if config.account_key:
        # Use account key authentication
        spark.conf.set(f"fs.azure.account.key.{config.storage_account_name}.dfs.core.windows.net", config.account_key)
        spark.conf.set("fs.azure.account.auth.type", "SharedKey")
    else:
        # Use OAuth authentication
        credential = get_azure_credential(config)
        token = credential.get_token("https://storage.azure.com/.default").token
        
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{config.storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{config.storage_account_name}.dfs.core.windows.net", config.client_id or "")
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{config.storage_account_name}.dfs.core.windows.net", config.client_secret or "")
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{config.storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{config.tenant_id}/oauth2/token")
        spark.conf.set("fs.azure.account.auth.type", "OAuth")
    
    return spark

def create_spark_session_with_azure(app_name: str, azure_config: AzureConfig, master: str = "local[*]", memory: str = "4g"):
    """Create Spark session with Azure Data Lake support"""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    
    builder = (SparkSession.builder
               .appName(app_name)
               .master(master)
               .config("spark.driver.memory", memory)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.jars.packages", 
                       "org.apache.hadoop:hadoop-azure:3.3.6"))
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Configure Azure after session creation
    configure_spark_for_azure(spark, azure_config)
    
    return spark

def get_azure_data_paths(config: AzureConfig, year: int, month: int) -> dict:
    """Get Azure Data Lake paths for flight data"""
    base_path = f"abfss://{config.container_name}@{config.storage_account_name}.dfs.core.windows.net"
    
    return {
        "bronze": f"{base_path}/bronze",
        "silver": f"{base_path}/silver", 
        "gold": f"{base_path}/gold",
        "raw_flights": f"{base_path}/bronze/flights/{year}/{month}",
        "airports": f"{base_path}/bronze/lookup/L_AIRPORT_ID.csv",
        "carriers": f"{base_path}/bronze/lookup/L_UNIQUE_CARRIERS.csv",
        "year": year,
        "month": month
    }

def list_azure_files(config: AzureConfig, path: str) -> list:
    """List files in Azure Data Lake path"""
    try:
        credential = get_azure_credential(config)
        service_client = DataLakeServiceClient(
            account_url=config.account_url,
            credential=credential
        )
        
        file_system_client = service_client.get_file_system_client(file_system=config.container_name)
        paths = file_system_client.get_paths(path=path)
        
        return [path.name for path in paths if not path.is_directory]
    except Exception as e:
        print(f"Error listing files: {e}")
        return []
