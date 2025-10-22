import os
import tempfile
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from configs.data_schema import FLIGHT_DATA_SCHEMA

class AzureDataDownloader:
    """Download data from Azure Data Lake using Azure SDK"""
    
    def __init__(self, storage_account_name: str, container_name: str, 
                 account_key: str = None, tenant_id: str = None, 
                 client_id: str = None, client_secret: str = None):
        self.storage_account_name = storage_account_name
        self.container_name = container_name
        self.account_key = account_key
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        
        # Initialize Azure client
        self.service_client = self._get_service_client()
    
    def _get_service_client(self):
        """Get Azure Data Lake service client"""
        account_url = f"https://{self.storage_account_name}.dfs.core.windows.net"
        
        if self.account_key:
            # Use account key authentication
            from azure.storage.filedatalake import DataLakeServiceClient
            return DataLakeServiceClient(account_url=account_url, credential=self.account_key)
        else:
            # Use OAuth authentication
            if self.client_id and self.client_secret and self.tenant_id:
                credential = ClientSecretCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
            else:
                credential = DefaultAzureCredential()
            
            return DataLakeServiceClient(account_url=account_url, credential=credential)
    
    def download_file_to_local(self, remote_path: str, local_path: str = None):
        """Download file from Azure Data Lake to local filesystem"""
        if local_path is None:
            local_path = tempfile.mktemp(suffix=".csv")
        
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=self.container_name)
            file_client = file_system_client.get_file_client(file_path=remote_path)
            
            with open(local_path, "wb") as local_file:
                download = file_client.download_file()
                local_file.write(download.readall())
            
            print(f"Downloaded {remote_path} to {local_path}")
            return local_path
            
        except Exception as e:
            print(f"Error downloading {remote_path}: {e}")
            return None
    
    def list_files(self, path: str):
        """List files in Azure Data Lake path"""
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=self.container_name)
            paths = file_system_client.get_paths(path=path)
            return [path.name for path in paths if not path.is_directory]
        except Exception as e:
            print(f"Error listing files: {e}")
            return []
    
    def load_flight_data_with_azure_sdk(self, spark: SparkSession, year: int, month: int):
        """Load flight data using Azure SDK"""
        temp_dir = None
        try:
            # Define paths
            flight_path = f"bronze/flights/{year}/{month}/T_ONTIME_FLIGHT_REPORTING_{month.upper()}_{year}.csv"
            airport_path = "bronze/lookup/L_AIRPORT_ID.csv"
            carrier_path = "bronze/lookup/L_UNIQUE_CARRIERS.csv"
            
            # Download files to local temp directory
            temp_dir = tempfile.mkdtemp()
            flight_file = self.download_file_to_local(flight_path, os.path.join(temp_dir, "flights.csv"))
            airport_file = self.download_file_to_local(airport_path, os.path.join(temp_dir, "airports.csv"))
            carrier_file = self.download_file_to_local(carrier_path, os.path.join(temp_dir, "carriers.csv"))
            
            if not all([flight_file, airport_file, carrier_file]):
                print("Failed to download some files from Azure")
                return None
            
            # Load data using Spark with local files
            data = (spark.read
                    .option("header", "true")
                    .option("timestampFormat", "M/d/yyyy hh:mm:ss a")
                    .schema(FLIGHT_DATA_SCHEMA)
                    .csv(flight_file))
            
            airport_lookup = spark.read.option("header", "true").csv(airport_file)
            carrier_lookup = spark.read.option("header", "true").csv(carrier_file)
            
            # Cache lookup tables
            airport_lookup.cache()
            carrier_lookup.cache()
            
            # Perform joins
            df = (data
                  .join(airport_lookup, data.ORIGIN_AIRPORT_ID == airport_lookup.Code, "left")
                  .withColumnRenamed("Description", "ORIGIN_AIRPORT_NAME")
                  .drop("Code")
                  .join(airport_lookup, data.DEST_AIRPORT_ID == airport_lookup.Code, "left")
                  .withColumnRenamed("Description", "DEST_AIRPORT_NAME")
                  .drop("Code")
                  .join(carrier_lookup, data.OP_UNIQUE_CARRIER == carrier_lookup.Code, "left")
                  .withColumnRenamed("Description", "CARRIER_NAME")
                  .drop("Code"))
            
            print(f"Successfully loaded {df.count():,} records from Azure (via SDK)")
            return df
            
        except Exception as e:
            print(f"Error loading from Azure: {e}")
            return None
        # Note: Don't clean up temp files here - let them persist until Spark job completes
        # Store temp_dir for later cleanup
        self._temp_dir = temp_dir
    
    def cleanup_temp_files(self):
        """Clean up temporary files after Spark job completes"""
        if hasattr(self, '_temp_dir') and self._temp_dir:
            try:
                import shutil
                shutil.rmtree(self._temp_dir)
                print(f"Cleaned up temp directory: {self._temp_dir}")
            except Exception as e:
                print(f"Error cleaning up temp files: {e}")
    
    def upload_local_delta_to_azure(self, local_delta_path: str, azure_delta_path: str):
        """Upload local Delta table to Azure Data Lake"""
        try:
            import shutil
            import os
            
            # Create Azure path
            azure_path_parts = azure_delta_path.replace("abfss://", "").split("/")
            container_name = azure_path_parts[0].split("@")[0]
            storage_account = azure_path_parts[0].split("@")[1].split(".")[0]
            azure_folder = "/".join(azure_path_parts[1:])
            
            print(f"Uploading Delta table from {local_delta_path} to Azure {azure_folder}")
            
            # Get file system client
            file_system_client = self.service_client.get_file_system_client(file_system=container_name)
            
            # Upload all files in the Delta table directory
            for root, dirs, files in os.walk(local_delta_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, local_delta_path)
                    azure_file_path = f"{azure_folder}/{relative_path}".replace("\\", "/")
                    
                    # Create directory if needed
                    azure_dir = "/".join(azure_file_path.split("/")[:-1])
                    if azure_dir:
                        try:
                            file_system_client.create_directory(azure_dir)
                        except:
                            pass  # Directory might already exist
                    
                    # Upload file
                    file_client = file_system_client.get_file_client(file_path=azure_file_path)
                    with open(local_file_path, "rb") as data:
                        file_client.upload_data(data, overwrite=True)
                    
                    print(f"Uploaded: {azure_file_path}")
            
            print(f"Successfully uploaded Delta table to Azure: {azure_delta_path}")
            return True
            
        except Exception as e:
            print(f"Error uploading Delta table to Azure: {e}")
            return False

def ingest_to_bronze_with_azure_sdk(spark: SparkSession, year: int, month: str, bronze_output_path: str):
    """Ingest data using Azure SDK approach"""
    from configs.azure_config import load_azure_config_from_env
    
    # Load Azure configuration
    config = load_azure_config_from_env()
    
    # Create Azure downloader
    downloader = AzureDataDownloader(
        storage_account_name=config.storage_account_name,
        container_name=config.container_name,
        account_key=config.account_key,
        tenant_id=config.tenant_id,
        client_id=config.client_id,
        client_secret=config.client_secret
    )
    
    # Load data using Azure SDK
    df = downloader.load_flight_data_with_azure_sdk(spark, year, month)
    
    if df is None:
        raise ValueError(f"Failed to load data for {month} {year}")
    
    # Add metadata and write to Delta
    df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp())\
        .withColumn("source_file", lit(f"T_ONTIME_FLIGHT_REPORTING_{month}_{year}.csv"))\
        .withColumn("partition_date", col("FL_DATE").cast("date"))
    
    # df_with_metadata.write\
    #     .format("delta")\
    #     .mode("overwrite")\
    #     .option("overwriteSchema", "true")\
    #     .partitionBy("partition_date")\
    #     .save(bronze_output_path)

    return df_with_metadata