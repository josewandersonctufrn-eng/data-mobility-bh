"""
CKAN API ingestion module.
Fetches GPS position data from PBH CKAN and saves to Bronze (Parquet).
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import requests
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, col, to_timestamp

logger = logging.getLogger(__name__)


class CKANIngestor:
    """Handles data ingestion from CKAN API."""
    
    def __init__(self, config_path: str = "config/endpoints.yml"):
        """
        Initialize CKAN ingestor with configuration.
        
        Args:
            config_path: Path to endpoints YAML configuration
        """
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        
        self.base_url = self.config["ckan"]["base_url"]
        self.resource_id = self.config["ckan"]["resource_id_posicao"]
        self.session = requests.Session()
        logger.info(f"CKANIngestor initialized with base_url: {self.base_url}")
    
    def fetch_data(self, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """
        Fetch GPS position data from CKAN API.
        
        Args:
            limit: Maximum records to fetch
            offset: Starting offset for pagination
            
        Returns:
            JSON response from CKAN API
            
        Raises:
            requests.RequestException: If API call fails
        """
        url = f"{self.base_url}/api/3/action/datastore_search"
        params = {
            "resource_id": self.resource_id,
            "limit": limit,
            "offset": offset,
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            logger.info(f"Fetched {len(response.json()['result']['records'])} records from CKAN")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"CKAN API error: {e}")
            raise
    
    def ingest_to_bronze(
        self,
        spark: SparkSession,
        bronze_path: str,
        limit: int = 1000
    ) -> DataFrame:
        """
        Ingest data from CKAN to Bronze layer (Parquet).
        
        Args:
            spark: SparkSession instance
            bronze_path: Path to Bronze storage
            limit: Records to fetch per batch
            
        Returns:
            DataFrame written to Bronze
        """
        logger.info(f"Starting ingestion to Bronze: {bronze_path}")
        
        # Fetch data from CKAN
        data = self.fetch_data(limit=limit)
        records = data["result"]["records"]
        
        if not records:
            logger.warning("No records fetched from CKAN")
            return spark.createDataFrame([], "id_veiculo string")
        
        # Create DataFrame
        df = spark.createDataFrame(records)
        
        # Add ingestion timestamp
        df = df.withColumn("data_ingestao", current_timestamp())
        
        # Partition by date
        ingest_date = datetime.now().strftime("%Y-%m-%d")
        partition_path = f"{bronze_path}/data_ingestao={ingest_date}"
        
        # Write to Parquet
        df.write.mode("append").parquet(partition_path)
        logger.info(f"Ingested {len(records)} records to {partition_path}")
        
        return df
