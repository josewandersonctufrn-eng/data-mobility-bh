"""
Silver layer transformation: data cleaning, deduplication, and quality checks.
"""
import logging
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, row_number, when, to_timestamp, unix_timestamp
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# BH geographic boundaries (approximate)
BH_LAT_MIN, BH_LAT_MAX = -20.05, -19.75
BH_LON_MIN, BH_LON_MAX = -44.05, -43.85


class SilverTransformer:
    """Handles Silver layer transformations."""
    
    @staticmethod
    def clean_gps_data(df: DataFrame) -> DataFrame:
        """
        Clean GPS data: handle nulls, fix data types, remove invalid records.
        
        Args:
            df: Raw DataFrame from Bronze
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Starting GPS data cleaning")
        
        # Convert timestamp to proper type if needed
        if df.schema["timestamp_posicao"].dataType.simpleString() != "timestamp":
            df = df.withColumn(
                "timestamp_posicao",
                to_timestamp(col("timestamp_posicao"), "yyyy-MM-dd HH:mm:ss")
            )
        
        # Remove rows with null critical fields
        df = df.filter(
            col("id_veiculo").isNotNull() &
            col("timestamp_posicao").isNotNull() &
            col("latitude").isNotNull() &
            col("longitude").isNotNull()
        )
        
        logger.info(f"After cleaning: {df.count()} valid records")
        return df
    
    @staticmethod
    def filter_off_grid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Separate on-grid and off-grid records.
        Off-grid: coordinates outside BH boundaries (quarantine).
        
        Args:
            df: DataFrame to filter
            
        Returns:
            Tuple of (on_grid_df, off_grid_df)
        """
        logger.info("Filtering off-grid records")
        
        on_grid = df.filter(
            (col("latitude") >= BH_LAT_MIN) &
            (col("latitude") <= BH_LAT_MAX) &
            (col("longitude") >= BH_LON_MIN) &
            (col("longitude") <= BH_LON_MAX)
        )
        
        off_grid = df.filter(
            (col("latitude") < BH_LAT_MIN) |
            (col("latitude") > BH_LAT_MAX) |
            (col("longitude") < BH_LON_MIN) |
            (col("longitude") > BH_LON_MAX)
        )
        
        logger.info(f"On-grid: {on_grid.count()}, Off-grid: {off_grid.count()}")
        return on_grid, off_grid
    
    @staticmethod
    def filter_speed_outliers(df: DataFrame, max_speed: float = 100.0) -> Tuple[DataFrame, DataFrame]:
        """
        Filter speed outliers (unrealistic velocities).
        
        Args:
            df: DataFrame to filter
            max_speed: Maximum acceptable speed in km/h
            
        Returns:
            Tuple of (valid_speed_df, outlier_df)
        """
        logger.info(f"Filtering speed outliers (max: {max_speed} km/h)")
        
        valid = df.filter(col("velocidade") <= max_speed)
        outliers = df.filter(col("velocidade") > max_speed)
        
        logger.info(f"Valid speed: {valid.count()}, Outliers: {outliers.count()}")
        return valid, outliers
    
    @staticmethod
    def deduplicate(df: DataFrame, partition_cols: list = None) -> DataFrame:
        """
        Remove duplicates by (id_veiculo, timestamp_posicao).
        
        Args:
            df: DataFrame to deduplicate
            partition_cols: Columns to partition by for deduplication
            
        Returns:
            Deduplicated DataFrame
        """
        if partition_cols is None:
            partition_cols = ["id_veiculo", "timestamp_posicao"]
        
        logger.info(f"Deduplicating by {partition_cols}")
        
        window = Window.partitionBy(partition_cols).orderBy(col("data_ingestao").desc())
        df_dedup = df.withColumn("row_num", row_number().over(window)) \
                     .filter(col("row_num") == 1) \
                     .drop("row_num")
        
        logger.info(f"After deduplication: {df_dedup.count()} records")
        return df_dedup
    
    def transform_bronze_to_silver(
        self,
        spark: SparkSession,
        bronze_path: str,
        silver_path: str
    ) -> DataFrame:
        """
        Full Silver transformation pipeline.
        
        Args:
            spark: SparkSession instance
            bronze_path: Path to Bronze layer
            silver_path: Path to Silver layer
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Starting Bronze → Silver transformation")
        
        # Read Bronze
        df_bronze = spark.read.parquet(bronze_path)
        logger.info(f"Read {df_bronze.count()} records from Bronze")
        
        # Clean
        df_clean = self.clean_gps_data(df_bronze)
        
        # Filter off-grid and write to quarantine
        df_on_grid, df_off_grid = self.filter_off_grid(df_clean)
        if df_off_grid.count() > 0:
            df_off_grid.write.mode("append").parquet(f"{silver_path}/quarantine")
            logger.info(f"Wrote {df_off_grid.count()} off-grid records to quarantine")
        
        # Filter speed outliers
        df_valid_speed, df_speed_outliers = self.filter_speed_outliers(df_on_grid)
        if df_speed_outliers.count() > 0:
            df_speed_outliers.write.mode("append").parquet(f"{silver_path}/speed_outliers")
        
        # Deduplicate
        df_silver = self.deduplicate(df_valid_speed)
        
        # Write Silver
        df_silver.write.mode("append").parquet(silver_path)
        logger.info(f"Wrote {df_silver.count()} records to Silver")
        
        return df_silver