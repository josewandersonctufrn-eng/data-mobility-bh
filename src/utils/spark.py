"""
Spark session initialization and utilities.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "DataMobilityBH", enable_delta: bool = True) -> SparkSession:
    """
    Create and return a Spark session with Delta Lake support.
    
    Args:
        app_name: Application name for Spark
        enable_delta: Whether to enable Delta Lake extensions
        
    Returns:
        SparkSession configured instance
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    if enable_delta:
        try:
            spark = builder.getOrCreate()
            logger.info(f"Spark session '{app_name}' created with Delta Lake support")
        except Exception as e:
            logger.warning(f"Could not enable Delta Lake: {e}. Continuing without it.")
            spark = builder.getOrCreate()
    else:
        spark = builder.getOrCreate()
    
    return spark

def get_schema_gps_posicao() -> StructType:
    """
    Define schema for GPS position data from CKAN API.
    Matches the CKAN data structure for bus positions.
    
    Returns:
        StructType with the GPS position schema
    """
    return StructType([
        StructField("id_veiculo", StringType(), True),
        StructField("timestamp_posicao", TimestampType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("velocidade", DoubleType(), True),
        StructField("direcao", DoubleType(), True),
        StructField("id_linha", StringType(), True),
        StructField("nome_linha", StringType(), True),
        StructField("data_ingestao", TimestampType(), True),
    ])

def stop_spark_session(spark: SparkSession) -> None:
    """
    Gracefully stop a Spark session.
    
    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()
        logger.info("Spark session stopped")
