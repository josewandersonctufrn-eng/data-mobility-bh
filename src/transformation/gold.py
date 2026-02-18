"""
Gold layer transformation: business aggregations and analytics.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, max, min, count, round as spark_round,
    window, date_format, floor
)

logger = logging.getLogger(__name__)

# Grid resolution (degrees)
GRID_SIZE = 0.01  # ~1km


class GoldTransformer:
    """Handles Gold layer transformations and analytics."""
    
    @staticmethod
    def aggregate_speed_by_zone(df: DataFrame) -> DataFrame:
        """Aggregate average speed by geographic zones (grid).
        
        Args:
            df: Silver DataFrame
            
        Returns:
            Aggregated DataFrame with speed metrics per zone
        """
        logger.info("Aggregating speed by geographic zones")
        
        df_zones = df.withColumn(
            "zone_lat",
            floor(col("latitude") / GRID_SIZE) * GRID_SIZE
        ).withColumn(
            "zone_lon",
            floor(col("longitude") / GRID_SIZE) * GRID_SIZE
        )
        
        agg_df = df_zones.groupBy("zone_lat", "zone_lon").agg(
            spark_round(avg(col("velocidade")), 2).alias("velocidade_media"),
            spark_round(max(col("velocidade")), 2).alias("velocidade_max"),
            spark_round(min(col("velocidade")), 2).alias("velocidade_min"),
            count(col("id_veiculo")).alias("total_registros")
        )
        
        logger.info(f"Created {agg_df.count()} zones with speed aggregation")
        return agg_df
    
    @staticmethod
    def aggregate_speed_by_line_day(df: DataFrame) -> DataFrame:
        """Aggregate average speed by bus line and day.
        
        Args:
            df: Silver DataFrame
            
        Returns:
            Aggregated DataFrame with daily metrics per line
        """
        logger.info("Aggregating speed by bus line and day")
        
        df_date = df.withColumn(
            "data_posicao",
            date_format(col("timestamp_posicao"), "yyyy-MM-dd")
        )
        
        agg_df = df_date.groupBy("id_linha", "nome_linha", "data_posicao").agg(
            spark_round(avg(col("velocidade")), 2).alias("velocidade_media"),
            count(col("id_veiculo")).alias("total_registros"),
            count(col("id_veiculo").distinct()).alias("total_veiculos")
        )
        
        logger.info(f"Created {agg_df.count()} line-day combinations")
        return agg_df
    
    @staticmethod
    def aggregate_vehicle_daily_stats(df: DataFrame) -> DataFrame:
        """Daily statistics per vehicle: distance proxy, avg speed, activity hours.
        
        Args:
            df: Silver DataFrame
            
        Returns:
            Aggregated DataFrame with daily vehicle statistics
        """
        logger.info("Aggregating daily vehicle statistics")
        
        df_date = df.withColumn(
            "data_posicao",
            date_format(col("timestamp_posicao"), "yyyy-MM-dd")
        )
        
        agg_df = df_date.groupBy("id_veiculo", "data_posicao").agg(
            spark_round(avg(col("velocidade")), 2).alias("velocidade_media"),
            count(col("timestamp_posicao")).alias("total_registros"),
            count(col("id_linha").distinct()).alias("total_linhas"),
            spark_round(max(col("velocidade")), 2).alias("velocidade_max")
        )
        
        logger.info(f"Created {agg_df.count()} vehicle-day combinations")
        return agg_df
    
    def transform_silver_to_gold(
        self,
        spark: SparkSession,
        silver_path: str,
        gold_path: str
    ) -> dict:
        """Full Gold transformation pipeline.
        Generates multiple analytics tables.
        
        Args:
            spark: SparkSession instance
            silver_path: Path to Silver layer
            gold_path: Path to Gold layer
            
        Returns:
            Dictionary with all generated DataFrames
        """
        logger.info(f"Starting Silver → Gold transformation")
        
        # Read Silver
        df_silver = spark.read.parquet(silver_path)
        logger.info(f"Read {df_silver.count()} records from Silver")
        
        # Generate aggregations
        tables = {}
        
        # 1. Speed by zone
        agg_zone = self.aggregate_speed_by_zone(df_silver)
        agg_zone.write.mode("overwrite").parquet(f"{gold_path}/speed_by_zone")
        tables["speed_by_zone"] = agg_zone
        logger.info(f"Wrote speed_by_zone table")
        
        # 2. Speed by line and day
        agg_line = self.aggregate_speed_by_line_day(df_silver)
        agg_line.write.mode("overwrite").parquet(f"{gold_path}/speed_by_line_day")
        tables["speed_by_line_day"] = agg_line
        logger.info(f"Wrote speed_by_line_day table")
        
        # 3. Vehicle daily stats
        agg_vehicle = self.aggregate_vehicle_daily_stats(df_silver)
        agg_vehicle.write.mode("overwrite").parquet(f"{gold_path}/vehicle_daily_stats")
        tables["vehicle_daily_stats"] = agg_vehicle
        logger.info(f"Wrote vehicle_daily_stats table")
        
        logger.info(f"Gold layer transformation complete: {len(tables)} tables generated")
        return tables
