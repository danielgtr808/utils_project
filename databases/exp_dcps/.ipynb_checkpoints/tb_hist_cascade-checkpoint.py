from .tb_hist_cascade_stg import TB_HIST_CASCADE_STG
from .tb_ingtao_cascade import TB_INGTAO_CASCADE
from ...classes import HistoricalTable, IngestionTable, StagingTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from typing import Tuple



class TB_HIST_CASCADE(HistoricalTable):

    _database_name = "EXP_DCPS"
    _table_name = "TB_HIST_CASCADE"

    @property
    def ing_tb(self) -> IngestionTable:
        return TB_INGTAO_CASCADE(self.spark_session)

    def map_ing(self, ing_df: DataFrame, keep_deleted_rows: bool = False) -> DataFrame:
        return (ing_df if keep_deleted_rows else ing_df.where(ing_df["catulz_reg"] != "D")).select(
            ing_df["key_1"].alias("ID_1"),
            ing_df["key_2"].alias("ID_2"),
            ing_df["dt_cadt"],
            ing_df["stat"].alias("STATUS"),
            ing_df["dt_ingtao_ptcao"].alias("ref")
        )
    
    @property
    def stg_tb(self) -> StagingTable:
        return TB_HIST_CASCADE_STG(self.spark_session, self)
    
    @property
    def table_keys(self) -> Tuple[str]:
        return ("ID_1", "ID_2")
    
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return ("DT_CADT")
    
    @property
    def table_struct(self) -> StructType:
        return StructType([
            StructField("ID_1", StringType(), True),
            StructField("ID_2", StringType(), True),
            StructField("STATUS", StringType(), True),
			StructField("DT_CADT", StringType(), True),
			StructField("REF", StringType(), True)
        ])