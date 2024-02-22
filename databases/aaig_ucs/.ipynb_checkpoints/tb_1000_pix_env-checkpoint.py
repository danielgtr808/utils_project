from ...classes import HistoricalTable, IngestionTable, StagingTable
from ..aaig_wks import TB_2000_BGSL_CTAS_STG
from ..bgsl_ing import TGRP_CLIE_BGSL
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from typing import Tuple


class TB_2000_BGSL_CTAS(HistoricalTable):

    _database_name = "AAIG_UCS"
    _table_name = "TB_2000_BGSL_CTAS"

    @property
    def ing_tb(self) -> IngestionTable:
        return TGRP_CLIE_BGSL(self.spark_session)

    def map_ing(self, ing_df: DataFrame, keep_deleted_rows: bool = False) -> DataFrame:
        return ing_df.select(
            ing_df["id"],
            ing_df["dt_cadt"],
            ing_df["dt_ingtao_ptcao"].alias("ref")
        )
    
    @property
    def stg_tb(self) -> StagingTable:
        return TB_2000_BGSL_CTAS_STG(self.spark_session, self)
    
    @property
    def table_keys(self) -> Tuple[str]:
        return ("ID")
    
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return ("DT_CADT")
    
    @property
    def table_struct(self) -> StructType:
        return StructType([
            StructField("ID", IntegerType(), True),
			StructField("DT_CADT", StringType(), True),
			StructField("REF", StringType(), True)
        ])