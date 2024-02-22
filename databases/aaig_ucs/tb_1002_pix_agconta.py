from .tb_1000_pix_env import TB_1000_PIX_ENV
from .tb_1001_pix_rec import TB_1001_PIX_REC
from ..aaig_wks import TB_1002_PIX_AGCONTA_STG
from ...classes import ADACTable, CubeTable, StagingTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from typing import Dict, Callable, List, Tuple

def map_tb_1000_pix_env(df: DataFrame) -> DataFrame:
  return df.groupBy(
    df["cag_bcria"].alias("agencia"),
    df["ccta_corr"].alias("conta"),
    df["ref"]
  ).agg(
    _sum(df["qtd"]).alias("qtd_env"),
    _sum(df["vlr"]).alias("vlr_env")
  )

def map_tb_1001_pix_rec(df: DataFrame) -> DataFrame:
  return df.groupBy(
    df["cag_bcria"].alias("agencia"),
    df["ccta_corr"].alias("conta"),
    df["ref"]
  ).agg(
    _sum(df["qtd"]).alias("qtd_rec"),
    _sum(df["vlr"]).alias("vlr_rec")
  )

class TB_1002_PIX_AGCONTA(CubeTable):

    _database_name = "AAIG_UCS"
    _has_aggregation: bool = True
    _source_tables: List[ADACTable] = [TB_1000_PIX_ENV, TB_1001_PIX_REC]
    _stg_mappers: Dict[ADACTable, Callable[[DataFrame], DataFrame]] = {
      TB_1000_PIX_ENV: map_tb_1000_pix_env,
      TB_1001_PIX_REC: map_tb_1001_pix_rec
    }
    _table_name = "TB_1002_PIX_AGCONTA"

    @property
    def stg_tb(self) -> StagingTable:
        return TB_1002_PIX_AGCONTA_STG(self.spark_session, self)
    
    @property
    def table_keys(self) -> Tuple[str]:
        return ("agencia", "conta", "ref")
    
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return ("ref", "agencia")
    
    @property
    def table_struct(self) -> StructType:
        return StructType([
			      StructField("agencia", IntegerType(), True),
			      StructField("conta", IntegerType(), True),
            StructField("qtd_env", IntegerType(), True),
            StructField("vlr_env", StringType(), True),
            StructField("qtd_rec", IntegerType(), True),
            StructField("vlr_rec", StringType(), True),
            StructField("ref", StringType(), True)
        ])
