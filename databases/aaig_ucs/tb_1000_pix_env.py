from ...classes import ADACTable, CubeTable, IngestionTable, StagingTable
from ..aaig_wks import TB_1000_PIX_ENV_STG
from ..bspi_ing import TPIX_ENV
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from typing import Dict, Callable, List, Tuple

def map_tpix_env(df: DataFrame) -> DataFrame:
  return df.select(
    df["cid_trans"],
    df["cag_bcria"].alias("agencia"),
    df["ccta_corr"].alias("conta"),
    df["qtd"],
    df["vlr"],
    df["dt_trans"].alias("ref"),
    df["dt_ingtao_ptcao"]
  )

class TB_1000_PIX_ENV(CubeTable):

    _database_name = "AAIG_UCS"
    _source_tables: List[ADACTable] = [TPIX_ENV]
    _stg_mappers: Dict[str, Callable[[DataFrame], DataFrame]] = {
      "bspi_ing.tpix_env": map_tpix_env
    }
    _table_name = "TB_1000_PIX_ENV"

    @property
    def stg_tb(self) -> StagingTable:
        return TB_1000_PIX_ENV_STG(self.spark_session, self)
    
    @property
    def table_keys(self) -> Tuple[str]:
        return ("cid_trans")
    
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return ("dt_ingtao_ptcao")
    
    @property
    def table_struct(self) -> StructType:
        return StructType([
            StructField("cid_trans", StringType(), True),
			      StructField("cag_bcria", IntegerType(), True),
			      StructField("ccta_corr", IntegerType(), True),
            StructField("qtd", IntegerType(), True),
            StructField("vlr", StringType(), True),
            StructField("ref", StringType(), True),
            StructField("dt_ingtao_ptcao", StringType(), True)
        ])
