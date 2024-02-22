from ...classes import IngestionTable
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Tuple, Union


class TPIX_REC(IngestionTable):

    _catulz_reg_col_name: Union[None, str] = None
    _database_name = "bspi_ing"
    _hextr_cpu_col_name: str = "dt_ingtao_ptcao"
    _is_cascade_delete: bool = False
    _is_full_ingestion: bool = False
    _table_name = "tpix_rec"

    @property
    def table_keys(self) -> Tuple[str]:
        return ("cid_trans")
        
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return ("dt_ingtao_ptcao")
    