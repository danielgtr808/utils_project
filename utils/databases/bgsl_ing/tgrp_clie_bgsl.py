from ...classes import IngestionTable
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Tuple, Union


class TGRP_CLIE_BGSL(IngestionTable):

    _catulz_reg_col_name: Union[None, str] = None
    _database_name = "bgsl_ing"
    _hextr_cpu_col_name: str = "dt_ingtao_ptcao"
    _is_cascade_delete: bool = False
    _table_name = "tgrp_clie_bgsl"

    @property
    def table_keys(self) -> Tuple[str]:
        return ("idgrp")
        
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return ("dt_ingtao_ptcao")
    