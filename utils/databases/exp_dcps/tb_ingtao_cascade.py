from ...classes import IngestionTable
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Tuple, Union

# 
class TB_INGTAO_CASCADE(IngestionTable):

    _catulz_reg_col_name: Union[None, str] = "catulz_reg"
    _database_name = "exp_dcps"
    _hextr_cpu_col_name: str = "hextr_cpu"
    _is_cascade_delete: bool = True
    _table_name = "TB_INGTAO_CASCADE"

    def __init__(self, spark_session: SparkSession) -> None:
        super().__init__(spark_session)
    
    @property
    def table_keys(self) -> Tuple[str]:
        return ("key_1", "key_2")
        
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return ("dt_ingtao_ptcao")
    