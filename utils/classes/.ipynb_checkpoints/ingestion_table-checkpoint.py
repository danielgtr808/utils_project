from .adac_table import ADACTable
from ..functions import get_most_recent_data
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, List, Union

class IngestionTable(ADACTable):

    _catulz_reg_col_name: Union[None, str] = "catulz_reg"
    _hextr_cpu_col_name: str = "hextr_cpu"
    _is_cascade_delete: bool = True
    
    def __init__(self, spark_session: SparkSession) -> None:
        super().__init__(spark_session)

    @property
    def catulz_reg_col_name(self) -> bool:
        return self._catulz_reg_col_name

    @property
    def hextr_cpu_col_name(self) -> bool:
        return self._hextr_cpu_col_name

    @property
    def is_cascade_delete(self) -> bool:
        return self._is_cascade_delete

    def get_deleted_data(self, partition: Dict[str, str]) -> DataFrame:
        return self.get_partition(partition).where(self["catulz_reg"] == "D")

    def get_most_recent_data(self, partition: Union[Dict[str, str], None] = None) -> DataFrame:
        return get_most_recent_data(
            self.get_partition(partition) if partition is not None else self,
            self.table_keys,
            self.is_cascade_delete,
            self.catulz_reg_col_name,
            self.hextr_cpu_col_name
        )

    def get_partitions_after(self, dt_ingtao_ptcao: Union[None, str]) -> List[Dict[str, str]]:
        return list(filter(
            lambda x: True if dt_ingtao_ptcao is None else (x["dt_ingtao_ptcao"] > dt_ingtao_ptcao),
            self.table_partitions
        ))