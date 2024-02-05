from .productive_table import ProductiveTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Tuple

class StagingTable(ProductiveTable):
    # hist_table type is "HistoricalTable", but we cannot import it, because of circular
    # imports.
    def __init__(self, spark_session: SparkSession, hist_table) -> None:
        self._hist_tb = hist_table        
        super().__init__(spark_session)
    
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return self._hist_tb.table_partition_columns

    @property
    def table_struct(self) -> StructType:
        return self._hist_tb.table_struct