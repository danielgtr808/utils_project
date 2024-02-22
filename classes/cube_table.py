from .adac_table import ADACTable
from .productive_table import ProductiveTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from typing import Callable, Dict, List

class CubeTable(ProductiveTable):

  _has_aggregation: bool = False
  _source_tables: List[ADACTable] = []
  _stg_mappers: Dict[ADACTable, Callable[[DataFrame], DataFrame]] = { }

  @property
  def has_aggregation(self) -> bool:
    return self._has_aggregation

  @property
  def source_tables(self) -> List[ADACTable]:
    return self._source_tables

  def map_to_stg(self, data: DataFrame, table_reference: ADACTable) -> DataFrame:
    if (table_reference.__class__ not in self._stg_mappers):
      raise Exception(f"Não foi definido um mapper para a tabela \"{table_reference.table_full_name}\".")
    
    return self._stg_mappers[table_reference.__class__](data)
    # mapped_df: DataFrame = self._stg_mappers[table_reference.__class__](data)
    # for column_name in self.columns:
    #   if (column_name not in mapped_df.columns):
    #     mapped_df = mapped_df.withColumn(column_name, lit(None))

    # return mapped_df