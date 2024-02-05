from .ingestion_table import IngestionTable
from .productive_table import ProductiveTable
from .staging_table import StagingTable
from pyspark.sql import DataFrame

class HistoricalTable(ProductiveTable):
 
    @property
    def ing_tb(self) -> IngestionTable:
        raise Exception("Propriedade não implementada")

    @property
    def stg_tb(self) -> StagingTable:
        raise Exception("Propriedade não implementada")

    def map_ing(self, ing_df: DataFrame, keep_deleted_rows: bool = False) -> DataFrame:
        raise Exception("Método não implementado")