from .adac_table import ADACTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from typing import Dict, Tuple

class ProductiveTable(ADACTable):
    
    def __init__(self, spark_session: SparkSession) -> None:
        try:            
            super().__init__(spark_session)

        except Exception as e:
            if (str(e).startswith("[TABLE_OR_VIEW_NOT_FOUND]")):
                self.create_table()
                super().__init__(spark_session)
            else:
                raise e
    
    @property
    def table_exists(self) -> bool:
        return self.spark_session.catalog.tableExists(self.table_full_name)  
      
    @property
    def table_struct(self) -> StructType:
        raise Exception("Propriedade não implementada")

    def create_table(self) -> None:
        self.save_into_table(
            self.spark_session.createDataFrame([], self.table_struct),
            "overwrite"            
        )

    def delete_partition(self, partition: Dict[str, str]) -> None:
        partitions_str = ", ".join([f"{key} = '{value}'" for key, value in partition.items()])
        self.spark_session.sql(f"ALTER TABLE {self.table_full_name} DROP IF EXISTS PARTITION ({partitions_str})")
        
    def drop_table(self) -> None:
        self.spark_session.sql(f"DROP TABLE {self.table_full_name}")

    def truncate_table(self) -> None:
        self.spark_session.sql(f"TRUNCATE TABLE {self.table_full_name}")

    def save_into_table(self, data_frame: DataFrame, mode: str) -> None:
        df: DataFrame = data_frame.write

        if (len(self.table_partition_columns) > 0):
            df = df.partitionBy(self.table_partition_columns)

        df.mode(mode).format("hive").saveAsTable(self.table_full_name)    
        