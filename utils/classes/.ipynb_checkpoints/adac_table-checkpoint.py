from pyspark.sql import DataFrame, SparkSession
from typing import Dict, List, Tuple

class ADACTable():

    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session: SparkSession = spark_session
        self._data_frame: DataFrame = self.spark_session.table(self.table_full_name)

    @property
    def table_full_name(self) -> str:
        return f"{self._database_name}.{self._table_name}"

    @property
    def table_keys(self) -> Tuple[str]:
        raise Exception("Propriedade não implementado")
        
    @property
    def table_partition_columns(self) -> Tuple[str]:
        raise Exception("Propriedade não implementado")  
    
    @property
    def table_partitions(self) -> List[Dict[str, str]]:
        partitions: List[Dict[str, str]] = []
        
        for row in self.spark_session.sql(f"SHOW PARTITIONS {self.table_full_name}").collect():
            partition: Dict[str, str] = { }
            
            for partition_key_value in row["partition"].split("/"):
                key, value = partition_key_value.split("=")
                partition[key] = value
    
            partitions.append(partition)
    
        return partitions              
    
    def describe(self, n: int = 20, truncate: bool = False) -> None:
        self.spark_session.sql(f"DESCRIBE {self.table_full_name}").show(n = n, truncate=truncate)

    def get_partition(self, partition: Dict[str, str]) -> DataFrame:
        conditionals = data = [self[key] == value for key, value in partition.items()]
    
        filter = None
        for conditional in conditionals:
            if (filter is None):
                filter = conditional
            else:
                filter = (filter) & (conditional)
    
        return self.toPySpark().where(filter)        

    def toPySpark(self) -> DataFrame:
        return self._data_frame
    
    def __getattr__(self, name: str):
        return getattr(self.toPySpark(), name)

    def __getitem__(self, key: str):
        return self.toPySpark()[key]
