from ...classes import ProductiveTable
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from typing import Any, Dict, List, Tuple

class TB_SMFR(ProductiveTable):

    _database_name = "AAIG_UCS"
    _table_name = "TB_SMFR"

    @property
    def table_keys(self) -> Tuple[str]:
        return ("JOB_NAME")
    
    @property
    def table_partition_columns(self) -> Tuple[str]:
        return ("JOB_NAME")
    
    @property
    def table_struct(self) -> StructType:
        return StructType([
            StructField("JOB_NAME", StringType(), True),
			      StructField("STAT", IntegerType(), True),
			      StructField("DT_INGTAO_PTCAO", StringType(), True)
        ])

    def create_new_row(self, properties: Dict[str, Any] = { }) -> DataFrame:
        return self.spark_session.createDataFrame([properties], self.table_struct)

    def get_job_data(self, job_name: str) -> Dict[str, Any]:
      job_data: List[Row] = self.where(self["JOB_NAME"] == job_name).limit(1).collect()
      
      if (len(job_data) == 0):
        self.update(job_name, { })
        return self.get_job_data(job_name)

      return job_data[0].asDict()

    def update(self, job_name: str, properties: Dict[str, Any]) -> None:
        partition: Dict[str, str] = { "JOB_NAME": job_name }
        job_df: DataFrame = self.spark_session.createDataFrame(
            [x.asDict() for x in self.get_partition(partition).limit(1).collect()],
            schema=self.table_struct
        )
    
        for key, item in properties.items():
            job_df = job_df.withColumn(key, lit(item))
    
        if (job_df.count() > 0):
            self.delete_partition(partition)
        else:
            job_df = self.create_new_row({ "JOB_NAME": job_name, **properties })
            
        self.save_into_table(job_df, "append")