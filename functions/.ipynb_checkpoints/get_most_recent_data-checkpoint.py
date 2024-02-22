from pyspark.sql import DataFrame
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from typing import List, Union

def get_most_recent_data(
    data_frame: DataFrame,
    key_columns: List[str],
    cascade_delete: bool,
    catulz_reg_col_name: Union[None, str],
    hextr_cpu_col_name: str = "hextr_cpu"
) -> DataFrame:
    df: DataFrame = data_frame
    
    if (not cascade_delete):
        df = df.where(df[catulz_reg_col_name] != "D")

    df = df.withColumn("ROW_NMBR", row_number().over(
        Window.partitionBy(*key_columns).orderBy(*key_columns, df[hextr_cpu_col_name].desc())
    ))

    if (cascade_delete):
        df = df.where(df[catulz_reg_col_name] != "D")

    df.drop("ROW_NMBR")
    return df