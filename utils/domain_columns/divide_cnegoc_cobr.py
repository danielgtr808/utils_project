from pyspark.sql import Column, DataFrame, Row

import pyspark.sql.functions as f
import pyspark.sql.types as t



def divide_cnegoc_cobr(
    data_frame: DataFrame,
    cnegoc_cobr_column_name: str = "cnegoc_cobr",
    cag_bcria_column_name: str = "cag_bcria",
    ccta_bcria_column_name: str = "ccta_bcria"
) -> DataFrame:
    cnegoc_cobr_str: Column = f.col(cnegoc_cobr_column_name).cast("string")
    length_col: Column = f.length(cnegoc_cobr_str)

    return (
        data_frame
        .withColumn(
            cag_bcria_column_name,
            f.substring(cnegoc_cobr_str, 1, length_col - 7).cast(t.IntegerType())
        )
        .withColumn(
            ccta_bcria_column_name,
            f.substring(cnegoc_cobr_str, length_col - 6, 7).cast(t.IntegerType())
        )
    )
