from ..classes import HistoricalTable
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from typing import List

import traceback
import utils.algorithms as alg
import utils.environment as env
import utils.functions as fn

def hist_sync(job_name: str, spark_session: SparkSession, tb_smfr: HistoricalTable, hist_tb: HistoricalTable) -> None:
    fn.spark_default_config(spark_session)
    start_time: float = fn.default_start()

    try:
        tb_smfr.update(job_name, {"STAT": env.smfr_state["running"]})
        job_data: List[Row] = tb_smfr.where(tb_smfr["JOB_NAME"] == job_name).collect()

        alg.hist_sync(
            hist_tb,
            (None if (len(job_data) == 0) else job_data[0].asDict()["DT_INGTAO_PTCAO"]),
            (lambda x: tb_smfr.update(job_name, {"DT_INGTAO_PTCAO": x}))
        )

        tb_smfr.update(job_name, {"STAT": env.smfr_state["stopped"]})
    except Exception as e:
        print(traceback.format_exc())
    finally:
        fn.default_stop(spark_session)
