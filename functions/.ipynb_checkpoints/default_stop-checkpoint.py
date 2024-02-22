from pyspark.sql import SparkSession
from time import time



def default_stop(spark_session: SparkSession) -> None:
    print(("#"*100) + "\n" + ("#"*30))
    print("#"*30, "Aplicação finalizada")
    print(("#"*30) + "\n" + ("#"*100))
    print("\n"*10)

    spark_session.stop()