# Данный скрипт читает данные из кафки из топика login и пишет в сырой слой RAW

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from config import kafka_servers
from config import raw_path

topic = "login"
path_login = f"{raw_path}/login.parquet"

spark = (SparkSession
         .builder
         .appName("login_raw")
         .getOrCreate()
         )

spark.sparkContext.setLogLevel('WARN')  # Не выводим логи в консоль

# Читаем данные из kafka
df_read = (spark
           .read
           .format("kafka")
           .option("kafka.bootstrap.servers", kafka_servers)
           .option("subscribe", topic)
           .load()
           .selectExpr("CAST(value AS STRING)")
           )

# Пишем данные в parquet в слой RAW
df_write = (df_read
            .write
            .mode("append")
            .format("parquet")
            .option("truncate", "false")
            .save(path_login)
            )

spark.stop()
