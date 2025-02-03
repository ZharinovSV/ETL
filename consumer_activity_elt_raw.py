
# Данный скрипт читает данные из кафки из топика activity и пишет в сырой слой RAW

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from config import kafka_servers
from config import raw_path

topic = "activity"
path_activity = f"{raw_path}/activity.parquet"

spark = (SparkSession
         .builder
         .appName("activity_raw")
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
            .save(path_activity)
            )

spark.stop()
