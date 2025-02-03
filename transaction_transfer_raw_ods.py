from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from config import ods_path
from config import raw_path

activity_path = f'{ods_path}/activity.parquet'
payment_path = f'{ods_path}/payment.parquet'
transaction_path = f'{ods_path}/transaction.parquet'
login_path = f'{ods_path}/login.parquet'

spark = (SparkSession
         .builder
         .appName("transfer_row_ods")
         .getOrCreate())

spark.sparkContext.setLogLevel('WARN') #Не выводим логи в консоль

# Читаем файлы из слоя ROW
df_activity = spark.read.parquet(f'{raw_path}/activity.parquet', header=True)
df_payment = spark.read.parquet(f'{raw_path}/payment.parquet', header=True)
df_transaction = spark.read.parquet(f'{raw_path}/transaction.parquet', header=True)
df_login = spark.read.parquet(f'{raw_path}/login.parquet', header=True)

# Определяем схемы и обрабатываем вложенный Json 
schema_activity = StructType([
    StructField("client_id", IntegerType(), True),
    StructField("activity_date", TimestampType(), True),
    StructField("activity_type", StringType(), True),
    StructField("activity_location", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device", StringType(), True),
    StructField("location", StringType(), True)
])

query_activity = df_activity.withColumn('value', from_json(col("value"), ArrayType(schema_activity)))
query_activity = query_activity.select(explode(col('value')).alias('t')).select("t.client_id", "t.activity_date",
                               "t.activity_type", "t.activity_location", "t.ip_address", "t.device", "t.location")

schema_transaction = StructType([
    StructField("client_id", IntegerType(), True),
    StructField("transaction_id", IntegerType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("amount", FloatType(), True)
])

query_transaction = df_transaction.withColumn('value', from_json(col("value"), ArrayType(schema_transaction)))
query_transaction = query_transaction.select(explode(col('value')).alias('t')).select("t.client_id", "t.transaction_id",
                                     "t.transaction_date", "t.transaction_type", "t.account_number", "t.currency", "t.amount")

schema_payment = StructType([
    StructField("client_id", IntegerType(), True),
    StructField("payment_id", IntegerType(), True),
    StructField("payment_date", TimestampType(), True),
    StructField("currency", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("payment_method", StringType(), True),
    StructField("transaction_id", IntegerType(), True)
])

query_payment = df_payment.withColumn('value', from_json(col("value"), ArrayType(schema_payment)))
query_payment = query_payment.select(explode(col('value')).alias('t')).select("t.client_id", "t.payment_id", "t.payment_date", 
                              "t.currency", "t.amount", "t.payment_method", "t.transaction_id")

schema_login = StructType([
    StructField("client_id", IntegerType(), True),
    StructField("login_date", TimestampType(), True),
    StructField("ip_address", StringType(), True),
    StructField("location", StringType(), True),
    StructField("device", StringType(), True)
])

query_login = df_login.withColumn('value', from_json(col("value"), ArrayType(schema_login)))
query_login = query_login.select(explode(col('value')).alias('t')).select("t.client_id", "t.login_date", "t.ip_address", "t.location", "t.device")

# Очищаем данные таблицы activity
fill_dict = {'location': 0}
clean_data_activity = (query_activity
                .dropDuplicates()
                .na.drop("all")
                .na.fill(fill_dict))

# Очищаем данные таблицы transaction
clean_data_transaction = (query_transaction
                .dropDuplicates()
                .na.drop("all"))

# Очищаем данные таблицы payment
clean_data_payment = (query_payment
                .dropDuplicates()
                .na.drop("all"))

# Очищаем данные таблицы login
clean_data_login = (query_login
                .dropDuplicates()
                .na.drop("all"))

# Пишем файлы в слой ODS
clean_data_activity.write.parquet(activity_path, mode='append')
clean_data_transaction.write.parquet(transaction_path, mode='append')
clean_data_payment.write.parquet(payment_path, mode='append')
clean_data_login.write.parquet(login_path, mode='append')

# Останавливаем SparkSession
spark.stop()
