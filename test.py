from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import datetime


spark = SparkSession.builder \
    .appName("AzureBlobStorageExample") \
    .getOrCreate()
# Define the storage account name and container name
storage_account_name = "projectsstoragealejo"
container_name = "projects-spark"
access_key = "Ptoy4is6+VXkh91yufbxyHJPNS9nx8QIJ8zQrNKsVA8oDklZDxfqn4jiFmMFhEefB+HssdBcGd1Z+AStQVobgQ=="
# Define the Azure Blob Storage URL
azure_blob_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
# Provide your Azure Storage account access key
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", access_key)




num_rows = 10000000
df = spark.range(num_rows) \
    .withColumn("Temperature", (rand() * 50 + 50)) \
    .withColumn("Humidity", (rand() * 50 + 50)) \
    .withColumn("Pressure", (rand() * 100 + 900)) \
    .withColumn("Wind_Speed", (rand() * 20)) \
    .withColumn("Cloud_Cover", (rand() * 100)) \
    .withColumn("Weight", (expr("CASE WHEN Temperature > 70 THEN 5 " +
                                 "WHEN Humidity < 30 THEN 4 " +
                                 "WHEN Pressure < 950 THEN 3 " +
                                 "WHEN Wind_Speed > 15 THEN 2 " +
                                 "ELSE 1 END").cast(IntegerType()))) \
    .withColumn("Target", (expr("CASE WHEN Cloud_Cover < 20 THEN 'Sunny' " +
                                "WHEN Cloud_Cover < 70 THEN 'Partly Cloudy' " +
                                "ELSE 'Cloudy' END")))

def generate_timestamp(index):
    start_timestamp = datetime.datetime(2024, 3, 5, 0, 0, 0)
    return start_timestamp - datetime.timedelta(minutes=index*5)

spark.udf.register("generate_timestamp", generate_timestamp, TimestampType())
df = df.withColumn("timestamp", expr("generate_timestamp(id)"))

df.write.parquet(azure_blob_url+"weather_data.parquet")





print("________________________________")
print("Done")
print("________________________________")
