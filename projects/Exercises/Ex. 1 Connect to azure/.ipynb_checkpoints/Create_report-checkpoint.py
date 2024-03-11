from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
pd.set_option('display.max_columns', 500)

n_1 = datetime.datetime.now()


spark = SparkSession.builder \
    .appName("Project_d") \
    .getOrCreate()
# Define the storage account name and container name
storage_account_name = "projectsstoragealejo"
container_name = "projects-spark"
access_key = "Ptoy4is6+VXkh91yufbxyHJPNS9nx8QIJ8zQrNKsVA8oDklZDxfqn4jiFmMFhEefB+HssdBcGd1Z+AStQVobgQ=="
# Define the Azure Blob Storage URL
azure_blob_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
# Provide your Azure Storage account access key
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", access_key)


df = spark.read.parquet(azure_blob_url+"weather_data.parquet")


df.printSchema()

df = df.withColumn("Year",year(col("timestamp"))) \
    .withColumn("Month",month(col("timestamp"))) \
    .withColumn("Day",dayofmonth(col("timestamp"))) \
    .withColumn("month-year", concat(month(col("timestamp")), lit(" - "), year(col("timestamp"))))
#df.show(3)

# Get Previous Temperature for every month in every yerar and Compare
window_spec_1 = Window.partitionBy("month-year").orderBy(col("timestamp"))
df_temp_w = df.withColumn("Prev_Temperature", lag("Temperature").over(window_spec_1)) \
    .withColumn("Temp_Diff",col("Temperature") - col("Prev_Temperature")) \
    .select("Temperature","Prev_Temperature","Temp_Diff","timestamp","month-year")
#df_temp_w.show()
_chk_1 = df_temp_w.filter(
    col("month-year") == "11 - 1995").toPandas()

#Change in Temperature Aggregation
agg_temp = df_temp_w.groupBy(
        "month-year").agg(
            count("Temp_Diff").alias("Count"),
            avg("Temp_Diff").alias("Mean"),
            median("Temp_Diff").alias("median"),
            stddev("Temp_Diff").alias("stdv")
    )

agg_temp.write.csv(azure_blob_url+"agg_temp_data.csv",header=True)
agg_temp_pdf = agg_temp.withColumn("mth",split(col("month-year"),"-").getItem(0)) \
    .withColumn("yr",split(col("month-year"),"-").getItem(1)) \
    .toPandas()
#agg_temp.write.csv(azure_blob_url+"agg_temp_data.csv",header=True)
agg_temp.show(10)

agg_temp_pdf["mth"] = pd.to_numeric(agg_temp_pdf["mth"])
agg_temp_pdf["yr"] = pd.to_numeric(agg_temp_pdf["yr"])
agg_temp_pdf = agg_temp_pdf.sort_values(["yr","mth"], ascending = [True,True])
agg_temp_pdf["dt"] = agg_temp_pdf["mth"].astype(str) + "-" +agg_temp_pdf["yr"].astype(str)
agg_temp_pdf["dt"] = agg_temp_pdf["dt"].apply(lambda x: datetime.datetime(month=int(x.split("-")[0]), 
                                                                         year=int(x.split("-")[1]), 
                                                                         day=1))
#agg_temp_pdf["dt"] = agg_temp_pdf["dt"].apply(lambda x: [int(x.split("-")[0]), int(x.split("-")[1]), 1])
sns.set_theme(rc={'figure.figsize':(9,3)})
bar = sns.lineplot(
    data = agg_temp_pdf[
        (agg_temp_pdf["dt"] < datetime.datetime(1950,1,1)) &
        (agg_temp_pdf["dt"] >= datetime.datetime(1940,1,1))
    ],
    x = "dt",
    y = "Mean"
)
plt.xticks(rotation = 70)
plt.savefig("line_plot.png")
plt.clf()
box = sns.boxplot(
    data = agg_temp_pdf[
        (agg_temp_pdf["dt"] < datetime.datetime(1950,1,1)) &
        (agg_temp_pdf["dt"] >= datetime.datetime(1940,1,1))
    ],
    y = "Mean",
    x = "yr"

)
plt.savefig("boxplot.png")
                                                         
                                                         
n_2= datetime.datetime.now()

print(n_2-n_1)