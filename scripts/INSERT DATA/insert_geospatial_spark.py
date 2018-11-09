import os

def read_and_handle_data(path):
    df = (spark
          .read.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferschema", "true")
          .option("mode", "DROPMALFORMED")
          .load(path))
    drop_list = ["_c0"]

    df = df.select(
        [column for column in df.columns if column not in drop_list])
    return df

try:
  sc and spark
except (NameError, UnboundLocalError) as e:

  import findspark

  findspark.init()
  import pyspark
  import pyspark.sql

  sc = pyspark.SparkContext()
  spark = pyspark.sql.SparkSession(sc).builder.appName("E-Commerce Analytics").getOrCreate()


geo_spatial_df = read_and_handle_data("geolocation_olist_public_dataset.csv")
geo_spatial_df = geo_spatial_df.dropDuplicates(subset=["city"])

dataframes = {

    "geo_table": geo_spatial_df,

}

for item in dataframes.items():
    print("--- Creating SQL View for %s ---" % item[0])
    item[1].createOrReplaceTempView(item[0])

base_out_dir = "serialized_data/"
if not os.path.exists(base_out_dir):
    os.mkdir(base_out_dir)
    print("--- Output Directory Created---")

for item in dataframes.items():
    print("--- Serializing %s ---" % item[0])
    item[1].write.parquet(base_out_dir+item[0]+".parquet")
