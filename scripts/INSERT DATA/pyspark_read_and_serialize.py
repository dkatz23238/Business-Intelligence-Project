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

base = "/Users/davidkatzaudio/Desktop/bi_ecom_project/rawdata/"
cat_names = read_and_handle_data(base+"category_names.csv")
classified_orders = read_and_handle_data(base+"classified_orders.csv")
orders_general = read_and_handle_data(base+"orders_general.csv")
payments = read_and_handle_data(base+"payments.csv")
products = read_and_handle_data(base+"products.csv")
public_customers = read_and_handle_data(base+"public_customers.csv")
sellers = read_and_handle_data(base+"sellers.csv")

dataframes = {

    "cat_names": cat_names,
    "classified_orders": classified_orders,
    "orders_general": orders_general,
    "payments": payments,
    "products": products,
    "public_customers": public_customers,
    "sellers": sellers
}

for item in dataframes.items():
    print("--- Creating SQL View for %s ---"% item[0])
    item[1].createOrReplaceTempView(item[0])

base_out_dir = "serialized_data/"
if not os.path.exists(base_out_dir):
    os.mkdir(base_out_dir)
    print("--- Output Directory Created---")

for item in dataframes.items():
    print("--- Serializing %s ---" % item[0])
    item[1].write.parquet(base_out_dir+item[0]+".parquet")
