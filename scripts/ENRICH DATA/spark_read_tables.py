import glob
from pyspark.sql import SQLContext

try:
    sc and spark
except (NameError, UnboundLocalError) as e:

    import findspark

    findspark.init()
    import pyspark
    import pyspark.sql

    sc = pyspark.SparkContext()
    spark = pyspark.sql.SparkSession(sc).builder.appName(
        "E-Commerce Analytics").getOrCreate()
    sqlContext = SQLContext(spark)


def spark_read_parquet(path):
   return sqlContext.read.parquet(path)


base = "/Users/davidkatzaudio/Desktop/bi_ecom_project/serialized_data/"

cat_names = spark_read_parquet(base+"cat_names.parquet")
classified_orders = spark_read_parquet(base+"classified_orders.parquet")
orders_general = spark_read_parquet(base+"orders_general.parquet")
payments = spark_read_parquet(base+"payments.parquet")
products = spark_read_parquet(base+"products.parquet")
public_customers = spark_read_parquet(base+"public_customers.parquet")
sellers = spark_read_parquet(base+"sellers.parquet")
geo_table = spark_read_parquet(base+"geo_table.parquet")
orders_distance = spark_read_parquet(base+"orders_distance.parquet")

dataframes = {

    "cat_names": cat_names,
    "classified_orders": classified_orders,
    "orders_general": orders_general,
    "payments": payments,
    "products": products,
    "public_customers": public_customers,
    "sellers": sellers,
    "geo_table": geo_table,
    "orders_distance": orders_distance,
}

for item in dataframes.items():
    print("--- Creating SQL View for %s ---" % item[0])
    item[1].createOrReplaceTempView(item[0])


