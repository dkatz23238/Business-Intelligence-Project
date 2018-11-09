import numpy as np
import glob
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import FloatType
from pyspark.sql import SQLContext

def haversine(lon1, lat1, lon2, lat2):

    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

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
dataframes = {

    "cat_names": cat_names,
    "classified_orders": classified_orders,
    "orders_general": orders_general,
    "payments": payments,
    "products": products,
    "public_customers": public_customers,
    "sellers": sellers,
    "geo_table": geo_table
}

for item in dataframes.items():
    print("--- Creating SQL View for %s ---" % item[0])
    item[1].createOrReplaceTempView(item[0])

c1 = """
CREATE TEMPORARY VIEW sellers_geo
AS
SELECT sellers.order_id, 
geo_table.lat as seller_lat, 
geo_table.lng as seller_lng
FROM sellers
LEFT JOIN geo_table
ON sellers.seller_city = geo_table.city
"""
c2 = """
CREATE TEMPORARY
VIEW buyers_geo
AS
SELECT orders_general.order_id,
    geo_table.lat as buyer_lat,
    geo_table.lng as buyer_lng
FROM orders_general
    LEFT JOIN geo_table
    ON LOWER (orders_general.customer_city) = geo_table.city
"""
c3 = """
SELECT 
buyers_geo.order_id, 
buyers_geo.buyer_lat, buyers_geo.buyer_lng,
sellers_geo.seller_lat, sellers_geo.seller_lng
FROM sellers_geo
LEFT JOIN buyers_geo
ON buyers_geo.order_id = sellers_geo.order_id
"""

spark.sql(c1)
spark.sql(c2)
spark.sql(c3+" LIMIT 5").show()


spark_haversine = pandas_udf(haversine, returnType=FloatType())


df = (spark.sql(c3).
    select(col("order_id"),
        spark_haversine(col("buyer_lng"),
        col("buyer_lat"), col("seller_lng"),
        col("seller_lat")).alias("order_distance")))

base_out_dir = "serialized_data/"
df.write.parquet(base_out_dir+"orders_distance"+".parquet")
