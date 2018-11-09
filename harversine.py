import numpy as np
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import FloatType

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.    

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km


spark_haversine = pandas_udf(haversine, returnType=FloatType())

# lon1 = df.buyer_lng
# lat1 = df.buyer_lat
# lon2 = df.seller_lng
# lat2 = df.seller_lat
# haversine(lon1, lat1, lon2, lat2)

# df.select(col("order_id"), spark_haversine( col("buyer_lng"), col("buyer_lat"), col("seller_lng"), col("seller_lat")))
