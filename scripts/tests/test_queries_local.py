import pandas as pd
from sqlalchemy import inspect, create_engine
import glob

engine = create_engine('postgresql://davidkatzaudio:blackie@localhost:5432/ecomm_db')

path = "/Users/davidkatzaudio/Desktop/bi_ecom_project/scripts/EXPLORE DATA"
queries = glob.glob(path+"/*.sql")


for i, q in enumerate(queries):
    print("-"*100)
    print("Query %s out of %s" % (i, len(queries)))
    print("Conducting %s" % q.split("/")[-1])
    f = open(q, "r").read()
    result = engine.execute(f)
    headers = [i[0] for i in result.cursor.description]
    print(headers)
    for row in result:
        print(row)
    print("")
