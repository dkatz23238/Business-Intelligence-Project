import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
import time
import psycopg2

# Using d6stack we must ensure that our string don't contain commas.

f = open("/Users/davidkatzaudio/Desktop/bi_ecom_project/scripts/INSERT DATA/aiven_posgres_uri.txt", 'r').read()
f_engine = f.replace("+psycopg2", "")
engine = create_engine(f_engine)

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
