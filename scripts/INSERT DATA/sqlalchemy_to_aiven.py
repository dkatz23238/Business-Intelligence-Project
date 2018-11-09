import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
import d6tstack 
import time
import psycopg2

# Using d6stack we must ensure that our string don't contain commas.

f = open("/Users/davidkatzaudio/Desktop/bi_ecom_project/scripts/INSERT DATA/aiven_posgres_uri.txt", 'r').read()
f_engine = f.replace("+psycopg2", "")
engine = create_engine(f_engine)

def read_and_handle_data(path): 
    rename_d = {"Unnamed: 0": "index"}
    df = pd.read_csv(path).rename(columns=rename_d).set_index("index")
    for col in df.columns:
        if ("date" in col.split("_")) or ("timestamp") in col.split("_"):
            df[col] = pd.to_datetime(df[col])
        try:
            df[col] = df[col].str.replace(",", "")
        except:
            pass

    for col in df.columns:
        if "Unnamed" in col:
            del df[col]
    return df


cat_names = read_and_handle_data("category_names.csv")
classified_orders = read_and_handle_data("classified_orders.csv")
orders_general = read_and_handle_data("orders_general.csv")
payments = read_and_handle_data("payments.csv")
products = read_and_handle_data("products.csv")
public_customers = read_and_handle_data("public_customers.csv")
sellers = read_and_handle_data("sellers.csv")

dataframes = {

    "cat_names": cat_names,
    "classified_orders": classified_orders,
    "orders_general": orders_general,
    "payments": payments,
    "products": products,
    "public_customers": public_customers,
    "sellers": sellers

}


start_time = time.time()

for item in dataframes.items():
    print("Inserting %s into Database" % item[0])

    d6tstack.utils.pd_to_psql(
        item[1], f, item[0], if_exists='replace')
    
    print("Insert Complete!")
    print("")

print("--- Insert Complete %s seconds ---" % (time.time() - start_time))

print("--- Confirmation: ---")
print(engine.table_names())

