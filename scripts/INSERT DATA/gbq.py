from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from google.oauth2.service_account import Credentials
from pandas_gbq import to_gbq


def read_and_handle_data(path):
    rename_d = {"Unnamed: 0": "index"}
    df = pd.read_csv(path).rename(columns=rename_d).set_index("index")
    for col in df.columns:
        if ("date" in col.split("_")) or ("timestamp") in col.split("_"):
            df[col] = pd.to_datetime(df[col])

    for col in df.columns:
        if "Unnamed" in col:
            del df[col]
    return df


credentials = Credentials.from_service_account_file(
    '/Users/davidkatzaudio/Desktop/bi_ecom_project/ecomm-business-intelligence-b887b9e3fb8f.json',
)


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

for item in dataframes.items():
    print("Inserting %s into Database" % item[0])

    to_gbq(
        item[1],
        "ecomm_db."+item[0],
        "ecomm-business-intelligence",
        if_exists="append")

    print("Insert Complete!")
    print("")
