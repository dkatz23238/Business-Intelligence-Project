import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

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

engine = create_engine('postgresql://davidkatzaudio:blackie@localhost:5432/ecomm_db')

cat_names = read_and_handle_data("category_names.csv")
classified_orders = read_and_handle_data("classified_orders.csv")
orders_general = read_and_handle_data("orders_general.csv")
payments = read_and_handle_data("payments.csv")
products = read_and_handle_data("products.csv")
public_customers = read_and_handle_data("public_customers.csv")
sellers = read_and_handle_data("sellers.csv")

dataframes = {

    "cat_names":cat_names,
    "classified_orders":classified_orders,
    "orders_general":orders_general,
    "payments":payments,
    "products":products,
    "public_customers":public_customers,
    "sellers":sellers

}

for item in dataframes.items():
    print("Inserting %s into Database" % item[0])
    item[1].to_sql(name=item[0], con=engine,
                   if_exists="replace", index=False, chunksize=200)
    print("Insert Complete!")
    print("")
