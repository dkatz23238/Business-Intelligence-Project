import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

def read_and_handle_data(path):
    rename_d = {"Unnamed: 0": "index"}
    df = pd.read_csv(path, error_bad_lines=False).rename(
        columns=rename_d).set_index("index")
    for col in df.columns:
        if ("date" in col.split("_")) or ("timestamp") in col.split("_"):
            df[col] = pd.to_datetime(df[col])

    for col in df.columns:
        if "Unnamed" in col:
            del df[col]
    return df

f = open("/Users/davidkatzaudio/Desktop/bi_ecom_project/scripts/INSERT DATA/aiven_posgres_uri.txt", 'r').read()
f_engine = f.replace("+psycopg2", "")
engine = create_engine(f_engine)

geo_spatial_df = pd.read_csv(
    "geolocation_olist_public_dataset.csv",
    delimiter=",",
    error_bad_lines=False).drop_duplicates("city")

dataframes = {

    "geo_table": geo_spatial_df,

}

for item in dataframes.items():
    print("Inserting %s into Database" % item[0])
    item[1].to_sql(name=item[0], con=engine,
                   if_exists="replace", index=False, chunksize=200)
    print("Insert Complete!")
    print("")
