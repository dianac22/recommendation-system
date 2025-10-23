import os
import kagglehub
import pandas as pd
from define_items import (
    get_client, ensure_properties, build_rows, create_items
)

from users_import import (
    ensure_user_properties, build_user_rows, create_users
)

path = kagglehub.dataset_download("jealousleopard/goodreadsbooks")
print("Path to dataset files:", path)

csv_path = None
for root, _, files in os.walk(path):
    if "books.csv" in files:
        csv_path = os.path.join(root, "books.csv")
        break

if csv_path is None:
    raise FileNotFoundError(f"Couldn't find books.csv under {path}")

nRowsRead = 1000
df1 = pd.read_csv(csv_path, nrows=nRowsRead)
nRow, nCol = df1.shape
print(f"There are {nRow} rows and {nCol} columns")

print(df1.head(3))

client = get_client()
ensure_properties(client)
rows = build_rows(df1)
create_items(client, rows, batch_size=500)

PEOPLE_CSV_PATH = "/Users/dcaragui/Desktop/Proiect sisteme de recomandare/people.csv" 
if not os.path.exists(PEOPLE_CSV_PATH):
    raise FileNotFoundError(f"people.csv not found at {PEOPLE_CSV_PATH}")

df_users = pd.read_csv(PEOPLE_CSV_PATH)
print(f"\nLoaded users CSV: {df_users.shape[0]} rows, columns: {list(df_users.columns)}")

ensure_user_properties(client)
user_rows = build_user_rows(df_users)
create_users(client, user_rows, batch_size=1000)

print("Items and users uploaded.")
