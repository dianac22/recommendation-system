import os
import kagglehub
import pandas as pd
from define_items import (
    get_client, ensure_properties, build_rows, create_items
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
