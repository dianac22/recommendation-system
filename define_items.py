import os
import pandas as pd
from dotenv import load_dotenv
from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import (
    AddItem, AddItemProperty, SetItemValues, ListItemProperties, Batch
)

DEFAULT_REGION = Region.EU_WEST

ITEM_PROPERTIES = {
    "title": "string",
    "authors": "string",
    "average_rating": "double",
    "num_pages": "int",
    "language_code": "string",
    "publisher": "string",
    "ratings_count": "int",
    "text_reviews_count": "int",
    "publication_year": "int",
    "isbn": "string",
    "isbn13": "string",
}

def get_client():
    load_dotenv()
    db_id = os.getenv("RECOMBEE_DB_ID")
    token = os.getenv("RECOMBEE_PRIVATE_TOKEN")
    if not db_id or not token:
        raise RuntimeError("ID and token missing.")
    return RecombeeClient(db_id, token, region=DEFAULT_REGION)

def ensure_properties(client):
    try:
        existing = {p["name"]: p["type"] for p in client.send(ListItemProperties())}
    except Exception:
        existing = {}

    reqs = []
    for name, typ in ITEM_PROPERTIES.items():
        if name not in existing:
            reqs.append(AddItemProperty(name, typ))
        elif existing[name] != typ:
            print(f"[WARN] '{name}' exists as '{existing[name]}', requested '{typ}'. Type cannot be changed.")
    if reqs:
        client.send(Batch(reqs))
        print("Properties created.")
    else:
        print("Properties already exist.")

def build_rows(df):
    if "bookID" not in df.columns:
        raise ValueError("Missing 'bookID' column.")
    rows = []
    for _, r in df.iterrows():
        item_id = str(r["bookID"])

        title = None if pd.isna(r.get("title")) else str(r.get("title")).strip()
        authors = None if pd.isna(r.get("authors")) else str(r.get("authors")).strip()

        npg = pd.to_numeric(r.get("num_pages"), errors="coerce")
        num_pages = None if pd.isna(npg) else int(npg)

        ar = pd.to_numeric(r.get("average_rating"), errors="coerce")
        average_rating = None if pd.isna(ar) else float(ar)

        rows.append((item_id, {
            "title": title,
            "authors": authors,
            "num_pages": num_pages,
            "average_rating": average_rating,
        }))
    return rows

def upload_items_add_then_set(client, rows, batch_size=1000):
    add_reqs = [AddItem(item_id) for item_id, _ in rows]
    for i in range(0, len(add_reqs), batch_size):
        client.send(Batch(add_reqs[i:i+batch_size]))

    set_reqs = [SetItemValues(item_id, values, cascade_create=False) for item_id, values in rows]
    sent = 0
    for i in range(0, len(set_reqs), batch_size):
        client.send(Batch(set_reqs[i:i+batch_size]))
        sent += min(batch_size, len(set_reqs) - i)
        print(f"  progress: {sent}/{len(set_reqs)}")