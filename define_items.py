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
            print(f"[WARN] '{name}' exists as '{existing[name]}', requested '{typ}'.")
    if reqs:
        client.send(Batch(reqs))
        print("Properties created.")
    else:
        print("Properties already exist.")

def _norm_str(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None

def _year_from_date(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        parts = s.split('/')
        y = int(parts[-1])
        return y
    except Exception:
        try:
            dt = pd.to_datetime(s, errors="coerce")
            return int(dt.year) if pd.notna(dt) else None
        except Exception:
            return None

def build_rows(df):
    if "bookID" not in df.columns:
        raise ValueError("Missing 'bookID' column.")
    
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    if "publication_year" not in df.columns and "publication_date" in df.columns:
        df = df.copy()
        df["publication_year"] = df["publication_date"].apply(_year_from_date)

    rows = []
    for _, r in df.iterrows():
        item_id = str(r["bookID"])

        title = _norm_str(r.get("title"))
        authors = _norm_str(r.get("authors"))
        language_code = _norm_str(r.get("language_code"))
        publisher = _norm_str(r.get("publisher"))
        isbn = _norm_str(r.get("isbn"))
        isbn13 = _norm_str(r.get("isbn13"))

        npg = pd.to_numeric(r.get("num_pages"), errors="coerce")
        num_pages = None if pd.isna(npg) else int(npg)

        avg_raw = pd.to_numeric(r.get("average_rating"), errors="coerce")
        average_rating = None if pd.isna(avg_raw) else float(avg_raw)

        ratings_raw = pd.to_numeric(r.get("ratings_count"), errors="coerce")
        ratings_count = None if pd.isna(ratings_raw) else int(ratings_raw)

        texts_raw = pd.to_numeric(r.get("text_reviews_count"), errors="coerce")
        text_reviews_count = None if pd.isna(texts_raw) else int(texts_raw)

        year_raw = pd.to_numeric(r.get("publication_year"), errors="coerce")
        publication_year = None if pd.isna(year_raw) else int(year_raw)

        rows.append((item_id, {
            "title": title,
            "authors": authors,
            "average_rating": average_rating,
            "num_pages": num_pages,
            "language_code": language_code,
            "publisher": publisher,
            "ratings_count": ratings_count,
            "text_reviews_count": text_reviews_count,
            "publication_year": publication_year,
            "isbn": isbn,
            "isbn13": isbn13,
        }))
    return rows

def create_items(client, rows, batch_size=1000):
    add_reqs = [AddItem(item_id) for item_id, _ in rows]
    for i in range(0, len(add_reqs), batch_size):
        client.send(Batch(add_reqs[i:i+batch_size]))

    set_reqs = [SetItemValues(item_id, values, cascade_create=False) for item_id, values in rows]
    sent = 0
    for i in range(0, len(set_reqs), batch_size):
        client.send(Batch(set_reqs[i:i+batch_size]))
        sent += min(batch_size, len(set_reqs) - i)
        print(f"  progress: {sent}/{len(set_reqs)}")