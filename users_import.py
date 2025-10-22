import os
import re
import pandas as pd
from dotenv import load_dotenv
from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import (
    AddUser, AddUserProperty, SetUserValues, ListUserProperties, Batch
)

DEFAULT_REGION = Region.EU_WEST

USER_PROPERTIES = {
    "sales_person": "string",
    "sp_id": "string",
    "team": "string",
    "location": "string",
}

def get_client():
    load_dotenv()
    db_id = os.getenv("RECOMBEE_DB_ID")
    token = os.getenv("RECOMBEE_PRIVATE_TOKEN")
    if not db_id or not token:
        raise RuntimeError("ID and token missing.")
    return RecombeeClient(db_id, token, region=DEFAULT_REGION)

def ensure_user_properties(client):
    try:
        existing = {p["name"]: p["type"] for p in client.send(ListUserProperties())}
    except Exception:
        existing = {}
    reqs = []
    for name, typ in USER_PROPERTIES.items():
        if name not in existing:
            reqs.append(AddUserProperty(name, typ))
        elif existing[name] != typ:
            print(f"[WARN] user prop '{name}' exists as '{existing[name]}', requested '{typ}'.")
    if reqs:
        client.send(Batch(reqs))
        print("User properties created.")
    else:
        print("User properties already exist.")

def _norm_str(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None

def build_user_rows(df):
    df = df.rename(columns={c: re.sub(r"\s+", "_", c.strip().lower()) for c in df.columns})

    id_col = next((c for c in ["sp_id", "spid", "user_id", "id"] if c in df.columns), None)
    if not id_col:
        raise ValueError("Missing ID column (e.g., 'SP ID').")

    name_col = next((c for c in ["sales_person", "salesperson", "name", "full_name"] if c in df.columns), None)
    if not name_col:
        raise ValueError("Missing name column (e.g., 'Sales Person' / 'Name').")

    team_col = "team" if "team" in df.columns else None
    loc_col  = "location" if "location" in df.columns else None

    rows = []
    for _, r in df.iterrows():
        user_id = _norm_str(r.get(id_col))       
        if not user_id:
            continue

        sales_person = _norm_str(r.get(name_col))  
        sp_id_value = user_id

        team = _norm_str(r.get(team_col)) if team_col else None
        location = _norm_str(r.get(loc_col)) if loc_col else None

        rows.append((user_id, {
            "sales_person": sales_person,
            "sp_id": sp_id_value,
            "team": team,
            "location": location,
        }))
    return rows

def create_users(client, rows, batch_size=1000):
    add_reqs = [AddUser(user_id) for user_id, _ in rows]
    for i in range(0, len(add_reqs), batch_size):
        client.send(Batch(add_reqs[i:i+batch_size]))

    set_reqs = [SetUserValues(user_id, values, cascade_create=False) for user_id, values in rows]
    sent = 0
    for i in range(0, len(set_reqs), batch_size):
        client.send(Batch(set_reqs[i:i+batch_size]))
        sent += min(batch_size, len(set_reqs) - i)
        print(f"  users progress: {sent}/{len(set_reqs)}")
