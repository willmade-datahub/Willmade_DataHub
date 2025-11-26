import json
import os
import sqlite3
import tempfile
from typing import Any, Dict, List

import pandas as pd

try:
    import streamlit as st  # For secrets on Streamlit Cloud
except ImportError:
    st = None

# Switch to Firestore by setting DATA_BACKEND=firestore and providing
# GOOGLE_APPLICATION_CREDENTIALS that points to your Firebase service account JSON.
DB_PATH = r"C:\Willmade_DataHub\data.db"
DATA_BACKEND = os.getenv("DATA_BACKEND", "").lower()
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "willmade-datahub")

_firestore_client = None


def _use_firestore() -> bool:
    return DATA_BACKEND == "firestore"


def _get_service_account_path() -> str | None:
    # If running on Streamlit Cloud, allow secrets["firebase_key"]
    if st and hasattr(st, "secrets") and "firebase_key" in st.secrets:
        data = st.secrets["firebase_key"]
        if isinstance(data, str):
            data = json.loads(data)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tmp.write(json.dumps(data).encode("utf-8"))
        tmp.flush()
        return tmp.name
    return os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


def _get_firestore():
    """Create Firestore client once and reuse it."""
    global _firestore_client
    if _firestore_client is not None:
        return _firestore_client

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError as exc:  # pragma: no cover - runtime guidance
        raise RuntimeError(
            "firebase_admin is not installed. Install it or unset DATA_BACKEND=firestore."
        ) from exc

    cred_path = _get_service_account_path()
    cred = (
        credentials.Certificate(cred_path)
        if cred_path and os.path.exists(cred_path)
        else credentials.ApplicationDefault()
    )

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {"projectId": FIREBASE_PROJECT_ID})

    _firestore_client = firestore.client()
    return _firestore_client


def get_conn():
    return sqlite3.connect(DB_PATH)


def _to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return df.to_dict(orient="records")


def insert_excel_records(df: pd.DataFrame):
    if _use_firestore():
        client = _get_firestore()
        from firebase_admin import firestore

        batch = client.batch()
        col_ref = client.collection("excel_master")
        for row in _to_records(df):
            doc = col_ref.document()
            row["created_at"] = firestore.SERVER_TIMESTAMP
            batch.set(doc, row)
        batch.commit()
        return

    conn = get_conn()
    df.to_sql("excel_master", conn, if_exists="append", index=False)
    conn.close()


def load_excel_records() -> pd.DataFrame:
    if _use_firestore():
        client = _get_firestore()
        from firebase_admin import firestore

        docs = (
            client.collection("excel_master")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .stream()
        )
        rows = [doc.to_dict() | {"id": doc.id} for doc in docs]
        if not rows:
            return pd.DataFrame(columns=["id", "user_id", "phone", "memo", "created_at"])
        return pd.DataFrame(rows)

    conn = get_conn()
    df = pd.read_sql("SELECT * FROM excel_master ORDER BY id DESC", conn)
    conn.close()
    return df


def save_matched(df: pd.DataFrame):
    if _use_firestore():
        client = _get_firestore()
        from firebase_admin import firestore

        batch = client.batch()
        col_ref = client.collection("match_list")
        for row in _to_records(df):
            doc = col_ref.document()
            row["created_at"] = firestore.SERVER_TIMESTAMP
            batch.set(doc, row)
        batch.commit()
        return

    conn = get_conn()
    df.to_sql("match_list", conn, if_exists="append", index=False)
    conn.close()


def load_matched() -> pd.DataFrame:
    if _use_firestore():
        client = _get_firestore()
        from firebase_admin import firestore

        docs = (
            client.collection("match_list")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .stream()
        )
        rows = [doc.to_dict() | {"id": doc.id} for doc in docs]
        if not rows:
            return pd.DataFrame(columns=["id", "user_id", "phone", "memo", "created_at"])
        return pd.DataFrame(rows)

    conn = get_conn()
    df = pd.read_sql("SELECT * FROM match_list ORDER BY id DESC", conn)
    conn.close()
    return df


def _delete_collection(coll) -> None:
    docs = coll.stream()
    for doc in docs:
        doc.reference.delete()


def clear_all():
    if _use_firestore():
        client = _get_firestore()
        _delete_collection(client.collection("excel_master"))
        _delete_collection(client.collection("match_list"))
        return

    conn = get_conn()
    conn.execute("DELETE FROM excel_master")
    conn.execute("DELETE FROM match_list")
    conn.commit()
    conn.close()
