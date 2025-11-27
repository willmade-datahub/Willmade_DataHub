import json
import os
import re
import tempfile
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

@st.cache_data
def load_excel(path):
    import pandas as pd
    return pd.read_excel(path)

st.set_page_config(page_title="Willmade DataHub", layout="wide")
st.markdown(
    "<h1 style='text-align:center; color:#ff66cc;'>✨ Willmade DataHub ✨</h1>",
    unsafe_allow_html=True,
)

# ------------------------------------------------------------------
# Backend toggle (env 우선, 없으면 secrets 사용)
# ------------------------------------------------------------------
def _get_config(key: str, default: str = "") -> str:
    if os.getenv(key):
        return os.getenv(key)
    if key in st.secrets:
        return str(st.secrets[key])
    return default


DATA_BACKEND = _get_config("DATA_BACKEND", "").lower()
FIREBASE_PROJECT_ID = _get_config("FIREBASE_PROJECT_ID", "willmade-datahub")
MAX_FETCH = int(_get_config("MAX_FETCH", "3000"))
DEFAULT_VIEW_LIMIT = MAX_FETCH  # 화면 표시 시 기본 행 수 제한

STORE_CAFE = "blog_store.txt"  # ID,PHONE
STORE_BEST = "best_store.txt"  # BEST ID ONLY
MATCH_XLSX = "match_result.xlsx"

COL_CAFE = "cafe_store"
COL_BEST = "best_store"
COL_MATCH = "match_results"

_firestore_client = None


def _use_firestore() -> bool:
    return DATA_BACKEND == "firestore"


def _parse_service_account(raw: Any) -> Dict[str, Any]:
    """Accepts dict or string (even poorly escaped) and returns a dict."""
    if isinstance(raw, dict):
        return raw

    if not isinstance(raw, str):
        raise ValueError("firebase_key must be JSON string or dict")

    # Try a few safe normalizations
    candidates = [
        raw,
        raw.replace("\r\n", "\n"),
        raw.replace("\r\n", "\n").replace("\n", "\\n"),
    ]
    for cand in candidates:
        try:
            return json.loads(cand, strict=False)
        except json.JSONDecodeError:
            continue

    # Last resort: escape control chars
    cleaned = re.sub(r"[\x00-\x1f]", lambda m: f"\\u{ord(m.group()):04x}", raw)
    return json.loads(cleaned, strict=False)


def _get_service_account_path() -> str | None:
    """
    Streamlit Cloud에서 st.secrets["firebase_key"]에 서비스계정 JSON을 넣어두면
    임시 파일로 저장해 경로를 반환. 로컬/Cloud Run 등에서는
    GOOGLE_APPLICATION_CREDENTIALS 환경변수 또는 ADC를 사용.
    """
    if "firebase_key" in st.secrets:
        data = _parse_service_account(st.secrets["firebase_key"])
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tmp.write(json.dumps(data).encode("utf-8"))
        tmp.flush()
        return tmp.name
    return os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


def _get_firestore():
    """Lazy init Firestore client."""
    global _firestore_client
    if _firestore_client is not None:
        return _firestore_client

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError as exc:
        raise RuntimeError(
            "firebase_admin is not installed. Run `pip install -r requirements.txt`."
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


# ------------------------------------------------------------------
# Phone extraction helpers
# ------------------------------------------------------------------
CHAR_MAP = {
    "o": "0",
    "O": "0",
    "q": "0",
    "Q": "0",
    "l": "1",
    "I": "1",
    "i": "1",
    "L": "1",
    "Z": "2",
    "z": "2",
    "S": "5",
    "s": "5",
    "B": "8",
    "b": "8",
    "G": "6",
    "g": "6",
    "T": "7",
    "t": "7",
    "A": "4",
    "a": "4",
    "공": "0",
    "영": "0",
    "일": "1",
    "둘": "2",
    "셋": "3",
    "넷": "4",
    "다섯": "5",
    "여섯": "6",
    "칠": "7",
    "팔": "8",
    "아홉": "9",
}

PHONE_PATTERN = re.compile(r"010[0-9]{8}")


def _normalize(text: Any) -> str:
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    return "".join(CHAR_MAP.get(ch, ch) for ch in text)


def extract_phone_numbers(text: Any) -> List[str]:
    norm = _normalize(text)
    digits = re.sub(r"[^0-9]", "", norm)
    found = PHONE_PATTERN.findall(digits)
    return list({f"{f[:3]}-{f[3:7]}-{f[7:]}" for f in found})


# ------------------------------------------------------------------
# Storage helpers (Firestore / local fallback)
# ------------------------------------------------------------------
def _to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return df.to_dict(orient="records")


def save_cafe(df: pd.DataFrame) -> None:
    if _use_firestore():
        client = _get_firestore()
        from firebase_admin import firestore

        batch = client.batch()
        col = client.collection(COL_CAFE)
        for row in _to_records(df):
            doc_id = f"{row['블로그ID']}_{row['전화번호']}"
            payload = {
                "blog_id": row["블로그ID"],
                "phone": row["전화번호"],
                "created_at": firestore.SERVER_TIMESTAMP,
            }
            batch.set(col.document(doc_id), payload)
        batch.commit()
        return

    new_lines = [f"{row['블로그ID']},{row['전화번호']}\n" for _, row in df.iterrows()]
    existing = set()
    if os.path.exists(STORE_CAFE):
        with open(STORE_CAFE, "r", encoding="utf-8") as f:
            existing = set(f.readlines())
    merged = existing.union(new_lines)
    with open(STORE_CAFE, "w", encoding="utf-8") as f:
        f.writelines(sorted(list(merged)))


def _fs_query(collection: str, limit: int) -> List[Dict[str, Any]]:
    """Ordered, limited fetch to avoid unbounded stream latency."""
    client = _get_firestore()
    try:
        docs = (
            client.collection(collection)
            .order_by("created_at", direction=client._firestore.Query.DESCENDING)  # type: ignore[attr-defined]
            .limit(limit)
            .stream()
        )
    except Exception:
        # created_at 없을 때 fallback (index 없으면 느릴 수 있음)
        docs = client.collection(collection).limit(limit).stream()

    rows = [d.to_dict() for d in docs]
    return rows


def load_cafe(limit: int | None = None) -> pd.DataFrame:
    limit = limit or MAX_FETCH
    if _use_firestore():
        rows = _fs_query(COL_CAFE, limit)
        rows = [{"블로그ID": r.get("blog_id"), "전화번호": r.get("phone")} for r in rows if r]
        if not rows:
            return pd.DataFrame(columns=["블로그ID", "전화번호"])
        return pd.DataFrame(rows).drop_duplicates()

    if not os.path.exists(STORE_CAFE):
        return pd.DataFrame(columns=["블로그ID", "전화번호"])
    rows: List[List[str]] = []
    with open(STORE_CAFE, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx >= limit:
                break
            parts = line.strip().split(",")
            if len(parts) == 2:
                rows.append(parts)
    return pd.DataFrame(rows, columns=["블로그ID", "전화번호"])


def save_best(ids: List[str]) -> None:
    if _use_firestore():
        client = _get_firestore()
        from firebase_admin import firestore

        batch = client.batch()
        col = client.collection(COL_BEST)
        for bid in ids:
            payload = {"blog_id": bid, "created_at": firestore.SERVER_TIMESTAMP}
            batch.set(col.document(bid), payload)
        batch.commit()
        return

    new_lines = [f"{bid}\n" for bid in ids]
    existing = set()
    if os.path.exists(STORE_BEST):
        with open(STORE_BEST, "r", encoding="utf-8") as f:
            existing = set(f.readlines())
    merged = existing.union(new_lines)
    with open(STORE_BEST, "w", encoding="utf-8") as f:
        f.writelines(sorted(list(merged)))


def load_best(limit: int | None = None) -> pd.DataFrame:
    limit = limit or MAX_FETCH
    if _use_firestore():
        rows = _fs_query(COL_BEST, limit)
        ids = [r.get("blog_id") for r in rows if r.get("blog_id")]
        if not ids:
            return pd.DataFrame(columns=["블로그ID"])
        return pd.DataFrame(ids, columns=["블로그ID"]).drop_duplicates()

    if not os.path.exists(STORE_BEST):
        return pd.DataFrame(columns=["블로그ID"])
    ids: List[str] = []
    with open(STORE_BEST, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx >= limit:
                break
            if line.strip():
                ids.append(line.strip())
    return pd.DataFrame(ids, columns=["블로그ID"])


def save_match(df: pd.DataFrame) -> None:
    if _use_firestore():
        client = _get_firestore()
        from firebase_admin import firestore

        batch = client.batch()
        col = client.collection(COL_MATCH)
        for row in _to_records(df):
            bid = row["블로그ID"]
            doc = col.document(bid)
            payload = {
                "blog_id": bid,
                "phone": row.get("전화번호", ""),
                "memo": row.get("메모", ""),
                "created_at": firestore.SERVER_TIMESTAMP,
            }
            batch.set(doc, payload)
        batch.commit()
        return

    df.to_excel(MATCH_XLSX, index=False)


def load_match(limit: int | None = None) -> pd.DataFrame:
    limit = limit or MAX_FETCH
    if _use_firestore():
        rows_raw = _fs_query(COL_MATCH, limit)
        rows = [
            {
                "블로그ID": r.get("blog_id", ""),
                "전화번호": r.get("phone", ""),
                "메모": r.get("memo", ""),
            }
            for r in rows_raw
        ]
        if not rows:
            return pd.DataFrame(columns=["블로그ID", "전화번호", "메모"])
        df = pd.DataFrame(rows)
        if "메모" not in df.columns:
            df["메모"] = ""
        return df

    if not os.path.exists(MATCH_XLSX):
        return pd.DataFrame(columns=["블로그ID", "전화번호", "메모"])
    df = load_excel(MATCH_XLSX)
    df = df.head(limit)
    if "메모" not in df.columns:
        df["메모"] = ""
    return df


def clear_all():
    if _use_firestore():
        client = _get_firestore()
        for col_name in [COL_CAFE, COL_BEST, COL_MATCH]:
            docs = list(client.collection(col_name).stream())
            for d in docs:
                d.reference.delete()
        return

    for f in [STORE_CAFE, STORE_BEST, MATCH_XLSX]:
        if os.path.exists(f):
            os.remove(f)


# ------------------------------------------------------------------
# Session init
# ------------------------------------------------------------------
if "excel_df" not in st.session_state:
    st.session_state["excel_df"] = None
if "best_df" not in st.session_state:
    st.session_state["best_df"] = None


# ------------------------------------------------------------------
# UI
# ------------------------------------------------------------------
menu = st.sidebar.radio(
    "메뉴 선택",
    ["파일 업로드", "최적리스트 비교", "누적 저장소", "매칭 결과 & 메모", "데이터 초기화"],
)

if _use_firestore():
    st.sidebar.success(f"저장소: Firestore ({FIREBASE_PROJECT_ID})")
else:
    st.sidebar.info("저장소: 로컬 파일")


# ============================================================
# 파일 업로드
# ============================================================
if menu == "파일 업로드":
    st.header("📁 파일 업로드")
    uploaded = st.file_uploader("엑셀 파일 업로드", type=["xlsx", "xls"], key="excel_upload")

    if uploaded:
        df = load_excel(uploaded)
        st.session_state["excel_df"] = df
        st.success("엑셀을 불러왔습니다 (세션 저장됨)")
        st.write(df.head())

    if st.session_state["excel_df"] is not None and st.button("전화번호 추출 & 누적 저장"):
        extracted: List[List[str]] = []
        df = st.session_state["excel_df"]

        for i in range(1, len(df)):
            blog_id = str(df.iloc[i, 0]).strip()
            text = f"{df.iloc[i, 1]} {df.iloc[i, 3]}" if df.shape[1] > 3 else str(df.iloc[i, 1])
            phones = extract_phone_numbers(text)
            for p in phones:
                extracted.append([blog_id, p])

        result = pd.DataFrame(extracted, columns=["블로그ID", "전화번호"]).drop_duplicates()
        save_cafe(result)
        st.success("카페 DB에 저장 완료")
        st.metric("추출 개수", len(result))
        st.dataframe(result, use_container_width=True)


# ============================================================
# 최적리스트 TXT 업로드 + 매칭
# ============================================================
elif menu == "최적리스트 비교":
    st.header("📌 최적리스트 TXT 업로드")
    txt_file = st.file_uploader("TXT 파일 업로드", type=["txt"], key="best_upload")

    if txt_file:
        text = txt_file.read().decode("utf-8")
        ids = [i.strip() for i in text.splitlines() if i.strip()]
        st.session_state["best_df"] = pd.DataFrame(ids, columns=["블로그ID"])
        save_best(ids)
        st.success("최적리스트 DB 저장 완료")
        st.metric("TXT 업로드 개수", len(ids))

    if st.session_state["best_df"] is not None:
        st.dataframe(st.session_state["best_df"].head(50), use_container_width=True)

        cafe_df = load_cafe()
        if not cafe_df.empty:
            matched = cafe_df[cafe_df["블로그ID"].isin(st.session_state["best_df"]["블로그ID"])]
            matched = matched.drop_duplicates(subset=["블로그ID"])
            matched["메모"] = ""
            save_match(matched)
            st.metric("매칭된 개수", len(matched))
            st.dataframe(matched, use_container_width=True)


# ============================================================
# 누적 저장소
# ============================================================
elif menu == "누적 저장소":
    st.header("📦 누적 저장소 (2분할)")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📦 카페 누적 DB")
        if st.button("카페 데이터 불러오기", key="load_cafe_view"):
            with st.spinner("불러오는 중..."):
                df_cafe = load_cafe(limit=DEFAULT_VIEW_LIMIT)
            st.caption(f"표시 최대 {DEFAULT_VIEW_LIMIT}행")
            st.metric("표시 중", len(df_cafe))
            st.dataframe(df_cafe, use_container_width=True, height=360)
        else:
            st.info("버튼을 눌러 조회하세요 (대용량 보호)")

    with col2:
        st.subheader("📚 최적리스트 DB")
        if st.button("최적리스트 불러오기", key="load_best_view"):
            with st.spinner("불러오는 중..."):
                df_best = load_best(limit=DEFAULT_VIEW_LIMIT)
            st.caption(f"표시 최대 {DEFAULT_VIEW_LIMIT}행")
            st.metric("표시 중", len(df_best))
            st.dataframe(df_best, use_container_width=True, height=360)
        else:
            st.info("버튼을 눌러 조회하세요 (대용량 보호)")


# ============================================================
# 매칭 결과 & 메모
# ============================================================
elif menu == "매칭 결과 & 메모":
    st.header("📞 매칭 결과 & 메모")

    if st.button("매칭 데이터 불러오기", key="load_match_view"):
        with st.spinner("불러오는 중..."):
            df = load_match(limit=DEFAULT_VIEW_LIMIT)
        st.caption(f"표시 최대 {DEFAULT_VIEW_LIMIT}행 (전체 편집 시 성능 보호)")
        if df.empty:
            st.warning("매칭 데이터가 없습니다.")
        else:
            st.metric("표시 중", len(df))
            if "메모" not in df.columns:
                df["메모"] = ""
            edited = st.data_editor(df, use_container_width=True)

            if st.button("저장"):
                save_match(edited)
                st.success("저장 완료")
    else:
        st.info("버튼을 눌러 조회하세요 (대용량 보호)")


# ============================================================
# 초기화
# ============================================================
elif menu == "데이터 초기화":
    st.header("🧹 데이터 초기화")
    if st.button("모두 삭제"):
        clear_all()
        st.session_state.clear()
        st.success("모두 초기화했습니다.")
