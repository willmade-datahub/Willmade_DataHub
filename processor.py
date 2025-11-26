import pandas as pd
import re

# 한글/영문 숫자 → 숫자 변환 맵
CHAR_MAP = {
    '공': '0', '영': '0',
    '일': '1', '이': '2', '삼': '3', '사': '4',
    '오': '5', '육': '6', '륙': '6', '칠': '7', '팔': '8', '구': '9',
    'o': '0', 'O': '0',
    'l': '1', 'I': '1', 'i': '1',
    'Z': '2',
    'S': '5', 's': '5',
    'B': '8',
}

phone_pattern = re.compile(r'0\d{1,2}[-\s\.\)]*\d{3,4}[-\s\.\)]*\d{4}')


def _normalize_digits(text: str) -> str:
    if text is None:
        text = ""
    if not isinstance(text, str):
        text = str(text)

    return "".join(CHAR_MAP.get(ch, ch) for ch in text)


def extract_phone(text: str) -> str | None:
    """
    B열 / D열 문장에서 전화번호 모양만 뽑아서
    010-1234-5678 형태로 정리
    """
    norm = _normalize_digits(text)
    if not norm.strip():
        return None

    m = phone_pattern.search(norm)
    if not m:
        return None

    digits = re.sub(r'\D', '', m.group())
    if len(digits) < 9:
        return None

    # 서울번호(02) / 휴대폰(010~) 대충 맞춰서 포맷팅
    if digits.startswith("02") and len(digits) == 10:
        return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    elif len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11:
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    else:
        return digits  # 예외는 그냥 숫자만 반환


def process_excel(excel_file) -> pd.DataFrame:
    """
    엑셀 업로드 파일에서
    - A열: 아이디
    - B열: 제목(전화번호 있을 수도 있음)
    - D열: 본문(전화번호 있을 수도 있음)
    만 사용해서 user_id, phone, memo 데이터프레임 생성
    """
    df_raw = pd.read_excel(excel_file)

    # A / B / D 컬럼 인덱스로 강제 가져오기 (헤더 이름 상관 없음)
    col_a = df_raw.iloc[:, 0]  # 아이디
    col_b = df_raw.iloc[:, 1] if df_raw.shape[1] > 1 else ""
    col_d = df_raw.iloc[:, 3] if df_raw.shape[1] > 3 else ""

    user_ids = col_a.astype(str).str.strip()

    phones: list[str | None] = []
    for b, d in zip(col_b, col_d):
        combined_text = f"{b or ''} {d or ''}"
        phones.append(extract_phone(combined_text))

    result = pd.DataFrame(
        {
            "user_id": user_ids,
            "phone": phones,
            "memo": "",   # 메모는 UI에서 직접 입력 가능하게 남겨둠
        }
    )

    # 아이디 없는 행 정리 & 중복 제거
    result = result[result["user_id"].notna()]
    result["user_id"] = result["user_id"].str.strip()
    result = result.drop_duplicates(subset=["user_id", "phone"])

    return result


def process_best_list(best_file) -> pd.DataFrame:
    """
    최적 리스트(txt / csv 등)에서 user_id, phone 추출
    - "아이디,전화번호"
    - "아이디<TAB>전화번호"
    - "아이디 전화번호"
    대충 이런 형태를 전부 커버하게 구성
    """
    # sep=None + engine='python' 으로 구분자 자동 감지
    try:
        df = pd.read_csv(best_file, sep=None, engine="python", header=None)
    except Exception:
        best_file.seek(0)
        df = pd.read_csv(best_file, sep="\t", header=None)

    if df.shape[1] == 1:
        # 한 칼럼 안에 "아이디 전화번호" 같이 붙어있는 경우
        temp = df[0].astype(str).str.split(r'[\s,]+', n=1, expand=True)
        df = temp

    df = df.rename(columns={0: "user_id", 1: "phone"})
    df["user_id"] = df["user_id"].astype(str).str.strip()
    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str).str.strip()
    else:
        df["phone"] = ""

    df = df[["user_id", "phone"]]
    df = df.dropna(subset=["user_id"])
    df = df.drop_duplicates(subset=["user_id", "phone"])

    return df


def match_lists(excel_df: pd.DataFrame, best_df: pd.DataFrame) -> pd.DataFrame:
    """
    excel_df 와 best_df 를 아이디 기준으로 매칭
    - 아이디가 겹치는 것만 추출
    - 전화번호는 (엑셀 전화번호 우선) 없으면 최적리스트 전화번호 사용
    """
    merged = pd.merge(
        best_df,
        excel_df,
        on="user_id",
        how="left",
        suffixes=("_best", "_excel"),
    )

    merged["phone"] = merged["phone_excel"].combine_first(merged["phone_best"])
    matched = merged[["user_id", "phone"]]
    matched = matched.dropna(subset=["user_id", "phone"])
    matched["memo"] = ""
    matched = matched.drop_duplicates(subset=["user_id", "phone"])

    return matched
