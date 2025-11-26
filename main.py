import streamlit as st
import pandas as pd

from processor import process_excel, process_best_list, match_lists
from db import (
    DATA_BACKEND,
    FIREBASE_PROJECT_ID,
    clear_all,
    insert_excel_records,
    load_excel_records,
    load_matched,
    save_matched,
)


st.set_page_config(page_title="윌메이드 필터링 자동화 v2 - 로컬", layout="wide")


def main():
    st.title("📦 윌메이드 필터링 자동화 v2 (로컬)")
    if DATA_BACKEND == "firestore":
        st.success(f"??? ???: Firestore (project {FIREBASE_PROJECT_ID})")
    else:
        st.info("??? ???: ?? SQLite (DATA_BACKEND=sqlite)")


    # 세션에 현재 업로드 결과 잠깐 보관
    if "last_excel" not in st.session_state:
        st.session_state.last_excel = pd.DataFrame(columns=["user_id", "phone", "memo"])
    if "last_match" not in st.session_state:
        st.session_state.last_match = pd.DataFrame(columns=["user_id", "phone", "memo"])

    st.markdown("엑셀 + 최적리스트 업로드 후, 아래에서 **누적 리스트**를 관리합니다.")

    # =========================
    # 1) 파일 업로드 영역
    # =========================
    st.subheader("1️⃣ 파일 업로드")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**엑셀 파일 업로드 (xlsx)**")
        excel_file = st.file_uploader(
            "엑셀 파일 업로드 (A:아이디 / B,D:전화번호 있을 수 있음)",
            type=["xlsx"],
            key="excel_upload",
        )

    with col2:
        st.markdown("**최적 리스트 업로드 (txt / csv)**")
        best_file = st.file_uploader(
            "최적 리스트 업로드 (아이디,전화번호)",
            type=["txt", "csv"],
            key="best_upload",
        )

    run = st.button("🔍 필터링 실행", type="primary")

    if run:
        if not excel_file or not best_file:
            st.warning("엑셀 파일과 최적 리스트 파일을 모두 업로드해 주세요.")
        else:
            try:
                excel_df = process_excel(excel_file)
                best_df = process_best_list(best_file)
                match_df = match_lists(excel_df, best_df)

                # 세션에 최근 결과 저장
                st.session_state.last_excel = excel_df
                st.session_state.last_match = match_df

                # DB 누적 저장
                if not excel_df.empty:
                    insert_excel_records(excel_df)
                if not match_df.empty:
                    save_matched(match_df)

                st.success("필터링 및 DB 저장이 완료되었습니다!")

            except Exception as e:
                st.error(f"처리 중 오류가 발생했습니다: {e}")

    # =========================
    # 2) 방금 업로드한 결과 미리보기
    # =========================
    st.subheader("2️⃣ 이번 업로드 결과 (임시 미리보기)")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**엑셀에서 추출된 아이디+전화번호**")
        st.dataframe(st.session_state.last_excel, use_container_width=True, height=250)

    with col_b:
        st.markdown("**최적 매칭 결과 (이번 업로드)**")
        st.dataframe(st.session_state.last_match, use_container_width=True, height=250)

    st.markdown("---")

    # =========================
    # 3) 누적 리스트 관리
    # =========================
    st.subheader("3️⃣ 누적 리스트 관리")

    tab1, tab2 = st.tabs(["📒 엑셀 전체 누적 리스트", "🎯 최적 매칭 누적 리스트"])

    with tab1:
        excel_all = load_excel_records()
        st.caption(f"총 {len(excel_all)}건")
        st.dataframe(excel_all, use_container_width=True, height=350)

    with tab2:
        match_all = load_matched()
        st.caption(f"총 {len(match_all)}건")
        st.dataframe(match_all, use_container_width=True, height=350)

    st.markdown("---")

    # =========================
    # 4) 전체 데이터 초기화
    # =========================
    st.markdown("⚠️ **전체 데이터 초기화** (테스트용 / 잘못 넣었을 때만 사용)")

    if st.button("🗑 전체 데이터 초기화"):
        clear_all()
        st.session_state.last_excel = pd.DataFrame(columns=["user_id", "phone", "memo"])
        st.session_state.last_match = pd.DataFrame(columns=["user_id", "phone", "memo"])
        st.success("모든 누적 데이터가 삭제되었습니다.")


if __name__ == "__main__":
    main()
