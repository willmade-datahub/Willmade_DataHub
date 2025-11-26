import sqlite3
import os

DB_PATH = r"C:\Willmade_DataHub\data.db"

# 폴더 확인 및 생성
os.makedirs(r"C:\Willmade_DataHub", exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# excel_master 테이블 생성
cur.execute("""
CREATE TABLE IF NOT EXISTS excel_master (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blog_id TEXT,
    raw_b TEXT,
    raw_d TEXT,
    phone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

# match_list 테이블 생성
cur.execute("""
CREATE TABLE IF NOT EXISTS match_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blog_id TEXT,
    phone TEXT,
    memo TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

conn.commit()
conn.close()

print("DB 생성 완료 🎉  → C:\\Willmade_DataHub\\data.db")
