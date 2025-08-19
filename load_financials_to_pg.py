# load_financials_to_pg.py
import os, glob, re, itertools
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import zipfile
import config

load_dotenv()  # PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

# --- 수동 설정값 (환경변수 없을 때 fallback) ---
db_config = {
    "host": "121.167.148.247",
    "port": 5432,
    "database": "david",
    "user": "inhyuk",
    "password": "Dl65748274@",
}

# ---------- DB 접속 ----------
def get_env_or_default(var, default):
    return os.getenv(var) if os.getenv(var) not in (None, "") else default

PGHOST     = get_env_or_default("PGHOST",     db_config["host"])
PGPORT_RAW = get_env_or_default("PGPORT",     str(db_config["port"]))
PGUSER     = get_env_or_default("PGUSER",     db_config["user"])
PGPASSWORD = get_env_or_default("PGPASSWORD", db_config["password"])
PGDATABASE = get_env_or_default("PGDATABASE", db_config["database"])

if not str(PGPORT_RAW).isdigit():
    raise ValueError(f"PGPORT must be an integer (got '{PGPORT_RAW}')")
pg_port = int(PGPORT_RAW)

# PostgreSQL 연결 설정
conn_params = {
    "host": PGHOST,
    "port": pg_port,
    "database": PGDATABASE,
    "user": PGUSER,
    "password": PGPASSWORD
}
print(f"Connecting to PostgreSQL at {PGHOST}:{pg_port} as {PGUSER} …")

# ---------- 경로 ----------
COMPANY_DIR = "dart_financial_data_by_company"
file_paths = glob.glob(os.path.join(COMPANY_DIR, "*.xlsx"))

# ---------- 쿼리 헬퍼 ----------
def fetch_one(query, params=None):
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(query, params)
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result else None

def upsert_company(corp_code, company_name, stock_code):
    query = """
    INSERT INTO opendart.companies(corp_code, company_name, stock_code)
    VALUES (%s, %s, %s)
    ON CONFLICT (corp_code) DO NOTHING;
    """
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(query, (int(corp_code), str(company_name), int(stock_code)))
    cur.close()
    conn.close()

def get_or_create_account(fs_nm, sj_nm, account_nm):
    sql_sel = """
    SELECT account_id FROM opendart.accounts
    WHERE fs_nm=%s AND sj_nm=%s AND account_nm=%s
    """
    acc_id = fetch_one(sql_sel, (fs_nm, sj_nm, account_nm))
    if acc_id:
        return acc_id
    sql_ins = """
    INSERT INTO opendart.accounts(fs_nm, sj_nm, account_nm)
    VALUES (%s, %s, %s) RETURNING account_id
    """
    return fetch_one(sql_ins, (fs_nm, sj_nm, account_nm))

def get_or_create_report(year, quarter, report_name):
    sql_upsert = """
    INSERT INTO opendart.reports(year, quarter, report_name)
    VALUES (%s, %s, %s)
    ON CONFLICT (year, quarter) DO UPDATE
        SET report_name = EXCLUDED.report_name
    RETURNING report_id
    """
    return fetch_one(sql_upsert, (int(year), str(quarter), str(report_name)))

def upsert_fin_value(corp_code, acc_id, rep_id, amount, currency):
    query = """
    INSERT INTO opendart.fin_values
          (corp_code, account_id, report_id, amount, currency)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (corp_code, account_id, report_id) DO UPDATE
        SET amount = EXCLUDED.amount,
            currency = EXCLUDED.currency;
    """
    # Convert amount string to float, removing commas
    if pd.isna(amount):
        amount_val = None
    else:
        amount_str = str(amount).replace(',', '').strip()
        if amount_str == '-' or amount_str == '':
            amount_val = None
        else:
            try:
                amount_val = float(amount_str)
            except ValueError:
                print(f"[경고] 숫자로 변환할 수 없는 값: '{amount_str}'")
                amount_val = None
    
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(query, (int(corp_code), int(acc_id), int(rep_id), amount_val, str(currency)))
    cur.close()
    conn.close()

# ---------- 기간 파싱 ----------
Q_KOR_TO_STD = {"1분기": "1Q", "반기": "2Q", "3분기": "3Q", "사업보고서": "4Q"}

def parse_period(col_prefix):
    # '2021_3분기' → (2021, '3분기', '3Q')
    year, q_kor = col_prefix.split("_", 1)
    return int(year), q_kor, Q_KOR_TO_STD.get(q_kor, q_kor)

# ---------- TARGET_COMPANIES 필터링 ----------
if config.TARGET_COMPANIES:
    filtered_paths = []
    for path in file_paths:
        fname = os.path.basename(path)
        # 파일명에서 회사명 추출 (회사명_고유번호.xlsx 형식)
        company_name = fname.split('_')[0] if '_' in fname else fname.replace('.xlsx', '')
        if company_name in config.TARGET_COMPANIES:
            filtered_paths.append(path)
    file_paths = filtered_paths
    print(f"TARGET_COMPANIES 필터링: {len(file_paths)}개 파일 처리")

print(f"\n처리할 기간: {config.START_YEAR} Q{config.START_QUARTER} ~ {config.END_YEAR} Q{config.END_QUARTER}")

# ---------- 메인 루프 ----------
for path in file_paths:
    fname = os.path.basename(path)
    print(f"▶ processing {fname}")

    # 1) 간단 무결성 체크: XLSX는 ZIP 형식이어야 함
    if not zipfile.is_zipfile(path):
        print(f"    -> [SKIP] '{fname}' is not a valid .xlsx (zip) file")
        continue

    # 2) 안전하게 읽기
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"    -> [SKIP] '{fname}' read error: {e}")
        continue

    # key 컬럼
    id_cols = ['회사명', '고유번호', 'stock_code', 'fs_nm', 'sj_nm', 'account_nm']

    # 모든 기간별 2중 컬럼 prefix 파악
    triples = {}
    for c in df.columns:
        m = re.match(r"(\d{4}_.+)_thstrm_(amount|currency)", c)
        if m:
            prefix, kind = m.groups()
            triples.setdefault(prefix, {})[kind] = c

    # companies upsert
    corp_code = df['고유번호'].iloc[0]
    company_name = df['회사명'].iloc[0]
    stock_code = df['stock_code'].iloc[0]
    upsert_company(corp_code, company_name, stock_code)

    # accounts 고유 집합
    acc_meta = df[id_cols].drop_duplicates()

    for _, acc_row in acc_meta.iterrows():
        acc_id = get_or_create_account(acc_row.fs_nm,
                                       acc_row.sj_nm,
                                       acc_row.account_nm)

        # 원본 df에서 해당 account 하나만 추출
        one = df[
            (df.fs_nm == acc_row.fs_nm) &
            (df.sj_nm == acc_row.sj_nm) &
            (df.account_nm == acc_row.account_nm)
        ].iloc[0]

        # 각 기간별 값 삽입
        for prefix, parts in triples.items():
            year, q_kor, q_std = parse_period(prefix)
            
            # 기간 필터링 적용
            quarter_num_map = {'1Q': 1, '2Q': 2, '3Q': 3, '4Q': 4}
            quarter_num = quarter_num_map.get(q_std, 0)
            
            # 시작 기간 체크
            if year < config.START_YEAR:
                continue
            if year == config.START_YEAR and quarter_num < config.START_QUARTER:
                continue
            
            # 종료 기간 체크
            if year > config.END_YEAR:
                continue
            if year == config.END_YEAR and quarter_num > config.END_QUARTER:
                continue
            
            rep_id = get_or_create_report(year, q_std, q_kor)

            amount = one[parts['amount']]
            currency = one[parts['currency']]

            # NaN 방지
            if pd.isna(amount):
                continue
            upsert_fin_value(corp_code, acc_id, rep_id, amount, currency)

print("✅ Load finished")