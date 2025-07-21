# load_financials_to_pg.py
import os, glob, re, itertools
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import zipfile

load_dotenv()  # PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

# --- 수동 설정값 (환경변수 없을 때 fallback) ---
db_config = {
    "host": "127.0.0.1",
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

PG_URI = (
    f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}"
    f"@{PGHOST}:{pg_port}/{PGDATABASE}?host={PGHOST}"
)
print(f"Connecting to PostgreSQL at {PGHOST}:{pg_port} as {PGUSER} …")
engine = create_engine(PG_URI, isolation_level="AUTOCOMMIT")

# ---------- 경로 ----------
COMPANY_DIR = "dart_financial_data_by_company"
file_paths = glob.glob(os.path.join(COMPANY_DIR, "*.xlsx"))

# ---------- 쿼리 헬퍼 ----------
def fetch_one(sql, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()

def upsert_company(corp_code, company_name, stock_code):
    sql = """
    INSERT INTO opendart.companies(corp_code, company_name, stock_code)
    VALUES (:corp, :name, :stock)
    ON CONFLICT (corp_code) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"corp": int(corp_code), "name": str(company_name), "stock": int(stock_code)})

def get_or_create_account(fs_nm, sj_nm, account_nm):
    sql_sel = """
    SELECT account_id FROM opendart.accounts
    WHERE fs_nm=:fs AND sj_nm=:sj AND account_nm=:acc
    """
    acc_id = fetch_one(sql_sel, fs=fs_nm, sj=sj_nm, acc=account_nm)
    if acc_id:
        return acc_id
    sql_ins = """
    INSERT INTO opendart.accounts(fs_nm, sj_nm, account_nm)
    VALUES (:fs, :sj, :acc) RETURNING account_id
    """
    return fetch_one(sql_ins, fs=fs_nm, sj=sj_nm, acc=account_nm)

def get_or_create_report(year, quarter, report_name, period_dt):
    # Parse date from period_dt string like "2018.01.01 ~ 2018.03.31" or "2018-03-31 현재"
    # Take the start date if it's a range
    if pd.isna(period_dt):
        period_date = f"{year}-01-01"  # Default to year start if no date
    else:
        period_str = str(period_dt)
        if '~' in period_str:
            period_date = period_str.split('~')[0].strip()
        else:
            period_date = period_str
        
        # Extract date pattern using regex (YYYY.MM.DD or YYYY-MM-DD)
        date_match = re.search(r'(\d{4})[.-](\d{2})[.-](\d{2})', period_date)
        if date_match:
            period_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
        else:
            period_date = f"{year}-01-01"  # Default fallback
    
    sql_upsert = """
    INSERT INTO opendart.reports(year, quarter, report_name, period_dt)
    VALUES (:yr, :qtr, :rname, :pdt)
    ON CONFLICT (year, quarter, period_dt) DO UPDATE
        SET report_name = EXCLUDED.report_name
    RETURNING report_id
    """
    return fetch_one(sql_upsert, yr=int(year), qtr=str(quarter),
                     rname=str(report_name), pdt=period_date)

def upsert_fin_value(corp_code, acc_id, rep_id, amount, currency):
    sql = """
    INSERT INTO opendart.fin_values
          (corp_code, account_id, report_id, amount, currency)
    VALUES (:corp, :acc, :rep, :amt, :cur)
    ON CONFLICT (corp_code, account_id, report_id) DO UPDATE
        SET amount = EXCLUDED.amount,
            currency = EXCLUDED.currency;
    """
    # Convert amount string to float, removing commas
    if pd.isna(amount):
        amount_val = 0.0
    else:
        amount_str = str(amount).replace(',', '')
        amount_val = float(amount_str)
    
    with engine.begin() as conn:
        conn.execute(text(sql), {"corp": int(corp_code), "acc": int(acc_id),
                                 "rep": int(rep_id), "amt": amount_val, "cur": str(currency)})

# ---------- 기간 파싱 ----------
Q_KOR_TO_STD = {"1분기": "1Q", "반기": "2Q", "3분기": "3Q", "사업보고서": "4Q"}

def parse_period(col_prefix):
    # '2021_3분기' → (2021, '3분기', '3Q')
    year, q_kor = col_prefix.split("_", 1)
    return int(year), q_kor, Q_KOR_TO_STD.get(q_kor, q_kor)

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

    # 모든 기간별 3중 컬럼 prefix 파악
    triples = {}
    for c in df.columns:
        m = re.match(r"(\d{4}_.+)_thstrm_(dt|amount|currency)", c)
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
            rep_id = get_or_create_report(year, q_std, q_kor,
                                          one[parts['dt']])

            amount = one[parts['amount']]
            currency = one[parts['currency']]

            # NaN 방지
            if pd.isna(amount):
                continue
            upsert_fin_value(corp_code, acc_id, rep_id, amount, currency)

print("✅ Load finished")