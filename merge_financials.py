# merge_financials.py

import os, glob
import pandas as pd

def to_float(value):
    """콤마가 포함된 문자열을 float으로 변환"""
    if pd.isna(value):
        return 0.0
    try:
        # 콤마 제거 후 float 변환
        return float(str(value).replace(',', ''))
    except (ValueError, TypeError):
        return 0.0

# ---- Helper: 기간 컬럼 정렬 함수 ----
def sort_period_columns(cols):
    """
    주어진 컬럼 리스트에서 *_thstrm_dt / *_thstrm_amount / *_thstrm_currency 트리플을
    '년도 + 분기' 시간순(1분기→반기→3분기→사업보고서)으로 정렬한 리스트를 반환.
    key_cols, currency 등은 제외하고 prefix 기반으로만 정렬.
    """
    order_rank = {'1분기': 0, '반기': 1, '2분기': 1, '3분기': 2, '사업보고서': 3, '4분기': 3}
    dt_cols = [c for c in cols if c.endswith('_thstrm_dt')]
    prefixes = []
    for c in dt_cols:
        prefix = c.rsplit('_', 2)[0]  # e.g. 2018_1분기
        year, q = prefix.split('_', 1)
        rank = order_rank.get(q, 99)
        prefixes.append((int(year), rank, prefix))
    prefixes.sort()
    ordered_cols = []
    for _, __, p in prefixes:
        ordered_cols.extend([
            f"{p}_thstrm_dt",
            f"{p}_thstrm_amount",
            f"{p}_thstrm_currency",
        ])
    return ordered_cols

PERIOD_DIR = "dart_financial_data"
OUTPUT_DIR_COMPANY = "dart_financial_data_by_company"

def append_to_company_files():
    # 새 네이밍: <회사명>_<YEAR>_<REPORTNAME>_major_accounts.xlsx
    files = glob.glob(os.path.join(PERIOD_DIR, "*_major_accounts.xlsx"))
    # Excel 임시 파일 (~$로 시작하는 파일) 제외
    files = [f for f in files if not os.path.basename(f).startswith('~$')]
    
    # 사업보고서를 마지막에 처리하도록 정렬
    annual_reports = [f for f in files if '_사업보고서_' in f]
    other_files = [f for f in files if '_사업보고서_' not in f]
    files = other_files + annual_reports
    
    key_cols = ['회사명', '고유번호', 'stock_code', 'fs_nm', 'sj_nm', 'account_nm']

    for f in files:
        basename = os.path.basename(f).replace(".xlsx", "")  # 이수페타시스_2023_3분기_major_accounts
        parts = basename.split("_")
        # 마지막 두 토큰은 'major', 'accounts', 그 앞이 REPORTNAME, 그앞이 YEAR
        if len(parts) < 4:
            print(f"[건너뜀] 파일명 형식이 예상과 다름: {basename}")
            continue
        year = parts[-4]
        report_name = parts[-3]
        period_prefix = f"{year}_{report_name}"  # 예: 2023_3분기

        period_df = pd.read_excel(f, engine='openpyxl')
        # 필요한 컬럼만, 새 컬럼명으로
        df = period_df[key_cols + ['thstrm_dt', 'thstrm_amount', 'currency']].copy()
        # 이번 분기의 날짜 값을 기록해 두었다가 중복 여부를 판단한다
        period_dt_val = str(df['thstrm_dt'].iloc[0])
        df.rename(columns={
            'thstrm_dt': f"{period_prefix}_thstrm_dt",
            'thstrm_amount': f"{period_prefix}_thstrm_amount",
            'currency': f"{period_prefix}_thstrm_currency",
        }, inplace=True)

        for col in key_cols:
            df[col] = df[col].astype(str)

        for _, g in df.groupby(['회사명','고유번호']):
            corp_name = g['회사명'].iloc[0]
            corp_code = g['고유번호'].iloc[0]
            comp_file = os.path.join(OUTPUT_DIR_COMPANY, f"{corp_name}_{corp_code}.xlsx")

            if os.path.exists(comp_file):
                base = pd.read_excel(comp_file, engine='openpyxl')
                for col in key_cols:
                    base[col] = base[col].astype(str)

                # 과거 버전에서 남았을 수 있는 generic 'currency' 컬럼 제거
                if 'currency' in base.columns:
                    base = base.drop(columns=['currency'])

                # 이미 같은 날짜(thstrm_dt)가 저장돼 있으면 스킵
                dt_cols = [c for c in base.columns if c.endswith('_thstrm_dt')]
                existing_dates = set()
                for c in dt_cols:
                    existing_dates.update(base[c].dropna().astype(str).unique())
                if period_dt_val in existing_dates:
                    continue
                # 혹시 같은 prefix(연도_분기) 컬럼이 이미 있으면 스킵
                if f"{period_prefix}_thstrm_amount" in base.columns:
                    continue

                merged = pd.merge(base, g, on=key_cols, how='outer')
            else:
                # 신규 파일: g가 그대로
                merged = g.copy()

            # ---- 사업보고서 손익계산서 4분기값 계산 ----
            if report_name == '사업보고서':
                # 손익계산서 항목만 필터링
                pl_mask = merged['sj_nm'] == '손익계산서'
                if pl_mask.any():
                    print(f"[DEBUG] {year}_사업보고서에서 손익계산서 4분기 값 계산 중...")
                    # 현재 연도의 1분기, 반기, 3분기 컬럼 찾기
                    q1_col = f"{year}_1분기_thstrm_amount"
                    h1_col = f"{year}_반기_thstrm_amount"
                    q3_col = f"{year}_3분기_thstrm_amount"
                    annual_col = f"{year}_사업보고서_thstrm_amount"
                    
                    # 손익계산서 항목에 대해서만 계산
                    for idx in merged[pl_mask].index:
                        account_nm = merged.loc[idx, 'account_nm']  # try 블록 밖으로 이동
                        if annual_col in merged.columns:
                            annual_val = merged.loc[idx, annual_col]
                            if pd.notna(annual_val):
                                try:
                                    annual_val = to_float(annual_val)
                                    q1_val, q2_val, q3_val = 0, 0, 0
                                    
                                    # 1분기 값
                                    if q1_col in merged.columns:
                                        val = merged.loc[idx, q1_col]
                                        q1_val = to_float(val)
                                    
                                    # 2분기 값 = 반기 값 - 1분기 값
                                    if h1_col in merged.columns:
                                        h1_val = merged.loc[idx, h1_col]
                                        q2_val = to_float(h1_val)
                                    
                                    # 3분기 값
                                    if q3_col in merged.columns:
                                        val = merged.loc[idx, q3_col]
                                        q3_val = to_float(val)
                                    
                                    # 4분기 값 = 연간 값 - (1분기 + 2분기 + 3분기)
                                    q4_val = annual_val - q1_val - q2_val - q3_val
                                    
                                    print(f"[DEBUG] {account_nm}: 연간({annual_val:,.0f}) - Q1({q1_val:,.0f}) - Q2({q2_val:,.0f}) - Q3({q3_val:,.0f}) = Q4({q4_val:,.0f})")
                                    
                                    # 사업보고서 컬럼에 4분기 값으로 업데이트
                                    merged.loc[idx, annual_col] = int(q4_val)
                                except (ValueError, TypeError) as e:
                                    print(f"[DEBUG] 계산 오류 - {account_nm}: {e}")

            # ---- 컬럼 정렬: key_cols + 기간별(dt/amount/currency) ----
            period_cols_ordered = sort_period_columns(merged.columns)
            other_cols = [c for c in merged.columns if c in key_cols]
            final_cols = other_cols + period_cols_ordered
            merged = merged.reindex(columns=final_cols)
            os.makedirs(OUTPUT_DIR_COMPANY, exist_ok=True)
            merged.to_excel(comp_file, index=False, engine='openpyxl')
            print(f"[병합] {comp_file} ← {period_prefix}")

if __name__ == "__main__":
    append_to_company_files()