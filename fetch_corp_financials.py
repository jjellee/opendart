'''
정기보고서 재무정보
다중회사 주요계정
https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019017
'''

import requests
import pandas as pd
import time
import os
import io
import zipfile
import xmltodict
import config

# --- 사용자 설정 ---

# 2. 결과를 저장할 폴더 이름을 지정하세요.
OUTPUT_DIR = "dart_financial_data"
# OUTPUT_DIR_COMPANY = "dart_financial_data_by_company"  # <— 병합 전용 스크립트로 이동
# 대상 회사 목록이 저장된 엑셀 파일
CORP_CODES_FILE = "corp_codes_전체.xlsx"

# --- API 정보 ---
CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
ACCOUNTS_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"


# def append_period_to_company_file(corp_name, corp_code, stock_code, account_list, year, report_name):
#     """
#     회사별 엑셀 파일에 (thstrm_nm, thstrm_dt, thstrm_amount) 3개 컬럼을
#     '<year>_<report_name>_' 접두사로 붙여 누적 저장/갱신한다.
#     """
#     if not account_list:
#         return

#     # 1) 이번 분기 컬럼 이름 생성
#     period_prefix = f"{year}_{report_name}"
#     df = pd.DataFrame(account_list)

#     # 2) 필요한 컬럼만 선택하고 컬럼명 변환
#     df = df[['회사명', '고유번호', 'stock_code', 'fs_nm', 'sj_nm', 'account_nm',
#              'thstrm_nm', 'thstrm_dt', 'thstrm_amount', 'currency']].copy()
#     rename_map = {
#         'thstrm_nm': f"{period_prefix}_thstrm_nm",
#         'thstrm_dt': f"{period_prefix}_thstrm_dt",
#         'thstrm_amount': f"{period_prefix}_thstrm_amount",
#     }
#     df.rename(columns=rename_map, inplace=True)
#     # ---- 키 컬럼(dtype) 정규화 ----
#     key_cols = ['회사명', '고유번호', 'stock_code', 'fs_nm', 'sj_nm', 'account_nm']
#     for col in key_cols:
#         if col in df.columns:
#             df[col] = df[col].astype(str)

#     # 3) 회사별 파일 경로
#     if not os.path.exists(OUTPUT_DIR_COMPANY):
#         os.makedirs(OUTPUT_DIR_COMPANY)
#     company_file = os.path.join(OUTPUT_DIR_COMPANY, f"{corp_name}_{corp_code}.xlsx")

#     # 4) 기존 파일이 있으면 병합, 없으면 새로 생성
#     if os.path.exists(company_file):
#         existing_df = pd.read_excel(company_file, engine='openpyxl')
#         # 기존 파일도 동일 dtype(str)로 맞춰준다
#         for col in key_cols:
#             if col in existing_df.columns:
#                 existing_df[col] = existing_df[col].astype(str)
#         # 이미 같은 기간 데이터가 있으면 스킵
#         if f"{period_prefix}_thstrm_amount" in existing_df.columns:
#             print(f"    -> [스킵] {period_prefix} 데이터가 이미 존재합니다.")
#             return
#         merged_df = pd.merge(existing_df, df, on=key_cols, how='outer')
#     else:
#         merged_df = df

#     # 5) currency 컬럼을 가장 마지막으로 이동
#     if 'currency' in merged_df.columns:
#         currency_series = merged_df.pop('currency')
#         merged_df['currency'] = currency_series

#     merged_df.to_excel(company_file, index=False, engine='openpyxl')
#     print(f"    -> [저장] {period_prefix} 데이터가 '{company_file}'에 반영되었습니다.")


def run_batch_fetch():
    """
    2018년부터 2025년 1분기까지의 주요계정 정보를 분기별로 조회하고
    각각 별도의 엑셀 파일로 저장하는 메인 함수입니다.
    """
    # 결과 저장 폴더 생성
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"결과를 저장할 '{OUTPUT_DIR}' 폴더를 생성했습니다.")

    # if not os.path.exists(OUTPUT_DIR_COMPANY):
    #     os.makedirs(OUTPUT_DIR_COMPANY)
    #     print(f"회사별 데이터를 저장할 '{OUTPUT_DIR_COMPANY}' 폴더를 생성했습니다.")

    # 1. 대상 회사 목록 가져오기 (엑셀 파일 로드)
    try:
        corp_df = pd.read_excel(CORP_CODES_FILE)
        if corp_df.empty:
            print(f"엑셀 파일 '{CORP_CODES_FILE}'에 데이터가 없습니다. 프로그램을 종료합니다.")
            return
    except FileNotFoundError:
        print(f"엑셀 파일 '{CORP_CODES_FILE}'을 찾을 수 없습니다. 프로그램을 종료합니다.")
        return

    # corp_code가 8자리가 되도록 앞에 0을 채움
    corp_df['corp_code'] = corp_df['corp_code'].astype(str).str.zfill(8)

    # 지정된 기업만 필터링
    original_cnt = len(corp_df)
    
    # TARGET_COMPANIES가 비어있지 않으면 해당 기업만 필터링
    if config.TARGET_COMPANIES:
        corp_df = corp_df[corp_df['corp_name'].isin(config.TARGET_COMPANIES)].copy()
        if corp_df.empty:
            print("대상 회사 리스트에 해당하는 기업이 엑셀에 없습니다. 프로그램을 종료합니다.")
            return
        print(f"필터 전 {original_cnt:,}개 → TARGET_COMPANIES 필터 후 {len(corp_df):,}개 기업을 대상으로 조회를 시작합니다.")
    else:
        print(f"전체 {len(corp_df):,}개 기업을 대상으로 조회를 시작합니다.")

    # 2. 조회할 기간 설정 (config에서 가져옴)
    years = range(config.START_YEAR, config.END_YEAR + 1)
    report_codes = {
        '1분기': '11013',
        '반기': '11012',
        '3분기': '11014',
        '사업보고서': '11011'
    }

    # 3. 회사별로 반복하며 데이터 조회 및 저장
    # stock_code가 존재하는 회사만 필터링
    corp_df_with_stock = corp_df[corp_df['stock_code'].notna() & (corp_df['stock_code'] != '')].copy()
    print(f"stock_code가 있는 회사: {len(corp_df_with_stock):,}개\n")
    
    for index, row in corp_df_with_stock.iterrows():
        corp_code = row['corp_code']
        corp_name = row['corp_name']
        stock_code = row.get('stock_code', '')

        print(f"\n{'='*60}")
        print(f"▶ 회사 처리 시작: ({index + 1}/{len(corp_df_with_stock)}) {corp_name} ({corp_code})")
        print(f"{'='*60}")

        # 회사별 폴더 및 파일명 생성
        safe_corp_name = "".join(ch if ch.isalnum() else "_" for ch in corp_name)
        company_dir = os.path.join(OUTPUT_DIR, safe_corp_name)
        
        # 회사별 폴더 생성
        if not os.path.exists(company_dir):
            os.makedirs(company_dir)

        # 각 회사에 대해 모든 기간 조회
        for year in years:
            for report_name, report_code in report_codes.items():
                # 시작 기간 필터링
                if year == config.START_YEAR:
                    quarter_num = {'1분기': 1, '반기': 2, '3분기': 3, '사업보고서': 4}[report_name]
                    if quarter_num < config.START_QUARTER:
                        continue
                
                # 종료 기간 필터링
                if year == config.END_YEAR:
                    quarter_num = {'1분기': 1, '반기': 2, '3분기': 3, '사업보고서': 4}[report_name]
                    if quarter_num > config.END_QUARTER:
                        continue
                

                output_filename = os.path.join(
                    company_dir,
                    f"{year}_{report_name}_major_accounts.xlsx"
                )
                if os.path.exists(output_filename):
                    print(f"  {year}년 {report_name} -> 이미 존재, 스킵")
                    continue

                print(f"  {year}년 {report_name} 조회 중...")

                params = {
                    'crtfc_key': config.API_KEY,
                    'corp_code': corp_code,
                    'bsns_year': str(year),
                    'reprt_code': report_code,
                    'fs_div': 'CFS'  # 연결재무제표 기준
                }

                try:
                    response = requests.get(ACCOUNTS_URL, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()

                    if data.get('status') == '000':
                        account_list = data.get('list', [])
                        for item in account_list:
                            item['회사명'] = corp_name
                            item['고유번호'] = corp_code
                            item['stock_code'] = stock_code
                        if account_list:
                            try:
                                pd.DataFrame(account_list).to_excel(
                                    output_filename, index=False, engine='openpyxl'
                                )
                                print("    -> 저장 완료")
                            except Exception as e:
                                print(f"    -> 저장 오류: {e}")
                        else:
                            if config.CREATE_EMPTY_FILE_ON_NO_DATA:
                                # 데이터가 없어도 빈 파일 생성
                                print("    -> 데이터 없음 (빈 파일 생성)")
                                empty_df = pd.DataFrame([{
                                    '회사명': corp_name,
                                    '고유번호': corp_code,
                                    'stock_code': stock_code,
                                    'status': 'NO_DATA',
                                    'message': '조회된 데이터가 없습니다',
                                    'year': year,
                                    'report_name': report_name
                                }])
                                empty_df.to_excel(output_filename, index=False, engine='openpyxl')
                            else:
                                print("    -> 데이터 없음 (파일 생성 안 함)")
                    elif data.get('status') == '013':
                        if config.CREATE_EMPTY_FILE_ON_NO_DATA:
                            # 데이터가 없어도 빈 파일 생성
                            print("    -> 데이터 없음 (빈 파일 생성)")
                            empty_df = pd.DataFrame([{
                                '회사명': corp_name,
                                '고유번호': corp_code,
                                'stock_code': stock_code,
                                'status': 'NO_DATA',
                                'message': '조회된 데이터가 없습니다',
                                'year': year,
                                'report_name': report_name
                            }])
                            empty_df.to_excel(output_filename, index=False, engine='openpyxl')
                        else:
                            print("    -> 데이터 없음 (파일 생성 안 함)")
                    else:
                        error_status = data.get('status', 'N/A')
                        error_message = data.get('message', 'N/A')
                        print(f"    -> 오류 ({error_status}: {error_message})")
                        
                        # 사용한도 초과 오류 처리
                        if error_status == '020' or '사용한도를 초과하였습니다' in error_message:
                            print("\n⚠️  API 사용한도를 초과하였습니다. 프로그램을 종료합니다.")
                            return
                except requests.exceptions.RequestException as e:
                    print(f"    -> 네트워크 오류: {e}")
                except Exception as e:
                    print(f"    -> 알 수 없는 오류: {e}")
                
                time.sleep(0.15)  # DART 서버 부하 방지를 위한 지연
        
        print(f"▶ {corp_name} 회사 처리 완료")

    print("\n\n🎉 모든 기간에 대한 작업이 완료되었습니다.")


if __name__ == "__main__":
    run_batch_fetch()
