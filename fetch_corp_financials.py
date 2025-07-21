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

'''
# 조회할 대상 회사명 리스트
TARGET_COMPANIES = [
    "달바글로벌", "파마리서치", "대웅제약", "노바렉스", "원텍",
    "클래시스", "아이센스", "빙그레", "삼양식품", "비엠티",
    "코미코", "한미반도체", "에스앤에스텍", "티에스이",
    "리노공업", "이수페타시스", "HD현대일렉트릭", "엘에스일렉트릭",
    "삼성바이오로직스", "휴젤"
]
'''
TARGET_COMPANIES = [ "이수페타시스"
]

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


# def get_corp_codes(api_key: str, force_refresh: bool = False) -> pd.DataFrame:
#     """
#     DART에 등록된 전체 회사 목록을 다운로드하여 DataFrame으로 반환합니다.
#     상장된 회사(종목코드가 있는 회사)만 필터링합니다.
#     """
#     cache_file = "CORPCODE.xml"
#     if not force_refresh and os.path.exists(cache_file):
#         print(f"캐시 파일 '{cache_file}'을 사용합니다.")
#         with open(cache_file, "r", encoding="utf-8") as f:
#             xml_text = f.read()
#     else:
#         print("DART 서버에서 최신 회사 목록을 다운로드합니다...")
#         params = {"crtfc_key": api_key}
#         try:
#             resp = requests.get(CORP_CODE_URL, params=params, timeout=60)
#             resp.raise_for_status()  # HTTP 오류 발생 시 예외 발생
#             with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
#                 xml_text = z.read("CORPCODE.xml").decode("utf-8")
#             # 나중을 위해 캐시 파일 저장
#             with open(cache_file, "w", encoding="utf-8") as f:
#                 f.write(xml_text)
#         except requests.exceptions.RequestException as e:
#             print(f"회사 목록 다운로드 중 네트워크 오류 발생: {e}")
#             return pd.DataFrame()

#     # XML을 파싱하여 DataFrame으로 변환
#     try:
#         data = xmltodict.parse(xml_text)["result"]["list"]
#         df = pd.DataFrame(data)
#         # 종목코드가 있는 상장사만 필터링
#         df_listed = df[df['stock_code'].notna()].copy()
#         # 고유번호가 8자리가 아닐 경우 앞에 0을 채워줌
#         df_listed['corp_code'] = df_listed['corp_code'].str.zfill(8)
#         return df_listed
#     except Exception as e:
#         print(f"회사 목록 파싱 중 오류 발생: {e}")
#         return pd.DataFrame()


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
    corp_df = corp_df[corp_df['corp_name'].isin(TARGET_COMPANIES)].copy()
    if corp_df.empty:
        print("대상 회사 리스트에 해당하는 기업이 엑셀에 없습니다. 프로그램을 종료합니다.")
        return
    print(f"필터 전 {original_cnt:,}개 → 필터 후 {len(corp_df):,}개 기업을 대상으로 조회를 시작합니다.")

    # 2. 조회할 기간 설정 (2018년 ~ 2025년)
    years = range(2018, 2026)
    report_codes = {
        '1분기': '11013',
        '반기': '11012',
        '3분기': '11014',
        '사업보고서': '11011'
    }

    # 3. 기간별로 반복하며 데이터 조회 및 저장
    for year in years:
        for report_name, report_code in report_codes.items():
            # 2025년은 1분기까지만 조회
            if year == 2025 and report_code != report_codes['1분기']:
                continue

            print(f"\n{'='*60}")
            print(f"▶ 작업 시작: {year}년 {report_name}")
            print(f"{'='*60}")

            # 각 기간별로 모든 상장사에 대해 API 호출
            for index, row in corp_df.iterrows():
                corp_code = row['corp_code']
                corp_name = row['corp_name']
                stock_code = row.get('stock_code', '')

                # 회사별 파일명: <회사명>_<year>_<report_name>_major_accounts.xlsx
                safe_corp_name = "".join(ch if ch.isalnum() else "_" for ch in corp_name)
                output_filename = os.path.join(
                    OUTPUT_DIR,
                    f"{safe_corp_name}_{year}_{report_name}_major_accounts.xlsx"
                )
                if os.path.exists(output_filename):
                    print(f"  ({index + 1}/{len(corp_df)}) {corp_name} ({corp_code}) -> 이미 존재, 스킵")
                    continue

                print(f"  ({index + 1}/{len(corp_df)}) {corp_name} ({corp_code}) 조회 중...")

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
                        # print(" -> 성공")
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
                                print(" -> 저장 완료")
                            except Exception as e:
                                print(f" -> 저장 오류: {e}")
                        else:
                            print(" -> 데이터 없음")
                    elif data.get('status') == '013':
                        print(" -> 데이터 없음")
                    else:
                        print(f" -> 오류 ({data.get('status', 'N/A')}: {data.get('message', 'N/A')})")
                except requests.exceptions.RequestException as e:
                    print(f" -> 네트워크 오류: {e}")
                except Exception as e:
                    print(f" -> 알 수 없는 오류: {e}")
                
                time.sleep(0.15)  # DART 서버 부하 방지를 위한 지연

    print("\n\n🎉 모든 기간에 대한 작업이 완료되었습니다.")


if __name__ == "__main__":
    run_batch_fetch()
