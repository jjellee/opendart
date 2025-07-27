
'''
4.고유번호 DART에 등록되어있는 공시대상회사의 고유번호,회사명,종목코드, 최근변경일자를 파일로 제공합니다.
https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018 '''


import os, io, zipfile, requests, xmltodict, pandas as pd
import pandas as pd
from pathlib import Path
import config

DART_URL = "https://opendart.fss.or.kr/api/corpCode.xml"


def get_corp_codes(api_key: str,
                   cache_file: str = "CORPCODE.xml",
                   force_refresh: bool = False) -> pd.DataFrame:
    """
    OPENDART corpCode.xml을 내려받아 DataFrame으로 반환합니다.
    - api_key        : OPENDART 인증키(40자)
    - cache_file     : xml 캐시 파일명
    - force_refresh  : True면 항상 새로 다운로드
    """
    # 1) 캐시가 있으면 재사용
    if not force_refresh and os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf‑8") as f:
            xml_text = f.read()
    else:
        # 2) API 호출 (응답은 ZIP + 바이너리) 
        resp = requests.get(DART_URL,
                            params={"crtfc_key": api_key},
                            timeout=60)
        resp.raise_for_status()           # 4xx / 5xx 오류 처리
        # 3) ZIP 해제 → CORPCODE.xml 추출
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            xml_text = z.read("CORPCODE.xml").decode("utf‑8")
        # 4) 캐시 저장(선택)
        with open(cache_file, "w", encoding="utf‑8") as f:
            f.write(xml_text)

    # 5) XML → dict → DataFrame
    data = xmltodict.parse(xml_text)["result"]["list"]
    df = pd.DataFrame(data)

    # 숫자처럼 보이는 열은 문자열이므로 필요시 형변환
    return df


# pip install requests xmltodict pandas openpyxl   # ← 엑셀 저장용 openpyxl 필요

# get_corp_codes 함수는 이전 예시 그대로 가져오면 됩니다.

def save_corp_codes(api_key: str,
                    csv_path: str | Path | None = "corp_codes.csv",
                    xlsx_path: str | Path | None = "corp_codes.xlsx",
                    force_refresh: bool = False) -> pd.DataFrame:
    """
    corpCode.xml을 내려받아 DataFrame으로 변환 후
    - csv_path  : CSV 저장 경로(None이면 저장 생략)
    - xlsx_path : XLSX 저장 경로(None이면 저장 생략)
    반환값: DataFrame (필요 시 후속 가공에 사용)
    """
    df = get_corp_codes(api_key, force_refresh=force_refresh)

    if csv_path:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")     # BOM 포함 → 엑셀에서 한글 깨짐 방지
        print(f"[CSV]   {csv_path} ({len(df):,} rows) 저장 완료")

    if xlsx_path:
        df.to_excel(xlsx_path, index=False)                        # openpyxl 자동 사용
        print(f"[Excel] {xlsx_path} ({len(df):,} rows) 저장 완료")

    return df


if __name__ == "__main__":
    save_corp_codes(config.api_key,
                    csv_path="corp_codes_전체.csv",
                    xlsx_path="corp_codes_전체.xlsx",
                    force_refresh=True)   # 매번 최신 내려받기
