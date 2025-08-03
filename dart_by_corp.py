import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import logging
import json
import config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DartDisclosureCollector:
    """DART 회사별 전체 공시 수집기"""
    
    def __init__(self, api_key: str, corp_codes_file: str = 'corp_codes_전체.xlsx'):
        """
        초기화
        
        Args:
            api_key: DART OpenAPI 키
            corp_codes_file: 회사 코드 정보가 있는 엑셀 파일
        """
        self.api_key = api_key
        self.base_url = "https://opendart.fss.or.kr/api"
        self.session = requests.Session()
        
        # 회사 코드 정보 로드
        self.corp_codes_df = pd.read_excel(corp_codes_file)
        logger.info(f"총 {len(self.corp_codes_df)}개 회사 정보 로드 완료")
        
        # 저장 디렉토리 설정
        self.base_dir = Path("dart_disclosures")
        self.base_dir.mkdir(exist_ok=True)
        
    def get_corp_info(self, corp_name: str) -> Tuple[str, str, str]:
        """
        회사명으로 회사 정보 조회
        
        Args:
            corp_name: 회사명
            
        Returns:
            (corp_code, corp_name, stock_code) 튜플
        """
        # 정확한 매칭 우선
        corp_info = self.corp_codes_df[self.corp_codes_df['corp_name'] == corp_name]
        
        # 부분 매칭 시도
        if corp_info.empty:
            corp_info = self.corp_codes_df[self.corp_codes_df['corp_name'].str.contains(corp_name, na=False)]
        
        if corp_info.empty:
            raise ValueError(f"회사를 찾을 수 없습니다: {corp_name}")
        
        if len(corp_info) > 1:
            logger.warning(f"'{corp_name}'로 검색된 회사가 {len(corp_info)}개입니다. 첫 번째 결과를 사용합니다.")
            for idx, row in corp_info.iterrows():
                logger.info(f"  - {row['corp_name']} (종목코드: {row['stock_code']})")
        
        row = corp_info.iloc[0]
        return str(row['corp_code']).zfill(8), row['corp_name'], str(row.get('stock_code', ''))
    
    def get_disclosure_list(
        self, 
        corp_code: str,
        bgn_de: str,
        end_de: str,
        pblntf_ty: Optional[str] = None,
        pblntf_detail_ty: Optional[str] = None,
        page_no: int = 1,
        page_count: int = 100
    ) -> Dict:
        """공시 목록 조회"""
        
        url = f"{self.base_url}/list.json"
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bgn_de': bgn_de,
            'end_de': end_de,
            'page_no': page_no,
            'page_count': page_count
        }
        
        if pblntf_ty:
            params['pblntf_ty'] = pblntf_ty
        if pblntf_detail_ty:
            params['pblntf_detail_ty'] = pblntf_detail_ty
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"공시 목록 조회 실패: {e}")
            return {'status': '999', 'message': str(e)}
    
    
    def get_all_disclosures_by_type(
        self,
        corp_code: str,
        bgn_de: str,
        end_de: str,
        pblntf_ty: Optional[str] = None
    ) -> List[Dict]:
        """특정 유형의 모든 공시 조회 (페이징 처리)"""
        
        all_disclosures = []
        page_no = 1
        
        while True:
            result = self.get_disclosure_list(
                corp_code=corp_code,
                bgn_de=bgn_de,
                end_de=end_de,
                pblntf_ty=pblntf_ty,
                page_no=page_no,
                page_count=100
            )
            
            if result['status'] != '000':
                if result['status'] == '013':  # 조회된 데이터가 없습니다
                    logger.info(f"공시 유형 {pblntf_ty}: 데이터 없음")
                    break
                else:
                    logger.error(f"Error: {result.get('message', 'Unknown error')}")
                    break
            
            disclosures = result.get('list', [])
            if not disclosures:
                break
            
            all_disclosures.extend(disclosures)
            
            # 진행 상황 로깅
            total_count = int(result.get('total_count', 0))
            logger.info(f"공시 유형 {pblntf_ty}: {len(all_disclosures)}/{total_count} 건 수집")
            
            # 더 이상 페이지가 없으면 종료
            total_page = int(result.get('total_page', 0))
            if page_no >= total_page:
                break
                
            page_no += 1
            time.sleep(0.1)  # API 호출 제한 고려
        
        return all_disclosures
    
    def get_all_company_disclosures(
        self,
        corp_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        years: int = 5,
        all_time: bool = False
    ) -> pd.DataFrame:
        """
        회사의 모든 공시 조회
        
        Args:
            corp_name: 회사명
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            years: 조회할 연수 (start_date가 없을 경우)
            all_time: True일 경우 모든 기간의 공시 조회 (2000년 3월부터)
            
        Returns:
            공시 정보 DataFrame
        """
        # 회사 정보 조회
        corp_code, actual_corp_name, stock_code = self.get_corp_info(corp_name)
        logger.info(f"회사 정보: {actual_corp_name} (종목코드: {stock_code}, 고유번호: {corp_code})")
        
        # 날짜 설정
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            if all_time:
                # 2000년 3월부터 모든 공시 서류가 전자공시에 등록
                start_date = '20000301'
            else:
                start_date = (datetime.now() - timedelta(days=365 * years)).strftime('%Y%m%d')
        
        logger.info(f"조회 기간: {start_date} ~ {end_date}")
        
        # 공시 유형별 정의
        disclosure_types = {
            'A': '정기공시',
            'B': '주요사항보고',
            'C': '발행공시',
            'D': '지분공시',
            'E': '기타공시',
            'F': '외부감사관련',
            'G': '펀드공시',
            'H': '자산유동화',
            'I': '거래소공시',
            'J': '공정위공시'
        }
        
        all_disclosures = []
        
        # 각 공시 유형별로 조회
        for pblntf_ty, type_name in disclosure_types.items():
            logger.info(f"\n{type_name} 조회 중...")
            
            disclosures = self.get_all_disclosures_by_type(
                corp_code=corp_code,
                bgn_de=start_date,
                end_de=end_date,
                pblntf_ty=pblntf_ty
            )
            
            # 공시 유형 정보 추가
            for disc in disclosures:
                disc['pblntf_ty'] = pblntf_ty
                disc['pblntf_ty_nm'] = type_name
            
            all_disclosures.extend(disclosures)
            time.sleep(0.5)  # API 호출 제한 고려
        
        # DataFrame 변환
        if all_disclosures:
            df = pd.DataFrame(all_disclosures)
            df['rcept_dt'] = pd.to_datetime(df['rcept_dt'])
            df = df.sort_values('rcept_dt', ascending=False).reset_index(drop=True)
            
            # 컬럼 순서 조정
            cols = df.columns.tolist()
            priority_cols = ['rcept_no', 'rcept_dt', 'corp_name', 'report_nm', 'pblntf_ty', 'pblntf_ty_nm']
            other_cols = [col for col in cols if col not in priority_cols]
            df = df[priority_cols + other_cols]
            
            logger.info(f"\n총 {len(df)}건의 공시 수집 완료")
            
            # 결과 저장
            self._save_results(df, actual_corp_name, stock_code)
            
            return df
        else:
            logger.warning("조회된 공시가 없습니다.")
            return pd.DataFrame()
    
    def _save_results(self, df: pd.DataFrame, corp_name: str, stock_code: str):
        """결과 저장 (URL 포함)"""
        # 회사별 디렉토리 생성
        safe_corp_name = "".join(c for c in corp_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        corp_dir = self.base_dir / f"{safe_corp_name}_{stock_code}"
        corp_dir.mkdir(exist_ok=True)
        
        # 엑셀 파일로 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_file = corp_dir / f"공시목록_{timestamp}.xlsx"
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # 전체 공시
            df.to_excel(writer, sheet_name='전체공시', index=False)
            
            # 공시 유형별 시트
            for pblntf_ty, type_name in df.groupby(['pblntf_ty', 'pblntf_ty_nm']).size().index:
                type_df = df[df['pblntf_ty'] == pblntf_ty]
                sheet_name = f"{pblntf_ty}_{type_name}"[:31]  # 엑셀 시트명 길이 제한
                type_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        logger.info(f"엑셀 파일 저장: {excel_file}")
        
        # CSV 파일로도 저장
        csv_file = corp_dir / f"공시목록_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        logger.info(f"CSV 파일 저장: {csv_file}")
        
        # JSON 파일로도 저장 (프로그래밍 용도)
        json_file = corp_dir / f"공시목록_{timestamp}.json"
        df.to_json(json_file, orient='records', force_ascii=False, indent=2, date_format='iso')
        logger.info(f"JSON 파일 저장: {json_file}")
        
        # 통계 정보 저장
        stats_file = corp_dir / f"공시통계_{timestamp}.txt"
        with open(stats_file, 'w', encoding='utf-8') as f:
            f.write(f"회사명: {corp_name}\n")
            f.write(f"종목코드: {stock_code}\n")
            f.write(f"조회 기간: {df['rcept_dt'].min().strftime('%Y-%m-%d')} ~ {df['rcept_dt'].max().strftime('%Y-%m-%d')}\n")
            f.write(f"총 공시 건수: {len(df)}\n\n")
            
            f.write("공시 유형별 통계:\n")
            type_stats = df.groupby('pblntf_ty_nm').size().sort_values(ascending=False)
            for type_name, count in type_stats.items():
                f.write(f"  {type_name}: {count}건\n")
            
            f.write("\n최빈 공시 (Top 10):\n")
            top_reports = df['report_nm'].value_counts().head(10)
            for report_nm, count in top_reports.items():
                f.write(f"  {report_nm}: {count}건\n")
            
        
        logger.info(f"통계 파일 저장: {stats_file}")
    
    def get_disclosure_document(self, rcept_no: str) -> str:
        """공시 문서 조회 (HTML)"""
        url = f"{self.base_url}/document.xml"
        params = {
            'crtfc_key': self.api_key,
            'rcept_no': rcept_no
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"공시 문서 조회 실패 ({rcept_no}): {e}")
            return ""
    
    def download_disclosure_documents(self, df: pd.DataFrame, max_count: int = 10):
        """공시 문서 다운로드"""
        logger.info(f"\n최근 {max_count}건의 공시 문서 다운로드 시작...")
        
        for idx, row in df.head(max_count).iterrows():
            try:
                html_content = self.get_disclosure_document(row['rcept_no'])
                if html_content:
                    # 파일 저장
                    safe_report_nm = "".join(c for c in row['report_nm'] if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    filename = f"{row['rcept_no']}_{safe_report_nm}.html"
                    
                    corp_name = row['corp_name']
                    safe_corp_name = "".join(c for c in corp_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    
                    doc_dir = self.base_dir / f"{safe_corp_name}_{row.get('stock_code', '')}" / "documents"
                    doc_dir.mkdir(parents=True, exist_ok=True)
                    
                    filepath = doc_dir / filename
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    
                    logger.info(f"  [{idx+1}/{max_count}] {filename} 저장 완료")
                
                time.sleep(0.5)  # API 호출 제한
                
            except Exception as e:
                logger.error(f"  문서 다운로드 실패: {e}")


# 사용 예제
def main():
    # 수집기 생성
    collector = DartDisclosureCollector(config.API_KEY)
    
    # 회사명으로 공시 조회
    corp_name = "이수페타시스"  # 조회할 회사명
    
    # 모든 기간의 공시 조회 (2000년 3월부터)
    df = collector.get_all_company_disclosures(
        corp_name=corp_name,
        all_time=True  # 모든 기간
    )
    
    # 또는 최근 5년간만 조회하려면:
    # df = collector.get_all_company_disclosures(
    #     corp_name=corp_name,
    #     years=5
    # )
    
    if not df.empty:
        print(f"\n조회 결과:")
        print(f"총 공시 건수: {len(df)}")
        print(f"\n최근 10건:")
        print(df[['rcept_dt', 'report_nm', 'pblntf_ty_nm']].head(10))
        
        # 선택적: 최근 공시 문서 다운로드
        # collector.download_disclosure_documents(df, max_count=5)
    
    # 특정 기간 조회 예제
    # df = collector.get_all_company_disclosures(
    #     corp_name="LG전자",
    #     start_date="20230101",
    #     end_date="20231231"
    # )


if __name__ == "__main__":
    main()