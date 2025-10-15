import sqlite3
import re


import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from io import BytesIO

# from libs_stock.naver_util_sise import KOSPI_sise_market_sum_simple

BASE_URL='https://finance.naver.com/sise/sise_market_sum.nhn?sosok='

KOSPI_CODE = 0
KOSDAK_CODE = 1
START_PAGE = 1
fields = []



def _crawl(code, page):
    global fields
    data = {'menu': 'market_sum',
            'fieldIds':  fields,
            'returnUrl': BASE_URL + str(code) + "&page=" + str(page)}
    # requests.get 요청대신 post 요청
    res = requests.post('https://finance.naver.com/sise/field_submit.nhn', data=data)

    page_soup = BeautifulSoup(res.text, 'lxml')
    # 크롤링할 table html 가져오기
    table_html = page_soup.select_one('div.box_type_l')

    # Column명
    header_data  = [item.get_text().strip() for item in table_html.select('thead th')][1:-1]

    # 종목명 + 수치 추출 (a.title = 종목명, td.number = 기타 수치)
    inner_data = [item.get_text().strip() for item in table_html.find_all(lambda x:
                                                                           (x.name == 'a' and
                                                                            'tltle' in x.get('class', [])) or
                                                                           (x.name == 'td' and
                                                                            'number' in x.get('class', []))
                                                                           )]

    # page마다 있는 종목의 순번 가져오기
    no_data = [item.get_text().strip() for item in table_html.select('td.no')]
    number_data = np.array(inner_data)

    # 가로 x 세로 크기에 맞게 행렬화
    number_data.resize(len(no_data), len(header_data ))

    # 한 페이지에서 얻은 정보를 모아 DataFrame로 만들어 리턴
    df = pd.DataFrame(data=number_data, columns=header_data )
    return df

def crawl(code):
    # total_page을 가져오기 위한 requests
    res = requests.get(BASE_URL + str(code) + "&page=" + str(START_PAGE))
    page_soup = BeautifulSoup(res.text, 'lxml')

    # total_page 가져오기
    total_page_num = page_soup.select_one('td.pgRR > a')
    total_page_num = int(total_page_num.get('href').split('=')[-1])

    #가져올 수 있는 항목명들을 추출
    ipt_html = page_soup.select_one('div.subcnt_sise_item_top')
    global fields
    fields = [item.get('value') for item in ipt_html.select('input')]

    # page마다 정보를 긁어오게끔 하여 result에 저장
    result = [_crawl(code,str(page)) for page in range(1,total_page_num+1)]

    # page마다 가져온 정보를 df에 하나로 합침
    df = pd.concat(result, axis=0,ignore_index=True)

    # 엑셀로 내보내기
    # df.to_excel('NaverFinance_{}.xlsx'.format(code))
    # df.to_pickle('NaverFinance_{}.pkl'.format(code))
    return df




def download_korean_stock_list():
    # 다운로드 URL
    url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'

    # User-Agent 설정
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        # 데이터 다운로드
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # HTTP 오류 체크
        
        # 응답 내용 확인 (디버깅용)
        print(f"응답 상태 코드: {response.status_code}")
        print(f"응답 Content-Type: {response.headers.get('content-type')}")
        print(f"응답 크기: {len(response.content)} bytes")
        
        # 엔진을 명시적으로 지정하여 엑셀 파일 읽기
        # 먼저 openpyxl로 시도 (xlsx 형식)
        try:
            df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
            print("openpyxl 엔진으로 성공적으로 읽었습니다.")
        except Exception:
            # 실패하면 xlrd로 시도 (xls 형식)
            try:
                df = pd.read_excel(BytesIO(response.content), engine='xlrd')
                print("xlrd 엔진으로 성공적으로 읽었습니다.")
            except Exception as e:
                print(f"두 엔진 모두 실패: {e}")
                # 파일을 저장해서 직접 확인
                with open('상장법인목록.xls', 'wb') as f:
                    f.write(response.content)
                print("파일을 '상장법인목록.xls'로 저장했습니다. 직접 확인해보세요.")
                df = pd.read_html('상장법인목록.xls', encoding='euc-kr')[0]
                # exit()
        
        # 데이터 확인
        print("\n다운로드 완료! DataFrame 형태:")
        print(f"{df.shape[0]}행, {df.shape[1]}열")
        print("\n컬럼명:")
        print(df.columns.tolist())
        print("\n첫 3행 데이터:")
        print(df.head(3))

    except Exception as e:
        print(f"오류 발생: {e}")

    return df


# 데이터프레임이 df 변수에 이미 있다고 가정
# 데이터 정제 함수
def clean_stock_data(df):
    # 데이터 복사본 생성
    df_clean = df.copy()
    
    # '전일비' 컬럼 처리
    def parse_prev_day_change(value):
        if pd.isna(value) or value == '':
            return None, None, None
        
        # 문자열에서 방향과 금액 추출
        if '하락' in str(value):
            direction = '하락'
        elif '상승' in str(value):
            direction = '상승'
        else:
            direction = '보합'
        
        # 숫자 추출
        numbers = re.findall(r'\d+', str(value).replace(',', ''))
        if numbers:
            amount = int(numbers[0])
            # 등락률 계산 (현재가 대비)
            if '현재가' in df_clean.columns:
                current_price = df_clean.loc[df_clean['전일비'] == value, '현재가'].iloc[0]
                if isinstance(current_price, (int, float)) and current_price != 0:
                    change_rate = (amount / current_price) * 100
                    if direction == '하락':
                        change_rate = -change_rate
                else:
                    change_rate = None
            else:
                change_rate = None
        else:
            amount = None
            change_rate = None
            
        return direction, amount, change_rate
    
    # 전일비 컬럼 분리
    prev_day_info = df_clean['전일비'].apply(parse_prev_day_change)
    df_clean['전일비_방향'] = prev_day_info.apply(lambda x: x[0])
    df_clean['전일비_금액'] = prev_day_info.apply(lambda x: x[1])
    
    # 등락률 컬럼 정제 (퍼센트 제거)
    if '등락률' in df_clean.columns:
        df_clean['등락률'] = df_clean['등락률'].astype(str).str.replace('%', '').astype(float)
    
    # 숫자 컬럼 정제 (쉼표 제거)
    numeric_columns = ['현재가', '액면가', '거래량', '거래대금', '전일거래량', '시가', '고가', '저가', 
                      '매수호가', '매도호가', '매수총잔량', '매도총잔량', '상장주식수', '시가총액',
                      '매출액', '자산총계', '부채총계', '영업이익', '당기순이익', '주당순이익',
                      '보통주배당금', '매출액증가율', '영업이익증가율', '외국인비율', 'PER', 'ROE',
                      'ROA', 'PBR', '유보율']
    
    for col in numeric_columns:
        if col in df_clean.columns:
            # 문자열인 경우에만 처리
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].astype(str).str.replace(',', '').str.replace('N/A', '0')
                # 숫자로 변환 시도, 실패하면 NaN 유지
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # 불필요한 컬럼 제거
    if '전일비' in df_clean.columns:
        df_clean = df_clean.drop('전일비', axis=1)
    
    return df_clean

# SQLite 데이터베이스에 저장하는 함수
def save_to_sqlite(df, db_name, table_name):
    # 데이터베이스 연결
    conn = sqlite3.connect(db_name)
    
    # 데이터프레임을 SQLite 테이블로 저장
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    # 커밋 및 연결 종료
    conn.commit()
    conn.close()
    
    print(f"데이터가 {db_name}의 {table_name} 테이블에 성공적으로 저장되었습니다.")


# 데이터베이스 연결 및 테이블 정보 확인
def check_database():
    conn = sqlite3.connect('stock_supply_data.db')
    cursor = conn.cursor()
    
    # 테이블 목록 조회
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("데이터베이스 내 테이블 목록:")
    for table in tables:
        print(f"- {table[0]}")
    
    # stock_data 테이블 스키마 확인
    cursor.execute("PRAGMA table_info(stock_data);")
    columns = cursor.fetchall()
    print("\nstock_data 테이블 컬럼 정보:")
    for col in columns:
        print(f"- {col[1]} ({col[2]})")
    
    # 데이터 샘플 조회
    cursor.execute("SELECT 종목명, 현재가, 등락률, 거래량 FROM stock_data LIMIT 5;")
    sample_data = cursor.fetchall()
    print("\n상위 5개 종목:")
    for row in sample_data:
        print(f"- {row[0]}: {row[1]:,}원 ({row[2]}%), 거래량: {row[3]:,}")
    
    conn.close()


def analyze_stock_data():
    conn = sqlite3.connect('stock_supply_data.db')
    
    # 다양한 분석 쿼리 실행
    queries = {
        '상위_거래량': """
            SELECT 종목명, 거래량, 현재가, 등락률 
            FROM stock_data 
            ORDER BY 거래량 DESC 
            LIMIT 10
        """,
        '상위_시가총액': """
            SELECT 종목명, 시가총액, 현재가, PER 
            FROM stock_data 
            ORDER BY 시가총액 DESC 
            LIMIT 10
        """,
        '수익성_높은_종목': """
            SELECT 종목명, ROE, ROA, PER, 현재가 
            FROM stock_data 
            WHERE ROE IS NOT NULL 
            ORDER BY ROE DESC 
            LIMIT 10
        """
    }
    
    for query_name, query in queries.items():
        print(f"\n=== {query_name} ===")
        result_df = pd.read_sql_query(query, conn)
        print(result_df)
    
    conn.close()

# 메인 실행 부분
if __name__ == "__main__":
    df = download_korean_stock_list()
    save_to_sqlite(df, 'stock_supply_data.db', 'stock_Companies')

    df_kospi = crawl(KOSPI_CODE)
    df_kosdak = crawl(KOSDAK_CODE)
    df_kospi.insert(0, '시장구분', '코스피')
    df_kosdak.insert(0, '시장구분', '코스닥')

    df = pd.concat([df_kospi, df_kosdak], axis=0, ignore_index=True)
    df.to_excel('NaverFinance_stocks.xlsx')

    # 데이터 정제
    df_cleaned = clean_stock_data(df)
    
    # 데이터베이스 저장
    save_to_sqlite(df_cleaned, 'stock_supply_data.db', 'stock_data')
    
    # 결과 확인
    print("정제된 데이터 형태:", df_cleaned.shape)
    print("\n컬럼 목록:")
    print(df_cleaned.columns.tolist())
    print("\n첫 3행 데이터:")
    print(df_cleaned.head(3))