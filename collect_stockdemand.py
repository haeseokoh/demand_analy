import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import re
from typing import Dict, List, Optional

class StockSupplyDemandCollector:
    def __init__(self, db_path="stock_supply_data.db"):
        self.db_path = db_path
        self.base_url = "https://m.stock.naver.com/api/stock/{}/trend"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://m.stock.naver.com/'
        }
        self.init_database()
    
    def init_database(self):
        """수급 데이터베이스 초기화"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS stocks (
                stock_code TEXT PRIMARY KEY,
                stock_name TEXT NOT NULL,
                market_type TEXT NOT NULL,
                industry TEXT,
                product TEXT,
                listed_date TEXT,
                is_active INTEGER DEFAULT 1
            );
            
            CREATE TABLE IF NOT EXISTS stock_supply_demand (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                date TEXT NOT NULL,
                close_price REAL,
                price_change REAL,
                change_direction TEXT,
                change_rate REAL,
                foreigner_pure_buy INTEGER,
                foreigner_hold_ratio REAL,
                organ_pure_buy INTEGER,
                individual_pure_buy INTEGER,
                accumulated_volume INTEGER,
                net_institutional_buy INTEGER,
                supply_demand_balance INTEGER,
                FOREIGN KEY (stock_code) REFERENCES stocks(stock_code),
                UNIQUE(stock_code, date)
            );
            
            CREATE TABLE IF NOT EXISTS supply_trend_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                period_type TEXT NOT NULL,
                foreigner_buy_trend TEXT,
                foreigner_continuous_days INTEGER,
                foreigner_total_buy INTEGER,
                organ_buy_trend TEXT,
                organ_continuous_days INTEGER,
                organ_total_buy INTEGER,
                individual_buy_trend TEXT,
                individual_continuous_days INTEGER,
                individual_total_buy INTEGER,
                supply_score INTEGER,
                recommendation TEXT,
                FOREIGN KEY (stock_code) REFERENCES stocks(stock_code),
                UNIQUE(stock_code, analysis_date, period_type)
            );
        ''')
        
        # 인덱스 생성
        cursor.executescript('''
            CREATE INDEX IF NOT EXISTS idx_supply_stock_date ON stock_supply_demand(stock_code, date);
            CREATE INDEX IF NOT EXISTS idx_supply_foreigner ON stock_supply_demand(foreigner_pure_buy);
            CREATE INDEX IF NOT EXISTS idx_supply_organ ON stock_supply_demand(organ_pure_buy);
            CREATE INDEX IF NOT EXISTS idx_trend_stock_period ON supply_trend_analysis(stock_code, analysis_date, period_type);
        ''')
        
        conn.commit()
        conn.close()
        print("수급 데이터베이스 초기화 완료")
    
    def parse_numeric_string(self, value_str: str) -> int:
        """문자열 숫자를 정수로 변환 (콤마, +, - 제거)"""
        if not value_str or value_str == 'N/A' or value_str == '':
            return 0
        
        # 콤마, +, % 기호 제거
        cleaned = re.sub(r'[+,%]', '', str(value_str)).strip()
        if cleaned == '':
            return 0
        
        try:
            # 음수 처리
            if cleaned.startswith('-'):
                return -int(cleaned[1:].replace(',', ''))
            return int(cleaned.replace(',', ''))
        except ValueError:
            print(f"숫자 변환 오류: {value_str} -> {cleaned}")
            return 0
    
    def parse_ratio_string(self, ratio_str: str) -> float:
        """비율 문자열을 실수로 변환"""
        if not ratio_str or ratio_str == 'N/A' or ratio_str == '':
            return 0.0
        
        cleaned = re.sub(r'[%]', '', str(ratio_str)).strip()
        if cleaned == '':
            return 0.0
        
        try:
            return float(cleaned)
        except ValueError:
            print(f"비율 변환 오류: {ratio_str}")
            return 0.0
    
    def fetch_supply_data(self, stock_code: str, page_size: int = 60):
        """수급 데이터 수집"""
        url = self.base_url.format(stock_code)
        params = {
            'pageSize': page_size,
            # 'bizdate': datetime.now().strftime('%Y%m%d')
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"수급 데이터 수집 실패 - {stock_code}: {e}")
            return None
    
    def parse_supply_data(self, stock_code: str, raw_data: List) -> List[Dict]:
        """API 응답 데이터 파싱"""
        if not raw_data:
            return []
        
        parsed_data = []
        
        for item in raw_data:
            try:
                # 날짜 변환 (YYYYMMDD -> YYYY-MM-DD)
                date_str = item['bizdate']
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                
                # 기본 주가 데이터
                close_price = self.parse_numeric_string(item.get('closePrice', 0))
                price_change = self.parse_numeric_string(item.get('compareToPreviousClosePrice', 0))
                change_direction = item.get('compareToPreviousPrice', {}).get('text', '')
                
                # 등락률 계산
                prev_close = close_price - price_change
                change_rate = (price_change / prev_close * 100) if prev_close != 0 else 0
                
                # 수급 데이터
                foreigner_pure_buy = self.parse_numeric_string(item.get('foreignerPureBuyQuant', 0))
                foreigner_hold_ratio = self.parse_ratio_string(item.get('foreignerHoldRatio', 0))
                organ_pure_buy = self.parse_numeric_string(item.get('organPureBuyQuant', 0))
                individual_pure_buy = self.parse_numeric_string(item.get('individualPureBuyQuant', 0))
                accumulated_volume = self.parse_numeric_string(item.get('accumulatedTradingVolume', 0))
                
                # 파생 지표 계산
                net_institutional_buy = foreigner_pure_buy + organ_pure_buy
                supply_demand_balance = net_institutional_buy + individual_pure_buy  # 개인은 반대 sign
                
                parsed_item = {
                    'stock_code': stock_code,
                    'date': formatted_date,
                    'close_price': close_price,
                    'price_change': price_change,
                    'change_direction': change_direction,
                    'change_rate': change_rate,
                    'foreigner_pure_buy': foreigner_pure_buy,
                    'foreigner_hold_ratio': foreigner_hold_ratio,
                    'organ_pure_buy': organ_pure_buy,
                    'individual_pure_buy': individual_pure_buy,
                    'accumulated_volume': accumulated_volume,
                    'net_institutional_buy': net_institutional_buy,
                    'supply_demand_balance': supply_demand_balance
                }
                parsed_data.append(parsed_item)
                
            except Exception as e:
                print(f"데이터 파싱 오류 - {stock_code}: {e}")
                continue
        
        return parsed_data
    
    def save_supply_data(self, supply_data_list: List[Dict]):
        """수급 데이터 저장"""
        if not supply_data_list:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved_count = 0
        for data in supply_data_list:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_supply_demand 
                    (stock_code, date, close_price, price_change, change_direction, change_rate,
                     foreigner_pure_buy, foreigner_hold_ratio, organ_pure_buy,
                     individual_pure_buy, accumulated_volume,
                     net_institutional_buy, supply_demand_balance)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['stock_code'], data['date'],
                    data['close_price'], data['price_change'], data['change_direction'], data['change_rate'],
                    data['foreigner_pure_buy'], data['foreigner_hold_ratio'],
                    data['organ_pure_buy'], data['individual_pure_buy'],
                    data['accumulated_volume'], data['net_institutional_buy'],
                    data['supply_demand_balance']
                ))
                saved_count += 1
            except Exception as e:
                print(f"수급 데이터 저장 오류: {e}")
        
        conn.commit()
        conn.close()
        print(f"수급 데이터 {saved_count}개 저장 완료")
    
    def analyze_supply_trend(self, stock_code: str, days: int = 20):
        """수급 트렌드 분석"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
        SELECT date, foreigner_pure_buy, organ_pure_buy, individual_pure_buy
        FROM stock_supply_demand
        WHERE stock_code = ?
        ORDER BY date DESC
        LIMIT ?
        '''
        
        df = pd.read_sql_query(query, conn, params=(stock_code, days))
        conn.close()
        
        if df.empty:
            return None
        
        # 트렌드 분석
        analysis = {
            'stock_code': stock_code,
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            'period_type': 'daily',
            'foreigner_total_buy': df['foreigner_pure_buy'].sum(),
            'organ_total_buy': df['organ_pure_buy'].sum(),
            'individual_total_buy': df['individual_pure_buy'].sum()
        }
        
        # 외국인 트렌드 분석
        foreigner_data = df['foreigner_pure_buy']
        analysis['foreigner_continuous_days'] = self.calculate_continuous_days(foreigner_data)
        analysis['foreigner_buy_trend'] = self.assess_trend(foreigner_data)
        
        # 기관 트렌드 분석
        organ_data = df['organ_pure_buy']
        analysis['organ_continuous_days'] = self.calculate_continuous_days(organ_data)
        analysis['organ_buy_trend'] = self.assess_trend(organ_data)
        
        # 개인 트렌드 분석
        individual_data = df['individual_pure_buy']
        analysis['individual_continuous_days'] = self.calculate_continuous_days(individual_data)
        analysis['individual_buy_trend'] = self.assess_trend(individual_data)
        
        # 종합 수급 점수 계산
        analysis['supply_score'] = self.calculate_supply_score(analysis)
        analysis['recommendation'] = self.get_recommendation(analysis['supply_score'])
        
        return analysis
    
    def calculate_continuous_days(self, data: pd.Series) -> int:
        """연속 매수/매도 일수 계산"""
        if len(data) < 1:
            return 0
        
        current_sign = 1 if data.iloc[0] > 0 else -1 if data.iloc[0] < 0 else 0
        if current_sign == 0:
            return 0
        
        continuous_days = 1
        
        for i in range(1, len(data)):
            sign = 1 if data.iloc[i] > 0 else -1 if data.iloc[i] < 0 else 0
            if sign == current_sign:
                continuous_days += 1
            else:
                break
        
        return continuous_days * current_sign  # 양수: 매수, 음수: 매도
    
    def assess_trend(self, data: pd.Series) -> str:
        """트렌드 평가"""
        if len(data) < 3:
            return 'neutral'
        
        recent_data = data.head(5)
        positive_count = (recent_data > 0).sum()
        negative_count = (recent_data < 0).sum()
        total_count = len(recent_data)
        
        if positive_count == total_count:
            return 'strong_buy'
        elif positive_count >= total_count * 0.7:
            return 'buy'
        elif negative_count == total_count:
            return 'strong_sell'
        elif negative_count >= total_count * 0.7:
            return 'sell'
        else:
            return 'neutral'
    
    def calculate_supply_score(self, analysis: Dict) -> int:
        """종합 수급 점수 계산 (0-100)"""
        score = 50  # 기본 점수
        
        # 외국인 가중치 (40%)
        foreigner_weight = 40
        foreigner_map = {'strong_buy': 100, 'buy': 75, 'neutral': 50, 'sell': 25, 'strong_sell': 0}
        score += (foreigner_map[analysis['foreigner_buy_trend']] - 50) * foreigner_weight / 100
        
        # 기관 가중치 (40%)
        organ_weight = 40
        organ_map = {'strong_buy': 100, 'buy': 75, 'neutral': 50, 'sell': 25, 'strong_sell': 0}
        score += (organ_map[analysis['organ_buy_trend']] - 50) * organ_weight / 100
        
        # 개인 가중치 (20%, 반대 지표)
        individual_weight = 20
        individual_map = {'strong_buy': 0, 'buy': 25, 'neutral': 50, 'sell': 75, 'strong_sell': 100}
        score += (individual_map[analysis['individual_buy_trend']] - 50) * individual_weight / 100
        
        return max(0, min(100, int(score)))
    
    def get_recommendation(self, score: int) -> str:
        """수급 점수 기반 매수/매도 추천"""
        if score >= 80:
            return 'strong_buy'
        elif score >= 60:
            return 'buy'
        elif score >= 40:
            return 'hold'
        elif score >= 20:
            return 'sell'
        else:
            return 'strong_sell'
    
    def save_trend_analysis(self, analysis: Dict):
        """트렌드 분석 결과 저장"""
        if not analysis:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO supply_trend_analysis
            (stock_code, analysis_date, period_type, 
             foreigner_buy_trend, foreigner_continuous_days, foreigner_total_buy,
             organ_buy_trend, organ_continuous_days, organ_total_buy,
             individual_buy_trend, individual_continuous_days, individual_total_buy,
             supply_score, recommendation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            analysis['stock_code'], analysis['analysis_date'], analysis['period_type'],
            analysis['foreigner_buy_trend'], analysis['foreigner_continuous_days'], analysis['foreigner_total_buy'],
            analysis['organ_buy_trend'], analysis['organ_continuous_days'], analysis['organ_total_buy'],
            analysis['individual_buy_trend'], analysis['individual_continuous_days'], analysis['individual_total_buy'],
            analysis['supply_score'], analysis['recommendation']
        ))
        
        conn.commit()
        conn.close()

# 종목 관리 및 데이터 수집 클래스
class StockManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.collector = StockSupplyDemandCollector(db_path)
    
    def add_stock(self, stock_info):
        """종목 추가"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO stocks 
            (stock_code, stock_name, market_type, industry)
            VALUES (?, ?, ?, ?)
        ''', (
            stock_info['code'], stock_info['name'], 
            stock_info['market'], stock_info.get('industry', '')
        ))
        
        conn.commit()
        conn.close()
        print(f"종목 추가: {stock_info['name']}({stock_info['code']})")
    
    def collect_stock_data(self, stock_code, page_size=60):
        """특정 종목 데이터 수집"""
        print(f"수급 데이터 수집 중: {stock_code}")
        
        raw_data = self.collector.fetch_supply_data(stock_code, page_size)
        if raw_data:
            supply_data = self.collector.parse_supply_data(stock_code, raw_data)
            self.collector.save_supply_data(supply_data)
            
            # 수급 트렌드 분석
            trend_analysis = self.collector.analyze_supply_trend(stock_code)
            if trend_analysis:
                self.collector.save_trend_analysis(trend_analysis)
                print(f"  - 수급 점수: {trend_analysis['supply_score']}, 추천: {trend_analysis['recommendation']}")
                return trend_analysis
        return None
    
    def collect_all_stocks(self, stock_list, page_size=60, delay=0.5):
        """모든 종목 데이터 수집"""
        results = []
        
        for stock in stock_list:
            # 종목 정보 저장
            self.add_stock(stock)
            
            # 데이터 수집
            result = self.collect_stock_data(stock['code'], page_size)
            if result:
                results.append(result)
            
            # 서버 부하 방지를 위한 딜레이
            time.sleep(delay)
        
        return results

def get_stock_codes(limit=700):
    conn = sqlite3.connect('stock_supply_data.db')
    
    # 시가총액 상위 700개 종목명 조회
    query1 = "SELECT 종목명 FROM stock_data ORDER BY 시가총액 DESC LIMIT {}".format(limit)
    df_stocks = pd.read_sql_query(query1, conn)
    
    # 회사 정보 조회
    query2 = "SELECT 회사명, 시장구분, 종목코드, 업종, 주요제품 FROM stock_Companies"
    df_companies = pd.read_sql_query(query2, conn)
    
    # 종목명 매칭
    stock_codes = []
    stock_list = []
    
    for stock_name in df_stocks['종목명']:
        match = df_companies[df_companies['회사명'] == stock_name]
        if not match.empty:
            stock_codes.append(match.iloc[0]['종목코드'])
            stock_list.append({'code': match.iloc[0]['종목코드'], 
             'name': match.iloc[0]['회사명'], 
             'market': match.iloc[0]['시장구분'], 
             'industry': match.iloc[0]['업종'], 
             'product': match.iloc[0]['주요제품']})
            
    conn.close()
    
    print(f"총 {len(df_stocks)}개 중 {len(stock_codes)}개 매칭됨")
    # print("매칭된 종목:", stock_list)
    # print("매칭된 종목코드:", stock_codes)
    
    # return stock_codes
    return stock_list

# 사용 예제
def main():
    # 종목 매니저 생성
    manager = StockManager("stock_supply.db")
    
    # 관심 종목 리스트
    stock_list = get_stock_codes(limit=5)
    
    # 데이터 수집 실행
    print("주식 수급 데이터 수집 시작...")
    results = manager.collect_all_stocks(stock_list, page_size=60, delay=0.5)
    
    # 결과 요약
    print("\n=== 수집 결과 요약 ===")
    for result in results:
        print(f"{result['stock_code']}: 수급점수 {result['supply_score']} - {result['recommendation']}")

if __name__ == "__main__":
    main()