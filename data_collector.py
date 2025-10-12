# data_collector.py

import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import re
from typing import Dict, List, Optional
from config import DB_PATH, STOCK_LIST

class StockDataCollector:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.base_url = "https://m.stock.naver.com/api/stock/{}/trend"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://m.stock.naver.com/'
        }
        self.init_database()
    
    def init_database(self):
        """데이터베이스 초기화"""
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
                stock_name TEXT NOT NULL,
                date TEXT NOT NULL,
                close_price INTEGER,
                price_change INTEGER,
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
        ''')
        
        # 인덱스 생성
        cursor.executescript('''
            CREATE INDEX IF NOT EXISTS idx_supply_stock_date ON stock_supply_demand(stock_code, date);
            CREATE INDEX IF NOT EXISTS idx_supply_foreigner ON stock_supply_demand(foreigner_pure_buy);
            CREATE INDEX IF NOT EXISTS idx_supply_organ ON stock_supply_demand(organ_pure_buy);
        ''')
        
        conn.commit()
        conn.close()
        print("데이터베이스 초기화 완료")
    
    def safe_int_convert(self, value):
        """안전한 정수 변환 함수"""
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, bytes):
            try:
                return int(value.decode('utf-8'))
            except Exception:
                return 0
        if isinstance(value, str):
            cleaned = re.sub(r'[+,]', '', value)
            try:
                return int(cleaned)
            except Exception:
                return 0
        return 0
    
    def safe_float_convert(self, value):
        """안전한 실수 변환 함수"""
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, bytes):
            try:
                return float(value.decode('utf-8'))
            except Exception:
                return 0.0
        if isinstance(value, str):
            cleaned = re.sub(r'[%]', '', value)
            try:
                return float(cleaned)
            except Exception:
                return 0.0
        return 0.0
    
    def fetch_stock_data(self, stock_code: str, page_size: int = 60):
        """주식 데이터 수집"""
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
            print(f"데이터 수집 실패 - {stock_code}: {e}")
            return None
    
    def parse_stock_data(self, stock_code: str, stock_name: str, raw_data: List) -> List[Dict]:
        """API 응답 데이터 파싱"""
        if not raw_data:
            return []
        
        parsed_data = []
        
        for item in raw_data:
            try:
                # 날짜 변환
                date_str = item['bizdate']
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                
                # 기본 주가 데이터
                close_price = self.safe_int_convert(item.get('closePrice', 0))
                price_change = self.safe_int_convert(item.get('compareToPreviousClosePrice', 0))
                change_direction = item.get('compareToPreviousPrice', {}).get('text', '')
                
                # 등락률 계산
                prev_close = close_price - price_change
                change_rate = (price_change / prev_close * 100) if prev_close != 0 else 0
                
                # 수급 데이터
                foreigner_pure_buy = self.safe_int_convert(item.get('foreignerPureBuyQuant', 0))
                foreigner_hold_ratio = self.safe_float_convert(item.get('foreignerHoldRatio', 0))
                organ_pure_buy = self.safe_int_convert(item.get('organPureBuyQuant', 0))
                individual_pure_buy = self.safe_int_convert(item.get('individualPureBuyQuant', 0))
                accumulated_volume = self.safe_int_convert(item.get('accumulatedTradingVolume', 0))
                
                # 파생 지표 계산
                net_institutional_buy = foreigner_pure_buy + organ_pure_buy
                supply_demand_balance = net_institutional_buy + individual_pure_buy
                
                parsed_item = {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'date': formatted_date,
                    'close_price': close_price,
                    'price_change': price_change,
                    'change_direction': change_direction,
                    'change_rate': round(change_rate, 2),
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
                print(f"데이터 파싱 오류 - {stock_code} {item.get('bizdate', 'unknown')}: {e}")
                continue
        
        return parsed_data
    
    def save_stock_info(self, stock_list):
        """종목 기본 정보 저장"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for stock in stock_list:
            cursor.execute('''
                INSERT OR REPLACE INTO stocks 
                (stock_code, stock_name, market_type, industry, product)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                stock['code'], stock['name'], 
                stock['market'], stock.get('industry', ''), stock.get('product', '')
            ))
        
        conn.commit()
        conn.close()
        print(f"종목 정보 {len(stock_list)}개 저장 완료")
    
    def save_stock_data(self, stock_data_list: List[Dict]):
        """주식 데이터 저장"""
        if not stock_data_list:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved_count = 0
        for data in stock_data_list:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_supply_demand 
                    (stock_code, stock_name, date, close_price, price_change, change_direction, change_rate,
                     foreigner_pure_buy, foreigner_hold_ratio, organ_pure_buy,
                     individual_pure_buy, accumulated_volume,
                     net_institutional_buy, supply_demand_balance)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['stock_code'], data['stock_name'], data['date'],
                    data['close_price'], data['price_change'], data['change_direction'], data['change_rate'],
                    data['foreigner_pure_buy'], data['foreigner_hold_ratio'],
                    data['organ_pure_buy'], data['individual_pure_buy'],
                    data['accumulated_volume'], data['net_institutional_buy'],
                    data['supply_demand_balance']
                ))
                saved_count += 1
            except Exception as e:
                print(f"데이터 저장 오류: {e}")
        
        conn.commit()
        conn.close()
        print(f"주식 데이터 {saved_count}개 저장 완료")
    
    def collect_all_stocks(self, stock_list, page_size=60, delay=0.5):
        """모든 종목 데이터 수집"""
        # 종목 정보 저장
        self.save_stock_info(stock_list)
        
        all_data = []
        
        for stock in stock_list:
            print(f"데이터 수집 중: {stock['name']}({stock['code']})")
            
            raw_data = self.fetch_stock_data(stock['code'], page_size)
            if raw_data:
                parsed_data = self.parse_stock_data(stock['code'], stock['name'], raw_data)
                all_data.extend(parsed_data)
                print(f"  - {len(parsed_data)}개 데이터 파싱 완료")
            else:
                print(f"  - 데이터 수집 실패")
            
            time.sleep(delay)
        
        # 모든 데이터 저장
        self.save_stock_data(all_data)
        print("모든 종목 데이터 수집 완료")

def collect_data():
    """데이터 수집 실행 함수"""
    collector = StockDataCollector()
    collector.collect_all_stocks(STOCK_LIST, page_size=60, delay=0.5)

if __name__ == "__main__":
    collect_data()