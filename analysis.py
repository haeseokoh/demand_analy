# analysis.py

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from config import DB_PATH

class StockAnalyzer:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
    
    def get_recent_data(self, stock_code=None, days=60):
        """최근 데이터 조회"""
        conn = sqlite3.connect(self.db_path)
        
        if stock_code:
            query = '''
            SELECT * FROM stock_supply_demand 
            WHERE stock_code = ? 
            ORDER BY date DESC 
            LIMIT ?
            '''
            df = pd.read_sql_query(query, conn, params=(stock_code, days))
        else:
            query = '''
            SELECT * FROM stock_supply_demand 
            ORDER BY date DESC 
            LIMIT ?
            '''
            df = pd.read_sql_query(query, conn, params=(days * 10,))  # 더 많은 데이터 가져오기
        
        conn.close()
        return df
    
    def calculate_supply_trend(self, stock_code, window=5):
        """수급 트렌드 분석"""
        data = self.get_recent_data(stock_code, window * 2)
        if data.empty:
            return None
        
        # 최근 데이터만 사용
        recent_data = data.head(window)
        
        analysis = {
            'stock_code': stock_code,
            'stock_name': recent_data.iloc[0]['stock_name'] if not recent_data.empty else '',
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            'window_days': window
        }
        
        # 외국인 트렌드
        foreigner_data = recent_data['foreigner_pure_buy']
        analysis['foreigner_total'] = foreigner_data.sum()
        analysis['foreigner_avg'] = foreigner_data.mean()
        analysis['foreigner_trend'] = self.assess_trend(foreigner_data)
        
        # 기관 트렌드
        organ_data = recent_data['organ_pure_buy']
        analysis['organ_total'] = organ_data.sum()
        analysis['organ_avg'] = organ_data.mean()
        analysis['organ_trend'] = self.assess_trend(organ_data)
        
        # 개인 트렌드
        individual_data = recent_data['individual_pure_buy']
        analysis['individual_total'] = individual_data.sum()
        analysis['individual_avg'] = individual_data.mean()
        analysis['individual_trend'] = self.assess_trend(individual_data)
        
        # 종합 점수 계산
        analysis['supply_score'] = self.calculate_supply_score(analysis)
        analysis['recommendation'] = self.get_recommendation(analysis['supply_score'])
        
        return analysis
    
    def assess_trend(self, data):
        """트렌드 평가"""
        if len(data) < 2:
            return 'neutral'
        
        positive_count = (data > 0).sum()
        negative_count = (data < 0).sum()
        total_count = len(data)
        
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
    
    def calculate_supply_score(self, analysis):
        """종합 수급 점수 계산"""
        score = 50
        
        # 외국인 가중치 (40%)
        foreigner_map = {'strong_buy': 100, 'buy': 75, 'neutral': 50, 'sell': 25, 'strong_sell': 0}
        score += (foreigner_map[analysis['foreigner_trend']] - 50) * 0.4
        
        # 기관 가중치 (40%)
        organ_map = {'strong_buy': 100, 'buy': 75, 'neutral': 50, 'sell': 25, 'strong_sell': 0}
        score += (organ_map[analysis['organ_trend']] - 50) * 0.4
        
        # 개인 가중치 (20%, 반대)
        individual_map = {'strong_buy': 0, 'buy': 25, 'neutral': 50, 'sell': 75, 'strong_sell': 100}
        score += (individual_map[analysis['individual_trend']] - 50) * 0.2
        
        return max(0, min(100, int(score)))
    
    def get_recommendation(self, score):
        """추천 등급"""
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
    
    def find_institutional_favorites(self, days=5, min_score=60):
        """기관/외국인 관심 종목 찾기"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
        SELECT stock_code, stock_name, 
               SUM(foreigner_pure_buy) as total_foreigner_buy,
               SUM(organ_pure_buy) as total_organ_buy,
               AVG(close_price) as avg_price
        FROM stock_supply_demand
        WHERE date >= date('now', '-' || ? || ' days')
        GROUP BY stock_code, stock_name
        HAVING total_foreigner_buy > 0 AND total_organ_buy > 0
        ORDER BY (total_foreigner_buy + total_organ_buy) DESC
        '''
        
        df = pd.read_sql_query(query, conn, params=(days,))
        conn.close()
        
        # 각 종목에 대해 수급 점수 계산
        results = []
        for _, row in df.iterrows():
            analysis = self.calculate_supply_trend(row['stock_code'])
            if analysis and analysis['supply_score'] >= min_score:
                results.append({
                    'stock_code': row['stock_code'],
                    'stock_name': row['stock_name'],
                    'foreigner_total_buy': row['total_foreigner_buy'],
                    'organ_total_buy': row['total_organ_buy'],
                    'total_institutional_buy': row['total_foreigner_buy'] + row['total_organ_buy'],
                    'supply_score': analysis['supply_score'],
                    'recommendation': analysis['recommendation']
                })
        
        return pd.DataFrame(results)

class ReportGenerator:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.analyzer = StockAnalyzer(db_path)
    
    def safe_format_number(self, value):
        """숫자 안전 포맷팅"""
        if value is None:
            return "0"
        try:
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            num = int(float(str(value)))
            return f"{num:,}"
        except (ValueError, TypeError):
            return "0"
    
    def generate_daily_report(self):
        """일일 리포트 생성"""
        print("=== 주식 수급 분석 일일 리포트 ===")
        print(f"생성일자: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 50)
        
        # 모든 종목 분석
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT stock_code, stock_name FROM stocks")
        stocks = cursor.fetchall()
        conn.close()
        
        all_analysis = []
        for stock_code, stock_name in stocks:
            analysis = self.analyzer.calculate_supply_trend(stock_code)
            if analysis:
                all_analysis.append(analysis)
        
        # 강력 매수 추천
        strong_buy = [a for a in all_analysis if a['recommendation'] == 'strong_buy']
        if strong_buy:
            print("\n[🔴 강력 매수 추천 종목]")
            for analysis in strong_buy:
                print(f"  {analysis['stock_name']}({analysis['stock_code']})")
                print(f"    수급점수: {analysis['supply_score']} | 외국인: {analysis['foreigner_trend']} | 기관: {analysis['organ_trend']}")
        
        # 매수 추천
        buy = [a for a in all_analysis if a['recommendation'] == 'buy']
        if buy:
            print("\n[🟢 매수 추천 종목]")
            for analysis in buy:
                print(f"  {analysis['stock_name']}({analysis['stock_code']}) - 수급점수: {analysis['supply_score']}")
        
        # 기관/외국인 관심 종목
        print("\n[🏢 기관/외국인 동반 매수 종목]")
        institutional_fav = self.analyzer.find_institutional_favorites()
        for _, row in institutional_fav.iterrows():
            foreigner_buy = self.safe_format_number(row['foreigner_total_buy'])
            organ_buy = self.safe_format_number(row['organ_total_buy'])
            total_buy = self.safe_format_number(row['total_institutional_buy'])
            
            print(f"  {row['stock_name']}({row['stock_code']})")
            print(f"    외국인: {foreigner_buy}주, 기관: {organ_buy}주")
            print(f"    총합: {total_buy}주, 수급점수: {row['supply_score']} - {row['recommendation']}")
        
        return all_analysis
    
    def export_to_excel(self, filename="stock_analysis_report.xlsx"):
        """엑셀 리포트 생성"""
        # 모든 종목 분석
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT stock_code, stock_name FROM stocks")
        stocks = cursor.fetchall()
        conn.close()
        
        all_analysis = []
        for stock_code, stock_name in stocks:
            analysis = self.analyzer.calculate_supply_trend(stock_code)
            if analysis:
                all_analysis.append(analysis)
        
        # 기관 관심종목
        institutional_fav = self.analyzer.find_institutional_favorites()
        
        # 엑셀 저장
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            if all_analysis:
                analysis_df = pd.DataFrame(all_analysis)
                analysis_df.to_excel(writer, sheet_name='종목별수급분석', index=False)
            
            if not institutional_fav.empty:
                institutional_fav.to_excel(writer, sheet_name='기관관심종목', index=False)
        
        print(f"\n엑셀 리포트 저장 완료: {filename}")

def analyze_data():
    """분석 실행 함수"""
    reporter = ReportGenerator()
    reporter.generate_daily_report()
    reporter.export_to_excel()

if __name__ == "__main__":
    analyze_data()