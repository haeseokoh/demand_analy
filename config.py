# config.py

# 데이터베이스 경로
from collect_stockinfo import get_stock_codes


DB_PATH = "stock_supply_data.db"

# 관심 종목 리스트 (수정된 형식)
STOCK_LIST = get_stock_codes(limit=700)