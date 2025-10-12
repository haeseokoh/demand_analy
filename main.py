# main.py

from data_collector import collect_data
from analysis import analyze_data

def main():
    print("주식 수급 분석 시스템")
    print("1. 데이터 수집 실행")
    print("2. 데이터 분석 실행") 
    print("3. 전체 프로세스 실행")
    
    choice = input("선택 (1/2/3): ").strip()
    
    if choice == "1":
        print("\n데이터 수집을 시작합니다...")
        collect_data()
    elif choice == "2":
        print("\n데이터 분석을 시작합니다...")
        analyze_data()
    elif choice == "3":
        print("\n전체 프로세스를 시작합니다...")
        collect_data()
        analyze_data()
    else:
        print("잘못된 선택입니다.")

if __name__ == "__main__":
    main()