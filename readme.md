# 네이버 상품별 최저가 데이터 집계
python Naver_Price.py

# pip install
pip install -r requirements.txt

# 실행 파일 생성
pyinstaller Naver_Price.py --onefile    

# Flask 설치
pip install Flask

# Flask를 이용한 배치 서비스 구현
python naver_price_backend.py
127.0.0.1:5000/run_batch 에 접속하면 배치 서비스 실행
브라우저 화면에 Naver_Price.py 로그를 스트림하게 표시