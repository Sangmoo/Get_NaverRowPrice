import urllib.request
import urllib.parse
import json
import html
import oracledb
import os
import re
import time
import random
from datetime import datetime
import sys
import io
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 읽기
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
ORACLE_CLIENT_PATH = os.getenv("ORACLE_CLIENT_PATH")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),  # 문자열이므로 정수형 변환
    "sid": os.getenv("DB_SID"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# 🔹 Thick 모드 활성화
oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)

# UTF-8 인코딩 강제 설정
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 🔹 Oracle DB 연결 함수
def connect_to_oracle():
    print("🔌 Oracle DB 연결 중...")
    dsn = oracledb.makedsn(DB_CONFIG["host"], DB_CONFIG["port"], service_name=DB_CONFIG["sid"])
    connection = oracledb.connect(user=DB_CONFIG["user"], password=DB_CONFIG["password"], dsn=dsn)
    print("✅ Oracle DB 연결 성공!")
    return connection

# 🔹 상품 데이터 조회 함수
def get_product_data():
    connection = connect_to_oracle()
    cursor = connection.cursor()

    print("📡 상품 데이터 조회 중...")

    query = """
    SELECT  ONL.ONLINE_ID, 
            TO_CHAR(SYSDATE, 'YYYYMMDD') AS DT, 
            ONL.TIME AS TIME, 
            ONL.PRDT_CD AS PRDT_CD, 
            decode(ONL.PRICE_FLAG, '1', ONL.PRICE, 
                   ss10dev.F_GET_REALSAMT_0 ('*' , ONL.PRDT_CD , '*' , 
                   to_char(sysdate, 'yyyymmdd') , '0' , 'A01C01' )) AS PRICE
    FROM    T_REAL_SAMT TRS
            , T_REAL_SAMT_PRDT TRSP
            , T_SELECT_ONLINE_MNG_S TSO
            , (
                SELECT  OD.PRDT_CD, OD.BRD_CD, OD.PRICE, OH.ONLINE_ID, OT.TIME, OH.PRICE_FLAG
                FROM    T_SELECT_ONLINE_MNG OH
                        , T_SELECT_ONLINE_MNG_S OD
                        , T_SELECT_ONLINE_MNG_T OT
                WHERE   OH.ONLINE_ID = OD.ONLINE_ID
                AND     OH.ONLINE_ID = OT.ONLINE_ID
                AND     TO_CHAR(SYSDATE, 'YYYYMMDD') BETWEEN OH.STR_DT AND OH.END_DT
              ) ONL
    WHERE   TRS.REAL_SAMT_ID = TRSP.REAL_SAMT_ID
    AND     TRSP.PRDT_CD = TSO.PRDT_CD
    AND     TRSP.BRD_CD = TSO.BRD_CD
    AND     TRSP.PRDT_CD = ONL.PRDT_CD
    AND     TRSP.BRD_CD = ONL.BRD_CD
    AND     TRSP.PRDT_CD = 'SSKVTP12030'
    --AND     TSO.ONLINE_ID IN (1100, 1101, 1102, 1103, 1104, 1105)
    AND     TO_CHAR(SYSDATE, 'YYYYMMDD') BETWEEN TRS.START_DT AND TRS.END_DT
    AND     TRSP.DEL_DAY IS NULL
    GROUP BY ONL.ONLINE_ID, ONL.TIME, ONL.PRICE_FLAG, ONL.PRICE, ONL.PRDT_CD
    """
    
    cursor.execute(query)
    product_data = cursor.fetchall()

    print(f"✅ 상품 데이터 조회 완료! 총 {len(product_data)}건 검색됨.")

    cursor.close()
    connection.close()

    return product_data  

# 🔹 API 요청 속도 조절 (429 Too Many Requests 방지)
def throttle_requests():
    time.sleep(random.uniform(0.2, 0.5))

def search_naver_shopping(prdt_cd, max_price):
    print(f"🔍 '{prdt_cd}' 상품 검색 시작... (최대 가격: {max_price}원)")
    encText = urllib.parse.quote(prdt_cd)
    url = f"https://openapi.naver.com/v1/search/shop.json?query={encText}&display=40&sort=sim&fo=true" # Display = 40 / 화면노출 개수

    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", CLIENT_ID)
    request.add_header("X-Naver-Client-Secret", CLIENT_SECRET)
    # request.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)") # Web PC
    request.add_header("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Mobile Safari/537.36") # Mob


    retry_count = 0  
    while retry_count < 5:
        try:
            response = urllib.request.urlopen(request)
            rescode = response.getcode()

            if rescode == 200:
                response_body = response.read()
                shop_data = json.loads(response_body.decode('utf-8'))

                filtered_items = [
                    {
                        "title": re.sub(r"<.*?>", "", html.unescape(item["title"])),
                        "price": item["lprice"],
                        "mallName": html.unescape(item["mallName"]),
                        "link": html.unescape(item["link"])
                    }
                    for item in shop_data["items"]
                    # if int(item["lprice"]) <= max_price # 금액 이하
                    if int(item["lprice"]) < max_price # 금액 미만
                    # if int(item["lprice"]) < max_price and "search.shopping.naver.com/catalog" not in item["link"] # 금액 미만이면서, 네이버 금액 몰 집께 링크 제외
                ]

                print(f"✅ '{prdt_cd}' 검색 완료! 총 {len(filtered_items)}건 발견")
                return filtered_items  

            elif rescode == 429:
                wait_time = 2 ** retry_count  
                print(f"⚠️ [경고] Too Many Requests (429), {wait_time}초 후 재시도...")
                time.sleep(min(wait_time, 10))
                retry_count += 1

        except urllib.error.URLError as e:
            print(f"❌ [오류] '{prdt_cd}' 검색 요청 실패: {e.reason}")
            return []

    print(f"🚫 '{prdt_cd}' 검색 실패: 최대 재시도 횟수 초과")
    return []

# 🔹 데이터 INSERT 함수
def insert_into_db(data):
    if not data:
        print("⚠️ INSERT할 데이터가 없습니다.")
        return  

    connection = connect_to_oracle()
    cursor = connection.cursor()

    # select_online_seq.nextval - seq 변경 필요
    insert_query = """
    MERGE INTO SS10DEV.T_SELECT_ONLINE_MNG_R_TEMP T
    USING (
            SELECT :1 AS ONLINE_ID, :2 AS DT, :3 AS TIME, :4 AS PRDT_CD, 
                :5 AS PRICE, :6 AS DC_PRICE, :7 AS URL, :8 AS MALL_NM, 
                :9 AS TITLE, :10 AS RESULT_CLSBY, TO_CHAR(SYSDATE, 'YYYYMMDDHH24MI') AS INS_DAY
            FROM DUAL
    )   NEW
    ON (
            T.ONLINE_ID = NEW.ONLINE_ID
            AND T.DT = NEW.DT
            AND T.TIME = NEW.TIME
            AND T.PRDT_CD = NEW.PRDT_CD
            AND T.PRICE = NEW.PRICE
            AND T.DC_PRICE = NEW.DC_PRICE
            AND T.URL = NEW.URL
            AND T.MALL_NM = NEW.MALL_NM
            AND T.TITLE = NEW.TITLE
        )
    WHEN NOT MATCHED THEN
        INSERT (ONLINE_ID, DT, TIME, SEQ, PRDT_CD, PRICE, DC_PRICE, URL, MALL_NM, TITLE, RESULT_CLSBY, INS_DAY)
        VALUES (NEW.ONLINE_ID, NEW.DT, NEW.TIME, SS10DEV.SEQ_ONLINE_MNG_R_TEMP.NEXTVAL, 
                NEW.PRDT_CD, NEW.PRICE, NEW.DC_PRICE, NEW.URL, NEW.MALL_NM, NEW.TITLE, NEW.RESULT_CLSBY, NEW.INS_DAY)
    """

    print(f"📥 {len(data)}건 INSERT 중... (중복 데이터 제외)")
    cursor.executemany(insert_query, data)
    connection.commit()
    print(f"✅ {len(data)}건 INSERT 완료!")

    cursor.close()
    connection.close()

# 🔹 실행 메인 함수
def main():
    start_time = time.time()
    print(f"\n🕒 [스크립트 시작]: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    product_list = get_product_data()
    if not product_list:
        print("❌ DB에서 조회된 상품 데이터가 없습니다.")
        return

    total_items = 0  
    insert_data = []  
    retry_list = []  

    for online_id, dt, time_, prdt_cd, price in product_list:
        print(f"\n🔎 [상품 검색] PRDT_CD: {prdt_cd}, 예상 가격: {price}원")
        throttle_requests()  
        search_results = search_naver_shopping(prdt_cd, price)

        if search_results:
            for item in search_results:
                result_clsby = "M" if item['link'].startswith("m.") else "W"
                insert_data.append((online_id, dt, time_, prdt_cd, price
                                    , item['price'], item['link'], item['mallName'], item['title'], result_clsby))
                print(f"✅ [상품 추가] {item['title']} - {item['mallName']} ({item['price']}원)")
                total_items += 1
        else:
            print(f"🔄 [재요청 대기] '{prdt_cd}' 검색 결과 없음. 재시도 리스트에 추가.")
            retry_list.append((online_id, dt, time_, prdt_cd, price))

    insert_into_db(insert_data)

    if retry_list:
        print("\n🔁 [429 오류 상품 재시도 시작]\n")
        insert_data = []
        for data in retry_list:
            print(f"🔄 [재검색] PRDT_CD: {data[3]}")
            time.sleep(0.2)  
            search_results = search_naver_shopping(data[3], data[4])
            for item in search_results:
                insert_data.append((*data, item['price'], item['link'], item['mallName'], item['title'], "M" if item['link'].startswith("m.") else "W"))
                print(f"✅ [재요청 성공] {item['title']} - {item['mallName']} ({item['price']}원)")

        insert_into_db(insert_data)

    print(f"\n📊 [총 검색된 상품 개수]: {total_items}건")
    print(f"⏳ 총 실행 시간: {time.time() - start_time:.2f}초")

if __name__ == "__main__":
    main()
