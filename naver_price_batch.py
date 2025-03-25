from flask import Flask, Response, stream_with_context
import subprocess

app = Flask(__name__)


# 🔹 실시간 로그 스트리밍 함수
def generate_logs():
    process = subprocess.Popen(
        ["python", "Naver_InsertPrice.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",  # 🔹 UTF-8 인코딩 적용
    )

    # 🔹 실행되는 동안 실시간 로그 반환
    for line in process.stdout:
        yield line.strip() + "\n"

    # 🔹 오류 발생 시 stderr 출력
    for err in process.stderr:
        yield "[ERROR] " + err.strip() + "\n"


# 🔹 API 호출 시 실시간 로그 반환
# 🔹 127.0.0.1:5000/run_batch 에 접속하면 배치 서비스 실행
@app.route("/run_batch", methods=["GET"])
def run_batch():
    return Response(
        stream_with_context(generate_logs()), content_type="text/plain; charset=utf-8"
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)  # Flask 서버 실행 / 127.0.0.1:5000 접속
