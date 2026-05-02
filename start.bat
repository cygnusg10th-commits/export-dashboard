@echo off
chcp 65001 > nul
echo ============================================
echo  수출 통계 대시보드 시작
echo ============================================
cd /d "%~dp0"

:: 패키지 확인
python -c "import streamlit" 2>nul || (
    echo 패키지 설치 중...
    pip install -r requirements.txt -q
)

:: DB 없으면 파싱 먼저
if not exist "export_data.db" (
    echo 최초 데이터 파싱 중...
    python parser.py
)

echo 브라우저에서 http://localhost:8501 열림
streamlit run app.py --server.port 8501 --server.headless false
pause
