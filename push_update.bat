@echo off
chcp 65001 > nul
echo ============================================
echo  수출 통계 데이터 업데이트 + GitHub 푸시
echo ============================================
cd /d "%~dp0"

echo [1/3] Excel 파싱 중...
python parser.py
if errorlevel 1 (
    echo 파싱 실패!
    pause
    exit /b 1
)

echo [2/3] Git 커밋 중...
git add export_data.db
git commit -m "data: update %date%"
if errorlevel 1 (
    echo 변경사항 없음 - 이미 최신 상태입니다.
    pause
    exit /b 0
)

echo [3/3] GitHub 푸시 중...
git push
if errorlevel 1 (
    echo 푸시 실패! GitHub 연결을 확인하세요.
    pause
    exit /b 1
)

echo.
echo 완료! Streamlit Cloud가 1-2분 내 자동 갱신됩니다.
echo 배포 주소를 브라우저에서 새로고침하세요.
pause
