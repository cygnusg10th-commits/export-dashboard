@echo off
chcp 65001 > nul
echo ============================================
echo  수출 통계 Excel 강제 전체 재파싱
echo ============================================
cd /d "%~dp0"
python parser.py --force
echo.
echo 완료. 대시보드를 새로고침하세요.
pause
