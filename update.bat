@echo off
chcp 65001 > nul
echo ============================================
echo  수출 통계 Excel 파싱 (변경분만)
echo ============================================
cd /d "%~dp0"
python parser.py
echo.
echo 완료. 대시보드를 새로고침하세요.
pause
