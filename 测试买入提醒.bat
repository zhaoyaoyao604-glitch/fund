@echo off
chcp 65001 >nul
rem 双击测试：立刻扫一遍自选票并弹一条桌面通知（不等到收盘前 10 分钟）
powershell -NoProfile -Command "try{(Invoke-WebRequest http://127.0.0.1:8000/api/health -UseBasicParsing -TimeoutSec 2)|Out-Null;exit 0}catch{exit 1}"
if errorlevel 1 (
  echo 后端没在运行，请先双击「启动看盘.bat」
  pause
  exit /b 1
)
echo 正在扫描自选票，请稍候...
curl.exe -s -m 120 "http://127.0.0.1:8000/api/notify/scan"
echo.
echo 已发桌面通知，看屏幕右下角。
pause
