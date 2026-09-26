@echo off
chcp 65001 >nul
rem 一键启动看盘（双击运行）：后端未启动则自动后台启动，然后打开页面
powershell -NoProfile -Command "try{(Invoke-WebRequest http://127.0.0.1:8000/api/health -UseBasicParsing -TimeoutSec 2)|Out-Null;exit 0}catch{exit 1}"
if errorlevel 1 (
  echo 正在启动后端服务...
  start "fund-server" /min cmd /c "python -m uvicorn main:app --app-dir f:\fund\server --port 8000"
  timeout /t 3 /nobreak >nul
)
start "" "http://127.0.0.1:8000"
