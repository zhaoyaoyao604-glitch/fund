@echo off
chcp 65001 >nul
rem 双击测试：立刻发一条「全额&趋势安全·60分」买卖点推送（桌面 + 微信通道），用来验证 server\push.json 配好没有
powershell -NoProfile -Command "try{(Invoke-WebRequest http://127.0.0.1:8000/api/health -UseBasicParsing -TimeoutSec 2)|Out-Null;exit 0}catch{exit 1}"
if errorlevel 1 (
  echo 后端没在运行，请先双击「启动看盘.bat」
  pause
  exit /b 1
)
echo 正在发测试推送...
curl.exe -s -m 60 "http://127.0.0.1:8000/api/push/test"
echo.
echo 通道为空时只会弹桌面通知；微信要收到需先填 server\push.json 的 channel 与对应密钥。
pause
