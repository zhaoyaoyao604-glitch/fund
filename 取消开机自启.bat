@echo off
chcp 65001 >nul
rem 取消开机自启：删除「启动」文件夹里的快捷方式（不影响手动双击 启动看盘.bat）
set "LNK=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\基金看盘.lnk"
if exist "%LNK%" (
  del "%LNK%"
  echo.
  echo [OK] 已取消开机自启，下次开机不再自动打开。
) else (
  echo.
  echo [提示] 未找到开机自启快捷方式，无需取消。
)
echo.
pause
