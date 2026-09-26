@echo off
chcp 65001 >nul
rem 运行一次即可注册开机自启：在「启动」文件夹创建指向 开机自启.vbs 的快捷方式
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ws = New-Object -ComObject WScript.Shell; $lnk = $ws.CreateShortcut([Environment]::GetFolderPath('Startup') + '\基金看盘.lnk'); $lnk.TargetPath = 'wscript.exe'; $lnk.Arguments = 'f:\fund\开机自启.vbs'; $lnk.WorkingDirectory = 'f:\fund'; $lnk.WindowStyle = 7; $lnk.Save()"

set "LNK=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\基金看盘.lnk"
if exist "%LNK%" (
  echo.
  echo [OK] 已注册开机自启：下次开机登录后约 15 秒自动打开基金看盘页面。
  echo      后端没在运行会先静默拉起，全程不弹黑框。
  echo      快捷方式位置：%LNK%
  echo      如需取消，双击运行「取消开机自启.bat」即可。
) else (
  echo.
  echo [失败] 未能创建快捷方式，请检查后重试。
)
echo.
pause
