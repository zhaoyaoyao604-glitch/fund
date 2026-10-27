@echo off
net session >nul 2>&1
if %errorlevel% neq 0 (
  echo Requesting administrator permission, please click YES...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)
set LOG=F:\fund\_probe\fix_docker_log.txt
echo ===== Docker fix run4 %date% %time% ===== > "%LOG%"
echo [1] enable VirtualMachinePlatform >> "%LOG%"
dism /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart >> "%LOG%" 2>&1
echo VMP_exit=%errorlevel% >> "%LOG%"
echo [2] enable Microsoft-Windows-Subsystem-Linux >> "%LOG%"
dism /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart >> "%LOG%" 2>&1
echo WSL_exit=%errorlevel% >> "%LOG%"
echo [3] run Docker Desktop installer >> "%LOG%"
"C:\Program Files\Docker\Docker\Docker Desktop Installer.exe" install --quiet --accept-license >> "%LOG%" 2>&1
echo INST_exit=%errorlevel% >> "%LOG%"
echo [4] feature states >> "%LOG%"
dism /online /get-featureinfo /featurename:VirtualMachinePlatform >> "%LOG%" 2>&1
dism /online /get-featureinfo /featurename:Microsoft-Windows-Subsystem-Linux >> "%LOG%" 2>&1
echo [5] service check >> "%LOG%"
sc query com.docker.service >> "%LOG%" 2>&1
echo ===== DONE ===== >> "%LOG%"
echo.
echo All done.
echo.
pause
