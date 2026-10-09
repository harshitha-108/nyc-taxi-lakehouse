@echo off
setlocal
cd /d "%~dp0.."
python -B -m scripts.open_powerbi_chat
if errorlevel 1 (
    echo.
    echo Press any key to close this window.
    pause >nul
)
endlocal
