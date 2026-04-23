@echo off
setlocal
cd /d "%~dp0"
if not exist logs mkdir logs
set "LOG_FILE=logs\run-%date:~0,4%%date:~5,2%%date:~8,2%.log"
node "%~dp0generate-report.mjs" >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%date% %time%] exit=%EXIT_CODE%>> "%LOG_FILE%"
exit /b %EXIT_CODE%
