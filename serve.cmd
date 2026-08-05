@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0deploy\windows\start-services.ps1" %*
exit /b %ERRORLEVEL%
