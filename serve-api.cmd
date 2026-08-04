@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0deploy\windows\run-api.ps1" %*
exit /b %ERRORLEVEL%
