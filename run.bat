@echo off
setlocal
cd /d "%~dp0"

if not exist worker\.venv\Scripts\python.exe (
  echo Creating virtual environment...
  python -m venv worker\.venv || (echo Could not create venv: install Python 3.10+ & exit /b 1)
  worker\.venv\Scripts\python -m pip install -q -r worker\requirements.txt || exit /b 1
)

worker\.venv\Scripts\python worker\worker.py %*
if errorlevel 1 exit /b %errorlevel%

rem Publish: commit + push update the Vercel site.
rem For a test run without publishing: set NO_PUSH=1
if defined NO_PUSH goto :eof
git add public/results.json
git diff --cached --quiet && goto :eof
git commit -q -m "update results"
git push -q
