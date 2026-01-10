@echo off
echo === Pedro Organiza CLI Installer ===

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python not found.
  echo Please install Python 3.9+ from https://www.python.org
  pause
  exit /b 1
)

echo [OK] Python found

if not exist venv (
  python -m venv venv
)

call venv\Scripts\activate

python -m pip install --upgrade pip setuptools wheel
pip install .

echo.
echo === Installation complete ===
echo.
echo Activate with:
echo   venv\Scripts\activate
echo.
echo Try:
echo   pedro status
pause
exit /b 0