@echo off
REM build.bat
REM =========
REM Run this from your project root to build TallySyncManager.exe
REM Double-click it or run it in your terminal.

echo.
echo ============================================
echo   TallySyncManager - EXE Builder
echo ============================================
echo.

REM Step 1: Install PyInstaller into the project
echo [1/3] Installing PyInstaller...
uv add pyinstaller --dev
if %errorlevel% neq 0 (
    echo ERROR: Failed to install PyInstaller
    pause
    exit /b 1
)

REM Step 2: Clean previous build artifacts
echo.
echo [2/3] Cleaning old build files...
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist

REM Step 3: Build the EXE
echo.
echo [3/3] Building EXE - this takes 2-5 minutes...
uv run pyinstaller tally_app.spec

if %errorlevel% neq 0 (
    echo.
    echo ERROR: Build failed. Check the output above for errors.
    pause
    exit /b 1
)

echo.
echo ============================================
echo   BUILD SUCCESSFUL!
echo   EXE location: dist\TallySyncManager\TallySyncManager.exe
echo ============================================
echo.
pause
