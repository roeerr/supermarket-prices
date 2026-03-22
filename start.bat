@echo off
setlocal
chcp 65001 >nul
title Supermarket Price Checker

echo.
echo  ============================================
echo   Supermarket Price Checker - Starting...
echo  ============================================
echo.

:: Check Node.js
where node >nul 2>&1
if %errorlevel% neq 0 (
    echo  [ERROR] Node.js is not installed.
    echo.
    echo  Please install Node.js:
    echo    1. Go to: https://nodejs.org
    echo    2. Click the green LTS button to download
    echo    3. Run the installer ^(click Next, Next, Install^)
    echo    4. RESTART your computer
    echo    5. Run this file again
    echo.
    pause
    exit /b 1
)

for /f "tokens=*" %%v in ('node --version 2^>nul') do set NODE_VER=%%v
echo  [OK] Node.js found: %NODE_VER%

:: Check minimum version (16+)
for /f "tokens=1 delims=." %%m in ('node -e "process.stdout.write(process.version.slice(1))" 2^>nul') do set NODE_MAJOR=%%m
if %NODE_MAJOR% LSS 16 (
    echo.
    echo  [ERROR] Node.js version 16 or higher required. Found: %NODE_VER%
    echo          Download a newer version from: https://nodejs.org
    echo.
    pause
    exit /b 1
)

:: Install npm packages if needed (first run only)
if not exist "%~dp0node_modules\express\package.json" (
    echo  [*] Installing packages - this only happens once...
    echo.
    cd /d "%~dp0"
    call npm install --ignore-scripts
    if %errorlevel% neq 0 (
        echo.
        echo  [ERROR] Failed to install packages.
        echo          Check your internet connection and try again.
        pause
        exit /b 1
    )
    echo.
    echo  [OK] Packages installed successfully.
)

:: Start server
echo  [OK] Starting server...
echo.
echo  ============================================
echo   Open browser at: http://localhost:3000
echo   To stop: press Ctrl+C in this window
echo  ============================================
echo.

cd /d "%~dp0"

:: Open browser after 2 seconds
start "" /b cmd /c "timeout /t 2 >nul && start http://localhost:3000"

node server.js

echo.
echo  Server stopped.
pause
