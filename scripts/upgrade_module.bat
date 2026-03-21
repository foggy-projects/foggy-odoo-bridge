@echo off
REM Odoo Module Upgrade Script for Windows
REM Stops Odoo, upgrades a specified module, then restarts Odoo.
REM
REM Usage:
REM   upgrade_module.bat [module_name] [database_name] [container_name]
REM
REM Examples:
REM   upgrade_module.bat                    - Upgrade foggy_mcp in odoo database
REM   upgrade_module.bat foggy_mcp          - Upgrade foggy_mcp in odoo database
REM   upgrade_module.bat sale odoo odoo-17  - Upgrade sale module with custom container

setlocal enabledelayedexpansion

REM Default values
if "%~1"=="" (set "MODULE_NAME=foggy_mcp") else (set "MODULE_NAME=%~1")
if "%~2"=="" (set "DATABASE_NAME=odoo") else (set "DATABASE_NAME=%~2")
if "%~3"=="" (set "CONTAINER_NAME=foggy-odoo") else (set "CONTAINER_NAME=%~3")
set "WAIT_SECONDS=5"

echo ==========================================
echo Odoo Module Upgrade Script
echo ==========================================
echo Container:  %CONTAINER_NAME%
echo Database:   %DATABASE_NAME%
echo Module:     %MODULE_NAME%
echo ==========================================

REM Check if container exists
docker ps -a --format "{{.Names}}" 2>nul | findstr /x "%CONTAINER_NAME%" >nul
if errorlevel 1 (
    echo Error: Container '%CONTAINER_NAME%' not found
    echo Available containers:
    docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
    exit /b 1
)

REM Step 1: Stop container (avoids two Odoo processes competing)
echo.
echo [1/4] Stopping container '%CONTAINER_NAME%'...
docker stop %CONTAINER_NAME%
if errorlevel 1 (
    echo Error: Failed to stop container
    exit /b 1
)

REM Step 2: Start container and wait for readiness
echo.
echo [2/4] Starting container and waiting %WAIT_SECONDS%s...
docker start %CONTAINER_NAME%
if errorlevel 1 (
    echo Error: Failed to start container
    exit /b 1
)
timeout /t %WAIT_SECONDS% /nobreak >nul

REM Step 3: Upgrade module
echo.
echo [3/4] Upgrading module '%MODULE_NAME%' in database '%DATABASE_NAME%'...
docker exec %CONTAINER_NAME% bash -c "odoo -d %DATABASE_NAME% -u %MODULE_NAME% --stop-after-init"
if errorlevel 1 (
    echo Error: Module upgrade failed
    exit /b 1
)

REM Step 4: Restart to load the upgraded module in the main Odoo process
echo.
echo [4/4] Restarting Odoo to apply changes...
docker restart %CONTAINER_NAME%
if errorlevel 1 (
    echo Error: Failed to restart container
    exit /b 1
)

echo.
echo ==========================================
echo Module upgrade completed successfully!
echo Odoo is restarting -- wait a few seconds before accessing the web UI.
echo ==========================================

endlocal
