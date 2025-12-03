@echo off
REM =================================================================
REM  start_environment.bat - tdSynchManager Environment Launcher
REM  - Starts InfluxDB 3 (separate window)
REM  - Starts ThetaData Terminal (if installed, separate window)
REM  - Starts Jupyter Lab in project directory with conda env
REM =================================================================

REM ======================== CONFIGURATION ==========================
set "PROJECT_ROOT=%~dp0"
set "INFLUX_DIR=C:\Program Files\influxdb3-enterprise"
set "THETA_TERMINAL=C:\Program Files\ThetaData\ThetaTerminal.exe"
set "CONDA_ENV=tdsync"

REM Remove trailing backslash from PROJECT_ROOT
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"

echo.
echo ╔════════════════════════════════════════════════════════════════╗
echo ║        tdSynchManager Environment Launcher                     ║
echo ╚════════════════════════════════════════════════════════════════╝
echo.
echo Project Directory: %PROJECT_ROOT%
echo Conda Environment: %CONDA_ENV%
echo.

REM ======================== CHECK FILES ============================
echo ===[ Checking Components ]========================================
if exist "%INFLUX_DIR%\run_influxdb3.bat" (
    echo [OK]   InfluxDB found: "%INFLUX_DIR%"
) else (
    echo [WARN] InfluxDB not found: "%INFLUX_DIR%"
    echo        InfluxDB startup will be skipped
)

if exist "%THETA_TERMINAL%" (
    echo [OK]   ThetaData Terminal found: "%THETA_TERMINAL%"
) else (
    echo [WARN] ThetaData Terminal not found
    echo        Terminal startup will be skipped
)
echo ================================================================
echo.

REM ======================== START INFLUXDB =========================
if not exist "%INFLUX_DIR%\run_influxdb3.bat" goto SKIP_INFLUX
echo [INFO] Starting InfluxDB 3 Enterprise...
start "InfluxDB 3 Enterprise" /D "%INFLUX_DIR%" cmd /k call "run_influxdb3.bat"
echo [OK]   InfluxDB window opened
echo.

REM Wait for InfluxDB to start
timeout /t 3 /nobreak >nul

:SKIP_INFLUX

REM ===================== START THETA TERMINAL ======================
if not exist "%THETA_TERMINAL%" goto SKIP_THETA
echo [INFO] Starting ThetaData Terminal...
start "ThetaData Terminal" "%THETA_TERMINAL%"
echo [OK]   ThetaData Terminal launched
echo.

:SKIP_THETA

REM ====================== FIND CONDA ===============================
set "CONDA_BAT="
set "CAND1=%UserProfile%\anaconda3\condabin\conda.bat"
set "CAND2=%UserProfile%\AppData\Local\anaconda3\condabin\conda.bat"
set "CAND3=%ProgramData%\Anaconda3\condabin\conda.bat"
set "CAND4=%UserProfile%\miniconda3\condabin\conda.bat"
set "CAND5=%ProgramData%\Miniconda3\condabin\conda.bat"

if exist "%CAND1%" set "CONDA_BAT=%CAND1%"
if not defined CONDA_BAT if exist "%CAND2%" set "CONDA_BAT=%CAND2%"
if not defined CONDA_BAT if exist "%CAND3%" set "CONDA_BAT=%CAND3%"
if not defined CONDA_BAT if exist "%CAND4%" set "CONDA_BAT=%CAND4%"
if not defined CONDA_BAT if exist "%CAND5%" set "CONDA_BAT=%CAND5%"

if defined CONDA_BAT goto HAVE_CONDA

REM Fallback: try from PATH
where conda.bat > "%TEMP%\conda_loc.txt" 2>nul
if exist "%TEMP%\conda_loc.txt" (
  for /f "usebackq delims=" %%L in ("%TEMP%\conda_loc.txt") do (
    if not defined CONDA_BAT set "CONDA_BAT=%%L"
  )
  del "%TEMP%\conda_loc.txt" >nul 2>&1
)

:HAVE_CONDA
if not defined CONDA_BAT (
  echo [WARN] conda.bat not found. Trying to start Jupyter Lab without conda...
  goto START_JUPYTER_NO_CONDA
)

echo [INFO] Found conda at: %CONDA_BAT%
echo [INFO] Starting Jupyter Lab in "%PROJECT_ROOT%" with env "%CONDA_ENV%"...
echo.
start "JupyterLab (%CONDA_ENV%)" /D "%PROJECT_ROOT%" cmd /k call "%CONDA_BAT%" activate %CONDA_ENV% ^&^& jupyter lab
echo [OK]   Jupyter Lab window opened
goto END

:START_JUPYTER_NO_CONDA
echo [INFO] Attempting to start Jupyter Lab without conda (requires jupyter in PATH)...
start "JupyterLab" /D "%PROJECT_ROOT%" cmd /k jupyter lab

:END
echo.
echo ╔════════════════════════════════════════════════════════════════╗
echo ║                    All Components Started                      ║
echo ╚════════════════════════════════════════════════════════════════╝
echo.
echo Windows opened:
echo   - InfluxDB 3 Enterprise (if installed)
echo   - ThetaData Terminal (if installed)
echo   - Jupyter Lab (in %CONDA_ENV% environment)
echo.
echo You can now:
echo   1. Access Jupyter Lab (opens in browser automatically)
echo   2. Access InfluxDB UI at http://localhost:8086
echo   3. Use ThetaData Terminal for manual queries
echo.
echo Press any key to close this launcher window...
pause >nul
exit /b 0
