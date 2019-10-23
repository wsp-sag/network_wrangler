:: build_scenario.bat
:: batch file to create scenarios using network wrangler library
@ECHO off

SET PYTHON_PATH=C:\ProgramData\Anaconda3
SET CONFIG_FILE_PATH=C:\Users\kulshresthaa\OneDrive\OneDrive - WSP O365\network_wrangler\example
SET SCRIPT_PATH=C:\Users\kulshresthaa\OneDrive\OneDrive - WSP O365\network_wrangler\scripts

::SET sPath=%~dp0

SET CONFIG_FILE=%CONFIG_FILE_PATH%\config_1.yml

CALL "%PYTHON_PATH%\Scripts\activate.bat"

"%PYTHON_PATH%\python.exe" "%SCRIPT_PATH%\build_scenario.py" "%CONFIG_FILE%"

PAUSE
