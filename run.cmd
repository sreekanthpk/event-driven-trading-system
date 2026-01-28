@echo off
setlocal

:: --- CONFIGURATION ---
:: Set how many seconds to wait between starting modules
set SLEEP_TIME=5

echo Starting Streaming System Modules...
echo ----------------------------------

echo Starting Kafka
start "Event bus" cmd /c "mvn exec:java -pl kafka"
timeout /t %SLEEP_TIME% /nobreak

echo Starting Redis
start "Position Store" cmd /c "mvn exec:java -pl redis"
timeout /t %SLEEP_TIME% /nobreak

echo Starting Websocket Server...
start "Websocket Server" cmd /c "mvn exec:java -pl websocket-backend"
timeout /t %SLEEP_TIME% /nobreak

echo Starting Position Service...
start "Position Service" cmd /c "mvn exec:java -pl position-service"
timeout /t %SLEEP_TIME% /nobreak

echo Starting Auto trader...
start "Auto Trader" cmd /c "mvn exec:java -pl auto-trader"
timeout /t %SLEEP_TIME% /nobreak

echo Starting inquiry generator...
start "Inquiry Generator" cmd /c "mvn exec:java -pl inquiry-generator"
timeout /t %SLEEP_TIME% /nobreak

start "Web server" cmd /c "http-server ."
timeout /t %SLEEP_TIME% /nobreak

start chrome http://127.0.0.1:8081/web

echo ----------------------------------
echo All modules triggered. Check individual windows for logs.
pause
