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

echo Starting Auto trader...
start "Auto Trader" cmd /c "cd integration-test &&  mvn exec:java -Dexec.mainClass=com.trade.stream.InquiryAutoTraderForTesting"
timeout /t %SLEEP_TIME% /nobreak

echo Starting Websocket client...
start "Websocket client to verify results" cmd /c "cd integration-test && mvn exec:java -Dexec.mainClass=com.trade.stream.WebSocketIntegrationTest"
timeout /t %SLEEP_TIME% /nobreak

echo Starting inquiry generator...
start "Inquiry Generator" cmd /c "cd integration-test && mvn exec:java -Dexec.mainClass=com.trade.stream.InquiryProducerAppForIntegrationTesting"
timeout /t %SLEEP_TIME% /nobreak



echo ----------------------------------
echo All modules triggered. Check individual windows for logs.
pause