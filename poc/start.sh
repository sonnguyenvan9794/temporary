#!/bin/bash
BASE=$(cd "$(dirname "$0")" && pwd)
LOGS="$BASE/logs"
mkdir -p "$LOGS"

# Kill existing processes on our ports
for port in 8080 8081 8082 8720 8721 8858 18730; do
    pid=$(lsof -ti tcp:$port 2>/dev/null)
    [ -n "$pid" ] && kill $pid && echo "Stopped process on port $port"
done
sleep 1

echo "=== [1/5] Starting Sentinel Dashboard (port 8858) ==="
java -Dserver.port=8858 \
     -jar "$BASE/sentinel-dashboard.jar" \
     > "$LOGS/sentinel-dashboard.log" 2>&1 &
echo $! > "$LOGS/sentinel-dashboard.pid"

echo "=== [2/5] Starting Sentinel Token Server (port 8720 REST / 18730 Netty) ==="
java -jar "$BASE/sentinel-server/target/"*.jar \
     > "$LOGS/sentinel-server.log" 2>&1 &
echo $! > "$LOGS/sentinel-server.pid"

echo "=== [3/5] Starting downstream-fake (port 8081) ==="
java -jar "$BASE/downstream-fake/target/"*.jar \
     > "$LOGS/downstream.log" 2>&1 &
echo $! > "$LOGS/downstream.pid"

echo "=== [4/5] Starting mulesoft-fake (port 8082) ==="
java -jar "$BASE/mulesoft-fake/target/"*.jar \
     > "$LOGS/mulesoft.log" 2>&1 &
echo $! > "$LOGS/mulesoft.pid"

echo "Waiting for infrastructure to start (8s)..."
sleep 8

echo "=== [5/5] Starting service-order (port 8080) ==="
java -jar "$BASE/service-order/target/"*.jar \
     > "$LOGS/service-order.log" 2>&1 &
echo $! > "$LOGS/service-order.pid"

echo ""
echo "All services started!"
echo ""
echo "  POC Dashboard:       http://localhost:8080/dashboard.html"
echo "  Sentinel Dashboard:  http://localhost:8858  (sentinel/sentinel)"
echo "  Token Server API:    http://localhost:8721/admin/status
  Token Server Netty:  port 18730 (clients connect here)
  Sentinel Cmd Center: port 8720 (dashboard communication)"
echo "  Downstream stats:    http://localhost:8081/admin/stats"
echo ""
echo "Logs: $LOGS/"
echo "Stop: ./stop.sh"
