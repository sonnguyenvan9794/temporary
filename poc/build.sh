#!/bin/bash
set -e
BASE=$(cd "$(dirname "$0")" && pwd)

echo "=== Building sentinel-server (Token Server) ==="
cd "$BASE/sentinel-server" && mvn package -DskipTests -q

echo "=== Building downstream-fake ==="
cd "$BASE/downstream-fake" && mvn package -DskipTests -q

echo "=== Building mulesoft-fake ==="
cd "$BASE/mulesoft-fake" && mvn package -DskipTests -q

echo "=== Building service-order ==="
cd "$BASE/service-order" && mvn package -DskipTests -q

# Download Sentinel Dashboard jar nếu chưa có
DASHBOARD_JAR="$BASE/sentinel-dashboard.jar"
if [ ! -f "$DASHBOARD_JAR" ]; then
    echo "=== Downloading Sentinel Dashboard 1.8.8 ==="
    curl -L -o "$DASHBOARD_JAR" \
        "https://github.com/alibaba/Sentinel/releases/download/1.8.8/sentinel-dashboard-1.8.8.jar"
fi

echo ""
echo "Build xong. Chay: ./start.sh"
