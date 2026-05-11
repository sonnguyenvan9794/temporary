#!/bin/bash
BASE=$(cd "$(dirname "$0")" && pwd)
LOGS="$BASE/logs"

for svc in service-order mulesoft downstream sentinel-server sentinel-dashboard; do
    pid_file="$LOGS/$svc.pid"
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        kill "$pid" 2>/dev/null && echo "Stopped $svc (pid $pid)" || echo "$svc already stopped"
        rm "$pid_file"
    fi
done

# Fallback: kill by port
for port in 8080 8081 8082 8720 8858 18730; do
    pid=$(lsof -ti tcp:$port 2>/dev/null)
    [ -n "$pid" ] && kill $pid
done

echo "All stopped."
