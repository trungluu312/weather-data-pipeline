#!/bin/bash
set -e

# 1. Start Prefect Server in the background
echo "🚀 Starting Prefect Server..."
prefect server start --host 0.0.0.0 > /app/logs/server.log 2>&1 &
SERVER_PID=$!

# 2. Wait for Server to be ready
echo "⏳ Waiting for Prefect API to be ready..."
until curl -s http://localhost:4200/api/health > /dev/null; do
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "❌ Prefect Server process died!"
        echo "--- Server Log Tail ---"
        tail -n 20 /app/logs/server.log
        exit 1
    fi
    sleep 2
done
echo "✅ Prefect Server is up!"

# 3. Configure local CLI to use the local server
export PREFECT_API_URL="http://localhost:4200/api"

# 4. Start the Deployment (Scheduler + Worker)
echo "🌩️ Starting Weather Pipeline Worker..."
python -m orchestration.deploy
