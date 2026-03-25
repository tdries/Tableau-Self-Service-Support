#!/bin/bash
cd "$(dirname "$0")"

echo "Starting Tableau Self-Service Support..."

# Kill any existing instances
pkill -f "node server.js" 2>/dev/null
pkill -f "node agent.js"  2>/dev/null
sleep 1

# Start server in background
node server.js &
SERVER_PID=$!

sleep 1

# Start agent in background
node agent.js &
AGENT_PID=$!

echo "✓ Server PID: $SERVER_PID"
echo "✓ Agent  PID: $AGENT_PID"
echo ""
echo "Press Ctrl+C to stop both."

# Stop both on exit
trap "kill $SERVER_PID $AGENT_PID 2>/dev/null; echo 'Stopped.'" EXIT INT TERM
wait
