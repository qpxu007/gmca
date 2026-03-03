#!/bin/bash
SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
ROOT_DIR=$(dirname "$SCRIPT_DIR")

# Setup environment
source "$ROOT_DIR/bin/qp2_env.sh"
source ~/nvm.sh

# Start Backend
BACKEND_PORT=${WEB_APP_PORT:-8000}
echo "Starting Backend on port $BACKEND_PORT..."
cd "$SCRIPT_DIR/backend"
# Use python -m uvicorn to ensure we use the project python
$MYPYTHON -m uvicorn main:app --reload --port $BACKEND_PORT &
BACKEND_PID=$!

# Start Frontend
FRONTEND_PORT=${FRONTEND_PORT:-5173}
echo "Starting Frontend on port $FRONTEND_PORT..."
cd "$SCRIPT_DIR/frontend"
# Pass the backend URL to the frontend build/dev process
VITE_API_URL="http://localhost:$BACKEND_PORT" npm run dev -- --port $FRONTEND_PORT &
FRONTEND_PID=$!

# Trap Ctrl+C to kill both
trap "kill $BACKEND_PID $FRONTEND_PID; exit" SIGINT

echo "Web App running."
echo "Backend: http://localhost:$BACKEND_PORT"
echo "Frontend: http://localhost:$FRONTEND_PORT"

wait
