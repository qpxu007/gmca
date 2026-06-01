#!/bin/bash
SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
ROOT_DIR=$(dirname "$SCRIPT_DIR")

# Setup environment
source "$ROOT_DIR/bin/qp2_env.sh"
source ~/nvm.sh

# Override to use the current directory's code, not the deployed installation
export PROJECT_DIR="$ROOT_DIR"
export PROJECT_ROOT="$(dirname "$ROOT_DIR")"
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Prefer local Python env over the one found by qp2_env.sh
for local_py in "$PROJECT_ROOT/qp2_env/bin/python" "$PROJECT_DIR/qp2_env/bin/python" "$PROJECT_ROOT/.venv/bin/python"; do
    if [ -x "$local_py" ]; then
        MYPYTHON="$local_py"
        break
    fi
done
echo "Using code from: $PROJECT_DIR"
echo "Using Python:    $MYPYTHON"

# --- Security configuration ---
# QP2_ENV:        Set to "test" to enable test credentials (admin/admin, user/user).
#                 Leave unset for production.
# QP2_DATA_DIR:   Root directory for HDF5 file serving (default: /mnt/beegfs/DATA).

# Generate a persistent JWT secret per machine (stable across restarts)
JWT_SECRET_FILE="$ROOT_DIR/.jwt_secret"
if [ ! -f "$JWT_SECRET_FILE" ]; then
    python3 -c "import secrets; print(secrets.token_urlsafe(32))" > "$JWT_SECRET_FILE"
    chmod 600 "$JWT_SECRET_FILE"
fi
export QP2_JWT_SECRET="${QP2_JWT_SECRET:-$(cat "$JWT_SECRET_FILE")}"
# QP2_CORS_ORIGINS: Comma-separated allowed CORS origins.
export QP2_DATA_DIR="${QP2_DATA_DIR:-/mnt/beegfs/DATA}"

# Start Backend
BACKEND_PORT=${WEB_APP_PORT:-9000}
CORS_DEFAULT="http://localhost:5173,http://localhost:${BACKEND_PORT}"
export QP2_CORS_ORIGINS="${QP2_CORS_ORIGINS:-$CORS_DEFAULT}"

# Kill any existing processes on the ports
lsof -ti:$BACKEND_PORT 2>/dev/null | xargs kill -9 2>/dev/null
lsof -ti:${FRONTEND_PORT:-5555} 2>/dev/null | xargs kill -9 2>/dev/null
sleep 1

echo "Starting Backend on port $BACKEND_PORT..."
cd "$SCRIPT_DIR/backend"
$MYPYTHON -m uvicorn main:app --reload --port $BACKEND_PORT &
BACKEND_PID=$!

# Start Frontend
FRONTEND_PORT=${FRONTEND_PORT:-5173}
echo "Starting Frontend on port $FRONTEND_PORT..."
cd "$SCRIPT_DIR/frontend"
VITE_API_URL="http://localhost:$BACKEND_PORT" npm run dev -- --port $FRONTEND_PORT &
FRONTEND_PID=$!

# Trap Ctrl+C to kill both
trap "kill $BACKEND_PID $FRONTEND_PID; exit" SIGINT

echo "Web App running."
echo "  Backend:  http://localhost:$BACKEND_PORT"
echo "  Frontend: http://localhost:$FRONTEND_PORT"
echo "  Data dir: $QP2_DATA_DIR"

wait
