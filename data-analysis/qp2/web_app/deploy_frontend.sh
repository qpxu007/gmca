#!/bin/bash
# Build the React frontend and deploy to bl2ws8.
# Run from any directory; script locates itself via readlink.
set -e

SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
FRONTEND_DIR="$SCRIPT_DIR/frontend"

echo "=== Build ==="
cd "$FRONTEND_DIR"
source ~/nvm.sh 2>/dev/null || source ~/.nvm/nvm.sh
nvm use
VITE_BASE_PATH=/data_portal/ VITE_API_URL=/data_portal/api npm run build

echo ""
echo "=== Deploy ==="
echo "dist/ is ready at: $FRONTEND_DIR/dist/"
echo ""
echo "On bl2ws8, run:"
echo "  sudo rsync -av --delete /mnt/beegfs/qxu/data-analysis/qp2/web_app/frontend/dist/ /var/www/html/data_portal/"
echo "  sudo chown -R www-data:www-data /var/www/html/data_portal"
echo "  sudo find /var/www/html/data_portal -type d -exec chmod 755 {} \;"
echo "  sudo find /var/www/html/data_portal -type f -exec chmod 644 {} \;"
