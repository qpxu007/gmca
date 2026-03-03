#!/bin/bash

# Determine the absolute path of the directory containing this script (qp2/image_viewer/ai/)
SCRIPT_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

# Resolve project root (3 levels up: ai -> image_viewer -> qp2 -> root)
# Note: readlink -f handles resolution even if intermediate paths don't strictly exist or are symlinks
PROJECT_ROOT=$(readlink -f "$SCRIPT_DIR/../../..")

CHAT_BIN="$PROJECT_ROOT/qp2/bin/chat"
ICON_FILE="$SCRIPT_DIR/qp2_chat_bubble_icon.svg"
DESKTOP_FILENAME="qp2-chat.desktop"

# Check permissions to decide install location
if [ "$EUID" -eq 0 ]; then
    echo "Running as root. Installing system-wide..."
    DEST_DIR="/usr/share/applications"
    SYSTEM_WIDE=true
else
    echo "Running as user. Installing locally..."
    DEST_DIR="$HOME/.local/share/applications"
    SYSTEM_WIDE=false
fi

# Ensure destination exists
mkdir -p "$DEST_DIR"

TARGET_FILE="$DEST_DIR/$DESKTOP_FILENAME"

# Generate the .desktop file dynamically
echo "Creating desktop entry at: $TARGET_FILE"
cat <<EOF > "$TARGET_FILE"
[Desktop Entry]
Name=QP2 AI Chat Widget
Comment=AI Assistant Chat for QP2
Exec=$CHAT_BIN --widget
Icon=$ICON_FILE
Type=Application
Categories=Utility;Science;
Terminal=false
StartupNotify=true
EOF

# Set Permissions
if [ "$SYSTEM_WIDE" = true ]; then
    chmod 644 "$TARGET_FILE"
    # Update database for system-wide installs
    if command -v update-desktop-database &> /dev/null; then
        echo "Updating desktop database..."
        update-desktop-database "$DEST_DIR"
    fi
else
    chmod +x "$TARGET_FILE"
fi

echo "--------------------------------------------------------"
echo "Installation complete!"
echo "Location: $TARGET_FILE"
echo "Exec path: $CHAT_BIN"
echo "Icon path: $ICON_FILE"
echo "--------------------------------------------------------"
echo "You can now find 'QP2 AI Chat Widget' in your application menu."
echo "If installed locally, you can add it to 'Startup Applications' to launch on login."
