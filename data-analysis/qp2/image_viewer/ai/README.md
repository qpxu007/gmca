# QP2 AI Assistant Chat Widget

## Overview
This is a standalone AI chat assistant for the QP2 image viewer. It can run as a standard window or as a "Desktop Widget" that stays on top of other windows. It also integrates with the system tray.

## Features
- **Always on Top**: Use the `--widget` flag to keep the chat window above others.
- **System Tray Integration**:
  - Closing the window minimizes it to the system tray.
  - Click the tray icon to show/hide the chat.
  - Right-click the tray icon to "Quit" completely.
- **Notifications**: The window automatically un-minimizes and raises to the front when a new message is received.

## Installation

### For Current User
Run the install script to add the widget to your application menu:
```bash
./install_widget.sh
```

### System-Wide
To install for all users (requires sudo):
```bash
sudo ./install_system_wide.sh
```

## Usage
Launch **"QP2 AI Chat Widget"** from your desktop's application menu.

Alternatively, run from the command line:
```bash
/path/to/qp2/bin/chat --widget
```
