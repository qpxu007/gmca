#!/bin/bash
source ~/nvm.sh

# Cleanup previous attempt
rm -rf frontend

echo "Creating Vite App..."
# Pipe newline to accept default "No" for experimental features
echo | npm create vite@latest frontend -y -- --template react

echo "Installing dependencies..."
cd frontend
npm install
npm install axios @dnd-kit/core @dnd-kit/sortable @dnd-kit/utilities react-modal lucide-react

echo "Frontend setup complete."