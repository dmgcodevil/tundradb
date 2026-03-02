#!/bin/bash

# TundraDB Interactive Documentation Launcher
# Starts a local web server to view the interactive documentation

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${1:-8000}"

echo "🗄️  TundraDB Interactive Documentation"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📖 Pages:"
echo "   Home:           http://localhost:${PORT}/"
echo "   Architecture:   http://localhost:${PORT}/architecture.html"
echo "   TundraQL:       http://localhost:${PORT}/tundraql.html"
echo "   Live Demo:      http://localhost:${PORT}/query-execution.html"
echo "   Algorithms:     http://localhost:${PORT}/algorithm-deep-dive.html"
echo ""
echo "Press Ctrl+C to stop the server"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if command -v python3 &>/dev/null; then
    python3 -m http.server "$PORT"
elif command -v python &>/dev/null; then
    python -m http.server "$PORT"
else
    echo "❌ Python not found. Open index.html directly in your browser."
    exit 1
fi
