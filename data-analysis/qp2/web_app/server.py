"""Entry point for qp2-web-server command."""

import os
import sys
import argparse


def main():
    parser = argparse.ArgumentParser(description="QP2 Web Server")
    parser.add_argument("--port", type=int, default=int(os.environ.get("WEB_APP_PORT", 8000)))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    import uvicorn
    uvicorn.run("qp2.web_app.backend.main:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
