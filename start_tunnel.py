#!/usr/bin/env python3
"""
start_tunnel.py — Auto-downloads cloudflared and starts a public URL tunnel.
=============================================================================
Run this in a SEPARATE terminal while volsurge_v5 is running on port 5002.

    python start_tunnel.py

It will print the exact webhook URL to paste into TradingView.
No signup required. No account needed. Cloudflare Quick Tunnels are free.
"""

import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

PORT    = 5002
SECRET  = os.getenv("PARITY_SECRET", "volsurge-parity-token")
EXE     = Path(__file__).parent / "cloudflared.exe"

DOWNLOAD_URL = (
    "https://github.com/cloudflare/cloudflared/releases/latest/download/"
    "cloudflared-windows-amd64.exe"
)


def download_cloudflared():
    print("[TUNNEL] cloudflared.exe not found — downloading from Cloudflare...")
    print(f"[TUNNEL] Source: {DOWNLOAD_URL}")
    try:
        req = urllib.request.Request(
            DOWNLOAD_URL,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp, open(EXE, "wb") as f:
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            chunk = 65536
            while True:
                block = resp.read(chunk)
                if not block:
                    break
                f.write(block)
                downloaded += len(block)
                if total:
                    pct = int(100 * downloaded / total)
                    print(f"\r[TUNNEL] Downloading... {pct}%", end="", flush=True)
        print(f"\n[TUNNEL] Downloaded {downloaded // 1024} KB -> {EXE}")
    except Exception as e:
        print(f"\n[TUNNEL] Download failed: {e}")
        print("[TUNNEL] Please download manually from:")
        print(f"         {DOWNLOAD_URL}")
        print(f"         Save as: {EXE}")
        sys.exit(1)


def start_tunnel():
    if not EXE.exists():
        download_cloudflared()

    print(f"\n[TUNNEL] Starting Cloudflare Quick Tunnel -> http://localhost:{PORT}")
    print("[TUNNEL] (this may take 5–10 seconds)\n")

    proc = subprocess.Popen(
        [str(EXE), "tunnel", "--url", f"http://localhost:{PORT}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    public_url = None
    try:
        for line in proc.stdout:
            line = line.rstrip()
            # cloudflared prints the URL in a line like:
            # "... https://xxxx.trycloudflare.com ..."
            match = re.search(r"https://[a-z0-9\-]+\.trycloudflare\.com", line)
            if match:
                public_url = match.group(0)
                break
            # Print cloudflared output so user can see what's happening
            if any(k in line.lower() for k in ("err", "warn", "info", "tunnel", "connect")):
                print(f"  cf> {line}")
    except KeyboardInterrupt:
        proc.terminate()
        print("\n[TUNNEL] Stopped.")
        sys.exit(0)

    if not public_url:
        print("[TUNNEL] Could not detect public URL from cloudflared output.")
        print("[TUNNEL] Check the output above for any errors.")
        proc.terminate()
        sys.exit(1)

    # ── Print the result ──────────────────────────────────────────────────────
    webhook_url = f"{public_url}/parity/pine-webhook?token={SECRET}"

    print()
    print("=" * 65)
    print("  TUNNEL ACTIVE")
    print("=" * 65)
    print(f"  Public URL   : {public_url}")
    print(f"  Health check : {public_url}/health")
    print(f"  Dashboard    : {public_url}/parity/dashboard")
    print()
    print("  TRADINGVIEW WEBHOOK URL (copy this):")
    print(f"  {webhook_url}")
    print("=" * 65)
    print()
    print("  Keep this window open. Closing it kills the tunnel.")
    print("  Press Ctrl+C to stop.\n")

    # Stream remaining cloudflared output (reconnects, stats etc.)
    try:
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                print(f"  cf> {line}")
    except KeyboardInterrupt:
        pass
    finally:
        proc.terminate()
        print("\n[TUNNEL] Tunnel closed.")


if __name__ == "__main__":
    start_tunnel()
