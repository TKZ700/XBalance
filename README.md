# XBalance v0.1.1-alpha

A multi-socket SOCKS5 load balancer for V2Ray/Xray configs that bypasses per-connection bandwidth throttling.

## The Problem

Certain networks impose a strict speed limit on **each individual TCP socket**. Even with high total bandwidth, a single VPN connection may be capped at 1-2 Mbps. More tabs or bigger downloads won't help because the limit is per-connection.

## How It Works

XBalance runs multiple local Xray workers (each connected to a different remote server/CDN IP) and a Python SOCKS5 proxy in front that **round-robins every new TCP connection** across them.

```
Browser  -->  XBalance (SOCKS5 :7070)  -->  Worker 1 (20001) --> Server A
                                        -->  Worker 2 (20002) --> Server B
                                        -->  Worker 3 (20003) --> Server C
                                        -->  ... (round-robin per connection)
```

Every image, API call, and page resource goes through a different connection and IP, bypassing the per-socket rate limit.

## Features

- Round-robin load balancing across multiple Xray workers
- Async high-performance relay (1MB buffer, zero-copy transport writes)
- TCP_NODELAY + kernel buffer tuning for low latency
- Automatic Xray binary download (if not present)
- Kills orphan xray processes on startup/shutdown
- Live terminal dashboard with connection stats
- Logs all activity to `xbalance.log` for debugging

## Supported Protocols

| Protocol | Format |
|---|---|
| VMess | `vmess://base64encodedjson` |
| VLESS | `vless://uuid@host:port?params` |
| Trojan | `trojan://password@host:port?params` |
| Shadowsocks | `ss://base64(method:password)@host:port` |
| Subscription URL | `https://example.com/sub` (Base64-encoded list) |

## Requirements

- Python 3.10+
- Windows (primary), Linux (experimental)

## Step-by-Step Guide

### 1. Clone the repository

```bash
git clone https://github.com/your-username/XBalance.git
cd XBalance
```

### 2. Install dependencies

```bash
pip install colorama
```

Or if using `uv`:

```bash
uv sync
```

### 3. Configure your proxy links

Open `configs.txt` and paste your proxy links. See [configs.txt Format](#configstxt-format) below for details.

```txt
vmess://eyJhZGQiOi...
vless://uuid@server.com:443?encryption=none&security=tls&type=ws&path=%2F#Name
trojan://password@server.com:443?security=tls&type=grpc&serviceName=grpc#Name
https://your-subscription-url.com/sub
```

You can mix protocols and add multiple links (one per line).

### 4. Run XBalance

```bash
python main.py
```

On first run, XBalance will automatically download the Xray binary (~20MB).

### 5. Configure your browser/proxy client

Set your system or browser proxy to:

| Setting | Value |
|---|---|
| Proxy Type | **SOCKS5** |
| Address | `127.0.0.1` |
| Port | `7070` |

For Firefox, go to **Settings > Network Settings > Manual proxy configuration** and enter the above.

For system-wide proxy on Windows, you can use **Proxifier** or **Proxycap** to route all traffic through the SOCKS5 proxy.

### 6. Verify it's working

The terminal dashboard shows:
- Active connections
- Total routed requests
- Per-worker connection distribution

If you see traffic on multiple workers, load balancing is working.

### 7. Stop XBalance

Press **Ctrl+C** to stop. All xray processes and temp files are cleaned up automatically.

## configs.txt Format

Each line in `configs.txt` is either a proxy link or a subscription URL. Lines starting with `#` are comments.

### Direct Links

```txt
# VMess (base64-encoded JSON)
vmess://eyJhZGQiOiJleGFtcGxlLmNvbSIsImhvc3QiOiIiLCJpZCI6IjEyMzQ1Njc4LTktMCIsIm5ldCI6IndzIiwicGF0aCI6Ii93cyIsInBvcnQiOiI0NDMiLCJwcyI6IiIsInNlY3VyaXR5IjoiVExTIiwic25pIjoiMTIzNDU2Nzg5MCIsInRscyI6InRscyIsInR5cGUiOiJhdXRvIiwidmVyc2lvbiI6IjEifQ==

# VLESS with TLS + WebSocket
vless://uuid@server.com:443?encryption=none&security=tls&type=ws&host=server.com&path=%2Fws#MyServer

# Trojan with TLS + gRPC
trojan://password@server.com:443?security=tls&type=grpc&serviceName=mygrpc#ServerName

# Shadowsocks
ss://aes-256-gcm:password@server.com:8388#MySS
```

### Subscription URLs

```txt
# Base64-encoded list of links
https://example.com/subscription

# HTTP links are fetched automatically
http://sub.example.com/v2ray
```

Subscription URLs should return a Base64-encoded list of proxy links (one per line). XBalance handles the decoding automatically.

### Comments

```txt
# This line is ignored
vmess://...  # This trailing comment is also ignored
```

## Project Structure

```
XBalance/
├── main.py              # Entrypoint, CLI, stats dashboard
├── configs.txt          # Your proxy config links
├── xbalance.log         # Log file (created on run)
├── pyproject.toml       # Project metadata
├── utils/
│   ├── __init__.py
│   ├── parser.py        # URI decoder (vmess/vless/trojan/ss)
│   ├── balancer.py      # Async SOCKS5 round-robin server
│   └── xray.py          # Xray binary & subprocess manager
└── xray/                # Auto-created: xray.exe + temp configs
    ├── xray.exe
    ├── geoip.dat
    ├── geosite.dat
    └── temp_config_*.json  # Per-worker configs (cleaned up on exit)
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

## Disclaimer

This software is provided "as is" without warranty. The authors and contributors are **not responsible** for any misuse, including but not limited to violating applicable laws, unauthorized access, or any damages resulting from use. **Use at your own risk and in compliance with all applicable laws.**

## License

MIT License

```
Copyright (c) 2026 XBalance Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Acknowledgments

Developed with the assistance of [openCode](https://opencode.ai).
