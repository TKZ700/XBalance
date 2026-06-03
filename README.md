# XBalance v0.2.0-alpha

A multi-socket load balancer for V2Ray/Xray configs with smart worker selection, HTTP CONNECT proxy, and parallel download support.

## The Problem

Certain networks impose a strict speed limit on **each individual TCP socket**. Even with high total bandwidth, a single VPN connection may be capped at 1-2 Mbps. More tabs or bigger downloads won't help because the limit is per-connection.

Additionally, some services impose **per-IP rate limits** — too many requests from a single IP get throttled or blocked.

## How It Works

XBalance runs multiple local Xray workers (each connected to a different remote server/CDN IP) and provides two proxy frontends:

- **SOCKS5 proxy** (:7070) — general-purpose, routes connections via smart worker selection
- **HTTP CONNECT proxy** (:7071) — can inspect HTTP headers, enables parallel file downloads

```
Browser  -->  XBalance SOCKS5 (:7070)  -->  Worker 1 (20001) --> Server A
              XBalance HTTP   (:7071)  -->  Worker 2 (20002) --> Server B
                                          -->  Worker 3 (20003) --> Server C
                                          -->  ... (smart selection per connection)
```

### Worker Selection Strategies

| Strategy | Description |
|---|---|
| `round-robin` | Sequential cycling (default, original behavior) |
| `least-conn` | Routes to the worker with fewest active connections |
| `response-time` | Routes to the fastest-responding worker |
| `weighted` | Combines connection count, response time, and failure rate |

### Parallel Downloads

For large HTTP files (>10MB), XBalance can split the download across multiple workers using HTTP Range requests:

```
Client  -->  HTTP Proxy (:7071)
               |-- Worker 1: bytes 0-9MB
               |-- Worker 2: bytes 9-18MB
               |-- Worker 3: bytes 18-27MB
               v
          Reassembled response → Client
```

## Features

- **4 load balancing strategies**: round-robin, least-connection, response-time, weighted
- **HTTP CONNECT proxy** with header inspection for parallel downloads
- **Parallel file downloads**: splits large HTTP files across all workers via Range requests
- **SOCKS5 HTTP peek**: detects HTTP traffic inside SOCKS5 tunnels
- **Health checks**: auto-disables failing workers, retries after cooldown
- Async high-performance relay (1MB buffer, zero-copy transport writes)
- TCP_NODELAY + kernel buffer tuning for low latency
- Automatic Xray binary download (if not present)
- Kills orphan xray processes on startup/shutdown
- Live terminal dashboard with per-worker stats (connections, response time, failures)
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
- Per-worker connection distribution with response times

If you see traffic on multiple workers, load balancing is working.

### 7. Stop XBalance

Press **Ctrl+C** to stop. All xray processes and temp files are cleaned up automatically.

## CLI Options

```
--strategy {round-robin,least-conn,response-time,weighted}
    Worker selection strategy (default: round-robin)

--socks-port PORT
    SOCKS5 proxy port (default: 7070)

--http-port PORT
    HTTP CONNECT proxy port (default: 7071, 0 = disabled)

--parallel-downloads
    Enable parallel download feature for large HTTP files

--parallel-threshold BYTES
    Minimum file size for parallel download (default: 10485760 = 10MB)

--peek-http
    Peek at SOCKS5 tunnel to detect HTTP traffic
```

### Examples

```bash
# Default: round-robin, SOCKS5 only
python main.py

# Least-connections with HTTP proxy + parallel downloads
python main.py --strategy least-conn --parallel-downloads

# All features enabled
python main.py --strategy weighted --http-port 7071 --parallel-downloads --peek-http

# Custom ports
python main.py --socks-port 1080 --http-port 8080
```

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
├── main.py                  # Entrypoint, CLI, stats dashboard
├── configs.txt              # Your proxy config links
├── xbalance.log             # Log file (created on run)
├── pyproject.toml           # Project metadata
├── utils/
│   ├── __init__.py
│   ├── parser.py            # URI decoder (vmess/vless/trojan/ss)
│   ├── balancer.py          # Async SOCKS5 server with smart selection
│   ├── worker_selector.py   # Load balancing strategies
│   ├── http_proxy.py        # HTTP CONNECT proxy + parallel download trigger
│   ├── downloader.py        # Parallel download engine (Range requests)
│   ├── peekable.py          # Peekable StreamReader for HTTP detection
│   └── xray.py              # Xray binary & subprocess manager
└── xray/                    # Auto-created: xray.exe + temp configs
    ├── xray.exe
    ├── geoip.dat
    ├── geosite.dat
    └── temp_config_*.json    # Per-worker configs (cleaned up on exit)
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
