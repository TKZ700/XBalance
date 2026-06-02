# XBalance

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

## Supported Protocols

- `vmess://`
- `vless://`
- `trojan://`
- `ss://`
- Subscription URLs (Base64-encoded lists)

## Requirements

- Python 3.10+
- Windows (primary)

## Usage

```bash
# Install dependency
pip install colorama

# Add your configs to configs.txt (one link per line)
# Then run
python main.py
```

Set your system/browser proxy to **SOCKS5** on **127.0.0.1:7070**.

## Project Structure

```
XBalance/
├── main.py              # Entrypoint
├── configs.txt          # Your config links
├── pyproject.toml
├── utils/
│   ├── __init__.py
│   ├── parser.py        # URI decoder (vmess/vless/trojan/ss)
│   ├── balancer.py      # Async SOCKS5 round-robin server
│   └── xray.py          # Xray binary & subprocess manager
└── xray/                # Auto-created: xray.exe + temp configs
```

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
