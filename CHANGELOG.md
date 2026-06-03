# Changelog

## v0.1.1-alpha

- Fixed connection interruption at ~5MB (drain() deadlock in relay)
- Replaced StreamWriter relay with transport-level zero-copy writes
- Increased relay buffer from 64KB to 1MB for streaming
- Added TCP_NODELAY, SO_SNDBUF, SO_RCVBUF socket tuning
- Added TCP keepalive to detect dead connections
- Added proper half-close (write_eof) signaling
- Added kill_stale_xray() to terminate orphan processes on startup
- Added file logging to xbalance.log
- Converted start_workers to async (no more blocking event loop)
- Added sniffing to xray inbound config
- Replaced os.system("cls") with ANSI escape codes
- Removed redundant atexit.register

## v0.1.0-alpha

- Initial release
