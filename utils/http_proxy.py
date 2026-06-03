import asyncio
import logging
import socket
import struct
import sys
from typing import List

from colorama import Fore

from utils.downloader import ParallelDownloader, PARALLEL_THRESHOLD
from utils.worker_selector import WorkerSelector

log = logging.getLogger("xbalance.http_proxy")


def _tune_socket(sock: socket.socket):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 262144)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if sys.platform == "win32":
            sock.ioctl(
                socket.SIO_KEEPALIVE_VALS,
                (1, 60000, 30000),
            )
    except Exception:
        pass


class HTTPConnectProxy:
    """HTTP CONNECT proxy that can inspect headers for parallel downloads."""

    def __init__(
        self,
        host: str,
        port: int,
        selector: WorkerSelector,
        parallel_downloads: bool = False,
        parallel_threshold: int = PARALLEL_THRESHOLD,
    ):
        self.host = host
        self.port = port
        self.selector = selector
        self.parallel_downloads = parallel_downloads
        self.active_connections = 0
        self.downloader = (
            ParallelDownloader(selector.worker_ports, parallel_threshold)
            if parallel_downloads
            else None
        )

    async def start(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        sock = server.sockets[0]
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _tune_socket(sock)

        addr = sock.getsockname()
        print(f"{Fore.GREEN}[+] HTTP CONNECT proxy listening on {addr[0]}:{addr[1]}{Fore.RESET}")
        log.info("HTTP proxy listening on %s:%d", addr[0], addr[1])

        async with server:
            await server.serve_forever()

    async def handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ):
        self.active_connections += 1
        client_addr = client_writer.get_extra_info("peername")
        log.debug("HTTP proxy connection from %s", client_addr)

        try:
            client_sock = client_writer.transport.get_extra_info("socket")
            if client_sock:
                _tune_socket(client_sock)

            request_line = await asyncio.wait_for(client_reader.readline(), timeout=10)
            if not request_line:
                return

            parts = request_line.decode("utf-8", errors="replace").strip().split()
            if len(parts) < 3:
                client_writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                await client_writer.drain()
                return

            method, target, version = parts[0], parts[1], parts[2]

            raw_headers = await asyncio.wait_for(client_reader.readuntil(b"\r\n\r\n"), timeout=10)
            headers = self._parse_headers(raw_headers)

            if method == "CONNECT":
                await self._handle_connect(client_reader, client_writer, target, headers, client_addr)
            elif method in ("GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"):
                await self._handle_http(client_reader, client_writer, method, target, version, headers, client_addr)
            else:
                client_writer.write(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                await client_writer.drain()

        except asyncio.TimeoutError:
            log.debug("HTTP proxy timeout from %s", client_addr)
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            log.debug("HTTP proxy connection dropped: %s", client_addr)
        except Exception as e:
            log.error("HTTP proxy handler error: %s", e, exc_info=True)
        finally:
            self.active_connections -= 1
            if not client_writer.is_closing():
                client_writer.close()

    def _parse_headers(self, raw: bytes) -> dict:
        headers = {}
        for line in raw.split(b"\r\n"):
            if b":" in line:
                key, value = line.decode("utf-8", errors="replace").split(":", 1)
                headers[key.strip()] = value.strip()
        return headers

    async def _handle_connect(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        target: str,
        headers: dict,
        client_addr,
    ):
        """Handle CONNECT method — tunnel through a worker."""
        if ":" in target:
            host, port_str = target.rsplit(":", 1)
            port = int(port_str)
        else:
            host, port = target, 443

        worker_port = self.selector.next_worker()
        self.selector.on_connection_open(worker_port)

        try:
            worker_reader, worker_writer = await asyncio.open_connection(
                "127.0.0.1", worker_port
            )
            worker_sock = worker_writer.transport.get_extra_info("socket")
            if worker_sock:
                _tune_socket(worker_sock)
        except Exception as e:
            client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            await client_writer.drain()
            log.error("Failed to connect to worker %d: %s", worker_port, e)
            self.selector.on_failure(worker_port)
            self.selector.on_connection_close(worker_port)
            return

        try:
            connect_req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n"
            worker_writer.write(connect_req.encode())
            await worker_writer.drain()

            resp = await asyncio.wait_for(worker_reader.readuntil(b"\r\n\r\n"), timeout=10)
            status_line = resp.split(b"\r\n")[0]
            if b"200" not in status_line:
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await client_writer.drain()
                worker_writer.close()
                self.selector.on_failure(worker_port)
                self.selector.on_connection_close(worker_port)
                return

            client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await client_writer.drain()

            log.info("CONNECT tunnel: %s -> worker %d", client_addr, worker_port)

            client_transport = client_writer.transport
            worker_transport = worker_writer.transport

            pipe_c2w = asyncio.create_task(self._relay(client_reader, worker_transport, "c2w"))
            pipe_w2c = asyncio.create_task(self._relay(worker_reader, client_transport, "w2c"))

            done, pending = await asyncio.wait(
                [pipe_c2w, pipe_w2c],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            for transport in (client_transport, worker_transport):
                try:
                    if not transport.is_closing():
                        transport.write_eof()
                except Exception:
                    pass

            self.selector.on_success(worker_port, 0.0)

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            log.debug("CONNECT tunnel dropped: %s", client_addr)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("CONNECT handler error: %s", e, exc_info=True)
        finally:
            self.selector.on_connection_close(worker_port)
            if not worker_writer.is_closing():
                worker_writer.close()

    async def _handle_http(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        method: str,
        target: str,
        version: str,
        headers: dict,
        client_addr,
    ):
        """Handle plain HTTP request — optionally split across workers."""
        if not target.startswith("http://"):
            client_writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            await client_writer.drain()
            return

        url = target

        if (
            self.parallel_downloads
            and self.downloader
            and method == "GET"
            and "Range" not in headers
        ):
            used_parallel = await self.downloader.download(
                url, client_writer, headers, method
            )
            if used_parallel:
                log.info("Parallel download served: %s", url)
                return

        worker_port = self.selector.next_worker()
        self.selector.on_connection_open(worker_port)

        try:
            parsed = self._parse_url(url)
            if not parsed:
                client_writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                await client_writer.drain()
                self.selector.on_connection_close(worker_port)
                return

            host, port, path, use_tls = parsed

            if use_tls:
                worker_reader, worker_writer = await asyncio.open_connection(
                    "127.0.0.1", worker_port
                )
            else:
                worker_reader, worker_writer = await asyncio.open_connection(
                    "127.0.0.1", worker_port
                )

            connect_req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n"
            worker_writer.write(connect_req.encode())
            await worker_writer.drain()

            resp = await asyncio.wait_for(worker_reader.readuntil(b"\r\n\r\n"), timeout=10)
            if b"200" not in resp.split(b"\r\n")[0]:
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await client_writer.drain()
                worker_writer.close()
                self.selector.on_failure(worker_port)
                self.selector.on_connection_close(worker_port)
                return

            http_request = f"{method} {path} {version}\r\n"
            for k, v in headers.items():
                if k.lower() != "proxy-connection":
                    http_request += f"{k}: {v}\r\n"
            http_request += "\r\n"

            worker_writer.write(http_request.encode())
            await worker_writer.drain()

            log.info("HTTP %s %s -> worker %d", method, url, worker_port)

            client_transport = client_writer.transport
            worker_transport = worker_writer.transport

            pipe_c2w = asyncio.create_task(self._relay(client_reader, worker_transport, "c2w"))
            pipe_w2c = asyncio.create_task(self._relay(worker_reader, client_transport, "w2c"))

            done, pending = await asyncio.wait(
                [pipe_c2w, pipe_w2c],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            for transport in (client_transport, worker_transport):
                try:
                    if not transport.is_closing():
                        transport.write_eof()
                except Exception:
                    pass

            self.selector.on_success(worker_port, 0.0)

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            log.debug("HTTP tunnel dropped: %s", client_addr)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("HTTP handler error: %s", e, exc_info=True)
        finally:
            self.selector.on_connection_close(worker_port)
            if not worker_writer.is_closing():
                worker_writer.close()

    def _parse_url(self, url: str):
        if url.startswith("https://"):
            use_tls = True
            url = url[8:]
        elif url.startswith("http://"):
            use_tls = False
            url = url[7:]
        else:
            return None

        path = "/"
        if "/" in url:
            host_port, path = url.split("/", 1)
            path = "/" + path
        else:
            host_port = url

        if ":" in host_port:
            host, port_str = host_port.rsplit(":", 1)
            port = int(port_str)
        else:
            host = host_port
            port = 443 if use_tls else 80

        return host, port, path, use_tls

    @staticmethod
    async def _relay(reader: asyncio.StreamReader, transport: asyncio.Transport, label: str):
        try:
            while True:
                data = await reader.read(1048576)
                if not data:
                    break
                transport.write(data)
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        except Exception as e:
            log.debug("Relay %s error: %s", label, e)
