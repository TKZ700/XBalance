import asyncio
import logging
import socket
import struct
import sys
from typing import List

from colorama import Fore

from utils.peekable import PeekableStreamReader
from utils.worker_selector import WorkerSelector

log = logging.getLogger("xbalance.balancer")

PIPE_BUFFER_SIZE = 1048576  # 1MB read buffer for streaming throughput


def _tune_socket(sock: socket.socket):
    """Apply high-throughput socket options."""
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


class AsyncSOCKS5Balancer:
    """Async SOCKS5 proxy with smart worker selection and optional HTTP peek."""

    def __init__(
        self,
        host: str,
        port: int,
        selector: WorkerSelector,
        peek_http: bool = False,
    ):
        self.host = host
        self.port = port
        self.selector = selector
        self.peek_http = peek_http
        self.active_connections = 0
        self.rr_index = 0  # kept for backward compat with stats display

    @property
    def worker_ports(self):
        return self.selector.worker_ports

    @property
    def connection_stats(self):
        return self.selector.stats.active_conns

    @property
    def rr_index_display(self):
        return sum(self.selector.stats.total_requests.values())

    async def start(self):
        if not self.worker_ports:
            raise ValueError("Cannot start SOCKS5 balancer with empty worker port list.")

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        sock = server.sockets[0]
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _tune_socket(sock)

        addr = sock.getsockname()
        print(f"{Fore.GREEN}[+] Load balancer listening on socks5://{addr[0]}:{addr[1]}{Fore.RESET}")
        log.info("Balancer listening on %s:%d with %d workers", addr[0], addr[1], len(self.worker_ports))

        async with server:
            await server.serve_forever()

    async def handle_client(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ):
        self.active_connections += 1
        worker_reader = None
        worker_writer = None
        client_addr = client_writer.get_extra_info("peername")
        log.debug("New connection from %s (active=%d)", client_addr, self.active_connections)

        try:
            client_sock = client_writer.transport.get_extra_info("socket")
            if client_sock:
                _tune_socket(client_sock)

            # === Step 1: SOCKS5 handshake with client ===
            header = await client_reader.readexactly(2)
            version, nmethods = struct.unpack("!BB", header)
            if version != 5:
                log.warning("Invalid SOCKS version %d from %s", version, client_addr)
                return

            methods = await client_reader.readexactly(nmethods)
            if 0x00 not in methods:
                client_writer.write(struct.pack("!BB", 5, 0xFF))
                await client_writer.drain()
                return

            client_writer.write(struct.pack("!BB", 5, 0x00))
            await client_writer.drain()

            # === Step 2: Read CONNECT request from client ===
            req_header = await client_reader.readexactly(4)
            _ver, cmd, _rsv, atyp = struct.unpack("!BBBB", req_header)

            if cmd != 1:  # Only CONNECT supported
                client_writer.write(struct.pack("!BBBBIH", 5, 0x07, 0, 1, 0, 0))
                await client_writer.drain()
                log.warning("Unsupported command %d from %s", cmd, client_addr)
                return

            # Read address payload
            if atyp == 1:  # IPv4
                addr_data = await client_reader.readexactly(4 + 2)
            elif atyp == 3:  # Domain
                domain_len_byte = await client_reader.readexactly(1)
                domain_len = domain_len_byte[0]
                addr_data = domain_len_byte + await client_reader.readexactly(domain_len + 2)
            elif atyp == 4:  # IPv6
                addr_data = await client_reader.readexactly(16 + 2)
            else:
                client_writer.write(struct.pack("!BBBBIH", 5, 0x08, 0, 1, 0, 0))
                await client_writer.drain()
                return

            # === Step 3: Pick a worker via smart selector ===
            worker_port = self.selector.next_worker()

            # === Step 4: Connect to the local Xray SOCKS5 worker ===
            try:
                worker_reader, worker_writer = await asyncio.open_connection(
                    "127.0.0.1", worker_port
                )
                worker_sock = worker_writer.transport.get_extra_info("socket")
                if worker_sock:
                    _tune_socket(worker_sock)
            except Exception as e:
                client_writer.write(struct.pack("!BBBBIH", 5, 0x04, 0, 1, 0, 0))
                await client_writer.drain()
                log.error("Failed to connect to worker port %d: %s", worker_port, e)
                self.selector.on_failure(worker_port)
                return

            # SOCKS5 handshake with worker (coalesced into single write)
            worker_writer.write(struct.pack("!BB", 5, 1) + struct.pack("!B", 0x00))
            await worker_writer.drain()

            resp = await worker_reader.readexactly(2)
            if resp[1] != 0x00:
                client_writer.write(struct.pack("!BBBBIH", 5, 0x01, 0, 1, 0, 0))
                await client_writer.drain()
                log.warning("Worker port %d auth failed", worker_port)
                self.selector.on_failure(worker_port)
                return

            # Forward the exact CONNECT request to the worker
            worker_writer.write(req_header + addr_data)
            await worker_writer.drain()

            # Read worker's CONNECT reply
            reply_header = await worker_reader.readexactly(4)
            _r_ver, rep, _r_rsv, r_atyp = struct.unpack("!BBBB", reply_header)

            # Read the rest of the reply address based on type
            if r_atyp == 1:
                reply_addr = await worker_reader.readexactly(4 + 2)
            elif r_atyp == 3:
                r_dlen = (await worker_reader.readexactly(1))[0]
                reply_addr = await worker_reader.readexactly(r_dlen + 2)
            elif r_atyp == 4:
                reply_addr = await worker_reader.readexactly(16 + 2)
            else:
                reply_addr = b""

            # Relay reply to client
            client_writer.write(reply_header + reply_addr)
            await client_writer.drain()

            if rep != 0:
                log.warning("Worker port %d CONNECT rejected (rep=%d)", worker_port, rep)
                self.selector.on_failure(worker_port)
                return

            log.info("Relay established: %s -> port %d", client_addr, worker_port)
            self.selector.on_connection_open(worker_port)

            # === Step 5: Peek for HTTP traffic (optional) ===
            if self.peek_http:
                peekable = PeekableStreamReader(client_reader)
                try:
                    peek_data = await peekable.peek(7, timeout=2.0)
                    if peek_data.startswith((b"GET ", b"POST ", b"HEAD ", b"PUT ")):
                        log.info("HTTP detected in SOCKS5 tunnel from %s", client_addr)
                    # Whether HTTP or not, proceed with relay using the peekable reader
                    await self._relay_with_peekable(peekable, worker_reader, client_writer, worker_writer)
                    return
                except asyncio.TimeoutError:
                    # Not HTTP or no data yet — fall through to normal relay
                    pass
                except Exception:
                    pass

            # === Step 6: Bidirectional relay (cancel-safe) ===
            await self._relay_standard(client_reader, worker_reader, client_writer, worker_writer, worker_port)

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            log.debug("Connection dropped: %s", client_addr)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("Client handler error from %s: %s", client_addr, e, exc_info=True)
        finally:
            self.active_connections -= 1
            if not client_writer.is_closing():
                client_writer.close()
            if worker_writer and not worker_writer.is_closing():
                worker_writer.close()

    async def _relay_standard(
        self,
        client_reader,
        worker_reader,
        client_writer,
        worker_writer,
        worker_port,
    ):
        """Standard bidirectional relay."""
        client_transport = client_writer.transport
        worker_transport = worker_writer.transport

        pipe_c2w = asyncio.create_task(
            self._relay(client_reader, worker_transport, "c2w")
        )
        pipe_w2c = asyncio.create_task(
            self._relay(worker_reader, client_transport, "w2c")
        )

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
        self.selector.on_connection_close(worker_port)

    async def _relay_with_peekable(
        self,
        peekable: PeekableStreamReader,
        worker_reader,
        client_writer,
        worker_writer,
    ):
        """Relay using a PeekableStreamReader for the client side."""
        client_transport = client_writer.transport
        worker_transport = worker_writer.transport

        pipe_c2w = asyncio.create_task(
            self._relay(peekable, worker_transport, "c2w")
        )
        pipe_w2c = asyncio.create_task(
            self._relay(worker_reader, client_transport, "w2c")
        )

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

    @staticmethod
    async def _relay(reader, transport: asyncio.Transport, label: str):
        """High-performance relay: reads from reader and writes directly to Transport."""
        try:
            while True:
                data = await reader.read(PIPE_BUFFER_SIZE)
                if not data:
                    break
                transport.write(data)
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        except Exception as e:
            log.debug("Relay %s error: %s", label, e)
