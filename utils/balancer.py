import asyncio
import socket
import struct
from typing import List

from colorama import Fore


class AsyncSOCKS5Balancer:
    """
    An asynchronous SOCKS5 proxy server that listens on a local port
    and routes every incoming request to a pool of local SOCKS5 workers
    sequentially (Round-Robin).
    """

    def __init__(self, host: str, port: int, worker_ports: List[int]):
        self.host = host
        self.port = port
        self.worker_ports = worker_ports
        self.rr_index = 0
        self.active_connections = 0
        self.connection_stats = dict.fromkeys(worker_ports, 0)

    async def start(self):
        if not self.worker_ports:
            raise ValueError("Cannot start SOCKS5 balancer with empty worker port list.")

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f"{Fore.GREEN}[+] Load balancer listening on socks5://{addr[0]}:{addr[1]}{Fore.RESET}")

        async with server:
            await server.serve_forever()

    def get_next_worker_port(self) -> int:
        port = self.worker_ports[self.rr_index % len(self.worker_ports)]
        self.connection_stats[port] += 1
        self.rr_index += 1
        return port

    async def handle_client(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ):
        self.active_connections += 1
        worker_reader = None
        worker_writer = None
        upstream_connected = False

        try:
            # === Step 1: SOCKS5 handshake with client ===
            header = await client_reader.readexactly(2)
            version, nmethods = struct.unpack("!BB", header)
            if version != 5:
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
                return

            # Read address payload (we buffer it exactly as-is to forward later)
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

            # === Step 3: Pick a worker via Round-Robin ===
            worker_port = self.get_next_worker_port()

            # === Step 4: Connect to the local Xray SOCKS5 worker ===
            try:
                worker_reader, worker_writer = await asyncio.open_connection(
                    "127.0.0.1", worker_port
                )
            except Exception:
                client_writer.write(struct.pack("!BBBBIH", 5, 0x04, 0, 1, 0, 0))
                await client_writer.drain()
                return

            # SOCKS5 handshake with worker
            worker_writer.write(struct.pack("!BB", 5, 1))
            worker_writer.write(struct.pack("!B", 0x00))  # No Auth
            await worker_writer.drain()

            resp = await worker_reader.readexactly(2)
            if resp[1] != 0x00:
                client_writer.write(struct.pack("!BBBBIH", 5, 0x01, 0, 1, 0, 0))
                await client_writer.drain()
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
                return

            upstream_connected = True

            # === Step 5: Bidirectional relay ===
            await asyncio.gather(
                self._pipe(client_reader, worker_writer),
                self._pipe(worker_reader, client_writer),
            )

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception:
            pass
        finally:
            self.active_connections -= 1
            if not client_writer.is_closing():
                client_writer.close()
            if worker_writer and not worker_writer.is_closing():
                worker_writer.close()

    @staticmethod
    async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Forwards data from reader to writer until EOF."""
        try:
            while True:
                data = await reader.read(8192)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except Exception:
            pass
