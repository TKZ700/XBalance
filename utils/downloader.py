import asyncio
import logging
from typing import List, Optional, Tuple

log = logging.getLogger("xbalance.downloader")

PARALLEL_THRESHOLD = 10 * 1024 * 1024  # 10MB default


class ParallelDownloader:
    """Splits large HTTP downloads across multiple Xray workers using Range requests."""

    def __init__(self, worker_ports: List[int], threshold: int = PARALLEL_THRESHOLD):
        self.worker_ports = worker_ports
        self.threshold = threshold

    async def download(
        self,
        url: str,
        client_writer,
        headers: dict,
        method: str = "GET",
    ) -> bool:
        """Attempt parallel download. Returns True if parallel was used."""
        if len(self.worker_ports) < 2:
            return False

        parsed = self._parse_url(url)
        if not parsed:
            return False

        host, port, path, use_tls = parsed

        file_size, accepts_ranges = await self._probe(
            host, port, path, use_tls, headers
        )
        if file_size is None or not accepts_ranges or file_size < self.threshold:
            return False

        log.info(
            "Parallel download: %s (%d bytes) across %d workers",
            url,
            file_size,
            len(self.worker_ports),
        )

        chunk_size = file_size // len(self.worker_ports)
        ranges: List[Tuple[int, int, int]] = []

        for i, worker_port in enumerate(self.worker_ports):
            start = i * chunk_size
            end = (start + chunk_size - 1) if i < len(self.worker_ports) - 1 else file_size - 1
            ranges.append((start, end, worker_port))

        try:
            chunks = await asyncio.gather(
                *[self._fetch_chunk(host, port, path, use_tls, s, e, w, headers) for s, e, w in ranges]
            )
        except Exception as e:
            log.warning("Parallel download failed, falling back: %s", e)
            return False

        if any(c is None for c in chunks):
            log.warning("One or more chunks failed, falling back")
            return False

        for chunk in chunks:
            client_writer.write(chunk)
        await client_writer.drain()

        log.info("Parallel download complete: %s (%d bytes)", url, file_size)
        return True

    def _parse_url(self, url: str) -> Optional[Tuple[str, int, str, bool]]:
        """Parse http(s)://host:port/path into components."""
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

    async def _probe(
        self,
        host: str,
        port: int,
        path: str,
        use_tls: bool,
        request_headers: dict,
    ) -> Tuple[Optional[int], bool]:
        """Send HEAD request to check file size and Range support."""
        try:
            reader, writer = await asyncio.open_connection(host, port, ssl=use_tls)

            head_request = (
                f"HEAD {path} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"Connection: close\r\n"
            )
            for k, v in request_headers.items():
                if k.lower() not in ("host", "connection", "range"):
                    head_request += f"{k}: {v}\r\n"
            head_request += "\r\n"

            writer.write(head_request.encode())
            await writer.drain()

            response = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
            writer.close()

            status_line = response.split(b"\r\n")[0].decode("utf-8", errors="replace")
            if "200" not in status_line and "206" not in status_line:
                return None, False

            headers_text = response.decode("utf-8", errors="replace").lower()
            file_size = None
            accepts_ranges = False

            for line in response.split(b"\r\n"):
                line_str = line.decode("utf-8", errors="replace").lower()
                if line_str.startswith("content-length:"):
                    try:
                        file_size = int(line_str.split(":", 1)[1].strip())
                    except ValueError:
                        pass
                if line_str.startswith("accept-ranges:") and "bytes" in line_str:
                    accepts_ranges = True

            return file_size, accepts_ranges
        except Exception as e:
            log.debug("Probe failed for %s:%d: %s", host, port, e)
            return None, False

    async def _fetch_chunk(
        self,
        host: str,
        port: int,
        path: str,
        use_tls: bool,
        start: int,
        end: int,
        worker_port: int,
        request_headers: dict,
    ) -> Optional[bytes]:
        """Fetch a byte range through a specific local Xray worker."""
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", worker_port)

            connect_req = (
                f"CONNECT {host}:{port} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n\r\n"
            )
            writer.write(connect_req.encode())
            await writer.drain()

            resp = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
            status_line = resp.split(b"\r\n")[0].decode("utf-8", errors="replace")
            if "200" not in status_line:
                writer.close()
                return None

            range_header = f"Range: bytes={start}-{end}\r\n"
            get_request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"{range_header}"
                f"Connection: close\r\n"
            )
            for k, v in request_headers.items():
                if k.lower() not in ("host", "connection", "range"):
                    get_request += f"{k}: {v}\r\n"
            get_request += "\r\n"

            writer.write(get_request.encode())
            await writer.drain()

            header_data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=30)

            body_start = 0
            content_length = None
            for line in header_data.split(b"\r\n"):
                line_str = line.decode("utf-8", errors="replace").lower()
                if line_str.startswith("content-length:"):
                    try:
                        content_length = int(line_str.split(":", 1)[1].strip())
                    except ValueError:
                        pass

            body = bytearray()
            body_data = header_data.split(b"\r\n\r\n", 1)
            if len(body_data) > 1:
                body.extend(body_data[1])

            if content_length is not None:
                while len(body) < content_length:
                    chunk = await asyncio.wait_for(
                        reader.read(min(1048576, content_length - len(body))),
                        timeout=30,
                    )
                    if not chunk:
                        break
                    body.extend(chunk)
            else:
                while True:
                    chunk = await asyncio.wait_for(reader.read(1048576), timeout=30)
                    if not chunk:
                        break
                    body.extend(chunk)

            writer.close()
            return bytes(body)

        except Exception as e:
            log.warning("Chunk fetch failed (worker %d, range %d-%d): %s", worker_port, start, end, e)
            return None
