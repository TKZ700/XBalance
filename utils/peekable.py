import asyncio


class PeekableStreamReader:
    """Wraps an asyncio.StreamReader to support peek + unread.

    After peeking bytes, they can be unread back into the buffer
    so subsequent reads see them in order.
    """

    def __init__(self, reader: asyncio.StreamReader):
        self._reader = reader
        self._buffer = bytearray()

    async def peek(self, n: int, timeout: float = 2.0) -> bytes:
        """Read up to n bytes without consuming them (they stay in the buffer)."""
        data = await asyncio.wait_for(self._reader.read(n), timeout=timeout)
        self._buffer.extend(data)
        return bytes(data)

    def unread(self, data: bytes):
        """Put bytes back at the front of the read buffer."""
        self._buffer = bytearray(data) + self._buffer

    async def read(self, n: int = -1) -> bytes:
        """Read from buffer first, then underlying reader."""
        if n == -1:
            # Read all available
            if self._buffer:
                buf = bytes(self._buffer)
                self._buffer.clear()
                rest = await self._reader.read(-1)
                return buf + rest
            return await self._reader.read(-1)

        if not self._buffer:
            return await self._reader.read(n)

        if len(self._buffer) >= n:
            result = bytes(self._buffer[:n])
            self._buffer = self._buffer[n:]
            return result

        result = bytes(self._buffer)
        self._buffer.clear()
        remaining = await self._reader.read(n - len(result))
        return result + remaining

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes, pulling from buffer then underlying reader."""
        if len(self._buffer) >= n:
            result = bytes(self._buffer[:n])
            self._buffer = self._buffer[n:]
            return result

        result = bytes(self._buffer)
        self._buffer.clear()
        remaining = await self._reader.readexactly(n - len(result))
        return result + remaining

    async def readline(self) -> bytes:
        """Read a line, pulling from buffer then underlying reader."""
        newline_pos = self._buffer.find(b"\n")
        if newline_pos >= 0:
            end = newline_pos + 1
            result = bytes(self._buffer[:end])
            self._buffer = self._buffer[end:]
            return result

        buf = bytes(self._buffer)
        self._buffer.clear()
        rest = await self._reader.readline()
        return buf + rest

    @property
    def at_eof(self) -> bool:
        return not self._buffer and self._reader.at_eof

    def get_extra_info(self, key: str, default=None):
        return self._reader.transport.get_extra_info(key, default) if self._reader.transport else default

    @property
    def transport(self):
        return self._reader.transport
