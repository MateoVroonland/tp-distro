import socket

LENGTH_PREFIX_SIZE = 4
MIN_RECV_SIZE = 4096

class CompleteSocket:
    def __init__(self, socket):
        self._sock = socket
        self._buffer = bytearray()

    def append_message_length(self, data):
        data_bytes = data.encode('utf-8')
        length = len(data_bytes)
        length_prefix = length.to_bytes(LENGTH_PREFIX_SIZE, byteorder='big')
        return length_prefix + data_bytes

    def send_all(self, data: str):
        self._buffer.clear()
        final_data = self.append_message_length(data)
        total_sent = 0
        while total_sent < len(final_data):
            sent = self._sock.send(final_data[total_sent:])
            if sent == 0:
                raise OSError
            total_sent += sent
        return total_sent
    
    def recv_length(self):
        data = self._sock.recv(LENGTH_PREFIX_SIZE)
        while data < LENGTH_PREFIX_SIZE:
            if not data:
                raise ConnectionError("Connection closed while reading length prefix")
            data += self._sock.recv(LENGTH_PREFIX_SIZE - len(data))
        return int.from_bytes(data, byteorder='big')

    def recv_exact(self, length):
        self._buffer.clear()
        remaining = length
        
        while remaining > 0:
            chunk = self._sock.recv(min(remaining, MIN_RECV_SIZE))
            
            if not chunk:
                raise ConnectionError(f"Connection closed after reading {length - remaining} of {length} bytes")
            
            self._buffer.extend(chunk)
            remaining -= len(chunk)
        
        return bytes(self._buffer)

    def recv_all(self):
        try:
            length = self.recv_length()
            payload = self.recv_exact(length)
            return payload
        except (ConnectionError, ValueError) as e:
            raise

    def close(self):
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()