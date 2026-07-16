import argparse
import logging
import signal
import socket
import sys
import threading
from typing import Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tcp_server")

MAX_MESSAGE_SIZE = 64 * 1024  # 64 KB cap per message
RECV_CHUNK = 4096
CLIENT_TIMEOUT = 30  # seconds of inactivity before a client is dropped


class TCPServer:
    def __init__(self, host: str, port: int, backlog: int = 5):
        self.host = host
        self.port = port
        self.backlog = backlog
        self._sock: socket.socket | None = None
        self._shutdown_event = threading.Event()

    def start(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(self.backlog)
        self._sock.settimeout(1.0)  # accept() can periodically check shutdown flag
        log.info("Listening on %s:%d", self.host, self.port)

        try:
            while not self._shutdown_event.is_set():
                try:
                    client_sock, addr = self._sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                log.info("Accepted connection from %s:%d", addr[0], addr[1])
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_sock, addr),
                    name=f"client-{addr[0]}:{addr[1]}",
                    daemon=True,
                )
                thread.start()
        finally:
            self.stop()

    def stop(self) -> None:
        if self._shutdown_event.is_set():
            return
        self._shutdown_event.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        log.info("Server stopped.")

    def _handle_client(self, client_sock: socket.socket, addr: Tuple[str, int]) -> None:
        client_sock.settimeout(CLIENT_TIMEOUT)
        try:
            with client_sock:
                data = self._recv_all(client_sock)
                if data is None:
                    return
                log.info("Received %d bytes from %s:%d", len(data), addr[0], addr[1])
                log.debug("Payload: %r", data)

                response = self._build_response(data)
                client_sock.sendall(response)
        except socket.timeout:
            log.warning("Connection from %s:%d timed out", addr[0], addr[1])
        except ConnectionResetError:
            log.warning("Connection reset by %s:%d", addr[0], addr[1])
        except Exception:
            log.exception("Unhandled error handling client %s:%d", addr[0], addr[1])

    @staticmethod
    def _recv_all(client_sock: socket.socket) -> bytes | None:
        #Read until the client closes its side or it hits the size limit.
        chunks = []
        total = 0
        while True:
            chunk = client_sock.recv(RECV_CHUNK)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_MESSAGE_SIZE:
                log.warning("Message exceeded %d bytes, truncating", MAX_MESSAGE_SIZE)
                chunks.append(chunk[: MAX_MESSAGE_SIZE - (total - len(chunk))])
                break
            chunks.append(chunk)
        return b"".join(chunks) if chunks else None

    @staticmethod
    def _build_response(data: bytes) -> bytes:
        # can implement a custom logic for this
        return b"ACK!"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple multi-threaded TCP server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=9999, help="Bind port (default: 9999)")
    parser.add_argument("--backlog", type=int, default=5, help="Listen backlog (default: 5)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)

    server = TCPServer(args.host, args.port, args.backlog)

    def _signal_handler(signum, frame):
        log.info("Received signal %s, shutting down...", signum)
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    server.start()


if __name__ == "__main__":
    main()