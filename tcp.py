import argparse
import logging
import socket
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tcp_client")

MAX_RESPONSE_SIZE = 64 * 1024 # same size as the server can modify
RECV_CHUNK = 4096


def recv_all(sock: socket.socket) -> bytes:
    # Read until server closes connection or hit limit
    chunks = []
    total = 0
    while True:
        chunk = sock.recv(RECV_CHUNK)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_RESPONSE_SIZE:
            log.warning("Response exceeded %d bytes, truncating", MAX_RESPONSE_SIZE)
            chunks.append(chunk[: MAX_RESPONSE_SIZE - (total - len(chunk))])
            break
        chunks.append(chunk)
    return b"".join(chunks)


def send_message(host: str, port: int, message: bytes, timeout: float, retries: int, backoff: float) -> bytes | None:
    attempt = 0
    while True:
        attempt += 1
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.settimeout(timeout)
                log.info("Connecting to %s:%d (attempt %d)", host, port, attempt)
                client.connect((host, port))

                client.sendall(message)
                log.info("Sent %d bytes", len(message))

                response = recv_all(client)
                log.info("Received %d bytes", len(response))
                return response

        except socket.timeout:
            log.warning("Connection/response timed out")
        except ConnectionRefusedError:
            log.warning("Connection refused by %s:%d", host, port)
        except socket.gaierror as e:
            log.error("Hostname resolution failed for %s: %s", host, e)
            return None
        except OSError as e:
            log.warning("Socket error: %s", e)

        if attempt > retries:
            log.error("Giving up after %d attempt(s)", attempt)
            return None

        sleep_for = backoff * attempt
        log.info("Retrying in %.1fs...", sleep_for)
        time.sleep(sleep_for)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple TCP client")
    parser.add_argument("--host", default="127.0.0.1", help="Target host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9999, help="Target port (default: 9999)")
    parser.add_argument(
        "--message",
        default="GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        help="Message to send (default: a basic HTTP GET request line)",
    )
    parser.add_argument("--timeout", type=float, default=5.0, help="Socket timeout in seconds (default: 5)")
    parser.add_argument("--retries", type=int, default=2, help="Number of retries on failure (default: 2)")
    parser.add_argument("--backoff", type=float, default=1.0, help="Base backoff seconds between retries (default: 1)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)

    response = send_message(
        host=args.host,
        port=args.port,
        message=args.message.encode(),
        timeout=args.timeout,
        retries=args.retries,
        backoff=args.backoff,
    )

    if response is None:
        sys.exit(1)

    print(response.decode(errors="replace"))


if __name__ == "__main__":
    main()