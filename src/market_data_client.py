import socket, select, sys

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    instrument = sys.argv[3] if len(sys.argv) > 3 else None

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    if instrument:                   # auto-subscribe if an instrument was passed
        sock.sendall(f"SUBSCRIBE {instrument}\n".encode())

    recv_buffer = b""
    inputs = [sock, sys.stdin]

    while True:
        readable, _, _ = select.select(inputs, [], [])
        for source in readable:
            if source is sock:
                chunk = sock.recv(4096)
                if not chunk:
                    print("server closed connection")
                    return
                recv_buffer += chunk
                while b"\n" in recv_buffer:
                    line, recv_buffer = recv_buffer.split(b"\n", 1)
                    print(line.decode().strip())
            else:
                text = source.readline()
                if not text:
                    sock.sendall(b"QUIT\n")
                    return
                text = text.strip()
                if text:
                    sock.sendall((text + "\n").encode())
                if text.upper() == "QUIT":
                    return

if __name__ == "__main__":
    main()