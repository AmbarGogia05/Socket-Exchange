import socket, select, sys

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    username = sys.argv[3] if len(sys.argv) > 3 else None

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    if username:                     # auto-login if a name was passed
        sock.sendall(f"LOGIN {username}\n".encode())

    recv_buffer = b""
    inputs = [sock, sys.stdin]

    while True:
        readable, _, _ = select.select(inputs, [], [])
        for source in readable:
            if source is sock:
                chunk = sock.recv(4096)
                if not chunk:                    # server closed
                    print("server closed connection")
                    return
                recv_buffer += chunk
                while b"\n" in recv_buffer:
                    line, recv_buffer = recv_buffer.split(b"\n", 1)
                    print(line.decode().strip())
            else:                                # stdin: user typed a command
                text = source.readline()
                if not text:                     # EOF on stdin (Ctrl-D)
                    sock.sendall(b"QUIT\n")
                    return
                text = text.strip()
                if text:
                    sock.sendall((text + "\n").encode())
                if text.upper() == "QUIT":
                    return

if __name__ == "__main__":
    main()