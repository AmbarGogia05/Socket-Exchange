# Bonus (§6.9) client-generation program.
# Opens idle TCP connections to the Exchange Server in stages, pausing after
# each stage so measurements can be taken while a known number are established.
# Connections are spread across the server's ports round-robin so each
# 4-tuple stays unique (a single client IP + one server port tops out < 65,536).
#
# Usage:
#     python3 connection_flood.py <host> <step> <port> [port ...]
# Adds <step> connections, then waits for Enter before adding the next <step>.
# Ctrl-C (or EOF at the prompt) stops and releases everything.

import resource
import socket
import sys
import time


def raise_fd_limit(want):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(max(soft, want), hard), hard))
    except (ValueError, OSError) as e:
        print(f"could not raise fd limit: {e}", file=sys.stderr)
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    print(f"fd limit: soft={soft} hard={hard}", file=sys.stderr)


def add_connections(host, ports, sockets, count):
    """Open `count` more connections; return how many succeeded."""
    added = 0
    retries = 0
    while added < count:
        port = ports[len(sockets) % len(ports)]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except OSError as e:               # ENOBUFS/EMFILE creating the socket
            print(f"stopped (socket create): {e}", file=sys.stderr)
            break
        try:
            sock.connect((host, port))
        except OSError as e:               # often transient: backlog full -> ECONNRESET
            sock.close()
            retries += 1
            if retries > 500:              # persistent -> real ceiling, give up
                print(f"stopped (connect, gave up): {e}", file=sys.stderr)
                break
            time.sleep(0.002)              # let the server drain its accept queue
            continue
        sockets.append(sock)
        added += 1
        retries = 0                        # progress made; reset the retry budget
    return added


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)

    host = sys.argv[1]
    step = int(sys.argv[2])
    ports = [int(p) for p in sys.argv[3:]]

    raise_fd_limit(step * 8 + 64)          # headroom; raise the hard limit for more

    sockets = []
    try:
        while True:
            add_connections(host, ports, sockets, step)
            print(f"established {len(sockets)} connections", flush=True)
            print("Take measurements now. Press Enter to add more, Ctrl-C to stop.")
            input()
    except (KeyboardInterrupt, EOFError):
        print(f"\nMaximum connections successfully established: {len(sockets)}")
    finally:
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass


if __name__ == "__main__":
    main()
