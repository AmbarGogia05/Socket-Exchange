from order_book import Order, OrderBook
import socket, select

class ClientState:
    def __init__(self, connection_id):
        self.connection_id = connection_id   # stable identity; never reused (unlike fd)
        self.role = "undecided"
        self.recv_buffer = b""
        self.username = None
        self.subscriptions = set()

class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.order_book = OrderBook()
        self.connections = {}               # fd -> socket
        self.client_states = {}             # fd -> ClientState
        self.taken_usernames = set()
        self.subscribers = {"JNST": set(), "IMCT": set()}
        self.next_connection_id = 0         # monotonic; owner handles come from here
        self.connection_id_to_fd = {}       # connection_id -> current fd
        self.listener = None
        self.k_queue = None

        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((self.host, self.port))
        self.listener.listen()
        self.listener.setblocking(False)

        self.k_queue = select.kqueue()
        reg = select.kevent(self.listener.fileno(),
                            filter=select.KQ_FILTER_READ,
                            flags=select.KQ_EV_ADD)
        self.k_queue.control([reg], 0, None)

    def run(self):
        while True:
            events = self.k_queue.control(None, 32, None)
            for event in events:
                fd = event.ident
                if fd == self.listener.fileno():
                    self.on_accept(event.data)
                elif fd in self.connections:
                    self.on_read(fd)
                else:
                    continue                # stale event for a torn-down fd

    def on_accept(self, count):
        for _ in range(count):
            connection, address = self.listener.accept()
            connection.setblocking(False)
            connection_fd = connection.fileno()

            connection_id = self.next_connection_id
            self.next_connection_id += 1

            self.connections[connection_fd] = connection
            self.client_states[connection_fd] = ClientState(connection_id)
            self.connection_id_to_fd[connection_id] = connection_fd

            reg = select.kevent(connection_fd,
                                filter=select.KQ_FILTER_READ,
                                flags=select.KQ_EV_ADD)
            self.k_queue.control([reg], 0, None)

    def on_read(self, fd):
        state = self.client_states[fd]
        chunk = self.connections[fd].recv(4096)
        if not chunk:                       # EOF -> client closed
            self.terminate(fd)
            return

        state.recv_buffer += chunk
        while b"\n" in state.recv_buffer:
            line, state.recv_buffer = state.recv_buffer.split(b"\n", 1)
            text = line.decode().strip()
            if text:
                self.resolve(fd, text)

    def terminate(self, fd):
        state = self.client_states.get(fd)
        if state:
            if state.username:
                self.taken_usernames.discard(state.username)
            for inst in state.subscriptions:
                self.subscribers[inst].discard(fd)
            self.connection_id_to_fd.pop(state.connection_id, None)
        if fd in self.connections:
            self.connections[fd].close()
        self.connections.pop(fd, None)
        self.client_states.pop(fd, None)

    def send(self, fd, msg):
        try:
            self.connections[fd].sendall((msg + "\n").encode())
        except (OSError, KeyError):
            pass                            # socket gone

    def resolve(self, fd, text):
        state = self.client_states[fd]
        parts = text.split()
        if not parts:
            return
        command = parts[0].upper()
        args = parts[1:]

        TRADER_CMDS = {"LOGIN", "BUY", "SELL", "CANCEL", "QUIT"}
        MD_CMDS     = {"SUBSCRIBE", "UNSUBSCRIBE", "QUIT"}

        # role gate
        if state.role == "undecided":
            if command == "LOGIN":
                state.role = "trader"
            elif command == "SUBSCRIBE":
                state.role = "market-data"
            elif command == "QUIT":
                pass
            else:
                self.send(fd, "ERROR must LOGIN or SUBSCRIBE first")
                return
        if state.role == "trader" and command not in TRADER_CMDS:
            self.send(fd, "ERROR command not allowed for trader")
            return
        if state.role == "market-data" and command not in MD_CMDS:
            self.send(fd, "ERROR command not allowed for market-data client")
            return

        if command == "LOGIN":
            if len(args) != 1:
                self.send(fd, "ERROR usage: LOGIN <username>")
                return
            username = args[0]
            if username in self.taken_usernames:
                self.send(fd, "ERROR username is already in use")
                return
            state.username = username
            self.taken_usernames.add(username)
            self.send(fd, "OK")

        elif command in ("BUY", "SELL"):
            if len(args) != 3:
                self.send(fd, f"ERROR usage: {command} <instrument> <quantity> <price>")
                return
            instrument = args[0]
            qty = self._as_int(args[1])
            price = self._as_int(args[2])
            if qty is None or price is None:
                self.send(fd, "ERROR quantity and price must be integers")
                return
            accepted, reason, order_id, trades = self.order_book.submit(
                state.connection_id, command, instrument, price, qty)
            if not accepted:
                self.send(fd, f"ERROR {reason}")
                return
            self.send(fd, f"ORDER_ACCEPTED {order_id}")
            for t in trades:
                if command == "BUY":
                    self.send(fd, f"BOUGHT {t.instrument} {t.quantity} {t.price}")
                    resting_fd = self.connection_id_to_fd.get(t.seller)
                    if resting_fd is not None:
                        self.send(resting_fd, f"SOLD {t.instrument} {t.quantity} {t.price}")
                else:
                    self.send(fd, f"SOLD {t.instrument} {t.quantity} {t.price}")
                    resting_fd = self.connection_id_to_fd.get(t.buyer)
                    if resting_fd is not None:
                        self.send(resting_fd, f"BOUGHT {t.instrument} {t.quantity} {t.price}")
                for subscriber_fd in self.subscribers[t.instrument]:
                    self.send(subscriber_fd, f"TRADE {t.instrument} {t.quantity} {t.price}")

        elif command == "CANCEL":
            if len(args) != 1:
                self.send(fd, "ERROR usage: CANCEL <order_id>")
                return
            order_id = self._as_int(args[0])
            if order_id is None:
                self.send(fd, "ERROR order_id must be an integer")
                return
            ok, reason = self.order_book.cancel(state.connection_id, order_id)
            if ok:
                self.send(fd, f"ORDER_CANCELLED {order_id}")
            else:
                self.send(fd, f"ERROR {reason}")

        elif command in ("SUBSCRIBE", "UNSUBSCRIBE"):
            if len(args) != 1:
                self.send(fd, f"ERROR usage: {command} <instrument>")
                return
            instrument = args[0]
            if instrument not in ("JNST", "IMCT"):
                self.send(fd, f"ERROR {instrument} is not tradeable")
                return
            if command == "SUBSCRIBE":
                self.subscribers[instrument].add(fd)
                state.subscriptions.add(instrument)
            else:
                self.subscribers[instrument].discard(fd)
                state.subscriptions.discard(instrument)
            self.send(fd, "OK")

        elif command == "QUIT":
            self.terminate(fd)

        else:
            self.send(fd, "ERROR unknown command")

    @staticmethod
    def _as_int(token):
        try:
            return int(token)
        except ValueError:
            return None


if __name__ == "__main__":
    import sys
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    Server(host, port).run()