from order_book import Order, OrderBook
import socket, select, sys, time


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


class ClientState:
    def __init__(self, connection_id):
        self.connection_id = connection_id   # stable identity; never reused (unlike fd)
        self.role = "undecided"
        self.recv_buffer = b""
        self.username = None
        self.subscriptions = set()


class Server:
    LISTEN_BACKLOG = 4096               # large backlog for the bonus connection storm

    def __init__(self, host, ports):
        self.host = host
        self.ports = ports              # one or more ports; multi-port lifts the
                                        # single 4-tuple (~65k) connection ceiling
        self.order_book = OrderBook()
        self.connections = {}               # fd -> socket
        self.client_states = {}             # fd -> ClientState
        self.taken_usernames = set()
        self.subscribers = {"JNST": set(), "IMCT": set()}
        self.next_connection_id = 0         # monotonic; owner handles come from here
        self.connection_id_to_fd = {}       # connection_id -> current fd
        self.listeners = {}                 # listener fd -> listener socket
        self.k_queue = select.kqueue()

        for port in self.ports:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, port))
            listener.listen(self.LISTEN_BACKLOG)
            listener.setblocking(False)
            self.listeners[listener.fileno()] = listener

            reg = select.kevent(listener.fileno(),
                                filter=select.KQ_FILTER_READ,
                                flags=select.KQ_EV_ADD)
            self.k_queue.control([reg], 0, None)
            log(f"LISTENING on {self.host}:{port} (listener fd={listener.fileno()})")

    def run(self):
        while True:
            events = self.k_queue.control(None, 32, None)
            for event in events:
                fd = event.ident
                if fd in self.listeners:
                    self.on_accept(self.listeners[fd], event.data)
                elif fd in self.connections:
                    self.on_read(fd)
                else:
                    continue                # stale event for a torn-down fd

    def on_accept(self, listener, count):
        for _ in range(count):
            try:
                connection, address = listener.accept()
            except OSError as e:            # EMFILE / ECONNABORTED / EWOULDBLOCK
                log(f"ACCEPT skipped: {e}")
                break
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
            log(f"ACCEPT fd={connection_fd} conn_id={connection_id} from {address[0]}:{address[1]}")

    def on_read(self, fd):
        state = self.client_states[fd]
        try:
            chunk = self.connections[fd].recv(4096)
        except OSError as e:
            log(f"RESET fd={fd} (abrupt close: {e})")
            self.terminate(fd)
            return
        if not chunk:                       # orderly EOF (FIN)
            log(f"EOF fd={fd} (orderly close)")
            self.terminate(fd)
            return

        log(f"RECV fd={fd} {len(chunk)} bytes: {chunk!r}")
        state.recv_buffer += chunk
        while b"\n" in state.recv_buffer:
            line, state.recv_buffer = state.recv_buffer.split(b"\n", 1)
            text = line.decode().strip()
            if text:
                log(f"CMD  fd={fd}: {text!r}")
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
        log(f"TEARDOWN fd={fd}")

    def send(self, fd, msg):
        try:
            self.connections[fd].sendall((msg + "\n").encode())
            log(f"SEND fd={fd}: {msg!r}")
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
            elif command == "SUBSCRIBE" or command == "UNSUBSCRIBE":
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
            if state.username is not None:
                self.send(fd, "ERROR already logged in")
                return
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
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    ports = [int(p) for p in sys.argv[2:]] if len(sys.argv) > 2 else [5000]
    Server(host, ports).run()