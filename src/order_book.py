class Trade:
    instrument: str
    quantity: int
    price: int
    buy_order_id: int
    sell_order_id: int
    buyer: int          # owner handle (fd) that gets BOUGHT
    seller: int         # owner handle (fd) that gets SOLD


class Order:
    def __init__(self, order_id, owner, type, instrument, price, quantity):
        self.order_id = order_id
        self.owner = owner          # eventual fd; book ignores it, just carries it
        self.type = type            # "BUY" | "SELL"
        self.instrument = instrument
        self.price = price
        self.quantity = quantity    # remaining quantity


class OrderBook:
    def __init__(self):
        self.order_dicts = {"BUY":  {"JNST": {}, "IMCT": {}},
                            "SELL": {"JNST": {}, "IMCT": {}}}
        self.orders = {}            # order_id -> Order, so cancel() can find resting orders

    def validate_order(self, order_id, type, instrument, price, quantity):
        if type not in ("BUY", "SELL"):
            return False, "Invalid order type"
        if instrument not in ("JNST", "IMCT"):
            return False, "Invalid instrument"
        if not (0 <= order_id <= 2**31 - 1):
            return False, "Order ID out of range"
        if not (1 <= price <= 2**31 - 1):
            return False, "Price out of range"
        if not (1 <= quantity <= 2**31 - 1):
            return False, "Quantity out of range"
        return True, ""

    def process_order(self, new_order):
        matching_type = "SELL" if new_order.type == "BUY" else "BUY"
        resting = self.order_dicts[matching_type][new_order.instrument].get(
            new_order.price, [])

        trades = []
        # front-of-list FIFO, remove from front — no mutate-while-iterating.
        while new_order.quantity > 0 and resting:
            book_order = resting[0]
            qty = min(book_order.quantity, new_order.quantity)
            book_order.quantity -= qty
            new_order.quantity -= qty

            # figure out which side is buy/sell for routing BOUGHT/SOLD
            if new_order.type == "BUY":
                buyer, seller = new_order, book_order
            else:
                buyer, seller = book_order, new_order
            trades.append(Trade(
                instrument=new_order.instrument,
                quantity=qty,
                price=new_order.price,
                buy_order_id=buyer.order_id,
                sell_order_id=seller.order_id,
                buyer=buyer.owner,
                seller=seller.owner,
            ))

            if book_order.quantity == 0:
                resting.pop(0)
                del self.orders[book_order.order_id]

        # rest whatever's left, storing Order objects (not tuples)
        if new_order.quantity > 0:
            self.order_dicts[new_order.type][new_order.instrument] \
                .setdefault(new_order.price, []).append(new_order)
            self.orders[new_order.order_id] = new_order

        return trades

    def cancel(self, owner, order_id):
        order = self.orders.get(order_id)
        if order is None:
            return False, "No such active order"      # filled, already cancelled, or never existed
        if order.owner != owner:
            return False, "You cannot cancel others' orders"            # a trader can't cancel someone else's
        price_list = self.order_dicts[order.type][order.instrument][order.price]
        price_list.remove(order)
        del self.orders[order_id]
        return True, ""

    def submit(self, owner, type, instrument, price, quantity):
        ok, reason = self.validate_order(type, instrument, price, quantity)
        if not ok:
            return False, reason, None, []            # rejected: handle() -> ERROR reason
        order_id = self._next_id                        # assigned ONLY on success — nothing to roll back
        self._next_id += 1
        order = Order(order_id, owner, type, instrument, price, quantity)
        trades = self.process_order(order)
        return True, "", order_id, trades               # handle() -> ORDER_ACCEPTED id, then route trades