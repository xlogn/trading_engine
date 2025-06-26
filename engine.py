import heapq
import logging
from dataclasses import dataclass, field
from typing import List, Literal

# ——— Data models ———

@dataclass(order=True)
class HeapItem:
    # The heap will sort by priority first, then count (to break ties in insertion order)
    priority: float
    count: int
    order: "Order" = field(compare=False)

@dataclass
class Order:
    type_op: Literal["CREATE"]
    account_id: str
    amount: int
    order_id: str
    pair: str
    limit_price: float
    side: Literal["BUY", "SELL"]

@dataclass
class Trade:
    buy_order: Order
    sell_order: Order
    amount_traded: int
    selling_price: float

@dataclass
class OrderBookEntry:
    order_id: str
    side: str
    amount: int
    price: float
    account_id: str

# ——— Core matching engine ———

class OrderOrch:
    def __init__(self):
        self.buy_heap: List[HeapItem] = []
        self.sell_heap: List[HeapItem] = []
        self.trades: List[Trade] = []
        self.order_book: List[OrderBookEntry] = []
        self._counter = 0
        logging.basicConfig(level=logging.INFO)

    def add_order(self, order: Order):
        """Insert an order into the appropriate heap, then attempt matching."""
        self._counter += 1
        if order.side == "BUY":
            # Max-heap for buys: invert the price
            item = HeapItem(priority=-order.limit_price, count=self._counter, order=order)
            heapq.heappush(self.buy_heap, item)
        else:
            # Min-heap for sells: natural price order
            item = HeapItem(priority=order.limit_price, count=self._counter, order=order)
            heapq.heappush(self.sell_heap, item)
        self._match_orders()

    def _pop_order(self, heap: List[HeapItem]) -> Order:
        """Helper to pop a HeapItem and return its Order."""
        return heapq.heappop(heap).order

    def _push_order(self, order: Order):
        """Helper to re‐insert a leftover order back into the correct heap."""
        self._counter += 1
        if order.side == "BUY":
            heapq.heappush(self.buy_heap,
                           HeapItem(priority=-order.limit_price, count=self._counter, order=order))
        else:
            heapq.heappush(self.sell_heap,
                           HeapItem(priority=order.limit_price, count=self._counter, order=order))

    def _match_orders(self):
        """Try to match as many orders as possible."""
        while True:
            removed_for_matching: List[Order] = []
            trade_occurred = False

            # Drain top of heaps while possible
            while self.buy_heap and self.sell_heap:
                buy = self._pop_order(self.buy_heap)
                sell = self._pop_order(self.sell_heap)

                # Skip same‐account matches by parking the buy order
                if buy.account_id == sell.account_id:
                    removed_for_matching.append(buy)
                    self._push_order(sell)
                    continue

                # If prices cross, execute trade
                if buy.limit_price >= sell.limit_price:
                    qty = min(buy.amount, sell.amount)
                    price = sell.limit_price
                    logging.info(f"Trade: {qty}@{price} between {buy.account_id}↔{sell.account_id}")

                    # Record the trade
                    trade = Trade(buy_order=buy, sell_order=sell,
                                  amount_traded=qty, selling_price=price)
                    self.trades.append(trade)

                    # Record in order book
                    self.order_book.append(OrderBookEntry(
                        order_id=buy.order_id, side=buy.side,
                        amount=qty, price=price, account_id=buy.account_id))
                    self.order_book.append(OrderBookEntry(
                        order_id=sell.order_id, side=sell.side,
                        amount=qty, price=price, account_id=sell.account_id))

                    # Push back any remaining quantity
                    buy.amount -= qty
                    sell.amount -= qty
                    if buy.amount > 0:
                        self._push_order(buy)
                    if sell.amount > 0:
                        self._push_order(sell)

                    trade_occurred = True
                else:
                    # No price match: put them back and stop
                    self._push_order(buy)
                    self._push_order(sell)
                    break

            # Re‐insert any parked orders for next pass
            for o in removed_for_matching:
                self._push_order(o)

            if not trade_occurred:
                break

