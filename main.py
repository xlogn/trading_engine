from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import Literal, List, Dict

from engine import OrderOrch, Order, Trade, OrderBookEntry

# ---- Pydantic models ----
class OrderIn(BaseModel):
    type_op: Literal["CREATE"]
    account_id: str
    amount: int = Field(..., gt=0)
    order_id: str
    pair: str
    limit_price: float = Field(..., gt=0)
    side: Literal["BUY", "SELL"]

class TradeOut(BaseModel):
    buy_order: dict
    sell_order: dict
    amount_traded: int
    selling_price: float

class OrderBookEntryOut(BaseModel):
    order_id: str
    side: str
    amount: int
    price: float
    account_id: str

class PendingOrder(BaseModel):
    order_id: str
    account_id: str
    pair: str
    side: Literal["BUY", "SELL"]
    amount: int
    limit_price: float

# ---- App & Engine ----
app = FastAPI(title="Order Matching API")
engine = OrderOrch()
# Track all seen account_ids
account_ids_set = set()

# ---- Endpoints ----
@app.post("/orders", status_code=201)
def create_order(order_in: OrderIn):
    """
    Submit a new order. Attempts to match immediately.
    """
    order = Order(
        type_op=order_in.type_op,
        account_id=order_in.account_id,
        amount=order_in.amount,
        order_id=order_in.order_id,
        pair=order_in.pair,
        limit_price=order_in.limit_price,
        side=order_in.side,
    )
    engine.add_order(order)
    account_ids_set.add(order.account_id)
    return {"message": "Order accepted and processed"}

@app.get("/trades", response_model=List[TradeOut])
def list_trades():
    """
    List all executed trades.
    """
    out: List[TradeOut] = []
    for t in engine.trades:
        out.append(TradeOut(
            buy_order=t.buy_order.__dict__,
            sell_order=t.sell_order.__dict__,
            amount_traded=t.amount_traded,
            selling_price=t.selling_price,
        ))
    return out

@app.get("/orderbook", response_model=List[OrderBookEntryOut])
def list_orderbook():
    """
    List all order book entries from trades.
    """
    return [OrderBookEntryOut(**e.__dict__) for e in engine.order_book]

@app.get("/orders/pending", response_model=Dict[str, List[PendingOrder]])
def list_all_pending_orders():
    """
    Return all unmatched (pending) orders grouped by account_id.
    """
    result: Dict[str, List[PendingOrder]] = {}
    # Iterate over every known account
    for acct in account_ids_set:
        pending: List[PendingOrder] = []
        # Scan buy heap
        for item in engine.buy_heap:
            o = item.order
            if o.account_id == acct:
                pending.append(PendingOrder(
                    order_id=o.order_id,
                    account_id=o.account_id,
                    pair=o.pair,
                    side=o.side,
                    amount=o.amount,
                    limit_price=o.limit_price,
                ))
        # Scan sell heap
        for item in engine.sell_heap:
            o = item.order
            if o.account_id == acct:
                pending.append(PendingOrder(
                    order_id=o.order_id,
                    account_id=o.account_id,
                    pair=o.pair,
                    side=o.side,
                    amount=o.amount,
                    limit_price=o.limit_price,
                ))
        result[acct] = pending
    return result

# To run:
# uvicorn main:app --reload
