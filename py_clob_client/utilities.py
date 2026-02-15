import hashlib
from enum import Enum
from .clob_types import OrderBookSummary, OrderSummary, TickSize


def parse_raw_orderbook_summary(raw_obs: any) -> OrderBookSummary:
    bids = []
    for bid in raw_obs["bids"]:
        bids.append(OrderSummary(size=bid["size"], price=bid["price"]))

    asks = []
    for ask in raw_obs["asks"]:
        asks.append(OrderSummary(size=ask["size"], price=ask["price"]))

    orderbookSummary = OrderBookSummary(
        market=raw_obs["market"],
        asset_id=raw_obs["asset_id"],
        timestamp=raw_obs["timestamp"],
        bids=bids,
        asks=asks,
        hash=raw_obs["hash"],
    )

    return orderbookSummary


def generate_orderbook_summary_hash(orderbook: OrderBookSummary) -> str:
    orderbook.hash = ""
    hash = hashlib.sha1(str(orderbook.json).encode("utf-8")).hexdigest()
    orderbook.hash = hash
    return hash


# #修改为允许枚举类型，防止类型警告
def order_to_json(order, owner, orderType, postOnly: bool = False) -> dict:
    body = {
        "order": order.dict(),
        "owner": owner,
        "orderType": orderType.value if isinstance(orderType, Enum) else orderType
    }
    
    # 只有当 post_only 为 True 时，才动态添加该字段,为了独立兼容旧版本
    if postOnly:
        body["postOnly"] = True
        
    return body

def is_tick_size_smaller(a: TickSize, b: TickSize) -> bool:
    return float(a) < float(b)


def price_valid(price: float, tick_size: TickSize) -> bool:
    return price >= float(tick_size) and price <= 1 - float(tick_size)
