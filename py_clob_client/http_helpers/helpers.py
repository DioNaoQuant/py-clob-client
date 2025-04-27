import requests
import aiohttp
from typing import Dict, Any, Optional, Union

from py_clob_client.clob_types import (
    DropNotificationParams,
    BalanceAllowanceParams,
    OrderScoringParams,
    OrdersScoringParams,
    TradeParams,
    OpenOrderParams,
)

from ..exceptions import PolyApiException

GET = "GET"
POST = "POST"
DELETE = "DELETE"
PUT = "PUT"


def overloadHeaders(method: str, headers: dict) -> dict:
    if headers is None:
        headers = dict()
    headers["User-Agent"] = "py_clob_client"

    headers["Accept"] = "*/*"
    headers["Connection"] = "keep-alive"
    headers["Content-Type"] = "application/json"

    if method == GET:
        headers["Accept-Encoding"] = "gzip"

    return headers


async def _read_error(resp: aiohttp.ClientResponse) -> str:
    """
    尝试优先解析 JSON，其次获取纯文本；都失败就退回 reason/status
    """
    try:
        return await resp.json()          # 已是 dict/str，可自行 str() 处理
    except aiohttp.ContentTypeError:
        try:
            return await resp.text()
        except Exception:
            return resp.reason or f"HTTP Error: {resp.status}"

async def request(endpoint: str, method: str,
                  headers=None, data=None, session: aiohttp.ClientSession | None = None):
    headers = overloadHeaders(method, headers)

    own_session = False
    if session is None:                   # 兼容旧调用
        session = aiohttp.ClientSession()
        own_session = True

    try:
        async with session.request(method, endpoint,
                                   headers=headers,
                                   json=data if data else None) as resp:

            if resp.status != 200:
                err_msg = await _read_error(resp)
                raise PolyApiException(resp, err_msg)   # 同时保留 resp 和 msg

            try:
                return await resp.json()
            except aiohttp.ContentTypeError:
                return await resp.text()
    finally:
        if own_session:                   # 仅当内部创建时才关闭
            await session.close()



async def get(endpoint, headers=None, session=None):
    """发送GET请求，支持会话复用"""
    return await request(endpoint, "GET", headers, session=session)


async def post(endpoint, headers=None, data=None, session=None):
    """发送POST请求，支持会话复用"""
    return await request(endpoint, "POST", headers, data, session=session)


async def delete(endpoint, headers=None, data=None, session=None):
    """发送DELETE请求，支持会话复用"""
    return await request(endpoint, "DELETE", headers, data, session=session)


def build_query_params(url: str, param: str, val: str) -> str:
    url_with_params = url
    last = url_with_params[-1]
    # if last character in url string == "?", append the param directly: api.com?param=value
    if last == "?":
        url_with_params = "{}{}={}".format(url_with_params, param, val)
    else:
        # else add "&", then append the param
        url_with_params = "{}&{}={}".format(url_with_params, param, val)
    return url_with_params


def add_query_trade_params(
    base_url: str, params: TradeParams = None, next_cursor="MA=="
) -> str:
    """
    Adds query parameters to a url
    """
    url = base_url
    url = url + "?"
    if params:
        if params.market:
            url = build_query_params(url, "market", params.market)
        if params.asset_id:
            url = build_query_params(url, "asset_id", params.asset_id)
        if params.after:
            url = build_query_params(url, "after", params.after)
        if params.before:
            url = build_query_params(url, "before", params.before)
        if params.maker_address:
            url = build_query_params(url, "maker_address", params.maker_address)
        if params.id:
            url = build_query_params(url, "id", params.id)
    if next_cursor:
        url = build_query_params(url, "next_cursor", next_cursor)
    return url


def add_query_open_orders_params(
    base_url: str, params: OpenOrderParams = None, next_cursor="MA=="
) -> str:
    """
    Adds query parameters to a url
    """
    url = base_url
    url = url + "?"
    if params:
        if params.market:
            url = build_query_params(url, "market", params.market)
        if params.asset_id:
            url = build_query_params(url, "asset_id", params.asset_id)
        if params.id:
            url = build_query_params(url, "id", params.id)
    if next_cursor:
        url = build_query_params(url, "next_cursor", next_cursor)
    return url


def drop_notifications_query_params(
    base_url: str, params: DropNotificationParams = None
) -> str:
    """
    Adds query parameters to a url
    """
    url = base_url
    if params:
        url = url + "?"
        if params.ids:
            url = build_query_params(url, "ids", ",".join(params.ids))
    return url


def add_balance_allowance_params_to_url(
    base_url: str, params: BalanceAllowanceParams = None
) -> str:
    """
    Adds query parameters to a url
    """
    url = base_url
    if params:
        url = url + "?"
        if params.asset_type:
            url = build_query_params(url, "asset_type", params.asset_type.__str__())
        if params.token_id:
            url = build_query_params(url, "token_id", params.token_id)
        if params.signature_type is not None:
            url = build_query_params(url, "signature_type", params.signature_type)
    return url


def add_order_scoring_params_to_url(
    base_url: str, params: OrderScoringParams = None
) -> str:
    """
    Adds query parameters to a url
    """
    url = base_url
    if params:
        url = url + "?"
        if params.orderId:
            url = build_query_params(url, "order_id", params.orderId)
    return url


def add_orders_scoring_params_to_url(
    base_url: str, params: OrdersScoringParams = None
) -> str:
    """
    Adds query parameters to a url
    """
    url = base_url
    if params:
        url = url + "?"
        if params.orderIds:
            url = build_query_params(url, "order_ids", ",".join(params.orderIds))
    return url
