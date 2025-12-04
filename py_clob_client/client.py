import logging
from typing import Optional, Dict, ClassVar
import asyncio
import aiohttp

from .order_builder.builder import OrderBuilder
from .headers.headers import create_level_1_headers, create_level_2_headers
from .signer import Signer
from .config import get_contract_config

from .endpoints import (
    CANCEL,
    CANCEL_ORDERS,
    CANCEL_MARKET_ORDERS,
    CANCEL_ALL,
    CREATE_API_KEY,
    DELETE_API_KEY,
    DERIVE_API_KEY,
    GET_API_KEYS,
    CLOSED_ONLY,
    GET_LAST_TRADE_PRICE,
    GET_ORDER,
    GET_ORDER_BOOK,
    MID_POINT,
    ORDERS,
    POST_ORDER,
    POST_ORDERS,
    PRICE,
    TIME,
    TRADES,
    GET_NOTIFICATIONS,
    DROP_NOTIFICATIONS,
    GET_BALANCE_ALLOWANCE,
    UPDATE_BALANCE_ALLOWANCE,
    IS_ORDER_SCORING,
    GET_TICK_SIZE,
    GET_NEG_RISK,
    ARE_ORDERS_SCORING,
    GET_SIMPLIFIED_MARKETS,
    GET_MARKETS,
    GET_MARKET,
    GET_SAMPLING_SIMPLIFIED_MARKETS,
    GET_SAMPLING_MARKETS,
    GET_MARKET_TRADES_EVENTS,
    GET_LAST_TRADES_PRICES,
    MID_POINTS,
    GET_ORDER_BOOKS,
    GET_PRICES,
    GET_SPREAD,
    GET_SPREADS,
    PRICES_HISTORY,
)
from .clob_types import (
    ApiCreds,
    TradeParams,
    OpenOrderParams,
    OrderArgs,
    PostOrdersArgs,
    RequestArgs,
    DropNotificationParams,
    OrderBookSummary,
    BalanceAllowanceParams,
    OrderScoringParams,
    TickSize,
    CreateOrderOptions,
    OrdersScoringParams,
    OrderType,
    PartialCreateOrderOptions,
    BookParams,
    MarketOrderArgs,
)
from .exceptions import PolyException
from .http_helpers.helpers import (
    add_query_trade_params,
    add_query_open_orders_params,
    delete as _origin_delete, 
    get as _origin_get,
    post as _origin_post,
    drop_notifications_query_params,
    add_balance_allowance_params_to_url,
    add_order_scoring_params_to_url,
)

from .constants import L0, L1, L1_AUTH_UNAVAILABLE, L2, L2_AUTH_UNAVAILABLE, END_CURSOR
from .utilities import (
    parse_raw_orderbook_summary,
    generate_orderbook_summary_hash,
    order_to_json,
    is_tick_size_smaller,
    price_valid,
)

# 2. 定义包装函数，统一拦截超时异常
async def get(*args, **kwargs):
    try:
        return await _origin_get(*args, **kwargs)
    except asyncio.TimeoutError:
        raise PolyException("Clob Timeout: Get Request timed out (sock_read > 3.5s)")

async def post(*args, **kwargs):
    try:
        return await _origin_post(*args, **kwargs)
    except asyncio.TimeoutError:
        raise PolyException("Clob Timeout: Post Request timed out (sock_read > 3.5s)")

async def delete(*args, **kwargs):
    try:
        return await _origin_delete(*args, **kwargs)
    except asyncio.TimeoutError:
        raise PolyException("Clob Timeout: Delete Request timed out (sock_read > 3.5s)")


class AsyncClobClient:
    # 单例存储 - 只保存一个实例
    _instance = None
    # 类级别锁，用于确保单例创建的线程安全
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_instance(cls, host, chain_id=None, key=None, creds=None, signature_type=None, funder=None):
        """获取客户端单例实例"""
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls(
                    host, chain_id, key, creds, signature_type, funder
                )
                # 确保创建好会话
                await cls._instance._ensure_session()
            return cls._instance
    
    @classmethod
    async def close_instance(cls):
        """关闭单例实例的会话资源"""
        async with cls._lock:
            if cls._instance and cls._instance._session and not cls._instance._session.closed:
                await cls._instance._session.close()
            cls._instance = None
            
    def __init__(
        self,
        host,
        chain_id: int = None,
        key: str = None,
        creds: ApiCreds = None,
        signature_type: int = None,
        funder: str = None,
    ):
        """
        Initializes the clob client
        The client can be started in 3 modes:
        1) Level 0: Requires only the clob host url
                    Allows access to open CLOB endpoints

        2) Level 1: Requires the host, chain_id and a private key.
                    Allows access to L1 authenticated endpoints + all unauthenticated endpoints

        3) Level 2: Requires the host, chain_id, a private key, and Credentials.
                    Allows access to all endpoints
        """
        self.host = host[0:-1] if host.endswith("/") else host
        self.chain_id = chain_id
        self.signer = Signer(key, chain_id) if key else None
        self.creds = creds
        self.mode = self._get_client_mode()

        if self.signer:
            self.builder = OrderBuilder(
                self.signer, sig_type=signature_type, funder=funder
            )

        # local cache
        self.__tick_sizes = {}
        self.__neg_risk = {}

        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 添加会话对象用于优化连接复用
        self._session = None
        # 添加会话锁以保护并发访问
        self._session_lock = asyncio.Lock()

    async def _ensure_session(self):
        """确保存在有效的会话，使用异步锁保护并发访问"""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                # 创建会话时使用较高的连接限制和超时设置
                conn = aiohttp.TCPConnector(
                    limit=1000,  # 最大同时连接数
                    ttl_dns_cache=300,  # DNS缓存TTL
                    enable_cleanup_closed=True  # 自动清理关闭的连接
                )
                timeout = aiohttp.ClientTimeout(
                    total=8,      # 总超时
                    connect=2,    # 连接超时
                    sock_read=3.5,  # 读取超时
                    sock_connect=2  # 套接字连接超时
                )
                self._session = aiohttp.ClientSession(
                    connector=conn,
                    timeout=timeout,
                    headers={"User-Agent": "py_clob_client"}
                )
            return self._session
        
    async def __aenter__(self):
        await self._ensure_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session and not self._session.closed:
            await self._session.close()
            
    def get_address(self):
        """
        Returns the public address of the signer
        """
        return self.signer.address() if self.signer else None

    def get_collateral_address(self):
        """
        Returns the collateral token address
        """
        contract_config = get_contract_config(self.chain_id)
        if contract_config:
            return contract_config.collateral

    def get_conditional_address(self):
        """
        Returns the conditional token address
        """
        contract_config = get_contract_config(self.chain_id)
        if contract_config:
            return contract_config.conditional_tokens

    def get_exchange_address(self, neg_risk=False):
        """
        Returns the exchange address
        """
        contract_config = get_contract_config(self.chain_id, neg_risk)
        if contract_config:
            return contract_config.exchange

    async def get_ok(self):
        """
        Health check: Confirms that the server is up
        Does not need authentication
        """
        session = await self._ensure_session()
        return await get("{}/".format(self.host), session=session)

    async def get_server_time(self):
        """
        Returns the current timestamp on the server
        Does not need authentication
        """
        session = await self._ensure_session()
        return await get("{}{}".format(self.host, TIME), session=session)

    async def create_api_key(self, nonce: int = None) -> ApiCreds:
        """
        Creates a new CLOB API key for the given
        """
        self.assert_level_1_auth()

        endpoint = "{}{}".format(self.host, CREATE_API_KEY)
        headers = create_level_1_headers(self.signer, nonce)

        session = await self._ensure_session()
        creds_raw = await post(endpoint, headers=headers, session=session)
        try:
            creds = ApiCreds(
                api_key=creds_raw["apiKey"],
                api_secret=creds_raw["secret"],
                api_passphrase=creds_raw["passphrase"],
            )
        except:
            self.logger.error("Couldn't parse created CLOB creds")
            return None
        return creds

    async def derive_api_key(self, nonce: int = None) -> ApiCreds:
        """
        Derives an already existing CLOB API key for the given address and nonce
        """
        self.assert_level_1_auth()

        endpoint = "{}{}".format(self.host, DERIVE_API_KEY)
        headers = create_level_1_headers(self.signer, nonce)

        session = await self._ensure_session()
        creds_raw = await get(endpoint, headers=headers, session=session)
        try:
            creds = ApiCreds(
                api_key=creds_raw["apiKey"],
                api_secret=creds_raw["secret"],
                api_passphrase=creds_raw["passphrase"],
            )
        except:
            self.logger.error("Couldn't parse derived CLOB creds")
            return None
        return creds

    async def create_or_derive_api_creds(self, nonce: int = None) -> ApiCreds:
        """
        Creates API creds if not already created for nonce, otherwise derives them
        """
        try:
            return await self.create_api_key(nonce)
        except:
            return await self.derive_api_key(nonce)

    def set_api_creds(self, creds: ApiCreds):
        """
        Sets client api creds
        """
        self.creds = creds
        self.mode = self._get_client_mode()

    async def get_api_keys(self):
        """
        Gets the available API keys for this address
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        request_args = RequestArgs(method="GET", request_path=GET_API_KEYS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await get("{}{}".format(self.host, GET_API_KEYS), headers=headers, session=session)

    async def get_closed_only_mode(self):
        """
        Gets the closed only mode flag for thsi address
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        request_args = RequestArgs(method="GET", request_path=CLOSED_ONLY)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await get("{}{}".format(self.host, CLOSED_ONLY), headers=headers, session=session)

    async def delete_api_key(self):
        """
        Deletes an API key
        Level 2 Auth required
        """
        self.assert_level_2_auth()

        request_args = RequestArgs(method="DELETE", request_path=DELETE_API_KEY)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await delete("{}{}".format(self.host, DELETE_API_KEY), headers=headers, session=session)

    async def get_midpoint(self, token_id):
        """
        Get the mid market price for the given market
        """
        session = await self._ensure_session()
        return await get("{}{}?token_id={}".format(self.host, MID_POINT, token_id), session=session)

    async def get_midpoints(self, params: list[BookParams]):
        """
        Get the mid market prices for a set of token ids
        """
        body = [{"token_id": param.token_id} for param in params]
        session = await self._ensure_session()
        return await post("{}{}".format(self.host, MID_POINTS), data=body, session=session)

    async def get_price(self, token_id, side):
        """
        Get the market price for the given market
        """
        session = await self._ensure_session()
        return await get("{}{}?token_id={}&side={}".format(self.host, PRICE, token_id, side), session=session)

    async def get_prices(self, params: list[BookParams]):
        """
        Get the market prices for a set
        """
        body = [{"token_id": param.token_id, "side": param.side} for param in params]
        session = await self._ensure_session()
        return await post("{}{}".format(self.host, GET_PRICES), data=body, session=session)

    async def get_spread(self, token_id):
        """
        Get the spread for the given market
        """
        session = await self._ensure_session()
        return await get("{}{}?token_id={}".format(self.host, GET_SPREAD, token_id), session=session)

    async def get_spreads(self, params: list[BookParams]):
        """
        Get the spreads for a set of token ids
        """
        body = [{"token_id": param.token_id} for param in params]
        session = await self._ensure_session()
        return await post("{}{}".format(self.host, GET_SPREADS), data=body, session=session)

    async def get_tick_size(self, token_id: str) -> TickSize:
        if token_id in self.__tick_sizes:
            return self.__tick_sizes[token_id]

        session = await self._ensure_session()
        result = await get("{}{}?token_id={}".format(self.host, GET_TICK_SIZE, token_id), session=session)
        self.__tick_sizes[token_id] = str(result["minimum_tick_size"])

        return self.__tick_sizes[token_id]

    async def get_neg_risk(self, token_id: str) -> bool:
        if token_id in self.__neg_risk:
            return self.__neg_risk[token_id]

        session = await self._ensure_session()
        result = await get("{}{}?token_id={}".format(self.host, GET_NEG_RISK, token_id), session=session)
        self.__neg_risk[token_id] = result["neg_risk"]

        return result["neg_risk"]

    async def __resolve_tick_size(
        self, token_id: str, tick_size: TickSize = None
    ) -> TickSize:
        min_tick_size = await self.get_tick_size(token_id)
        if tick_size is not None:
            if is_tick_size_smaller(tick_size, min_tick_size):
                raise Exception(
                    "invalid tick size ("
                    + str(tick_size)
                    + "), minimum for the market is "
                    + str(min_tick_size),
                )
        else:
            tick_size = min_tick_size
        return tick_size

    async def create_order(
        self, order_args: OrderArgs, options: Optional[PartialCreateOrderOptions] = None
    ):
        """
        Creates and signs an order
        Level 1 Auth required
        """
        self.assert_level_1_auth()

        # add resolve_order_options, or similar
        tick_size = await self.__resolve_tick_size(
            order_args.token_id,
            options.tick_size if options else None,
        )

        if not price_valid(order_args.price, tick_size):
            raise Exception(
                "price ("
                + str(order_args.price)
                + "), min: "
                + str(tick_size)
                + " - max: "
                + str(1 - float(tick_size))
            )

        neg_risk = (
            options.neg_risk
            if options and options.neg_risk
            else await self.get_neg_risk(order_args.token_id)
        )

        return self.builder.create_order(
            order_args,
            CreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            ),
        )

    async def create_market_order(
        self,
        order_args: MarketOrderArgs,
        options: Optional[PartialCreateOrderOptions] = None,
    ):
        """
        Creates and signs an order
        Level 1 Auth required
        """
        self.assert_level_1_auth()

        # add resolve_order_options, or similar
        tick_size = await self.__resolve_tick_size(
            order_args.token_id,
            options.tick_size if options else None,
        )

        if order_args.price is None or order_args.price <= 0:
            order_args.price = await self.calculate_market_price(
                order_args.token_id, order_args.side, order_args.amount
            )

        if not price_valid(order_args.price, tick_size):
            raise Exception(
                "price ("
                + str(order_args.price)
                + "), min: "
                + str(tick_size)
                + " - max: "
                + str(1 - float(tick_size))
            )

        neg_risk = (
            options.neg_risk
            if options and options.neg_risk
            else await self.get_neg_risk(order_args.token_id)
        )

        return self.builder.create_market_order(
            order_args,
            CreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            ),
        )

    async def post_order(self, order, orderType: OrderType = OrderType.GTC):
        """
        Posts the order
        """
        self.assert_level_2_auth()
        body = order_to_json(order, self.creds.api_key, orderType)
        headers = create_level_2_headers(
            self.signer,
            self.creds,
            RequestArgs(method="POST", request_path=POST_ORDER, body=body),
        )
        session = await self._ensure_session()
        return await post("{}{}".format(self.host, POST_ORDER), headers=headers, data=body, session=session)
    
    async def post_orders(self, args: list[PostOrdersArgs]):
        """
        Posts a list of orders
        """
        self.assert_level_2_auth()
        body = [order_to_json(arg.order, self.creds.api_key, arg.orderType) for arg in args]
        headers = create_level_2_headers(
            self.signer,
            self.creds,
            RequestArgs(method="POST", request_path=POST_ORDERS, body=body),
        )
        session = await self._ensure_session()
        return await post("{}{}".format(self.host, POST_ORDERS), headers=headers, data=body, session=session)


    async def create_and_post_order(
        self, order_args: OrderArgs, options: PartialCreateOrderOptions = None
    ):
        """
        Utility function to create and publish an order
        """
        ord = await self.create_order(order_args, options)
        return await self.post_order(ord)

    async def cancel(self, order_id):
        """
        Cancels an order
        Level 2 Auth required
        """
        self.assert_level_2_auth()
        body = {"orderID": order_id}

        request_args = RequestArgs(method="DELETE", request_path=CANCEL, body=body)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await delete("{}{}".format(self.host, CANCEL), headers=headers, data=body, session=session)

    async def cancel_orders(self, order_ids):
        """
        Cancels orders
        Level 2 Auth required
        """
        self.assert_level_2_auth()
        body = order_ids

        request_args = RequestArgs(
            method="DELETE", request_path=CANCEL_ORDERS, body=body
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await delete(
            "{}{}".format(self.host, CANCEL_ORDERS), headers=headers, data=body, session=session
        )

    async def cancel_all(self):
        """
        Cancels all available orders for the user
        Level 2 Auth required
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="DELETE", request_path=CANCEL_ALL)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await delete("{}{}".format(self.host, CANCEL_ALL), headers=headers, session=session)

    async def cancel_market_orders(self, market: str = "", asset_id: str = ""):
        """
        Cancels orders
        Level 2 Auth required
        """
        self.assert_level_2_auth()
        body = {"market": market, "asset_id": asset_id}

        request_args = RequestArgs(
            method="DELETE", request_path=CANCEL_MARKET_ORDERS, body=body
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await delete(
            "{}{}".format(self.host, CANCEL_MARKET_ORDERS), headers=headers, data=body, session=session
        )

    async def get_orders(self, params: OpenOrderParams = None, next_cursor="MA=="):
        """
        Gets orders for the API key
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=ORDERS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)

        results = []
        next_cursor = next_cursor if next_cursor is not None else "MA=="
        session = await self._ensure_session()
        while next_cursor != END_CURSOR:
            url = add_query_open_orders_params(
                "{}{}".format(self.host, ORDERS), params, next_cursor
            )
            response = await get(url, headers=headers, session=session)
            next_cursor = response["next_cursor"]
            results += response["data"]

        return results

    async def get_order_book(self, token_id) -> OrderBookSummary:
        """
        Fetches the orderbook for the token_id
        """
        session = await self._ensure_session()
        raw_obs = await get("{}{}?token_id={}".format(self.host, GET_ORDER_BOOK, token_id), session=session)
        return parse_raw_orderbook_summary(raw_obs)

    async def get_order_books(self, params: list[BookParams]) -> list[OrderBookSummary]:
        """
        Fetches the orderbook for a set of token ids
        """
        body = [{"token_id": param.token_id} for param in params]
        session = await self._ensure_session()
        raw_obs = await post("{}{}".format(self.host, GET_ORDER_BOOKS), data=body, session=session)
        return [parse_raw_orderbook_summary(r) for r in raw_obs]

    def get_order_book_hash(self, orderbook: OrderBookSummary) -> str:
        """
        Calculates the hash for the given orderbook
        """
        return generate_orderbook_summary_hash(orderbook)

    async def get_order(self, order_id):
        """
        Fetches the order corresponding to the order_id
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        endpoint = "{}{}".format(GET_ORDER, order_id)
        request_args = RequestArgs(method="GET", request_path=endpoint)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await get("{}{}".format(self.host, endpoint), headers=headers, session=session)

    async def get_trades(self, params: TradeParams = None, next_cursor="MA=="):
        """
        Fetches the trade history for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=TRADES)
        headers = create_level_2_headers(self.signer, self.creds, request_args)

        results = []
        next_cursor = next_cursor if next_cursor is not None else "MA=="
        session = await self._ensure_session()
        while next_cursor != END_CURSOR:
            url = add_query_trade_params(
                "{}{}".format(self.host, TRADES), params, next_cursor
            )
            response = await get(url, headers=headers, session=session)
            next_cursor = response["next_cursor"]
            results += response["data"]

        return results

    async def get_last_trade_price(self, token_id):
        """
        Fetches the last trade price token_id
        """
        session = await self._ensure_session()
        return await get("{}{}?token_id={}".format(self.host, GET_LAST_TRADE_PRICE, token_id), session=session)

    async def get_last_trades_prices(self, params: list[BookParams]):
        """
        Fetches the last trades prices for a set of token ids
        """
        body = [{"token_id": param.token_id} for param in params]
        session = await self._ensure_session()
        return await post("{}{}".format(self.host, GET_LAST_TRADES_PRICES), data=body, session=session)

    def assert_level_1_auth(self):
        """
        Level 1 Poly Auth
        """
        if self.mode < L1:
            raise PolyException(L1_AUTH_UNAVAILABLE)

    def assert_level_2_auth(self):
        """
        Level 2 Poly Auth
        """
        if self.mode < L2:
            raise PolyException(L2_AUTH_UNAVAILABLE)

    def _get_client_mode(self):
        if self.signer is not None and self.creds is not None:
            return L2
        if self.signer is not None:
            return L1
        return L0

    async def get_notifications(self):
        """
        Fetches the notifications for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=GET_NOTIFICATIONS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        url = "{}{}?signature_type={}".format(
            self.host, GET_NOTIFICATIONS, self.builder.sig_type
        )
        session = await self._ensure_session()
        return await get(url, headers=headers, session=session)

    async def drop_notifications(self, params: DropNotificationParams = None):
        """
        Drops the notifications for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="DELETE", request_path=DROP_NOTIFICATIONS)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        url = drop_notifications_query_params(
            "{}{}".format(self.host, DROP_NOTIFICATIONS), params
        )
        session = await self._ensure_session()
        return await delete(url, headers=headers, session=session)

    async def get_balance_allowance(self, params: BalanceAllowanceParams = None):
        """
        Fetches the balance & allowance for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=GET_BALANCE_ALLOWANCE)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        if params.signature_type == -1:
            params.signature_type = self.builder.sig_type
        url = add_balance_allowance_params_to_url(
            "{}{}".format(self.host, GET_BALANCE_ALLOWANCE), params
        )
        session = await self._ensure_session()
        return await get(url, headers=headers, session=session)

    async def update_balance_allowance(self, params: BalanceAllowanceParams = None):
        """
        Updates the balance & allowance for a user
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=UPDATE_BALANCE_ALLOWANCE)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        if params.signature_type == -1:
            params.signature_type = self.builder.sig_type
        url = add_balance_allowance_params_to_url(
            "{}{}".format(self.host, UPDATE_BALANCE_ALLOWANCE), params
        )
        session = await self._ensure_session()
        return await get(url, headers=headers, session=session)

    async def is_order_scoring(self, params: OrderScoringParams):
        """
        Check if the order is currently scoring
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        request_args = RequestArgs(method="GET", request_path=IS_ORDER_SCORING)
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        url = add_order_scoring_params_to_url(
            "{}{}".format(self.host, IS_ORDER_SCORING), params
        )
        session = await self._ensure_session()
        return await get(url, headers=headers, session=session)

    async def are_orders_scoring(self, params: OrdersScoringParams):
        """
        Check if the orders are currently scoring
        Requires Level 2 authentication
        """
        self.assert_level_2_auth()
        body = params.orderIds
        request_args = RequestArgs(
            method="POST", request_path=ARE_ORDERS_SCORING, body=body
        )
        headers = create_level_2_headers(self.signer, self.creds, request_args)
        session = await self._ensure_session()
        return await post(
            "{}{}".format(self.host, ARE_ORDERS_SCORING), headers=headers, data=body, session=session
        )

    async def get_sampling_markets(self, next_cursor="MA=="):
        """
        Get the current sampling markets
        """
        session = await self._ensure_session()
        return await get(
            "{}{}?next_cursor={}".format(self.host, GET_SAMPLING_MARKETS, next_cursor), session=session
        )

    async def get_sampling_simplified_markets(self, next_cursor="MA=="):
        """
        Get the current sampling simplified markets
        """
        session = await self._ensure_session()
        return await get(
            "{}{}?next_cursor={}".format(
                self.host, GET_SAMPLING_SIMPLIFIED_MARKETS, next_cursor
            ), session=session
        )

    async def get_markets(self, next_cursor="MA=="):
        """
        Get the current markets
        """
        session = await self._ensure_session()
        return await get("{}{}?next_cursor={}".format(self.host, GET_MARKETS, next_cursor), session=session)

    async def get_simplified_markets(self, next_cursor="MA=="):
        """
        Get the current simplified markets
        """
        session = await self._ensure_session()
        return await get(
            "{}{}?next_cursor={}".format(self.host, GET_SIMPLIFIED_MARKETS, next_cursor), session=session
        )

    async def get_market(self, condition_id):
        """
        Get a market by condition_id
        """
        session = await self._ensure_session()
        return await get("{}{}{}".format(self.host, GET_MARKET, condition_id), session=session)

    async def get_market_trades_events(self, condition_id):
        """
        Get the market's trades events by condition id
        """
        session = await self._ensure_session()
        return await get("{}{}{}".format(self.host, GET_MARKET_TRADES_EVENTS, condition_id), session=session)

    async def calculate_market_price(self, token_id: str, side: str, amount: float) -> float:
        """
        Calculates the matching price considering an amount and the current orderbook
        """
        book = await self.get_order_book(token_id)
        if book is None:
            raise Exception("no orderbook")
        if side == "BUY":
            if book.asks is None:
                raise Exception("no match")
            return self.builder.calculate_buy_market_price(book.asks, amount)
        else:
            if book.bids is None:
                raise Exception("no match")
            return self.builder.calculate_sell_market_price(book.bids, amount)
        
    async def get_prices_history(self, market: str, 
                                start_ts: int | None = None,
                                end_ts: int | None = None, 
                                interval: str | None = None, 
                                fidelity: int | None = None):
        """
        Get the price history for a given market token.
        
        Args:
            market: The CLOB token id for which to fetch price history
            start_ts: The start time, a unix timestamp in UTC
            end_ts: The end time, a unix timestamp in UTC
            interval: A string representing a duration ending at the current time
            fidelity: The resolution of the data, in minutes
            
        Returns:
            The price history data for the specified market token
        """
        params = {
            'market': market
        }
        
        if start_ts is not None:
            params['startTs'] = str(start_ts)
        if end_ts is not None:
            params['endTs'] = str(end_ts)
        if interval is not None:
            params['interval'] = interval
        if fidelity is not None:
            params['fidelity'] = str(fidelity)
        
        query_params = "&".join([f"{k}={v}" for k, v in params.items()])
        session = await self._ensure_session()
        return await get("{}{}?{}".format(self.host, PRICES_HISTORY, query_params), session=session)

    async def close(self):
        """关闭当前实例的会话资源"""
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None

