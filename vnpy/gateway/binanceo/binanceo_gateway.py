"""
Gateway for Binance Crypto Exchange.
"""

import urllib
import hashlib
import hmac
import time
import json
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from typing import Dict, List, Any

from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval,
    OptionType
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.utility import print_dict
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event, EventEngine

REST_HOST: str = "https://vapi.binance.com"
WEBSOCKET_TRADE_HOST: str = "wss://vstream.binance.com/ws/"
WEBSOCKET_DATA_HOST: str = "wss://vstream.binance.com/stream?streams="

TESTNET_RESTT_HOST: str = "https://testnet.binancefuture.com"
TESTNET_WEBSOCKET_TRADE_HOST: str = "wss://stream.binancefuture.com/ws/"
TESTNET_WEBSOCKET_DATA_HOST: str = "wss://stream.binancefuture.com/stream?streams="

STATUS_BINANCEO2VT: Dict[str, Status] = {
    "RECEIVED": Status.SUBMITTING,
    "ACCEPTED": Status.NOTTRADED,
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELLING": Status.CANCELLING,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
    "UNTRIGGERED": Status.REJECTED
}

# RECEIVED(0) 新建订单
# UNTRIGGERED(1) 下单失败
# ACCEPTED(2) 下单成功
# REJECTED(3) 订单被拒绝
# PARTIALLY_FILLED(4) 部分成交
# FILLED(5) 完全成交
# CANCELLING(6) 撤单中
# CANCELLED(7) 撤单完成
STATUS_BINANCEON2VT: Dict[str, Status] = {
    0: Status.SUBMITTING,
    1: Status.REJECTED,
    2: Status.NOTTRADED,
    3: Status.REJECTED,
    4: Status.PARTTRADED,
    5: Status.ALLTRADED,
    6: Status.CANCELLING,
    7: Status.CANCELLED
}
ORDERTYPE_VT2BINANCEO: Dict[OrderType, str] = {
    OrderType.LIMIT: "LIMIT",
    OrderType.MARKET: "MARKET"
}
ORDERTYPE_BINANCEO2VT: Dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2BINANCEO.items()}

DIRECTION_VT2BINANCEO: Dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCEO2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BINANCEO.items()}

INTERVAL_VT2BINANCEF: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}


class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2


symbol_name_map: Dict[str, str] = {}


class BinanceoGateway(BaseGateway):
    """
    VN Trader Gateway for Binance connection.
    币安欧式期权接口
    API开通： API管理=》API编辑=》勾选 ‘欧式期权’
    -大佳 28880502
    """

    default_setting = {
        "key": "",
        "secret": "",
        "session_number": 3,
        "server": ["TESTNET", "REAL"],
        "proxy_host": "",
        "proxy_port": 0,
    }

    exchanges: Exchange = [Exchange.BINANCE]

    def __init__(self, event_engine: EventEngine, gateway_name="BINANCEO"):
        """Constructor"""
        super().__init__(event_engine, gateway_name)
        self.count = 0

        self.trade_ws_api = BinanceoTradeWebsocketApi(self)
        self.market_ws_api = BinanceoDataWebsocketApi(self)
        self.rest_api = BinanceoRestApi(self)

    def connect(self, setting: dict) -> None:
        """"""
        key = setting["key"]
        secret = setting["secret"]
        session_number = setting["session_number"]
        server = setting["server"]
        proxy_host = setting["proxy_host"]
        proxy_port = setting["proxy_port"]

        self.rest_api.connect(key, secret, session_number, server,
                              proxy_host, proxy_port)
        self.market_ws_api.connect(proxy_host, proxy_port, server)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """"""
        self.market_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> Request:
        """"""
        self.rest_api.cancel_order(req)
        return True

    def query_account(self) -> None:
        """"""
        pass

    def query_position(self) -> None:
        """"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """"""
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """"""
        self.rest_api.keep_user_stream()
        if self.status.get('td_con', False) \
                and self.status.get('tdws_con', False) \
                and self.status.get('mdws_con', False):
            self.status.update({'con': True})

        self.count += 1
        if self.count < 60:
            return
        self.count = 0
        if len(self.query_functions) > 0:
            func = self.query_functions.pop(0)
            func()
            self.query_functions.append(func)

    def get_order(self, orderid: str):
        return self.rest_api.get_order(orderid)


class BinanceoRestApi(RestClient):
    """
    BINANCE REST API
    """

    def __init__(self, gateway: BinanceoGateway):
        """"""
        super().__init__()

        self.gateway: BinanceoGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.trade_ws_api: BinanceoTradeWebsocketApi = self.gateway.trade_ws_api

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.recv_window: int = 5000
        self.time_offset: int = 0

        self.contracts = {}

        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.orders = {}

        self.cache_position_symbols = {}

        self.accountid = ""

    def sign(self, request: Request) -> Request:
        """
        Generate BINANCE signature.
        """
        security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path = request.path

        if security == Security.SIGNED:
            timestamp = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query = urllib.parse.urlencode(sorted(request.params.items()))
            signature = hmac.new(self.secret, query.encode(
                "utf-8"), hashlib.sha256).hexdigest()

            query += "&signature={}".format(signature)
            path = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # Add headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
            self,
            key: str,
            secret: str,
            session_number: int,
            server: str,
            proxy_host: str,
            proxy_port: int
    ) -> None:
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.connect_time = (
                int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        if self.server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_RESTT_HOST, proxy_host, proxy_port)

        self.start(session_number)

        self.gateway.write_log("REST API启动成功")
        self.gateway.status.update({'td_con': True, 'td_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        if self.gateway.status.get('md_con', False):
            self.gateway.status.update({'con': True})
        self.query_time()
        self.query_account()
        self.query_position()

        self.query_contract()
        time.sleep(1)
        for symbol in list(self.contracts.keys()):
            self.query_order(symbol)
            self.query_trade(symbol)

        self.start_user_stream()

        # 添加到定时查询队列中
        self.gateway.query_functions = [self.query_account, self.query_position]

    def query_time(self) -> Request:
        """"""
        data = {
            "security": Security.NONE
        }
        path = "/vapi/v1/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def query_account(self) -> Request:
        """"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/vapi/v1/account",
            callback=self.on_query_account,
            data=data
        )

    def query_position(self) -> Request:
        """"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/vapi/v1/position",
            callback=self.on_query_position,
            data=data
        )

    def query_order(self, vt_symbol: str = "") -> Request:
        """"""
        data = {"security": Security.SIGNED}
        params = {}
        if vt_symbol:
            if '.' in vt_symbol:
                vt_symbol = vt_symbol.split('.')[0]
            params.update({'symbol': vt_symbol})

        self.add_request(
            method="GET",
            path="/vapi/v1/openOrders",
            callback=self.on_query_order,
            params=params,
            data=data
        )

    def query_contract(self) -> Request:
        """"""
        data = {
            "security": Security.NONE
        }
        self.add_request(
            method="GET",
            path="/vapi/v1/exchangeInfo",
            callback=self.on_query_contract,
            data=data
        )

    def query_trade(self, vt_symbol: str = '') -> Request:
        """"""
        data = {"security": Security.SIGNED}
        params = {}
        if vt_symbol:
            if '.' in vt_symbol:
                vt_symbol = vt_symbol.split('.')[0]
            params.update({'symbol': vt_symbol})

        self.add_request(
            method="GET",
            path="/vapi/v1/historyOrders",
            callback=self.on_query_trade,
            params=params,
            data=data
        )

    def _new_order_id(self) -> int:
        """"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def get_order(self, orderid: str):
        """返回缓存的Order"""
        return self.orders.get(orderid, None)

    def send_order(self, req: OrderRequest) -> str:
        """"""
        orderid = str(self.connect_time + self._new_order_id())
        order = req.create_order_data(
            orderid,
            self.gateway_name
        )
        order.accountid = self.accountid
        order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        order.datetime = datetime.now()
        self.orders.update({orderid: copy(order)})
        self.gateway.write_log(f'创建委托订单:{order.__dict__}')
        self.gateway.on_order(order)

        data = {
            "security": Security.SIGNED
        }

        params = {
            "symbol": req.symbol,
            "timeInForce": "GTC",
            "side": DIRECTION_VT2BINANCEO[req.direction],
            "reduceOnly": True if req.offset==Offset.CLOSE else False,
            "type": ORDERTYPE_VT2BINANCEO[req.type],
            "price": float(req.price),
            "quantity": float(req.volume),
            "clientOrderId": orderid,
            "newOrderRespType": "ACK"
        }
        if req.type == OrderType.MARKET:
            params.pop('timeInForce', None)
            params.pop('price', None)

        self.add_request(
            method="POST",
            path="/vapi/v1/order",
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> Request:
        """"""
        data = {
            "security": Security.SIGNED
        }
        params = {
            "symbol": req.symbol}

        order = self.orders.get(req.orderid)
        if order is not None:
            params.update({ "orderId": order.sys_orderid})
        else:
            params.update({"clientOrderId": req.orderid})

        self.add_request(
            method="DELETE",
            path="/vapi/v1/order",
            callback=self.on_cancel_order,
            params=params,
            data=data,
            extra=req
        )

    def start_user_stream(self) -> Request:
        """"""
        data = {
            "security": Security.SIGNED  # API_KEY
        }

        self.add_request(
            method="POST",
            path="/vapi/v1/userDataStream",
            callback=self.on_start_user_stream,
            data=data
        )

    def keep_user_stream(self) -> Request:
        """"""
        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0

        data = {
            "security": Security.NONE  # API_KEY
        }

        params = {
            "listenKey": self.user_stream_key
        }

        self.add_request(
            method="PUT",
            path="/vapi/v1/userDataStream",
            callback=self.on_keep_user_stream,
            params=params,
            data=data
        )

    def on_query_time(self, data: Any, request: Request) -> None:
        """"""
        # self.gateway.write_log(f'on_query_time: \n {print_dict(data)}')
        local_time = int(time.time() * 1000)
        server_time = int(data['data'])
        self.time_offset = local_time - server_time

    def on_query_account(self, data: dict, request: Request) -> None:
        """"""
        # self.gateway.write_log(f'on_query_account: \n {print_dict(data)}')
        # {
        #   "code": 0,
        #   "msg": "success",
        #   "data": [
        #     {
        #       "currency": "USDT",               // 资产类型
        #       "equity": 10094.44662,            // 账户权益
        #       "available": 8725.92524,          // 账户可用
        #       "orderMargin": 1084.52138,        // 委托保证金
        #       "positionMargin": 289.00138,      // 持仓保证金
        #       "unrealizedPNL": -5.00138,        // 未实现盈亏
        #       "maintMargin": 151.00138,         // 维持保证金
        #       "balance": 10099.448              // 账户余额
        #     }
        #   ]
        # }
        if data.get('code') != 0:
            return
        datas = data.get('data', [])
        for d in datas:
            if d.get('currency', "") != "USDT":
                continue
            if not self.accountid:
                self.accountid = f"{self.gateway_name}_{d['currency']}"
            account = AccountData(
                accountid=self.accountid,
                balance=float(d["balance"]),
                frozen=float(d["orderMargin"]),
                holding_profit=float(d['unrealizedPNL']),
                currency='USDT',
                margin=float(d["positionMargin"]),
                gateway_name=self.gateway_name,
                trading_day=datetime.now().strftime('%Y-%m-%d')
            )
            account.available = float(d["available"])
            if account.balance:
                self.gateway.on_account(account)

            # self.gateway.write_log("账户资金查询成功")

    def on_query_position(self, data: dict, request: Request) -> None:
        """持仓"""
        # self.gateway.write_log(f'on_query_position: \n {print_dict(data)}')
        if data.get('code') != 0:
            return
        data = data.get('data', [])
        for d in data:
            # self.gateway.write_log(d)
            direction = Direction.SHORT if d.get('side') == 'SHORT' else Direction.LONG
            volume = abs(float(d["quantity"]))  # 持仓数量（正数为多，负数为空） => 正数
            available_volume = abs(float(d['reducibleQty']))
            frozen_volume = volume - available_volume

            position = PositionData(
                accountid=self.accountid,
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                direction=direction,
                volume=volume,
                yd_volume=available_volume,
                frozen=frozen_volume,
                price=float(d["entryPrice"]),
                cur_price=float(d["markPrice"]),
                pnl=float(d["unrealizedPNL"]),
                gateway_name=self.gateway_name,
            )

            # 如果持仓数量为0，且不在之前缓存过的合约信息中，不做on_position
            if position.volume == 0:
                if position.symbol not in self.cache_position_symbols:
                    continue
            else:
                if position.symbol not in self.cache_position_symbols:
                    self.cache_position_symbols.update({position.symbol: position.volume})

            self.gateway.on_position(position)

        # self.gateway.write_log("持仓信息查询成功")

    def on_query_order(self, data: dict, request: Request) -> None:
        """期权查询当前挂单 """

        # self.gateway.write_log(f'on_query_order: \n {print_dict(data)}')
        if data.get('code') != 0:
            return
        data = data.get('data', [])

        for d in data:
            dt = datetime.fromtimestamp(d["createDate"] / 1000)
            time = dt.strftime("%Y-%m-%d %H:%M:%S")
            orderid = d["clientOrderId"]
            if len(orderid) == 0:
                orderid = str(d["id"])

            order = OrderData(
                accountid=self.accountid,
                orderid=orderid,
                sys_orderid=str(d["id"]),
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                price=float(d["price"]),
                volume=float(d["quantity"]),
                type=ORDERTYPE_BINANCEO2VT[d["type"]],
                direction=DIRECTION_BINANCEO2VT[d["side"]],
                offset=Offset.OPEN if d.get('reduceOnly', True) else Offset.CLOSE,
                traded=float(d["executedQty"]),
                status=STATUS_BINANCEO2VT.get(d["status"], None),
                datetime=dt,
                time=time,
                gateway_name=self.gateway_name,
            )

            self.orders.update({order.orderid: copy(order)})
            self.gateway.write_log(f'返回订单查询结果：{order.__dict__}')
            self.gateway.on_order(order)

        self.gateway.write_log("委托信息查询成功")

    def on_query_trade(self, data: dict, request: Request) -> None:
        """"""

        self.gateway.write_log(f'on_query_trade: \n {print_dict(data)}')
        return

        for d in data:
            dt = datetime.fromtimestamp(d["time"] / 1000)
            time = dt.strftime("%Y-%m-%d %H:%M:%S")

            trade = TradeData(
                accountid=self.accountid,
                symbol=d['symbol'],
                exchange=Exchange.BINANCE,
                orderid=d['orderId'],
                tradeid=d["id"],
                direction=Direction.SHORT if d['side'] == 'SELL' else Direction.LONG,
                offset=Offset.CLOSE if d['buyer'] else Offset.OPEN,
                price=float(d["price"]),
                volume=float(d['qty']),
                time=time,
                datetime=dt,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)

        self.gateway.write_log("委托信息查询成功")

    def on_query_contract(self, data: dict, request: Request) -> None:
        """处理合约配置"""
        import json
        # rate_limits = data.get('rateLimits')
        # rate_limits = json.dumps(rate_limits, indent=2)
        # self.gateway.write_log(f'速率限制:{rate_limits}')
        # self.gateway.write_log(f'on_query_contract: \n {print_dict(data)}')
        if data.get('code') != 0:
            return
        data = data.get('data', [])

        for d in data["optionSymbols"]:

            symbol = d["symbol"]
            if d['side'] == 'CALL':
                name = symbol.replace('-C', '看涨')
                option_type = OptionType.CALL
            else:
                name = symbol.replace('-P', '看跌')
                option_type = OptionType.PUT
            pricetick = 0.0001
            min_volume = d['minQty']

            # 合约乘数
            symbol_size = 1

            contract = ContractData(
                symbol=symbol,
                exchange=Exchange.BINANCE,
                name=name,
                pricetick=pricetick,
                size=symbol_size,
                margin_rate=float(d['initialMargin']),
                min_volume=min_volume,
                product=Product.FUTURES,
                history_data=True,
                option_strike=float(d['strikePrice']),
                option_underlying=d['underlying'],
                option_type=option_type,
                option_expiry=datetime.fromtimestamp(d['expiryDate'] / 1000),
                option_index=d['underlying'],
                gateway_name=self.gateway_name,
            )
            self.contracts.update({contract.symbol: contract})
            self.gateway.on_contract(contract)
            symbol_name_map[contract.symbol] = contract.name

        self.gateway.write_log("合约信息查询成功")

    def on_send_order(self, data: dict, request: Request) -> None:
        """"""
        pass

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.orders.update({order.orderid: copy(order)})
        self.gateway.write_log(f'订单委托失败:{order.__dict__}')
        if not order.accountid:
            order.accountid = self.accountid
            order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        if not order.datetime:
            order.datetime = datetime.now()
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_error(msg)

    def on_send_order_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.orders.update({order.orderid: copy(order)})
        self.gateway.write_log(f'发送订单异常:{order.__dict__}')
        if not order.accountid:
            order.accountid = self.accountid
            order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        if not order.datetime:
            order.datetime = datetime.now()
        self.gateway.on_order(order)

        msg = f"委托失败，拒单"
        self.gateway.write_error(msg)
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """"""
        pass

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """响应启动用户数据流"""
        if data.get('code') != 0:
            self.gateway.write_error(f'申请数据流失败')
            return
        data = data.get('data', {})
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.server == "REAL":
            url = WEBSOCKET_TRADE_HOST + self.user_stream_key
        else:
            url = TESTNET_WEBSOCKET_TRADE_HOST + self.user_stream_key
        self.gateway.write_log(f'申请用户数据流listenKey成功:{self.user_stream_key}')
        self.trade_ws_api.connect(url, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """"""
        history = []
        limit = 1000
        start_time = int(datetime.timestamp(req.start))

        while True:
            # Create query params
            params = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BINANCEF[req.interval],
                "limit": limit,
                "startTime": start_time * 1000,  # convert to millisecond
            }

            # Add end time if specified
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000  # convert to millisecond

            # Get response from server
            resp = self.request(
                "GET",
                "/vapi/v1/klines",
                data={"security": Security.NONE},
                params=params
            )

            # Break if request failed with other status code
            if resp.status_code // 100 != 2:
                msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data = resp.json()
                if not data:
                    msg = f"获取历史数据为空，开始时间：{start_time}"
                    self.gateway.write_log(msg)
                    break

                buf = []

                for l in data:
                    dt = datetime.fromtimestamp(l[0] / 1000)  # convert to second

                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(l[5]),
                        open_price=float(l[1]),
                        high_price=float(l[2]),
                        low_price=float(l[3]),
                        close_price=float(l[4]),
                        trading_day=dt.strftime('%Y-%m-%d'),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)

                begin = buf[0].datetime
                end = buf[-1].datetime
                msg = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                self.gateway.write_log(msg)

                # Break if total data count less than limit (latest date collected)
                if len(data) < limit:
                    break

                # Update start time
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

        return history


class BinanceoTradeWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway: BinanceoGateway):
        """"""
        super().__init__()

        self.gateway: BinanceoGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.accountid = ""

    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        """"""
        self.init(url, proxy_host, proxy_port)
        self.start()

    def on_connected(self) -> None:
        """"""
        self.gateway.write_log("交易Websocket API连接成功")
        self.gateway.status.update({'tdws_con': True, 'tdws_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        if self.gateway.status.get('td_con', False):
            self.gateway.status.update({'con': True})

        # 数据默认是GZIP压缩数据连接成功后发送
        # self.send_packet(packet={"method": "BINARY", "params": ["false"], "id": 1})

    def on_packet(self, packet: dict) -> None:  # type: (dict)->None
        """"""
        event = packet.get('e')
        if event is not None:
            if packet["e"] == "ACCOUNT_UPDATE":
                self.on_account(packet)
            elif packet["e"] == "ORDER_TRADE_UPDATE":
                self.on_order(packet)

    def on_account(self, packet: dict) -> None:
        """websocket返回得Balance/Position信息更新"""
        """
        {
          "B":[                             // 余额信息
            {
              "a":"USDT",                   // 资产名称
              "wb":"122624.12345678",       // 钱包余额
              "cw":"100.12345678"           // 除去逐仓保证金的钱包余额
            },
            {
              "a":"BNB",           
              "wb":"1.00000000",
              "cw":"0.00000000"         
            }
          ],
          "P":[
            {
              "s":"BTCUSDT",            // 交易对
              "pa":"1",                 // 仓位
              "ep":"9000",              // 入仓价格
              "cr":"200",               // (费前)累计实现损益
              "up":"0.2732781800",      // 持仓未实现盈亏
              "mt":"isolated",          // 保证金模式
              "iw":"0.06391979"         // 若为逐仓，仓位保证金
            }
          ]
        }
        """
        # 计算持仓收益
        holding_pnl = 0
        for pos_data in packet.get("P", []):
            # print(pos_data)
            volume = float(pos_data["c"])
            if not self.accountid:
                self.accountid = f"{self.gateway_name}_USDT"
            position = PositionData(
                accountid=self.accountid,
                symbol=pos_data["S"],
                exchange=Exchange.BINANCE,
                direction=Direction.NET,
                volume=volume,
                yd_volume=float(pos_data['r']),
                price=float(pos_data["a"]),
                # pnl=float(pos_data["cr"]),
                gateway_name=self.gateway_name,
            )
            #holding_pnl += float(pos_data['up'])
            # self.gateway.on_position(position)

        for acc_data in packet.get("B", []):
            if not self.accountid:
                self.accountid = f"{self.gateway_name}_USDT"
            account = AccountData(
                accountid=self.accountid,
                balance=round(float(acc_data["b"]), 7),
                frozen=round(float(acc_data["o"]), 7),
                holding_profit=round(float(acc_data["u"]), 7),
                currency='USDT',
                margin=round(float(acc_data["p"]), 7),
                gateway_name=self.gateway_name,
                trading_day=datetime.now().strftime('%Y-%m-%d')
            )

            if account.balance:
                # account.balance += account.holding_profit
                account.available = account.balance - account.margin - account.frozen
                self.gateway.on_account(account)

    def on_order(self, packet: dict) -> None:
        """ws处理on_order事件"""
        self.gateway.write_log('ws返回订单更新{}:\n'.format(print_dict(packet)))
        dt = datetime.fromtimestamp(packet["E"] / 1000)
        time = dt.strftime("%Y-%m-%d %H:%M:%S")

        ord_datas = packet["o"]
        for ord_data in ord_datas:
            orderid = str(ord_data["cid"])

            order = self.gateway.get_order(orderid)
            if order:
                order.traded = float(ord_data["e"])
                order.status = STATUS_BINANCEON2VT[ord_data["s"]]
                if order.status in [Status.CANCELLED, Status.REJECTED]:
                    order.cancel_time = time
                if len(order.sys_orderid) == 0:
                    order.sys_orderid = str(ord_data["oid"])

                self.gateway.write_log(f'WS订单更新:\n{order.__dict__}')
                self.gateway.on_order(order)

            # Push trade event
            trade_volume = float(ord_data["e"])
            if trade_volume <= 0:
                return

            trade_dt = datetime.fromtimestamp(ord_data["T"] / 1000)
            trade_time = trade_dt.strftime("%Y-%m-%d %H:%M:%S")

            tradeid = ord_data.get("t")
            if tradeid is None:
                tradeid = str(ord_data["T"])
            trade = TradeData(
                accountid=self.accountid,
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid=tradeid,
                direction=order.direction,
                offset=order.offset,
                price=float(ord_data["ec"]),
                volume=trade_volume,
                time=trade_time,
                datetime=trade_dt,
                gateway_name=self.gateway_name,
            )
            self.gateway.write_log(f'WS成交更新:\n{trade.__dict__}')
            self.gateway.on_trade(trade)


class BinanceoDataWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway: BinanceoGateway):
        """"""
        super().__init__()

        self.gateway: BinanceoGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: Dict[str, TickData] = {}

    def connect(
            self,
            proxy_host: str,
            proxy_port: int,
            server: str
    ) -> None:
        """"""
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.server = server

    def on_connected(self) -> None:
        """"""
        self.gateway.write_log("行情Websocket API连接刷新")
        self.gateway.status.update({'md_con': True, 'md_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        if self.gateway.status.get('td_con', False):
            self.gateway.status.update({'con': True})

        # 数据默认是GZIP压缩数据连接成功后发送
        # self.send_packet(packet={"method": "BINARY", "params": ["false"], "id": 1})

    def subscribe(self, req: SubscribeRequest) -> None:
        """"""
        if req.symbol not in symbol_name_map:
            self.gateway.write_log(f"找不到该合约代码{req.symbol}")
            return

        # Create tick buf data
        tick = TickData(
            symbol=req.symbol,
            name=symbol_name_map.get(req.symbol, ""),
            exchange=Exchange.BINANCE,
            datetime=datetime.now(),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol] = tick

        # Close previous connection
        if self._active:
            self.stop()
            self.join()

        # Create new connection
        channels = []
        for ws_symbol in self.ticks.keys():
            channels.append(ws_symbol + "@ticker")
            channels.append(ws_symbol + "@depth10")

        if self.server == "REAL":
            url = WEBSOCKET_DATA_HOST + "/".join(channels)
        else:
            url = TESTNET_WEBSOCKET_DATA_HOST + "/".join(channels)

        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def on_packet(self, packet: dict) -> None:
        """"""
        stream = packet.get('stream')
        if stream is None:
            return

        data = packet.get("data", None)
        if data is None:
            self.gateway.write_log(f'接收数据无data字段:{packet}')
            return

        symbol, _ = stream.split("@")
        event = data.get('e')
        tick = self.ticks[symbol]

        if event == "ticker":
            tick_dt = datetime.fromtimestamp(float(data['E']) / 1000)
            trading_day = tick_dt.strftime('%Y-%m-%d')
            today_volume = float(data['v'])
            if tick.trading_day == trading_day:
                volume_changed = max(0, today_volume - tick.volume)
            else:
                volume_changed = today_volume if len(tick.trading_day) > 0 else 1

            tick.volume = today_volume
            tick.last_volume = volume_changed
            tick.open_price = float(data['o'])
            tick.high_price = float(data['h'])
            tick.low_price = float(data['l'])
            tick.last_price = float(data['c'])
            tick.datetime = tick_dt
            tick.trading_day = trading_day
            tick.date = tick.trading_day
            tick.time = tick.datetime.strftime('%H:%M:%S.%f')
        elif event == 'depth':
            bids = data["b"]
            for n in range(min(5, len(bids))):
                price, volume = bids[n]
                tick.__setattr__("bid_price_" + str(n + 1), float(price))
                tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

            asks = data["a"]
            for n in range(min(5, len(asks))):
                price, volume = asks[n]
                tick.__setattr__("ask_price_" + str(n + 1), float(price))
                tick.__setattr__("ask_volume_" + str(n + 1), float(volume))

            if tick.last_price:
                self.gateway.on_tick(copy(tick))
