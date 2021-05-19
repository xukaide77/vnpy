import traceback
import json
from copy import deepcopy
from uuid import uuid1
from datetime import datetime, timedelta
from threading import Thread
from vnpy.event import Event
from vnpy.rpc import RpcClient
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    SubscribeRequest,
    CancelRequest,
    OrderRequest
)
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_TRADE,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_CONTRACT,
    EVENT_LOG)
from vnpy.trader.constant import Exchange
from vnpy.amqp.consumer import subscriber
from vnpy.amqp.producer import task_creator


class StockRpcGateway(BaseGateway):
    """
    股票交易得RPC接口
    交易使用RPC实现，
    行情使用RabbitMQ订阅获取
    需要启动单独得进程运行stock_tick_publisher
    Cta_Stock => 行情订阅 =》StockRpcGateway =》RabbitMQ (task)=》 stock_tick_publisher =》订阅(worker)
    stock_tick_publisher => restful接口获取股票行情 =》RabbitMQ(pub) => StockRpcGateway =>on_tick event
    """

    default_setting = {
        "主动请求地址": "tcp://127.0.0.1:2014",
        "推送订阅地址": "tcp://127.0.0.1:4102",
        "远程接口名称": "pb01"
    }

    exchanges = list(Exchange)

    def __init__(self, event_engine, gateway_name='StockRPC'):
        """Constructor"""
        super().__init__(event_engine, gateway_name)

        self.symbol_gateway_map = {}

        self.client = RpcClient()
        self.client.callback = self.client_callback
        self.rabbit_api = None
        self.rabbit_dict = {}
        # 远程RPC端，gateway_name
        self.remote_gw_name = gateway_name

    def connect(self, setting: dict):
        """"""
        req_address = setting["主动请求地址"]
        pub_address = setting["推送订阅地址"]
        self.remote_gw_name = setting['远程接口名称']

        self.write_log(f'请求地址:{req_address},订阅地址:{pub_address},远程接口:{self.remote_gw_name}')

        # 订阅事件
        self.client.subscribe_topic("")
        # self.client.subscribe_topic(EVENT_TRADE)
        # self.client.subscribe_topic(EVENT_ORDER)
        # self.client.subscribe_topic(EVENT_POSITION)
        # self.client.subscribe_topic(EVENT_ACCOUNT)
        # self.client.subscribe_topic(EVENT_CONTRACT)
        # self.client.subscribe_topic(EVENT_LOG)

        self.client.start(req_address, pub_address)

        self.rabbit_dict = setting.get('rabbit', {})
        self.write_log(f'激活RabbitMQ行情接口.配置：\n{self.rabbit_dict}')
        self.rabbit_api = SubMdApi(gateway=self)
        self.rabbit_api.connect(self.rabbit_dict)

        self.write_log("服务器连接成功，开始初始化查询")

        self.query_all()

    def check_status(self):

        if self.client:
            pass

        if self.rabbit_api:
            self.rabbit_api.check_status()

        return True

    def subscribe(self, req: SubscribeRequest):
        """行情订阅"""
        self.write_log(f'创建订阅任务=> rabbitMQ')
        host = self.rabbit_dict.get('host', 'localhost')
        port = self.rabbit_dict.get('port', 5672)
        user = self.rabbit_dict.get('user', 'admin')
        password = self.rabbit_dict.get('password', 'admin')
        exchange = 'x_work_queue'
        queue_name = 'subscribe_task_queue'
        routing_key = 'stock_subscribe'
        task = task_creator(
            host=host,
            port=port,
            user=user,
            password=password,
            exchange=exchange,
            queue_name=queue_name,
            routing_key=routing_key)

        mission = {}
        mission.update({'id': str(uuid1())})
        mission.update({'action': "subscribe"})
        mission.update({'vt_symbol': req.vt_symbol})
        mission.update({'is_stock': True})
        msg = json.dumps(mission)
        self.write_log(f'[=>{host}:{port}/{exchange}/{queue_name}/{routing_key}] create task :{msg}')
        task.pub(msg)
        task.close()
        # gateway_name = self.symbol_gateway_map.get(req.vt_symbol, "")
        # self.client.subscribe(req, gateway_name)
        if self.rabbit_api:
            self.rabbit_api.registed_symbol_set.add(req.vt_symbol)

    def send_order(self, req: OrderRequest):
        """
        RPC远程发单
        :param req:
        :return:
        """
        self.write_log(f'使用prc委托:{req.__dict__}')
        ref = self.client.send_order(req, self.remote_gw_name)

        local_ref = ref.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
        self.write_log(f'委托返回:{ref}=> {local_ref}')
        return local_ref

    def cancel_order(self, req: CancelRequest):
        """"""
        self.write_log(f'委托撤单:{req.__dict__}')
        # gateway_name = self.symbol_gateway_map.get(req.vt_symbol, "")
        self.client.cancel_order(req, self.remote_gw_name)

    def query_account(self):
        """"""
        pass

    def query_position(self):
        """"""
        pass

    def query_all(self):
        """"""
        contracts = self.client.get_all_contracts()
        for contract in contracts:
            self.symbol_gateway_map[contract.vt_symbol] = contract.gateway_name
            contract.gateway_name = self.gateway_name
            self.on_contract(contract)
        self.write_log("合约信息查询成功")

        accounts = self.client.get_all_accounts()
        for account in accounts:
            account.gateway_name = self.gateway_name
            self.on_account(account)
        self.write_log("资金信息查询成功")

        positions = self.client.get_all_positions()
        for position in positions:
            position.gateway_name = self.gateway_name
            # 更换 vt_positionid得gateway前缀
            position.vt_positionid = position.vt_positionid.replace(f'{position.gateway_name}.',
                                                                    f'{self.gateway_name}.')
            # 更换 vt_accountid得gateway前缀
            position.vt_accountid = position.vt_accountid.replace(f'{position.gateway_name}.', f'{self.gateway_name}.')

            self.on_position(position)
        self.write_log("持仓信息查询成功")

        orders = self.client.get_all_orders()
        for order in orders:
            # 更换gateway
            order.gateway_name = self.gateway_name
            # 更换 vt_orderid得gateway前缀
            order.vt_orderid = order.vt_orderid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            # 更换 vt_accountid得gateway前缀
            order.vt_accountid = order.vt_accountid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')

            self.on_order(order)
        self.write_log("委托信息查询成功")

        trades = self.client.get_all_trades()
        for trade in trades:
            trade.gateway_name = self.gateway_name
            # 更换 vt_orderid得gateway前缀
            trade.vt_orderid = trade.vt_orderid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            # 更换 vt_orderid得gateway前缀
            trade.vt_orderid = trade.vt_orderid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            # 更换 vt_accountid得gateway前缀
            trade.vt_accountid = trade.vt_accountid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            self.on_trade(trade)
        self.write_log("成交信息查询成功")

    def close(self):
        """"""
        self.client.stop()
        self.client.join()

    def client_callback(self, topic: str, event: Event):
        """"""
        if event is None:
            print("none event", topic, event)
            return
        if event.type == EVENT_TICK:
            return

        event = deepcopy(event)

        data = event.data

        if hasattr(data, "gateway_name"):
            data.gateway_name = self.gateway_name

            if hasattr(data, 'vt_orderid'):
                rpc_vt_orderid = data.vt_orderid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
                self.write_log(f' vt_orderid :{data.vt_orderid} => {rpc_vt_orderid}')
                data.vt_orderid = rpc_vt_orderid

            if hasattr(data, 'vt_tradeid'):
                rpc_vt_tradeid = data.vt_tradeid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
                self.write_log(f' vt_tradeid :{data.vt_tradeid} => {rpc_vt_tradeid}')
                data.vt_tradeid = rpc_vt_tradeid

            if hasattr(data, 'vt_accountid'):
                data.vt_accountid = data.vt_accountid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')
            if hasattr(data, 'vt_positionid'):
                data.vt_positionid = data.vt_positionid.replace(f'{self.remote_gw_name}.', f'{self.gateway_name}.')

            if event.type in [EVENT_ORDER, EVENT_TRADE]:
                self.write_log(f'{self.remote_gw_name} => {self.gateway_name} event:{data.__dict__}')

        self.event_engine.put(event)


class SubMdApi():
    """
    RabbitMQ Subscriber 数据行情接收API
    """

    def __init__(self, gateway):
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.symbol_tick_dict = {}  # 合约与最后一个Tick得字典
        self.registed_symbol_set = set()  # 订阅的合约记录集
        self.last_tick_dt = None
        self.sub = None
        self.setting = {}
        self.connect_status = False
        self.thread = None  # 用线程运行所有行情接收

    def check_status(self):
        """接口状态的健康检查"""

        # 订阅的合约
        d = {'sub_symbols': sorted(self.symbol_tick_dict.keys())}

        # 合约的最后时间
        if self.last_tick_dt:
            d.update({"sub_tick_time": self.last_tick_dt.strftime('%Y-%m-%d %H:%M:%S')})

        if len(self.symbol_tick_dict) > 0:
            dt_now = datetime.now()
            hh_mm = dt_now.hour * 100 + dt_now.minute
            # 期货交易时间内
            if 930 <= hh_mm <= 1130 or 1301 <= hh_mm <= 1500:
                # 未有数据到达
                if self.last_tick_dt is None:
                    d.update({"sub_status": False, "sub_error": u"rabbitmq未有行情数据到达"})
                else: # 有数据

                    # 超时5分钟以上
                    if (dt_now - self.last_tick_dt).total_seconds() > 60 * 5:
                        d.update({"sub_status": False,
                                  "sub_error": u"{}rabbitmq行情数据超时5分钟以上".format(hh_mm)})
                    else:
                        d.update({"sub_status": True})
                        self.gateway.status.pop("sub_error", None)

            # 非交易时间
            else:
                self.gateway.status.pop("sub_status", None)
                self.gateway.status.pop("sub_error", None)

        # 更新到gateway的状态中去
        self.gateway.status.update(d)

    def connect(self, setting={}):
        """连接"""
        self.setting = setting
        try:
            self.sub = subscriber(
                host=self.setting.get('host', 'localhost'),
                port=self.setting.get('port', 5672),
                user=self.setting.get('user', 'admin'),
                password=self.setting.get('password', 'admin'),
                exchange=self.setting.get('exchange', 'x_fanout_stock_tick'))

            self.sub.set_callback(self.on_message)
            self.thread = Thread(target=self.sub.start)
            self.thread.start()
            self.connect_status = True
            self.gateway.status.update({'sub_con': True, 'sub_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        except Exception as ex:
            self.gateway.write_error(u'连接RabbitMQ {} 异常:{}'.format(self.setting, str(ex)))
            self.gateway.write_error(traceback.format_exc())
            self.connect_status = False

    def on_message(self, chan, method_frame, _header_frame, body, userdata=None):
        # print(" [x] %r" % body)
        try:
            str_tick = body.decode('utf-8')
            d = json.loads(str_tick)
            d.pop('rawData', None)

            symbol = d.pop('symbol', None)
            str_datetime = d.pop('datetime', None)

            if '.' in str_datetime:
                dt = datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S.%f')
            else:
                dt = datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S')

            tick = TickData(gateway_name=self.gateway_name,
                            exchange=Exchange(d.get('exchange')),
                            symbol=symbol,
                            datetime=dt)
            d.pop('gateway_name', None)
            d.pop('exchange', None)
            d.pop('symbol', None)
            tick.__dict__.update(d)

            self.symbol_tick_dict[symbol] = tick
            self.gateway.on_tick(tick)
            self.last_tick_dt = tick.datetime

        except Exception as ex:
            self.gateway.write_error(u'RabbitMQ on_message 异常:{}'.format(str(ex)))
            self.gateway.write_error(traceback.format_exc())

    def close(self):
        """退出API"""
        self.gateway.write_log(u'退出rabbit行情订阅API')
        self.connection_status = False

        try:
            if self.sub:
                self.gateway.write_log(u'关闭订阅器')
                self.sub.close()

            if self.thread is not None:
                self.gateway.write_log(u'关闭订阅器接收线程')
                self.thread.join()
        except Exception as ex:
            self.gateway.write_error(u'退出rabbitMQ行情api异常:{}'.format(str(ex)))
