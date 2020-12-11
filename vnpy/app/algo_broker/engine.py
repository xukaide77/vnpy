# encoding: UTF-8

# 算法引擎的代理

from vnpy.amqp.consumer import worker, rpc_server, pika
import json, os, random
import traceback
import logging
from threading import Thread
from datetime import datetime

from vnpy.event import Event, EventEngine
from vnpy.trader.event import EVENT_LOG, EVENT_TIMER
from vnpy.trader.constant import (Direction, OrderType, Offset, Exchange)
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import SubscribeRequest, OrderRequest, CancelRequest, PositionData
from vnpy.trader.utility import get_stock_exchange
from vnpy.trader.util_logger import setup_logger
from vnpy.data.mongo.mongo_data import MongoData

APP_NAME = "AlgoBroker"
SERVICE_DB_NAME = 'Service'         # 运行服务信息记录库
ALGO_INFO_COL = 'algo_info'         # 算法引擎实例


class AlgoWorker(worker):
    """
    算法任务执行者
    它处理 algo task queue,
    """

    def __init__(self, algo_broker, gateway_name='', host='localhost', port=5672, user='admin', password='admin',
                 exchange='x_work_queue', queue='algo_task_queue', routing_key='default'):

        self.algo_broker = algo_broker
        self.gateway_name = gateway_name
        self.routing_key = routing_key
        self.start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        super().__init__(host=host, port=port, user=user, password=password, queue=queue, routing_key=routing_key)
        self.thread = Thread(target=self.start)
        self.thread.start()

    def callback(self, chan, method_frame, _header_frame, body, userdata=None):
        try:
            mission = body.decode('utf-8')
            self.algo_broker.write_log(" [AlgoWorker] received task: {}".format(mission))

            mission = json.loads(mission)

            action = mission.get('action')
            if action == 'add_algo':
                self.add_algo(mission.get('setting'))

            elif action == 'stop_algo':
                self.stop_algo(mission.get('algo_name'))

            chan.basic_ack(delivery_tag=method_frame.delivery_tag)
            self.algo_broker.write_log(" [AlgoWorker] task finished ")
        except Exception as ex:
            self.algo_broker.write_error('AlgoWorker Exception:{}'.format(str(ex)))
            self.algo_broker.write_error(traceback.format_exc())
            chan.basic_ack(delivery_tag=method_frame.delivery_tag)
            self.algo_broker.write_log(" [AlgoWorker] task fail ")

    def add_algo(self, algo_setting):
        """添加算法实例"""
        self.algo_broker.write_log(f'准备添加算法实例:{algo_setting}')
        algo_engine = getattr(self.algo_broker.main_engine, 'algo_engine', None)
        if algo_engine is None:
            self.algo_broker.write_error(u'算法引擎未启动')
            return False

        vt_symbol = algo_setting.get('vt_symbol')
        if vt_symbol.isdigit() and algo_setting.get('is_stock', False):
            exchange_str = get_stock_exchange(vt_symbol)
            if exchange_str:
                vt_symbol = vt_symbol + '.' + exchange_str
                algo_setting.update({'vt_symbol': vt_symbol})

        # 算法引擎
        algoName = algo_engine.start_algo(algo_setting)
        self.algo_broker.write_log(u'start_algo {} : params {}'.format(algoName, str(algo_setting)))

        return True

    def stop_algo(self, algo_name):
        """停止算法实例"""
        self.algo_broker.write_log(f'准备停止算法实例:{algo_name}')
        algo_engine = getattr(self.algo_broker.main_engine, 'algo_engine', None)
        if algo_engine is None:
            self.algo_broker.write_error(u'算法引擎未启动')
            return False

        algo_engine.stop_algo(algo_name)

        return True

    def stop(self):
        self.algo_broker.write_log(u'worker停止')
        try:
            self.channel.stop_comsuming()
            self.connection.close()
            if self.thread:
                self.thread.join()
        except:
            pass


class MqRpcServer(rpc_server):
    """rabbitmq rpc server"""

    def __init__(self, algo_broker, gateway_name='', host='localhost', port=5672, user='admin', password='admin',
                 exchange='x_rpc', queue='rpc_queue', routing_key='default'):
        """
        构造函数
        :param algo_broker: 代理应用
        :param gateway_name: 缺省gateway名称
        :param host: rabbitmq host
        :param port: rabbitmq port
        :param user: rabbitmq user
        :param password: rabbitmq password
        :param exchange: rabbitmq exchange for rpc
        :param queue:  rabbitmq queue for rpc
        :param routing_key: 接收过滤key，在这里，建议自身
        """
        # 算法代理APP
        self.algo_broker = algo_broker
        # 主引擎
        self.main_engine = self.algo_broker.main_engine
        super().__init__(host=host, port=port, user=user, password=password, exchange=exchange, queue=queue,
                         routing_key=routing_key)

        self.algo_broker.write_log(
            u'创建 rpc server: host:{}, port:{}, exchange:{},queue:{},routing_key:{}'.format(host, port, exchange, queue,
                                                                                           routing_key))
        self.thread = Thread(target=self.start)
        self.thread.start()

    def on_request(self, chan, method_frame, _header_frame, body, userdata=None):
        """
        响应rpc请求得处理函数
        :param chan:
        :param method_frame:
        :param _header_frame:
        :param body:
        :param userdata:
        :return:
        """
        if isinstance(body, bytes):
            body = body.decode('utf-8')
        if isinstance(body, str):
            body = json.loads(body)
        self.algo_broker.write_log(" [RPC Server] on_request: %r" % body)
        # 判断body内容类型
        if not isinstance(body, dict):
            resp_data = {'err_code': -1, 'err_msg': u'请求不是dict格式', 'corr_id': _header_frame.correlation_id}
            self.reply(chan, resp_data, _header_frame.reply_to, _header_frame.correlation_id, method_frame.delivery_tag)
            return

        method = body.get('method', None)
        params = body.get('params', {})
        request_time = body.get('time', None)
        if method is None or method not in self.method_dict:
            resp_data = {'err_code': -1, 'err_msg': u'请求方法:{}不在配置中'.format(method),
                         'corr_id': _header_frame.correlation_id}
            self.reply(chan, resp_data, _header_frame.reply_to, _header_frame.correlation_id,
                       method_frame.delivery_tag)
            return

        if request_time is not None:
            request_dt = datetime.strptime(request_time, '%Y-%m-%d %H:%M%S')
            now_dt = datetime.now()
            if (now_dt - request_dt).total_seconds() > 10:
                resp_data = {'err_code': -1,
                             'err_msg': u'响应超时:{},当前:{}'.format(request_time, now_dt.strftime('%Y-%m-%d %H:%M%S')),
                             'corr_id': _header_frame.correlation_id}
                self.reply(chan, resp_data, _header_frame.reply_to, _header_frame.correlation_id,
                           method_frame.delivery_tag)
                return

        function = self.method_dict.get(method)
        try:
            ret = function(**params)
            resp_data = {'err_code': 0, 'data': ret, 'corr_id': _header_frame.correlation_id}
            self.reply(chan, resp_data, _header_frame.reply_to, _header_frame.correlation_id,
                       method_frame.delivery_tag)
        except Exception as ex:
            self.algo_broker.write_error('mq rpc server exception:{}'.format(str(ex)))
            self.algo_broker.write_error(traceback.format_exc())
            resp_data = {'err_code': -1, 'err_msg': '执行异常:{}'.format(str(ex)), 'corr_id': _header_frame.correlation_id}
            self.reply(chan, resp_data, _header_frame.reply_to, _header_frame.correlation_id,
                       method_frame.delivery_tag)

    def stop(self):
        self.algo_broker.write_log(u'mq rpc server 停止')
        try:
            self.channel.stop_comsuming()
            self.connection.close()
            if self.thread:
                self.thread.join()
        except:
            pass


class AlgoBroker(BaseEngine):
    # 算法引擎的代理
    # ==》接收rabbitmq的指令请求，启动响应算法

    # ----------------------------------------------------------------------
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(
            main_engine, event_engine, APP_NAME)

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.app_name = APP_NAME
        self.gateway_name = ""
        self.settings = {}

        self.worker = None
        self.rpc_server = None
        from vnpy.trader.setting import SETTINGS
        self.mongo_db = MongoData(host=SETTINGS.get('hams.host', 'localhost'),
                                  port=SETTINGS.get('hams.port', 27017))


        #self.logger = None
        #self.create_logger()

        self.last_dt = None
        self.registerEvent()

        self.init_engine()

    def write_error(self, msg: str, strategy_name: str = ''):
        """写入错误日志"""
        self.write_log(msg=msg, source=strategy_name, level=logging.ERROR)

    # ----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event):
        """定时执行事件"""
        if self.last_dt is not None:
            if (datetime.now() - self.last_dt).total_seconds() < 60:
                return

        self.last_dt = datetime.now()

        if self.worker is None:
            return

        gw = self.main_engine.get_gateway(self.gateway_name)
        if gw:
            account_id = gw.accountid
        else:
            self.write_error(f'算法代理，找不到网关:{self.gateway_name}')
            return

        flt = {
            'gateway_name': self.gateway_name,
            'account_id': account_id
        }

        data = {
            'algo_engine_name': u'算法引擎',
            'gateway_name': self.gateway_name,
            'start_time': self.worker.start_time,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'exchange': self.worker.exchange,
            'queue': self.worker.queue,
            'routing_key': self.worker.routing_key,
            'status': 'opened'
        }

        self.mongo_db.db_update(db_name=SERVICE_DB_NAME,
                                col_name=ALGO_INFO_COL,
                                data_dict=data,
                                filter_dict=flt,
                                replace=True)

    def init_engine(self):
        """初始化worker"""
        if self.worker:
            return

        # 读取配置文件
        self.load_setting()

        host = self.settings.get('host', 'localhost')
        port = self.settings.get('port', 5672)
        user = self.settings.get('admin', 'admin')
        password = self.settings.get('password', 'admin')
        worker_exchange = self.settings.get('worker_exchange', 'x_work_queue')
        self.gateway_name = self.settings.get('gateway_name', 'gateway_name')
        worker_queue = self.settings.get('worker_queue', 'q_algo_task_{}'.format(self.gateway_name))
        rpc_exchange = self.settings.get('rpc_exchange', 'x_rpc')
        rpc_queue = self.settings.get('rpc_queue', 'q_rpc_svr_{}'.format(self.gateway_name))

        # 创建一个worker
        self.write_log(u'创建rabbitmq worker实例')
        self.worker = AlgoWorker(algo_broker=self, gateway_name=self.gateway_name,
                                 host=host, port=port, user=user, password=password,
                                 exchange=worker_exchange, routing_key=self.gateway_name)
        if self.rpc_server:
            return

        self.write_log(u'创建rabbitmq rpc server实例')
        self.rpc_server = MqRpcServer(algo_broker=self, gateway_name=self.gateway_name,
                                      host=host, port=port, user=user, password=password,
                                      exchange=rpc_exchange, queue=rpc_queue, routing_key=self.gateway_name)

        self.register_rpc_methods()

    def wrapper_send_order(self, **args):
        """
        发送委托  rpc-> vtEngine
        :param params:
        :return:
        """
        self.write_log(u'received rabbit mq rpc call send_order:{}'.format(args))

        symbol = str(args.get('symbol'))
        contract = self.main_engine.get_contract(symbol)
        if contract:
            exchange = contract.exchange
        else:
            self.write_error(f'合约{symbol}找不到配置信息,无法发送委托')
            return ""

        direction = Direction.LONG if args.get('direction', '') in [Direction.LONG, 'long'] else Direction.SHORT
        if str(args.get('offset', '')) in ['None', '']:
            offset = Offset.NONE
        else:
            offset = Offset.OPEN if args.get('offset', '') in [Offset.OPEN, 'open'] else Offset.CLOSE

        price_type = args.get('price_type')
        if price_type in [OrderType.MARKET, u'市价']:
            order_type = OrderType.MARKET
        elif price_type in [OrderType.FAK, 'FAK']:
            order_type = OrderType.FAK
        elif price_type in [OrderType.FOK, 'FOK']:
            order_type = OrderType.FOK
        else:
            order_type = OrderType.LIMIT

        order_req = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            offset=offset,
            type=order_type,
            price=float(args.get('price')),
            volume=args.get('volume'),
            strategy_name=args.get('strategy_name', "")
        )

        ref = self.main_engine.send_order(order_req, args.get('gateway_name'))
        self.write_log(u'委托编号:{}'.format(ref))
        return ref

    def wrapper_cancel_order(self, **args):
        """
        发送委托撤单  rpc-> vtEngine
        :param params:
        :return:
        """
        self.write_log(u'received rabbit mq rpc call cancel_order:{}'.format(args))

        symbol = args.get('symbol', None)
        contract = self.main_engine.get_contract(symbol)
        if contract:
            exchange = contract.exchange
        else:
            self.write_error(f'合约{symbol}找不到配置信息,无法发送撤销')
            return False

        cancel_req = CancelRequest(
            symbol=symbol,
            exchange=exchange,
            orderid=args.get('order_id')
        )

        ref = self.main_engine.cancel_order(cancel_req, args.get('gateway_name'))

        return ref

    def wrapper_cancel_all(self, **args):
        """
        发送委托撤单  rpc-> vtEngine
        :param params:
        :return:
        """
        self.write_log(u'received rabbit mq rpc call cancel_all:{}'.format(args))
        active_orders = self.main_engine.get_all_active_orders()
        cancel_count = len(active_orders)
        for order in active_orders:
            req = CancelRequest(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid
            )
            self.main_engine.cancel_order(req, order.gateway_name)
        return cancel_count

    def register_rpc_methods(self):
        """ 注册rpc的各个可调用方法"""
        if not self.rpc_server:
            return

        # 注册主引擎的方法到服务器的RPC函数
        self.rpc_server.register_method('send_order', self.wrapper_send_order)
        self.rpc_server.register_method('cancel_order', self.wrapper_cancel_order)
        self.rpc_server.register_method('cancel_all', self.wrapper_cancel_all)
        self.rpc_server.register_method('init_strategy', self.main_engine.init_strategy)
        self.rpc_server.register_method('start_strategy', self.main_engine.start_strategy)
        self.rpc_server.register_method('stop_strategy', self.main_engine.stop_strategy)
        self.rpc_server.register_method('reload_strategy', self.main_engine.reload_strategy)
        self.rpc_server.register_method('get_strategy_status', self.main_engine.get_strategy_status)
        self.rpc_server.register_method('save_strategy_data', self.main_engine.save_strategy_data)
        self.rpc_server.register_method('save_strategy_snapshot', self.main_engine.save_strategy_snapshot)
        self.rpc_server.register_method('clean_strategy_cache', self.main_engine.clean_strategy_cache)

        self.write_log(u'完成RPC函数注册:{}'.format(sorted(self.rpc_server.method_dict.keys())))

    def load_setting(self):
        """
        读取配置文件
        :return:
        """
        try:
            # 设置里面，所有得 {rabbitmq.xxxx: value} => { xxxx: value}
            from vnpy.trader.setting import SETTINGS
            self.settings = {k.replace('rabbitmq.', ''): v for k, v in SETTINGS.items() if k.startswith('rabbitmq')}
            self.settings.update({'gateway_name': SETTINGS.get('gateway_name')})

        except Exception as ex:
            self.write_error(u'加载算法代理配置异常:{},{}'.format(str(ex), traceback.format_exc()))

        self.write_log(u'加载算法代理配置成功:{}'.format(self.settings))

    def close(self):
        print('AlgoBroker close')
        if self.rpc_server:
            self.rpc_server.stop()

        if self.worker:
            self.worker.stop()
