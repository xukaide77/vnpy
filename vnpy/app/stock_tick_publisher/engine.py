# encoding: UTF-8

# 股票行情订阅/发布器
# 华富资产
import sys
import copy
import json
import traceback
from queue import Queue
from threading import Thread
from datetime import datetime, timedelta
from time import sleep
from logging import ERROR
from typing import Dict

from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.object import TickData
from vnpy.api.rest.rest_client import RestClient
from vnpy.trader.utility import load_json, save_json
from vnpy.amqp.producer import publisher
from vnpy.amqp.consumer import worker

APP_NAME = 'Stock_Publisher'
REST_HOST = 'http://49.234.35.135:8006'
SUBSCRIBE_FILE = 'today_subscribe.json'

# 市场交易代码 => vnpy
EXCHANGE_CODE2VT: Dict[str, Exchange] = {
    "USHI": Exchange.SSE,  # 沪市指数
    "USHA": Exchange.SSE,  # 沪市 A 股
    "USHB": Exchange.SSE,  # 沪市 B 股
    "USHD": Exchange.SSE,  # 沪市债券
    "USHJ": Exchange.SSE,  # 沪市基金
    "USHT": Exchange.SSE,  # 沪市风险
    "USZI": Exchange.SZSE,  # 深市指数
    "USZA": Exchange.SZSE,  # 深市 A 股
    "USZB": Exchange.SZSE,  # 深市 B 股
    "USZD": Exchange.SZSE,  # 深市债券
    "USZJ": Exchange.SZSE,  # 深市基金
}


class SubscribeWorker(worker):
    """
    订阅任务执行者
    它处理 subscribe task queue,
    """

    def __init__(self, parent, gateway_name='', host='localhost', port=5672, user='admin', password='admin',
                 exchange='x_work_queue', queue='subscribe_task_queue', routing_key='stock_subscribe'):

        self.parent = parent
        self.gateway_name = gateway_name
        self.routing_key = routing_key
        self.start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        super().__init__(host=host, port=port, user=user, password=password, exchange=exchange, queue=queue,
                         routing_key=routing_key)
        self.thread = Thread(target=self.start)
        self.thread.start()

    def callback(self, chan, method_frame, _header_frame, body, userdata=None):
        """
        处理任务的回调函数
        :param chan:
        :param method_frame:
        :param _header_frame:
        :param body:
        :param userdata:
        :return:
        """
        try:
            mission = body.decode('utf-8')
            self.parent.write_log(" [SubscribeWorker] received task: {}".format(mission))

            mission = json.loads(mission)

            action = mission.get('action')
            if action == 'subscribe':
                self.task_subscribe(mission)

            # 订阅任务执行完毕
            chan.basic_ack(delivery_tag=method_frame.delivery_tag)
            self.parent.write_log(" [SubscribeWorker] task finished ")
        except Exception as ex:
            self.parent.write_error('SubscribeWorker Exception:{}'.format(str(ex)))
            # self.parent.write_error(traceback.format_exc())
            chan.basic_ack(delivery_tag=method_frame.delivery_tag)
            self.parent.write_log(" [SubscribeWorker] task fail ")

    def task_subscribe(self, setting):
        """执行订阅任务"""
        self.parent.write_log(f'准备添加行情订阅:{setting}')

        self.parent.subscribe(setting.get('vt_symbol'))
        return True

    def stop(self):
        """
        停止运行
        :return:
        """
        self.parent.write_log(u'worker停止')
        try:
            self.channel.stop_comsuming()
            self.connection.close()
            if self.thread:
                self.thread.join()
        except:
            pass


class StockRestClient(RestClient):

    def __init__(self, parent=None):
        """
        构造函数
        """
        super().__init__()
        self.parent = parent
        self.init(url_base=REST_HOST)

    def write_log(self, msg):
        """日志"""
        if self.parent and hasattr(self.parent, 'write_log'):
            func = getattr(self.parent, 'write_log')
            func(msg)
        else:
            print(msg)

    def write_error(self, msg):
        """错误日志"""
        if self.parent and hasattr(self.parent, 'write_error'):
            func = getattr(self.parent, 'write_error')
            func(msg)
        else:
            print(msg, file=sys.stderr)


class StockTickPublisher(BaseEngine):
    # 股票tick发布服务
    # 接收RabbitMQ接受股票行情订阅请求
    # 通过URL的免费行情接口，获取股票行情tick，发布至rabbitMQ

    # ----------------------------------------------------------------------
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(
            main_engine, event_engine, APP_NAME)

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.create_logger(logger_name=APP_NAME)

        self.last_minute = None

        self.register_event()

        self.req_interval = 1  # 操作请求间隔1秒
        self.req_id = 0  # 操作请求编号
        self.connection_status = False  # 连接状态

        self.api = None  # API 的连接会话对象
        self.last_tick_dt = None  # 记录该会话对象的最后一个tick时间

        self.subscribe_symbols = set()

        # 获取持久化订阅得合约
        self.subscribe_dict = load_json(SUBSCRIBE_FILE, auto_save=True)
        cur_trading_day = datetime.now().strftime('%Y-%m-%d')
        # 对当前交易日得订阅合约，逐一添加
        for symbol in self.subscribe_dict.get(cur_trading_day, []):
            self.write_log(f'添加订阅{cur_trading_day}的股票:{symbol}')
            self.subscribe_symbols.add(symbol)
        # 排除非当前交易日点阅得合约
        for trading_day in list(self.subscribe_dict.keys()):
            if trading_day != cur_trading_day:
                self.subscribe_dict.pop(trading_day, None)

        # 重新保存
        save_json(SUBSCRIBE_FILE, self.subscribe_dict)

        # vt_setting.json内rabbitmq配置项
        self.conf = {}
        self.pub = None  # 行情推送器
        self.worker = None  # 订阅任务处理
        self.req_thread = None

        # 负责执行数据库插入的单独线程相关
        # 是否启动
        self.active = False
        self.pub_queue = Queue()  # 队列
        self.pub_thread = None

    def write_error(self, content: str):
        self.write_log(msg=content, level=ERROR)

    def create_publisher(self, conf):
        """创建rabbitmq 消息发布器"""
        self.write_log(f'创建rabbitmq 消息发布器')
        if self.pub:
            return
        host = conf.get('host', 'localhost')
        port = conf.get('port', 5672)
        user = conf.get('user', 'admin')
        password = conf.get('password', 'admin')
        channel_number = conf.get('channel_number', 1)
        queue_name = conf.get('queue_name', '')
        routing_key = conf.get('routing_key', 'default')
        exchange = conf.get('exchange', 'x_fanout_stock_tick')

        self.write_log(f'创建rabbit MQ消息发布器:{host}:{port}/ex:{exchange}/q_n:{queue_name}/r_k:{routing_key}')
        try:
            # 消息发布
            self.pub = publisher(host=host,
                                 port=port,
                                 user=user,
                                 password=password,
                                 channel_number=channel_number,
                                 queue_name=queue_name,
                                 routing_key=routing_key,
                                 exchange=exchange)

        except Exception as ex:
            self.write_error(u'创建tick发布器异常:{}'.format(str(ex)))

    def create_worker(self, conf):
        """
        创建订阅任务接收器
        :param conf:
        :return:
        """
        host = conf.get('host', 'localhost')
        port = conf.get('port', 5672)
        user = conf.get('user', 'admin')
        password = conf.get('password', 'admin')
        exchange = 'x_work_queue'
        queue = 'subscribe_task_queue'
        routing_key = 'stock_subscribe'
        self.write_error(f'创建订阅任务接收器:{host}:{port}/q_n:{queue}/r_k:{routing_key}')
        try:
            self.worker = SubscribeWorker(
                parent=self,
                host=host,
                port=port,
                user=user,
                password=password,
                exchange=exchange,
                queue=queue,
                routing_key=routing_key)
            self.worker.start()
        except Exception as ex:
            self.write_error(f'创建订阅任务接收器异常:{str(ex)}')

    # ----------------------------------------------------------------------
    def register_event(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event):
        """定时执行"""
        dt = datetime.now()

        if dt.minute == self.last_minute:
            return

    def connect(self, rabbit_config: dict):
        """
        连接股票行情服务器
        :param n:
        :return:
        """
        if self.connection_status:
            if self.api is not None or getattr(self.api, "client", None) is not None:
                self.write_log(u'当前已经连接,不需要重新连接')
                return

        self.write_log(u'开始接入股票行情服务器')
        try:
            self.write_log(f'创建Rest客户端')
            self.api = StockRestClient()
            self.write_log(f'启动Rest客户端服务')
            self.api.start()
            self.connection_status = True

        except Exception as ex:
            self.write_error(u'连接Rest服务器异常:{},{}'.format(str(ex), traceback.format_exc()))

            return

        # 更新配置
        self.conf.update(rabbit_config)

        # 创建推送线程
        self.pub_thread = Thread(target=self.run_pub)  # 线程

        self.pub_thread.start()

        # 创建请求线程
        self.req_thread = Thread(target=self.run_req)

        # 启动请求线程
        self.req_thread.start()

        # 创建处理订阅线程
        self.create_worker(self.conf)

    def subscribe(self, symbol):
        """订阅股票行情"""
        self.write_log(f'订阅股票行情:{symbol}')
        if '.' in symbol:
            symbol = symbol.split('.')[0]
            self.write_log(f'合约=>{symbol}')
        # 添加订阅
        self.subscribe_symbols.add(symbol)

        # 持久化=》json文件
        today = datetime.now().strftime('%Y-%m-%d')
        subscribe_symbols = self.subscribe_dict.get(today, [])
        if symbol not in subscribe_symbols:
            subscribe_symbols.append(symbol)
            self.subscribe_dict[today] = subscribe_symbols
            save_json(SUBSCRIBE_FILE, self.subscribe_dict)

    def close(self):
        """退出API"""
        self.write_log(u'退出Rest API')
        self.connection_status = False
        self.active = False
        if self.api:
            self.write_log(f'退出Rest客户端')
            self.api.stop()
            self.api.join()

        if self.req_thread is not None:
            self.write_log(u'退出请求线程')
            self.req_thread.join()

        if self.pub:
            self.write_log(u'退出rabbitMQ 发布器')
            self.pub.exit()

        if self.pub_thread is not None:
            self.write_log(f'退出pub线程')
            self.pub_thread.join()

    def run_req(self):

        try:
            last_dt = datetime.now()
            self.write_log(u'开始运行行情轮询,{}'.format(last_dt))
            while self.connection_status:
                try:
                    self.query_tick(
                        symbols=list(self.subscribe_symbols)
                    )

                except Exception as ex:
                    self.write_error(u'rest exception:{},{}'.format(str(ex), traceback.format_exc()))

                sleep(self.req_interval)
                dt = datetime.now()
                if last_dt.minute != dt.minute:
                    self.write_log('check point. {},last_tick_dt:{}'.format(dt, self.last_tick_dt))
                    last_dt = dt
        except Exception as ex:
            self.write_error(u'rest pool.run exception:{},{}'.format(str(ex), traceback.format_exc()))

        self.write_error(u'rest 线程 {}退出'.format(datetime.now()))

    # ----------------------------------------------------------------------
    def run_pub(self):
        """运行推送线程"""
        try:
            self.create_publisher(self.conf)
            self.active = True
            self.write_log(f'启动推送线程')
            while self.active:
                try:
                    d = self.pub_queue.get(block=True, timeout=1)
                    if self.pub:
                        self.pub.pub(d)
                except Exception as ex:  # noqa
                    pass
        except Exchange as ex:
            pass
        self.write_log(f'推送线程结束')

    def check_error(self, data: dict, func: str = ""):
        """"""
        if data["err_code"] == 0:
            return False

        error_msg = data["err_msg"]

        self.write_log(f"{func}请求出错，信息：{error_msg}")
        return True

    def query_tick(self, symbols):
        """
        查询基础行情数据，不包括买卖盘数据
        :param symbols:
        :return:
        """
        total_symbols = len(symbols)
        if total_symbols == 0:
            return
        # 每次请求，不超过10个股票
        for i in range(int(total_symbols / 10) + 1):
            path = "/stock/query/comm?code={}".format(','.join(symbols[i * 10:10 * (i + 1)]))
            self.api.add_request(
                method="GET",
                path=path,
                callback=self.on_query_tick
            )

    def on_query_tick(self, data, request):
        """处理股票行情tick"""

        if self.check_error(data, "查询股票实时行情"):
            return

        for d in data.get('data', []):
            symbol = d.get('code', None)
            if symbol is None or len(symbol) != 6:
                continue
            market = d.get('market')
            exchange = EXCHANGE_CODE2VT.get(market, None)
            if exchange is None:
                continue

            tick_datetime = datetime.now()

            # # 修正毫秒
            # last_tick = self.symbol_tick_dict.get(vn_symbol, None)
            # if (last_tick is not None) and tick_datetime.replace(microsecond=0) == last_tick.datetime:
            #     # 与上一个tick的时间（去除毫秒后）相同,修改为500毫秒
            #     tick_datetime = tick_datetime.replace(microsecond=500)
            # else:
            tick_datetime = tick_datetime.replace(microsecond=0)

            tick = TickData(
                gateway_name='tdx',
                symbol=symbol,
                datetime=tick_datetime,
                exchange=exchange
            )

            tick.pre_close = float(d.get('prev_close', 0.0))
            tick.high_price = float(d.get('high', 0.0))
            tick.open_price = float(d.get('open', 0.0))
            tick.low_price = float(d.get('low', 0.0))
            tick.last_price = float(d.get('price', 0.0))

            tick.volume = int(d.get('tran_volume', 0))
            tick.open_interest = d.get('tran_amount')

            tick.time = tick.datetime.strftime('%H:%M:%S.%f')[0:12]
            tick.date = tick.datetime.strftime('%Y-%m-%d')

            tick.trading_day = tick.date

            # 指数没有涨停和跌停，就用昨日收盘价正负10%
            tick.limit_up = float(d.get('high_limit'))
            tick.limit_down = float(d.get('low_limit'))

            # 只有一档行情
            tick.bid_price_1 = float(d.get('B1', 0.0))
            tick.bid_volume_1 = int(d.get('B1V', 0))
            tick.ask_price_1 = float(d.get('S1', 0.0))
            tick.ask_volume_1 = int(d.get('S1V', 0))

            # 排除非交易时间得tick

            if tick.datetime.hour not in [9, 10, 11, 13, 14, 15]:
                continue
            if tick.datetime.hour == 9 and tick.datetime.minute < 30:
                continue
            # 排除早盘 11:30~12:00
            if tick.datetime.hour == 11 and tick.datetime.minute >= 30:
                continue
            if tick.datetime.hour == 15 and tick.datetime.minute >= 1:
                continue

            self.last_tick_dt = tick.datetime

            # self.symbol_tick_dict[tick.symbol] = tick
            # =》写入本地队列
            d = copy.copy(tick.__dict__)
            if isinstance(tick.datetime, datetime):
                d.update({'datetime': tick.datetime.strftime('%Y-%m-%d %H:%M:%S.%f')})
            d.update({'exchange': tick.exchange.value})
            d = json.dumps(d)
            self.pub_queue.put(d)

            #if self.pub:
            #    self.pub.pub(d)
