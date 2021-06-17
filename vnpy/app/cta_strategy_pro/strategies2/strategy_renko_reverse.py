# encoding: UTF-8

# 首先写系统内置模块
import sys
from datetime import datetime, timedelta
from copy import copy
import traceback
from collections import OrderedDict

# 其次，导入vnpy的基础模块
from vnpy.app.cta_strategy_pro import (
    CtaProFutureTemplate,
    Direction,
    Status,
    Color,
    TickData,
    BarData
)
from vnpy.component.cta_grid_trade import LOCK_GRID, CtaGrid, CtaGridTrade
from vnpy.component.cta_policy import (
    TNS_STATUS_OPENED,
    TNS_STATUS_ORDERING,
    TNS_STATUS_OBSERVATE,
    CtaPolicy
)
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_renko_bar import CtaRenkoBar
from vnpy.component.cta_line_bar import CtaMinuteBar
from vnpy.data.renko.renko_source import RenkoSource
from vnpy.data.tdx.tdx_future_data import TdxFutureData
from vnpy.trader.utility import get_underlying_symbol
from vnpy.trader.setting import SETTINGS

# 信号
SIGNAL_HIGHEST = 'HIGHEST'  # 唐其安通道最高价，观测卖出信号
SIGNAL_LOWEST = 'LOWEST'  # 唐其安通道最低价，观测买入信号


class Reverse_Policy(CtaPolicy):
    """反转策略事务"""

    def __init__(self, strategy):
        super().__init__(strategy)

        self.last_signal = ''  # 最近一次信号
        self.last_signal_time = None  # 最近一次信号得发生时间

        self.x_last_signal = ''  # x分钟k线最后一次信号
        self.x_last_signal_time = None  # x分钟K线最后一次信号的时间

        self.tns_direction = None  # 持仓事务，Direction.LONG: 持有多单， Direction.SHORT,做空事务
        self.tns_open_price = 0  # 事务开启时的价格
        self.tns_stop_price = 0  # 事务止损价

        # 子事务，信号发现=》就绪，开始委托=》委托完成
        self.sub_tns = {}  # key = 信号  value= { 状态; 时间; 价格等}

    def to_json(self):
        """
        将数据转换成dict
        :return:
        """
        j = OrderedDict()
        j['create_time'] = self.create_time.strftime(
            '%Y-%m-%d %H:%M:%S') if self.create_time is not None else ''
        j['save_time'] = self.save_time.strftime('%Y-%m-%d %H:%M:%S') if self.save_time is not None else ''

        j['last_signal'] = self.last_signal
        j['last_signal_time'] = self.last_signal_time.strftime(
            '%Y-%m-%d %H:%M:%S') if self.last_signal_time is not None else ''

        j['x_last_signal'] = self.x_last_signal
        j['x_last_signal_time'] = self.x_last_signal_time.strftime(
            '%Y-%m-%d %H:%M:%S') if self.x_last_signal_time is not None else ''

        j['tns_direction'] = self.tns_direction.value if self.tns_direction else ''
        j['tns_open_price'] = self.tns_open_price
        j['tns_stop_price'] = self.tns_stop_price
        j['sub_tns'] = self.sub_tns

        return j

    def fromJson(self, json_data):
        """
        将dict转化为属性
        :param json_data:
        :return:
        """
        if 'create_time' in json_data:
            try:
                self.create_time = datetime.strptime(json_data['create_time'], '%Y-%m-%d %H:%M:%S')
            except Exception as ex:  # noqa
                self.create_time = datetime.now()

        if 'save_time' in json_data:
            try:
                self.save_time = datetime.strptime(json_data['save_time'], '%Y-%m-%d %H:%M:%S')
            except Exception as ex:  # noqa
                self.save_time = datetime.now()

        self.last_signal = json_data.get('last_signal', '')
        if 'last_signal_time' in json_data:
            try:
                if len(json_data['last_signal_time']) > 0:
                    self.last_signal_time = datetime.strptime(json_data['last_signal_time'], '%Y-%m-%d %H:%M:%S')
                else:
                    self.last_signal_time = None
            except Exception as ex:  # noqa
                self.last_signal_time = None

        self.x_last_signal = json_data.get('x_last_signal', '')
        if 'x_last_signal_time' in json_data:
            try:
                if len(json_data['x_last_signal_time']) > 0:
                    self.x_last_signal_time = datetime.strptime(json_data['x_last_signal_time'], '%Y-%m-%d %H:%M:%S')
                else:
                    self.x_last_signal_time = None
            except Exception as ex:  # noqa
                self.x_last_signal_time = None

        tns_direction = json_data.get('tns_direction', '')
        self.tns_direction = None if tns_direction == '' else Direction(tns_direction)
        self.tns_open_price = json_data.get('tns_open_price', 0)
        self.tns_stop_price = json_data.get('tns_stop_price', 0)
        self.sub_tns = json_data.get('sub_tns', {})

    def clean(self):
        """
        清空数据
        :return:
        """
        self.write_log(u'清空policy数据')
        self.last_signal = ''
        self.last_signal_time = None

        self.x_last_signal = ''
        self.x_last_signal_time = None

        self.tns_direction = None
        self.tns_open_price = 0
        self.tns_stop_price = 0
        self.sub_tns = {}


class Strategy_Renko_Reverse(CtaProFutureTemplate):
    """
        V 反策略
        砖图 + 15分钟 时间周期
    """

    author = u'李来佳'
    para_pre_len = 20
    para_yb_len = 10
    renko_height = '5'
    x_minute = 15
    parameters = ["max_invest_pos", "max_invest_margin", "max_invest_rate", "single_lost_percent",
                  "renko_height", "x_minute"
                  "para_pre_len", "para_yb_len",
                  "backtesting"]

    # ----------------------------------------------------------------------
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting=None):
        """Constructor"""
        super().__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        # 仓位状态
        self.position = CtaPosition(self)  # 0 表示没有仓位，1 表示持有多头，-1 表示持有空头
        self.position.maxPos = 1000

        # 创建网格交易,用来记录
        self.gt = CtaGridTrade(strategy=self)

        # 执行策略
        self.policy = Reverse_Policy(self)

        self.line_renko = None  # 砖图K线
        self.line_x = None  # 分钟K线

        # 读取策略配置json文件
        if setting:
            self.update_setting(setting)

            # 创建 renko K线
            renko_setting = {}
            renko_setting['name'] = u'Renko_{}'.format(self.renko_height)

            if isinstance(self.renko_height, str) and 'K' in self.renko_height:
                kilo_height = int(self.renko_height.replace('K', ''))
                # renko_height = self.price_tick * kilo_height
                self.write_log(u'使用价格千分比:{}'.format(kilo_height))
                renko_setting.update({'kilo_height': kilo_height})
            else:
                self.write_log(u'使用绝对砖块高度数:{}'.format(self.renko_height))
                renko_setting['height'] = self.renko_height * self.price_tick

            renko_setting['para_pre_len'] = self.para_pre_len
            renko_setting['para_boll_len'] = 26
            renko_setting['para_ma1_len'] = 5
            renko_setting['para_active_yb'] = True
            renko_setting['para_yb_len'] = self.para_yb_len
            renko_setting['para_active_skd'] = True

            renko_setting['price_tick'] = self.price_tick
            renko_setting['underly_symbol'] = get_underlying_symbol(vt_symbol).upper()

            self.line_renko = CtaRenkoBar(strategy=self, cb_on_bar=self.on_bar_renko, setting=renko_setting)
            self.klines.update({self.line_renko.name: self.line_renko})

            # 创建的x分钟 K线(使用分钟bar）
            kline_setting = {}
            kline_setting['name'] = u'M{}'.format(self.x_minute)  # k线名称
            kline_setting['bar_interval'] = self.x_minute  # K线的Bar时长
            kline_setting['para_ma1_len'] = 5  # 第1条均线
            kline_setting['para_ma2_len'] = 18  # 第2条均线
            kline_setting['para_ma3_len'] = 60  # 第3条均线
            kline_setting['para_atr1_len'] = 26  # ATR
            kline_setting['para_pre_len'] = 20  # 前高/前低
            kline_setting['price_tick'] = self.price_tick
            kline_setting['underly_symbol'] = get_underlying_symbol(vt_symbol).upper()
            self.line_x = CtaMinuteBar(self, self.on_bar_x, kline_setting)
            self.klines.update({self.line_x.name: self.line_x})

            # 第一次回测时，输出两种K线的所有数据以及指标
            self.export_klines()

        if self.backtesting:
            # 回测盘使用限价单方式
            self.on_init()

    # ----------------------------------------------------------------------
    def on_init(self, force=False):
        """初始化"""
        self.write_log(u'策略初始化')

        if self.inited:
            if force:
                self.write_log(u'策略强制初始化')
                self.inited = False
                self.trading = False  # 控制是否启动交易
                self.position.pos = 0  # 仓差
                self.position.longPos = 0  # 多头持仓
                self.position.shortPos = 0  # 空头持仓
                self.gt.upGrids = []
                self.gt.dnGrids = []
            else:
                self.write_log(u'策略初始化')
                self.write_log(u'已经初始化过，不再执行')
                return

        # 得到持久化的Policy中的子事务数据
        self.load_policy()
        self.display_tns()

        if not self.backtesting:
            # 实盘时，恢复klines的所有数据
            if not self.init_kline_datas():
                return

        self.write_log(u'策略初始化加载历史数据完成')
        self.init_position()  # 初始持仓数据
        self.inited = True
        if not self.backtesting:
            self.trading = True  # 控制是否启动交易

        self.write_log(u'策略初始化加载历史持仓、策略数据完成')
        self.display_grids()
        self.display_tns()

        self.write_log(u'策略初始化完成: Strategy({}) '.format(self.strategy_name))
        self.put_event()

    def init_kline_datas(self):
        """恢复K线数据"""
        # 从数据源加载最新的renko bar, 不足，则获取ticks
        try:
            dt_now = datetime.now()
            # renko bar 数据源
            ds = RenkoSource(strategy=self, setting={'host': SETTINGS.get('database.host'),
                                                     'port': SETTINGS.get('database.port')
                                                     })
            # 通达信的期货数据源
            tdx = TdxFutureData(strategy=self)

            # 从本地缓存文件中加载K线，并取得最后的bar时间
            last_bar_dt = self.load_klines_from_cache()

            if isinstance(last_bar_dt, datetime):
                self.write_log(u'缓存数据bar最后时间:{}'.format(last_bar_dt))
                self.cur_datetime = last_bar_dt
                start_date = (last_bar_dt - timedelta(days=1)).strftime('%Y-%m-%d')
                # 取renko bar
                ret, renko_bars = ds.get_bars(symbol=self.idx_symbol, height=self.renko_height, start_date=start_date,
                                              limit_num=3000)
                # 取 1分钟bar
                result, min1_bars = tdx.get_bars(symbol=self.idx_symbol, period='1min', callback=None, bar_freq=1,
                                                 start_dt=start_date)
            else:
                self.write_log(u'无本地缓存文件，取最后3000条renko数据')
                # 取renko bar
                ret, renko_bars = ds.get_bars(symbol=self.idx_symbol, height=self.renko_height, limit_num=3000)
                # 取 1分钟bar
                start_date = (last_bar_dt - timedelta(days=90)).strftime('%Y-%m-%d')
                self.write_log(u'无本地缓存文件，取90天1分钟数据')
                result, min1_bars = tdx.get_bars(symbol=self.idx_symbol, period='1min', callback=None, bar_freq=1,
                                                 start_dt=start_date)

            bar_len = len(renko_bars)
            self.write_log(u'一共获取{}条{}_{} Renko Bar'.format(bar_len, self.idx_symbol, self.renko_height))
            bar_count = 0

            for bar in renko_bars:
                if isinstance(last_bar_dt, datetime):
                    if bar.datetime + timedelta(seconds=bar.seconds) < last_bar_dt:
                        continue
                self.cur_datetime = bar.datetime + timedelta(seconds=bar.seconds)

                bar_count += 1
                if bar_count >= bar_len - 10 or bar_count == 1:
                    self.write_log(u'{} o:{};h:{};l:{};c:{},v:{},{}'
                                   .format(bar.date + ' ' + bar.time,
                                           bar.open_price, bar.high_price, bar.low_price, bar.close_price,
                                           bar.volume, bar.color))
                self.cur_99_price = bar.close_price
                self.line_renko.add_bar(bar)
            self.write_log(u'最后一根renko bar:{}'.format(self.line_renko.get_last_bar_str()))

            if not result:
                self.write_error(u'未能取回1分钟数据')
                return False

            for bar in min1_bars:
                if last_bar_dt and bar.datetime < last_bar_dt:
                    continue
                bar.datetime = bar.datetime - timedelta(minutes=1)
                bar.time = bar.datetime.strftime('%H:%M:%S')
                self.line_x.add_bar(bar, bar_freq=1)
            self.write_log(u'最后一根renko{} bar:{}'.format(self.line_x.name, self.line_x.get_last_bar_str()))

            ret, ticks = ds.get_ticks(symbol=self.idx_symbol, min_diff=self.price_tick, start_dt=self.cur_datetime)

            if ret:
                self.write_log(u'初始化tick:{} ~ {},共{}'.format(self.cur_datetime, datetime.now(), len(ticks)))
                for tick in ticks:
                    if tick.datetime > dt_now:
                        continue
                    # 丢弃超过20%变动得异常数据包
                    if self.cur_99_price > 0 and tick.last_price > 0:
                        if tick.last_price > self.cur_99_price * 1.2 or tick.last_price < self.cur_99_price * 0.8:
                            continue
                    self.cur_datetime = tick.datetime
                    self.cur_99_price = tick.last_price
                    self.line_x.on_tick(copy(tick))
                    self.line_renko.on_tick(tick)

        except Exception as e:
            self.write_error(u'{}策略初始化加载历史数据失败：{},{}'.format(self.strategy_name, str(e), traceback.format_exc()))
            return False

        return True

    def export_klines(self):
        """输出K线=》csv文件"""
        if not self.backtesting:
            return

        # 写入文件
        import os
        self.line_renko.export_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}.csv'.format(self.strategy_name, self.line_renko.name)))

        self.line_renko.export_fields = [
            {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
            {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
            {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
            {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
            {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
            {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
            {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
            {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'},
            {'name': 'pre_high', 'source': 'line_bar', 'attr': 'line_pre_high', 'type_': 'list'},
            {'name': 'pre_low', 'source': 'line_bar', 'attr': 'line_pre_low', 'type_': 'list'},
            {'name': 'boll_upper', 'source': 'line_bar', 'attr': 'line_boll_upper', 'type_': 'list'},
            {'name': 'boll_middle', 'source': 'line_bar', 'attr': 'line_boll_middle', 'type_': 'list'},
            {'name': 'boll_lower', 'source': 'line_bar', 'attr': 'line_boll_lower', 'type_': 'list'},
            {'name': f'ma{self.line_renko.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1', 'type_': 'list'},
            {'name': 'sk', 'source': 'line_bar', 'attr': 'line_sk', 'type_': 'list'},
            {'name': 'sd', 'source': 'line_bar', 'attr': 'line_sk', 'type_': 'list'},
            {'name': 'yb', 'source': 'line_bar', 'attr': 'line_yb', 'type_': 'list'},
        ]

        self.line_x.export_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}.csv'.format(self.strategy_name, self.line_x.name)))

        self.line_x.export_fields = [
            {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
            {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
            {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
            {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
            {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
            {'name': 'turnover', 'source': 'bar', 'attr': 'turnover', 'type_': 'float'},
            {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
            {'name': 'open_interest', 'source': 'bar', 'attr': 'open_interest', 'type_': 'float'},
            {'name': 'pre_high', 'source': 'line_bar', 'attr': 'line_pre_high', 'type_': 'list'},
            {'name': 'pre_low', 'source': 'line_bar', 'attr': 'line_pre_low', 'type_': 'list'},
            {'name': f'ma{self.line_x.para_ma1_len}', 'source': 'line_bar', 'attr': 'line_ma1',
             'type_': 'list'},
            {'name': f'ma{self.line_x.para_ma2_len}', 'source': 'line_bar', 'attr': 'line_ma2',
             'type_': 'list'},
            {'name': f'ma{self.line_x.para_ma3_len}', 'source': 'line_bar', 'attr': 'line_ma3',
             'type_': 'list'},
            {'name': 'atr', 'source': 'line_bar', 'attr': 'line_atr1', 'type_': 'list'},
        ]

    # ----------------------------------------------------------------------
    def on_tick(self, tick: TickData):
        """行情更新
        1、
        2、推送Tick到lineM
        3、强制清仓逻辑
        4、止损逻辑
        :type tick: object
        """
        # 实盘检查是否初始化数据完毕。如果数据未初始化完毕，则不更新tick，避免影响cur_99_price
        if not self.backtesting:
            if not self.inited:
                self.write_log(u'数据还没初始化完毕，不更新tick')
                return

        # 更新所有tick dict（包括 指数/主力/历史持仓合约)
        self.tick_dict.update({tick.vt_symbol: tick})

        if tick.vt_symbol == self.vt_symbol:
            self.cur_mi_tick = tick
            self.cur_mi_price = tick.last_price

        if tick.vt_symbol == self.idx_symbol:
            self.cur_99_tick = tick
            self.cur_99_price = tick.last_price

            # 如果指数得tick先到达，而主力价格未到，则丢弃这个tick
            if self.cur_mi_tick is None:
                self.write_log(u'主力tick未到达，先丢弃当前指数tick:{},价格:{}'.format(self.idx_symbol, self.cur_99_price))
                return
        else:
            # 所有非vtSymbol得tick，全部返回
            return

        # 更新策略执行的时间（用于回测时记录发生的时间）
        self.cur_datetime = tick.datetime

        # 丢弃超过20%变动得异常数据包
        if self.cur_99_price > 0 and tick.last_price > 0:
            if tick.last_price > self.cur_99_price * 1.2 or tick.last_price < self.cur_99_price * 0.8:
                return

        self.cur_99_price = tick.last_price

        if 3 <= tick.datetime.hour <= 8 or 16 <= tick.datetime.hour <= 20:
            self.write_log(u'休市/集合竞价排名时数据不处理')
            return
        self.line_x.on_tick(copy(tick))
        self.line_renko.on_tick(copy(tick))
        # 4、交易逻辑

        # 首先检查是否是实盘运行还是数据预处理阶段
        if not self.inited or not self.trading:
            return

        # 执行撤单逻辑
        self.tns_cancel_logic(tick.datetime, reopen=True)

        # 网格逐一止损/止盈检查
        self.grid_check_stop()

        # 处理每个信号的观测数据
        self.process_sub_tns()

        # onTick驱动每分钟执行
        if self.last_minute != tick.datetime.minute:
            self.last_minute = tick.datetime.minute

            # 更换合约检查
            if tick.datetime.minute >= 5:
                if self.position.long_pos > 0 and len(self.tick_dict) > 2:
                    # 有多单，且订阅的tick为两个以上
                    self.tns_switch_long_pos()
                elif self.position.short_pos < 0 and len(self.tick_dict) > 2:
                    # 有空单，且订阅的tick为两个以上
                    self.tns_switch_short_pos()

            if not self.backtesting:
                self.display_grids()
                self.write_log(self.line_renko.get_last_bar_str())
                self.display_tns()

            if self.cur_datetime.hour == 14 and self.cur_datetime.minute >= 55:
                self.tns_close_locked_grids(grid_type='unlock')

            self.put_event()

    # ----------------------------------------------------------------------
    def on_bar(self, bar: BarData):
        """
        1分钟K线数据
        :param bar:
        :return:
        """
        pass

    def on_bar_renko(self, *args, **kwargs):
        """
        运行分钟K线OnBar事件
        1、止盈/止损
        2、模拟计算盘中黄蓝信号
        3、更新风险度执行事务平多仓位
        4、更新可开仓位数量
        :return:
        """
        bar = None
        if len(args) > 0:
            bar = args[0]
        elif 'bar' in kwargs:
            bar = kwargs.get('bar')
        if bar is None:
            return

        if self.inited:
            self.write_log(self.line_renko.get_last_bar_str())

        if self.inited:
            # 计算/处理分钟信号
            self.tns_calculate_signal()
        if self.trading and not self.backtesting:
            self.display_tns()
        if self.inited and self.entrust == 0:
            if self.position.pos > 0 and self.policy.tns_direction == Direction.SHORT:
                self.write_log(u'系统在非做多周期内，持有多单，需要清除')
                self.tns_update_stop_price(direction=Direction.LONG, price=self.cur_99_price * 2)
                return
            if self.position.pos < 0 and self.policy.tns_direction == Direction.LONG:
                self.write_log(u'系统在非做空周期内，持有空单，需要清除')
                self.tns_update_stop_price(direction=Direction.SHORT, price=self.cur_99_price / 2)
                return

    def on_bar_x(self, bar):
        """x分钟的onBar事件"""
        # 调用kline_x的显示bar内容
        self.write_log(self.line_x.get_last_bar_str())

        # 未初始化完成
        if not self.inited:
            return

        self.tns_calculate_x_signal()

    def grid_check_stop(self):
        """
        网格逐一止损/止盈检查 (根据指数价格进行止损止盈）
        :return:
        """
        if self.entrust != 0:
            return

        if not self.trading:
            if not self.backtesting:
                self.write_error(u'当前不允许交易')
            return

        # 多单网格逐一止损/止盈检查：
        long_grids = self.gt.get_opened_grids_without_types(direction=Direction.LONG, types=[LOCK_GRID])

        if long_grids and self.line_renko.line_ma1[-1] < self.line_renko.line_ma1[-2] \
                and self.line_renko.line_yb[-1] < self.line_renko.line_yb[-2] \
                and self.cur_99_price > self.policy.tns_open_price:
            long_leave = True
        else:
            long_leave = False

        for g in long_grids:
            # 满足离场条件，或者碰到止损价格
            if (long_leave or (g.stop_price > 0 and g.stop_price > self.cur_99_price)) \
                    and g.open_status and not g.order_status:

                dist_record = OrderedDict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.idx_symbol
                dist_record['volume'] = g.volume
                dist_record['price'] = self.cur_99_price
                if long_leave:
                    dist_record['operation'] = 'long leave'
                    dist_record['signal'] = 'MA5_YB'
                    # 主动离场
                    self.write_log(u'{} 指数价:{} MA5/YB离场,{}当前价:{}。指数开仓价:{},主力开仓价:{},v：{}'.
                                   format(self.cur_datetime, self.cur_99_price, self.vt_symbol,
                                          self.cur_mi_price,
                                          g.open_price, g.snapshot.get('open_price'), g.volume))
                else:
                    dist_record['operation'] = 'stop leave'
                    dist_record['signal'] = '{}<{}'.format(self.cur_99_price, g.stop_price)
                    # 止损离场
                    self.write_log(u'{} 指数价:{} 触发多单止损线{},{}当前价:{}。指数开仓价:{},主力开仓价:{},v：{}'.
                                   format(self.cur_datetime, self.cur_99_price, g.stop_price, self.vt_symbol,
                                          self.cur_mi_price,
                                          g.open_price, g.snapshot.get('open_price'), g.volume))
                self.save_dist(dist_record)

                if self.tns_close_long_pos(g):
                    self.write_log(u'{}发生止损，移除subtns'.format(g.type))
                    self.remove_subtns(g.type)
                    self.write_log(u'多单止盈/止损委托成功')
                    self.policy.clean()
                else:
                    self.write_error(u'多单止损委托失败')

        # 空单网格止损检查
        short_grids = self.gt.get_opened_grids_without_types(direction=Direction.SHORT, types=[LOCK_GRID])
        if short_grids \
                and self.line_renko.line_ma1[-1] > self.line_renko.line_ma1[-2] \
                and self.line_renko.line_yb[-1] > self.line_renko.line_yb[-2]\
                and self.cur_99_price < self.policy.tns_open_price:
            short_leave = True
        else:
            short_leave = False

        for g in short_grids:
            if (short_leave or (g.stop_price > 0 and g.stop_price < self.cur_99_price)) \
                    and g.open_status and not g.order_status:

                dist_record = OrderedDict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.idx_symbol
                dist_record['volume'] = g.volume
                dist_record['price'] = self.cur_99_price
                if short_leave:
                    dist_record['operation'] = 'short leave'
                    dist_record['signal'] = 'MA5_YB'
                    # 主动离场
                    self.write_log(u'{} 指数价:{} MA5/YB离场,{}最新价:{}。指数开仓价:{},主力开仓价:{},v：{}'.
                                   format(self.cur_datetime, self.cur_99_price, self.vt_symbol,
                                          self.cur_mi_price,
                                          g.open_price, g.snapshot.get('open_price'), g.volume))
                else:
                    dist_record['operation'] = 'stop leave'
                    dist_record['signal'] = '{}<{}'.format(self.cur_99_price, g.stop_price)
                    # 网格止损
                    self.write_log(u'{} 指数价:{} 触发空单止损线:{},{}最新价:{}。指数开仓价:{},主力开仓价:{},v：{}'.
                                   format(self.cur_datetime, self.cur_99_price, g.stop_price, self.vt_symbol,
                                          self.cur_mi_price,
                                          g.open_price, g.snapshot.get('open_price'), g.volume))
                self.save_dist(dist_record)

                if self.tns_close_short_pos(g):
                    self.write_log(u'{}发生止损，移除subtns'.format(g.type))
                    self.remove_subtns(g.type)
                    self.write_log(u'空单止盈/止损委托成功')
                    self.policy.clean()
                else:
                    self.write_error(u'委托空单平仓失败')

    def tns_calculate_bar_colors(self, main_color, window, min_percent):
        """事务计算bar的颜色，在最近window个bar种，是否满足min_percent百分比"""
        color_list = [bar.color for bar in self.line_renko.line_bar[-window:]]

        if len(color_list) != window:
            return False
        match_list = [color for color in color_list if color == main_color]

        if len(match_list) / len(color_list) >= min_percent / 100:
            return True
        else:
            return False

    def tns_calculate_signal(self):
        """计算事务信号"""

        if len(self.line_renko.line_bar) < 2 \
                or len(self.line_renko.line_yb) < 2 \
                or len(self.line_renko.line_pre_high) < 2:
            return

        # 唐其安通道顶部信号
        if self.line_renko.line_bar[-1].color == Color.RED \
                and self.line_renko.line_pre_high[-1] == self.line_renko.line_bar[-1].high_price \
                and self.tns_calculate_bar_colors(main_color=Color.RED, window=self.para_pre_len,
                                                  min_percent=75) \
                and self.policy.last_signal != SIGNAL_HIGHEST:
            if isinstance(self.policy.last_signal_time, datetime) and self.policy.last_signal_time >= self.cur_datetime:
                if not self.backtesting:
                    self.write_log(u'{} 时间{} 比上一信号:{} 时间:{} 早,不处理'
                                   .format(SIGNAL_HIGHEST, self.cur_datetime,
                                           self.policy.last_signal, self.policy.last_signal_time))
                return

            self.write_log(u'信号变化:{} {} => {} {}'.format(
                self.policy.last_signal, self.policy.last_signal_time,
                SIGNAL_HIGHEST, self.cur_datetime))
            self.policy.last_signal = SIGNAL_HIGHEST
            self.policy.last_signal_time = self.cur_datetime

            sub_tns = self.policy.sub_tns.get(SIGNAL_HIGHEST, None)
            if sub_tns:
                tns_status = sub_tns.get('status', None)
                if tns_status == TNS_STATUS_OPENED:
                    self.write_log(u'{} {}信号已经完成，不再做空'.format(self.cur_datetime, SIGNAL_HIGHEST))
                    return
                if tns_status is None:
                    sub_tns['status'] = TNS_STATUS_OBSERVATE
                sub_tns['datetime'] = self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')
            else:
                sub_tns = {'status': TNS_STATUS_OBSERVATE,
                           'datetime': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                           'price': self.cur_99_price,
                           'stop_price': self.line_renko.line_pre_high[-1] + self.line_renko.height}
                self.write_log(u'添加做空子事务{}: {}'.format(SIGNAL_HIGHEST, sub_tns))

            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['volume'] = 0
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = 'new signal'
            dist_record['signal'] = self.policy.last_signal
            dist_record['stop_price'] = sub_tns.get('stop_price')
            self.save_dist(dist_record)

            self.policy.sub_tns.update({SIGNAL_HIGHEST: sub_tns})

            return

        # 唐其安通道底部信号
        if self.line_renko.line_bar[-1].color == Color.BLUE \
                and self.line_renko.line_pre_low[-1] == self.line_renko.line_bar[-1].low_price \
                and self.tns_calculate_bar_colors(main_color=Color.BLUE, window=self.para_pre_len,
                                                  min_percent=75) \
                and self.policy.last_signal != SIGNAL_LOWEST:
            if isinstance(self.policy.last_signal_time,
                          datetime) and self.policy.last_signal_time >= self.cur_datetime:
                if not self.backtesting:
                    self.write_log(u'{} 时间{} 比上一信号:{} 时间:{} 早,不处理'
                                   .format(SIGNAL_LOWEST, self.cur_datetime,
                                           self.policy.last_signal, self.policy.last_signal_time))
                return

            self.write_log(u'信号变化:{} {} => {} {}'.format(
                self.policy.last_signal, self.policy.last_signal_time,
                SIGNAL_LOWEST, self.cur_datetime))

            self.policy.last_signal = SIGNAL_LOWEST
            self.policy.last_signal_time = self.cur_datetime

            sub_tns = self.policy.sub_tns.get(SIGNAL_LOWEST, None)
            if sub_tns:
                tns_status = sub_tns.get('status', None)
                if tns_status == TNS_STATUS_OPENED:
                    self.write_log(u'{} {}信号已经完成，不再产生买入'.format(self.cur_datetime, SIGNAL_LOWEST))
                    return
                if tns_status is None:
                    sub_tns['status'] = TNS_STATUS_OBSERVATE
                sub_tns['datetime'] = self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S')
            else:
                sub_tns = {'status': TNS_STATUS_OBSERVATE,
                           'datetime': self.cur_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                           'price': self.cur_99_price,
                           'stop_price': self.line_renko.line_pre_low[-1] - self.line_renko.height}
                self.write_log(u'添加做多子事务{}:{}'.format(SIGNAL_LOWEST, sub_tns))

            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['volume'] = 0
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = 'new signal'
            dist_record['signal'] = self.policy.last_signal
            dist_record['stop_price'] = sub_tns.get('stop_price')
            self.save_dist(dist_record)

            self.policy.sub_tns.update({SIGNAL_LOWEST: sub_tns})

    def tns_calculate_x_signal(self):
        """计算x分钟信号"""

        if len(self.line_x.line_bar) < 2 \
                or len(self.line_x.line_ma3) < 2 \
                or len(self.line_x.line_pre_high) < 2:
            return

        # 唐其安通道顶部信号, bar为前高，均线发散向上
        if self.line_x.line_pre_high[-1] <= self.line_x.line_bar[-2].high_price \
                and self.line_x.line_ma1[-1] > self.line_x.line_ma2[-1] > self.line_x.line_ma3[-1] \
                and self.policy.x_last_signal != SIGNAL_HIGHEST:

            if isinstance(self.policy.x_last_signal_time, datetime) \
                    and self.policy.x_last_signal_time >= self.cur_datetime:
                if not self.backtesting:
                    self.write_log(u'{} {} 时间{} 比上一信号:{} 时间:{} 早,不处理'
                                   .format(self.line_x.name, SIGNAL_HIGHEST, self.cur_datetime,
                                           self.policy.x_last_signal, self.policy.x_last_signal_time))
                return

            self.write_log(u'{}信号变化:{} {} => {} {}'.format(
                self.line_x.name,
                self.policy.x_last_signal,
                self.policy.x_last_signal_time,
                SIGNAL_HIGHEST,
                self.cur_datetime))
            self.policy.x_last_signal = SIGNAL_HIGHEST
            self.policy.x_last_signal_time = self.cur_datetime

            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['volume'] = 0
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = f'{self.line_x.name} new signal'
            dist_record['signal'] = self.policy.x_last_signal
            self.save_dist(dist_record)

            return

        # 唐其安通道底部信号, 均线发散
        if self.line_x.line_pre_low[-1] >= self.line_x.line_bar[-2].low_price \
                and self.line_x.line_ma1[-1] < self.line_x.line_ma2[-1] < self.line_x.line_ma3[-1] \
                and self.policy.x_last_signal != SIGNAL_LOWEST:
            if isinstance(self.policy.x_last_signal_time, datetime) \
                    and self.policy.x_last_signal_time >= self.cur_datetime:
                if not self.backtesting:
                    self.write_log(u'{] {} 时间{} 比上一信号:{} 时间:{} 早,不处理'
                                   .format(self.line_x.name, SIGNAL_LOWEST, self.cur_datetime,
                                           self.policy.x_last_signal, self.policy.x_last_signal_time))
                return

            self.write_log(u'{} 信号变化:{} {} => {} {}'.format(
                self.line_x.name, self.policy.x_last_signal, self.policy.x_last_signal_time,
                SIGNAL_LOWEST, self.cur_datetime))

            self.policy.x_last_signal = SIGNAL_LOWEST
            self.policy.x_last_signal_time = self.cur_datetime

            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['volume'] = 0
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = f'{self.line_x.name} new signal'
            dist_record['signal'] = self.policy.x_last_signal
            self.save_dist(dist_record)

    def process_sub_tns(self):
        """
        处理所有子事务，只处理观测状态的信号
        :return:
        """
        # 排除不是最新datetime得sub_tns
        if len(self.policy.sub_tns) > 1:
            max_datetime = max([x.get('datetime') for x in self.policy.sub_tns.values()])
            if max_datetime:
                for k in list(self.policy.sub_tns.keys()):
                    sub_tns = self.policy.sub_tns.get(k, None)
                    if sub_tns and sub_tns.get('datetime') < max_datetime:
                        self.write_log(u'移除旧得sub_tns:{},最新sub_tns时间:{}'.format(sub_tns, max_datetime))
                        self.policy.sub_tns.pop(k, None)

        for k in list(self.policy.sub_tns.keys()):
            if k == SIGNAL_LOWEST:
                # 处理多头信号
                self.process_long_signal(k)

            if k == SIGNAL_HIGHEST:
                # 处理空头信号
                self.process_short_signal(k)

    def process_long_signal(self, long_signal):
        """
        处理多头的分钟周期信号
        :param long_signal:
        :return:
        """
        if self.entrust != 0:
            return

        # 获取执行策略中得信号子事务
        sub_long_tns = self.policy.sub_tns.get(long_signal, None)
        if sub_long_tns is None:
            return

        # 子事务已开仓，
        if sub_long_tns.get('status', None) == TNS_STATUS_OPENED \
                and self.position.pos > 0:
            return

        # 子事务开仓委托状态=》已开仓
        if sub_long_tns.get('status', None) == TNS_STATUS_ORDERING:
            if self.position.pos == 0 and len(self.active_orders) > 0:
                self.write_log(u'子事务仍处在{},当前存在委托单'.format(TNS_STATUS_ORDERING))
                return

            if self.position.pos > 0:
                self.write_log(u'更新{}子事务状态:{}=>{}'
                               .format(long_signal,
                                       sub_long_tns.get('status', None),
                                       TNS_STATUS_OPENED))
                sub_long_tns.update({'status': TNS_STATUS_OPENED})
                self.policy.sub_tns.update({long_signal: sub_long_tns})
                self.policy.save()
                return

        if self.line_renko.line_ma1[-1] > self.line_renko.line_ma1[-2] \
                and self.line_renko.line_bar[-1].color == Color.RED:

            if self.policy.x_last_signal != SIGNAL_LOWEST:
                self.write_log(f'{self.line_x.name} 信号{self.policy.x_last_signal}不是{SIGNAL_LOWEST}')
                return

            #if self.line_x.line_pre_low[-1] != self.line_renko.line_pre_low[-1]:
            #    self.write_log('{}前低: {}, {} 前低:{} 不一致'
            #                   .format(self.line_x.name, self.line_x.line_pre_low[-1],
            #                           self.line_renko.name, self.line_renko.line_pre_low[-1]))
            #    return
            # 事务开多仓
            self.write_log(u'{} 处理信号:{}，满足开仓条件,信号时间:{},子事务状态:{}'
                           .format(self.cur_datetime,
                                   long_signal,
                                   sub_long_tns.get('datetime'),
                                   sub_long_tns.get('status', None)))

            if self.tns_add_long(long_signal):
                dist_record = OrderedDict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.idx_symbol
                dist_record['volume'] = 0
                dist_record['price'] = self.cur_99_price
                dist_record['operation'] = '{}=>{}'.format(sub_long_tns['status'], TNS_STATUS_ORDERING)
                dist_record['signal'] = long_signal
                self.save_dist(dist_record)

                # 更新为委托开仓状态
                sub_long_tns['status'] = TNS_STATUS_ORDERING
                sub_long_tns['open_price'] = self.cur_99_price
                self.policy.tns_open_price = self.cur_99_price
                self.policy.save()  # Policy中的子事务改变了状态，不保存的话会重启后会重复开仓
                self.display_tns()

    def process_short_signal(self, short_signal):
        """
        处理空头的分钟周期信号
        :param short_signal:
        :return:
        """
        if self.entrust != 0:
            return

        # 从策略执行中，获取做空子事务
        sub_short_tns = self.policy.sub_tns.get(short_signal, None)
        if sub_short_tns is None:
            return

        # 子事务已开仓
        if sub_short_tns.get('status', None) == TNS_STATUS_OPENED \
                and self.position.pos < 0:
            return

        # 子事务开仓委托状态
        if sub_short_tns.get('status', None) == TNS_STATUS_ORDERING:
            if self.position.pos == 0 and len(self.active_orders) > 0:
                self.write_log(u'子事务仍处在{},当前存在委托单'.format(TNS_STATUS_ORDERING))
                return

            # 委托状态=》已开仓
            if self.position.pos < 0:
                self.write_log(u'更新{}子事务状态:{}=>{}'
                               .format(short_signal,
                                       sub_short_tns.get('status', None),
                                       TNS_STATUS_OPENED))
                sub_short_tns.update({'status': TNS_STATUS_OPENED})
                self.policy.sub_tns.update({short_signal: sub_short_tns})
                self.policy.save()
                return

        if self.line_renko.line_ma1[-1] < self.line_renko.line_ma1[-2] \
                and self.line_renko.line_bar[-1].color == Color.BLUE:

            if self.policy.x_last_signal != SIGNAL_HIGHEST:
                self.write_log(f'{self.line_x.name} 信号{self.policy.x_last_signal}不是{SIGNAL_HIGHEST}')
                return

            #if self.line_x.line_pre_high[-1] != self.line_renko.line_pre_high[-1]:
            #    self.write_log('{}前高: {}, {} 前高:{} 不一致'
            #                   .format(self.line_x.name, self.line_x.line_pre_high[-1],
            #                           self.line_renko.name, self.line_renko.line_pre_high[-1]))
            #    return

            self.write_log(u'{} 处理信号:{},满足开仓条件，信号时间:{},子事务状态:{}'
                           .format(self.cur_datetime,
                                   short_signal,
                                   sub_short_tns.get('datetime'),
                                   sub_short_tns.get('status', None)))

            if self.tns_add_short(short_signal):
                dist_record = OrderedDict()
                dist_record['datetime'] = self.cur_datetime
                dist_record['symbol'] = self.idx_symbol
                dist_record['volume'] = 0
                dist_record['price'] = self.cur_99_price
                dist_record['operation'] = '{}=>{}'.format(sub_short_tns['status'], TNS_STATUS_ORDERING)
                dist_record['signal'] = short_signal
                self.save_dist(dist_record)

                # 更新为委托开仓状态
                sub_short_tns['status'] = TNS_STATUS_ORDERING
                sub_short_tns['open_price'] = self.cur_99_price
                self.policy.tns_open_price = self.cur_99_price
                self.policy.save()  # Policy中的子事务改变了状态，不保存的话会重启后会重复开仓
                self.display_tns()

    def remove_subtns(self, signal):
        """
        移除policy内sub_tns
        :param direction:
        :return:
        """

        for k in list(self.policy.sub_tns.keys()):
            if k == signal:
                self.write_log(u'移除sub_tns:{}'.format(k))
                self.policy.sub_tns.pop(k, None)
                self.policy.save()

    def tns_remove_uncompleted_grids(self):
        """事务删除未开仓的开多请求"""
        remove_ids = []
        for g in self.gt.dn_grids:
            if not g.open_status:
                if g.order_ids:
                    for order_id in g.order_ids:
                        self.write_log(u'发出撤单请求')
                        if self.cancel_order(order_id):
                            if order_id in self.active_orders:
                                self.active_orders[order_id].update({'status': Status.CANCELING})

                    g.order_ids = []

                if g.tradedVolume == 0:
                    remove_ids.append(g.id)

        self.write_log(u'移除做多网格:{}'.format(id))
        self.gt.remove_grids_by_ids(direction=Direction.LONG, ids=remove_ids)

    def tns_get_volume(self):
        """获取开仓的数量"""
        # 当前权益，可用资金，当前比例，最大仓位比例
        balance, avaliable, percent, percent_limit = self.cta_engine.get_account()
        invest_money = float(balance * self.max_invest_rate)
        if self.max_invest_margin > 0:
            invest_money = min(invest_money, self.max_invest_margin)

        if invest_money <= 0:
            self.write_error(
                u'没有可使用的资金：balance:{},avaliable:{},percent:{},percentLimit:{}'.format(balance, avaliable, percent,
                                                                                      percent_limit))
            return 0

        if percent > percent_limit:
            self.write_error(
                u'超过仓位限制：balance:{},avaliable:{},percent:{},percentLimit:{}'.format(balance, avaliable, percent,
                                                                                    percent_limit))
            return 0

        # 投资资金总额允许的开仓数量
        max_unit = max(1, int(invest_money / (self.cur_mi_price * self.symbol_size * self.margin_rate)))
        if self.max_invest_pos > 0:
            max_unit = min(max_unit, self.max_invest_pos)

        avaliable_unit = int(avaliable / (self.cur_mi_price * self.symbol_size * self.margin_rate))
        self.write_log(u'投资资金总额{}允许的开仓数量：{},剩余资金允许得开仓数：{}，当前已经开仓手数:{}'
                       .format(invest_money, max_unit,
                               avaliable_unit,
                               self.position.long_pos + abs(self.position.short_pos)))

        return min(max_unit, avaliable_unit)

    def tns_add_long(self, signal):
        """
        事务开多仓
        :return:
        """
        self.write_log(u'{}开启事务多仓,信号:{}'.format(self.cur_datetime, signal))
        self.policy.tns_direction = Direction.LONG
        self.policy.tns_count = 1

        # 强制空头事务平仓
        self.tns_update_stop_price(Direction.SHORT, 1)

        sub_tns = self.policy.sub_tns.get(signal)

        # 获取开仓的仓位
        volume = self.tns_get_volume()
        if volume <= 0:
            self.write_log(u'可开仓0')
            return False

        if self.position.pos > 0:
            self.write_log(u'已经持有多单:{}，仓差:{}，不再开仓'.format(self.position.long_pos, self.position.pos))
            return True

        stop_price = sub_tns.get('stop_price', 0)
        if stop_price > 0:
            stop_price = min(self.line_renko.line_pre_low[-1], self.line_x.line_pre_low[-1],
                             self.line_x.line_bar[-1].low_price, stop_price)
        grid = self.tns_open_from_lock(open_symbol=self.vt_symbol, open_volume=volume, grid_type=signal,
                                       open_direction=Direction.LONG)

        if grid is None:
            if self.activate_today_lock:
                if self.position.long_pos >= volume * 3 > 0:
                    self.write_log(u'多单数量:{}(策略多单:{}),总数超过策略开仓手数:{}的3倍,不再开多仓'
                                   .format(self.position.long_pos, self.position.pos, volume))
                    return False

            grid = CtaGrid(direction=Direction.LONG,
                           open_price=self.cur_99_price,
                           vt_symbol=self.idx_symbol,
                           close_price=sys.maxsize,
                           stop_price=stop_price,
                           volume=volume,
                           type=signal)

            grid.snapshot.update({'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})
            ref = self.buy(price=self.cur_mi_price, volume=grid.volume, grid=grid, order_type=self.order_type)
            if len(ref) > 0:
                self.write_log(u'创建{}事务多单,开仓价：{}，数量：{}，止盈价:{},止损价:{}'
                               .format(grid.type, grid.open_price, grid.volume, grid.close_price, grid.stop_price))
                self.gt.dn_grids.append(grid)
                self.gt.save()
                return True
            else:
                self.write_error(u'创建{}事务多单,委托失败，开仓价：{}，数量：{}，止盈价:{}'
                                 .format(grid.type, grid.open_price, grid.volume, grid.close_price))
                return False
        else:
            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = 'reuse long {}=>{}'.format(grid.type, signal)
            dist_record['volume'] = volume
            self.save_dist(dist_record)

            self.write_log(u'使用对锁仓位,释放空单,保留多单,gid:{}'.format(grid.id))
            grid.open_price = self.cur_99_price
            grid.close_price = sys.maxsize
            grid.stop_price = stop_price
            self.write_log(u'多单 {} =>{},更新开仓价:{},止损价:{}'.format(grid.type, signal, grid.open_price, grid.stop_price))
            grid.type = signal
            grid.snapshot.update({'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})
            grid.open_status = True
            grid.close_status = False
            grid.order_status = False
            grid.order_ids = []
            return True

    def tns_add_short(self, signal):
        """
        事务开空仓
        :return:
        """

        self.write_log(u'{}开启事务空仓,信号:{}'.format(self.cur_datetime, signal))
        self.policy.tns_direction = Direction.SHORT
        self.policy.tns_count = -1
        # 强制多头事务平仓
        self.tns_update_stop_price(Direction.LONG, sys.maxsize)

        sub_tns = self.policy.sub_tns.get(signal)

        volume = self.tns_get_volume()

        if volume <= 0:
            self.write_log(u'可开仓0')
            return False

        if self.position.pos < 0:
            self.write_log(u'已经持有空单:{}，空单仓差:{}，不再开仓'.format(abs(self.position.short_pos), abs(self.position.pos)))
            return True

        stop_price = sub_tns.get('stop_price', 0)
        stop_price = max(self.line_renko.line_pre_high[-1], self.line_x.line_pre_high[-1], self.line_x.line_bar[-1].high_price, stop_price)
        grid = self.tns_open_from_lock(open_symbol=self.vt_symbol, open_volume=volume, grid_type=signal,
                                       open_direction=Direction.SHORT)
        if grid is None:
            if self.activate_today_lock:
                if abs(self.position.short_pos) >= volume * 3 > 0:
                    self.write_log(u'空单数量:{}(含实际策略空单:{}),总数超过策略开仓手数:{}的3倍,不再开多仓'
                                   .format(abs(self.position.short_pos), abs(self.position.pos), volume))
                    return False

            grid = CtaGrid(direction=Direction.SHORT,
                           open_price=self.cur_99_price,
                           vt_symbol=self.idx_symbol,
                           close_price=-sys.maxsize,
                           stop_price=stop_price,
                           volume=volume,
                           type=signal)
            grid.snapshot.update({'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})
            ref = self.short(price=self.cur_mi_price, volume=grid.volume, grid=grid, order_type=self.order_type)
            if len(ref) > 0:
                self.write_log(u'创建{}事务空单,指数开空价：{}，主力开仓价:{},数量：{}，止盈价:{},止损价:{}'
                               .format(grid.type, grid.open_price, self.cur_mi_price, grid.volume, grid.close_price,
                                       grid.stop_price))
                self.gt.up_grids.append(grid)
                self.gt.save()
                return True
            else:
                self.write_error(u'创建{}事务空单,委托失败,开仓价：{}，数量：{}，止盈价:{}'
                                 .format(grid.type, grid.open_price, grid.volume, grid.close_price))
                return False
        else:
            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = 'reuse short {}=>{}'.format(grid.type, signal)
            dist_record['volume'] = volume
            self.save_dist(dist_record)

            self.write_log(u'使用对锁仓位,释放多单,保留空单,gid:{}'.format(grid.id))
            grid.open_price = self.cur_99_price
            grid.close_price = 0 - sys.maxsize
            grid.stop_price = stop_price
            self.write_log(u'空单 {} =>{},开仓价:{},止损价:{}'.format(grid.type, signal, grid.open_price, grid.stop_price))
            grid.type = signal
            grid.snapshot.update({'mi_symbol': self.vt_symbol, 'open_price': self.cur_mi_price})
            grid.open_status = True
            grid.close_status = False
            grid.order_status = False
            grid.order_ids = []
            return True

    def tns_update_stop_price(self, direction, price):
        """
        事务更新止损价。
        更新所有同向网格止损价为price
        :return:
        """
        if not self.inited:
            return

        self.write_log(u'事务更新止损价，方向:{},止损价更新为:{}'.format(direction, price))

        # 获取止损方向得开仓网格(排除锁仓格)
        open_grids = self.gt.get_opened_grids_without_types(direction=direction, types=[LOCK_GRID])
        has_changed = False

        for g in open_grids:
            if g.stop_price == price:
                continue
            last_stop_price = g.stop_price
            g.stop_price = price
            self.write_log(u'更新跟随止损价:{}=》{},g.open_price:{}'
                           .format(last_stop_price,
                                   g.stop_price,
                                   g.open_price))
            dist_record = OrderedDict()
            dist_record['datetime'] = self.cur_datetime
            dist_record['symbol'] = self.idx_symbol
            dist_record['price'] = self.cur_99_price
            dist_record['operation'] = 'tns_update_stop_price'
            dist_record['stop_price'] = g.stop_price
            self.save_dist(dist_record)
            has_changed = True

        if has_changed:
            self.write_log(u'更新止损价成功，马上执行止损检查')
            self.gt.save()
            self.grid_check_stop()
