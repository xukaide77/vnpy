from datetime import date, datetime, timedelta
from tqsdk import TqApi, TqReplay, TqBacktest, TqSim, BacktestFinished, tafunc, ta
from vnpy.trader.util_logger import setup_logger
from vnpy.trader.constant import Direction
import logging
import os
from dateutil import parser
from enum import Enum
from typing import Dict, List, Set, Callable
import numpy as np
import talib
import math
from vnpy.trader.utility import BarData, TickData, extract_vt_symbol, get_trading_date

class KlineType(Enum):
    """
    Direction of order/trade/position.
    """
    TOP = "顶分型"
    BOTTOM = "底分型"
    CALLBACK_TOP = '回调顶'
    CALLBACK_BOTTOM = '回调底'

class KlineForm(object):

    stop_price: float = 0
    type: KlineType = KlineType.TOP
    timestamp: datetime
    direction: Direction = None
    tick_count: int = 0
    close_price: float = 0
    valid_period: int = 2
    condition_open_price: float = 0

    def __init__(self, stop_price: float, type: KlineType, timestamp: datetime, direction: Direction, tick_count: int, close_price: float):
        self.stop_price: float = stop_price
        self.type: KlineType = type
        self.timestamp = timestamp
        self.direction = direction
        self.tick_count = tick_count
        self.close_price: float = close_price

    def decrease_period(self):
        self.valid_period -= 1

os.path.dirname(__file__)
logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'in_day_trade_micro_wave'),
                           name='trade_pp',
                           log_level=logging.DEBUG,
                           backtesing=True)
price_logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'in_day_price_micro_wave'),
                      name='price_pp',
                      log_level=logging.DEBUG,
                      backtesing=True)

# 参数10380
lots = 2  # 下单手数
NT = 1  # 默认1是正常时间，2是凌晨2.30
profit_tick = 7  # 止盈跳
big_profit_tick = 10  # 止盈跳
loss_tick = 13  # 止损跳
trade_period = 3 #单位分钟
max_wait_period = 99
time_limit = 9 # 距离分型k线根数
max_profit_limit = 50 #超过限制后利润减半止盈
open_callback_ratio = 0.382

# 全局变量
runCoreModule = 0
is_trading = 0
PG = 0
PGG = 0
PNN = 0
total_vol = 0
red_or_green = 0
long_entry_price = 0
short_entry_price = 0
long_stop_price: float = 0
short_stop_price: float = 0
open_signal = 0
stop_signal = 0
wait_period = 0
condition_open_long_price = 0
condition_open_short_price = 0
kline_length = 45
consum_delta = 0
short_trend = 0
middle_trend = 0
long_trend = 0
last_kline_form: KlineForm = None
current_kline_form: KlineForm = None
last_open_kline_form: KlineForm = None
shock_upper_limit = 0
shock_down_limit = 0
profit_time_left = 1
max_profit = 0
open_ready_price = 0
traded = False


# 默认
# api = TqApi(backtest=TqReprofit_tickay(date(2020, 9, 18)), auth="songshu123,7088044")
api = TqApi(TqSim(10000), backtest=TqBacktest(start_dt=date(2020, 11, 4), end_dt=date(2020, 11, 4)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(10000), backtest=TqBacktest(start_dt=datetime(2021, 1, 15, 9, 0), end_dt=datetime(2021, 1, 15, 15, 0)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(100000), backtest=TqReprofit_tickay(reprofit_tickay_dt=date(2020, 9, 9)), web_gui=True, auth="xukaide77,xk82513994")
sym = "DCE.pp2101"
# sym = 'CFFEX.IC2011'6509
ex, sy = sym.split('.')
min5_kline = api.get_kline_serial(sym, trade_period * 60, data_length=kline_length)
day_kline = api.get_kline_serial(sym, 24 * 60 * 60, data_length=30)
# 映射交易合约
# SYMBOL = quote.underlying_symbol 2119
order = api.get_order()
position = api.get_position(sym)
# 获得 ag2012 tick序列的引用
ticks = api.get_tick_serial(sym)
# 获得最小变动单位
price_tick = api.get_quote(sym).price_tick
symbol, exchange = extract_vt_symbol(sy + '.' + ex)
kline_delta = np.zeros(kline_length - 1)

def log(msg: str):
    logger.debug('{} {}'.format(tafunc.time_to_datetime(min5_kline.iloc[-1].datetime), msg))


# ----------------核心计算模块(每个bar更新一次)--------#
def CoreModule():
    global last_kline_form, current_kline_form, last_open_kline_form
    # 判断上一根是阳线还是阴线
    if last_kline_form is not None:
        # if last_kline_form.valid_period == 0:
        last_kline_form = None
        # else:
        #     last_kline_form.decrease_period()

    cal_open_signal()
    cal_stop_signal()

    if current_kline_form is not None:
        last_kline_form = current_kline_form
        current_kline_form = None
    return 1


def cal_open_signal():
    global long_stop_price, short_stop_price, open_signal, wait_period, short_trend, current_kline_form, shock_upper_limit, shock_down_limit, profit_time_left, open_ready_price, middle_trend, last_open_kline_form, last_kline_form, traded
    # 画一次指标线
    ma5 = ta.MA(min5_kline, 5)
    ma10 = ta.MA(min5_kline, 10)
    ma20 = ta.EMA(min5_kline, 20)  # 使用 tqsdk 自带指标函数计算均线
    ma40 = ta.EMA(min5_kline, 40)
    # min5_kline["ma5_MAIN"] = ma5.ma
    # min5_kline["ma10_MAIN"] = ma10.ma
    min5_kline["ma20_MAIN"] = ma20.ema
    min5_kline["ma40_MAIN"] = ma40.ema
    # min5_kline['ma5_MAIN.color'] = 'white'
    # min5_kline['ma10_MAIN.color'] = 'yellow'
    min5_kline['ma20_MAIN.color'] = 'purple'
    min5_kline['ma40_MAIN.color'] = 'green'

    # 委托时间
    now_datetime = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime) + timedelta(minutes=trade_period)
    # now_datetime = parser.parse(now)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute
    int_time = can_time(now_hour, now_minute)
    latest_price_datetime = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime)
    # now_datetime = parser.parse(now)
    price_int_time = can_time(latest_price_datetime.hour, latest_price_datetime.minute)
    current_kline_form = top_or_bottom(price_int_time, ma5, ma10, ma20)
    # if last_open_kline_form is not None and current_kline_form is not None and current_kline_form.timestamp == last_open_kline_form.timestamp:
    #     log(f'已经根据{last_open_kline_form.timestamp}信号开过仓')
        # return
    if last_open_kline_form is not None and current_kline_form is not None and current_kline_form.timestamp == last_open_kline_form.timestamp:
        log(f'已经根据{last_open_kline_form.timestamp}信号开过仓')
        return
    if (now_hour == 9 and now_minute <= 30) or (now_hour == 21 and now_minute <= 30) or (now_hour == 11 and now_minute >= 27) or (1445 <= int_time <= 1500) or (2245 <= int_time <= 2300):
        # 前分钟不开仓
        return 0
    # if traded:
    #     log('本轮已经平仓，消除信号暂不开仓')
    #     current_kline_form = None
    #     traded = False
    #     return

    # if open_ready_price > 0:
    #     temp_ready_price = open_ready_price
    #     open_ready_price = 0
    if ticks.iloc[-1].bid_price1 > ma20.ema.iloc[-1]:
        # kdj = ta.KDJ(min5_kline, 9, 3, 3).j.iloc[-1]
        # if not (short_trend > 0 or (middle_trend > 0 and short_trend == 0)):
        #     # 做单边
        #     return
        # if ma20.ma.iloc[-1] < ma40.ma.iloc[-1] or ma40.ma.iloc[-1] < ma40.ma.iloc[-2]:
        #     return
        if current_kline_form is not None and current_kline_form.direction == Direction.LONG:
            # stop_condition = (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < current_kline_form.stop_price#or (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < ma20.ma.iloc[-1]
            # if kdj > 75:
            # open_signal = 1
            callback_ticks = 2
            if min5_kline.iloc[-1].low > ma20.ema.iloc[-1]:
                callback_ticks = 6
            current_kline_form.condition_open_price = ticks.iloc[-1].ask_price1 - price_tick * callback_ticks
            # long_stop_price = ticks.iloc[-1].ask_price1 - loss_tick * price_tick
            long_stop_price = current_kline_form.condition_open_price - loss_tick * price_tick
            short_stop_price = 0
            log(f'设置开多仓止损位至{long_stop_price}')
            # elif kdj > 75:
            #     callback_ticks = math.ceil(80 - kdj)
            #     current_kline_form.condition_open_price = ticks.iloc[-1].ask_price1 - price_tick * callback_ticks
            #     long_stop_price = ticks.iloc[-1].ask_price1 - loss_tick * price_tick - price_tick * callback_ticks
            #     short_stop_price = 0
            #     log(f'设置开多仓条件价位至{current_kline_form.condition_open_price},止损位至{long_stop_price}')
        # elif last_kline_form is not None:
        #     callback_ticks = 3 * (2 - last_kline_form.valid_period) if min5_kline.iloc[-1].close > min5_kline.iloc[-1].open else 0
        #     if kdj > 75:
        #         last_kline_form.condition_open_price = ticks.iloc[-1].ask_price1 - price_tick * callback_ticks
        #         long_stop_price = ticks.iloc[-1].ask_price1 - loss_tick * price_tick - callback_ticks
        #         short_stop_price = 0
        #         log(f'设置开多仓条件价位至{last_kline_form.condition_open_price},止损位至{long_stop_price}')
            # elif kdj > 75:
            #     callback_ticks = callback_ticks + math.ceil(80 - kdj)
            #     last_kline_form.condition_open_price = ticks.iloc[-1].ask_price1 - price_tick * callback_ticks
            #     long_stop_price = ticks.iloc[-1].ask_price1 - loss_tick * price_tick - callback_ticks
            #     short_stop_price = 0
            #     log(f'设置开多仓条件价位至{last_kline_form.condition_open_price},止损位至{long_stop_price}')
    elif ticks.iloc[-1].bid_price1 < ma20.ema.iloc[-1]:
        # kdj = ta.KDJ(min5_kline, 9, 3, 3).j.iloc[-1]
        # if not (short_trend > 0 or (middle_trend > 0 and short_trend == 0)):
        #     # 做单边
        #     return
        # if ma20.ma.iloc[-1] < ma40.ma.iloc[-1] or ma40.ma.iloc[-1] < ma40.ma.iloc[-2]:
        #     return
        if current_kline_form is not None and current_kline_form.direction == Direction.SHORT:
            # stop_condition = (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < current_kline_form.stop_price#or (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < ma20.ma.iloc[-1]
            # if kdj < 25:
            # open_signal = -1
            # short_stop_price = ticks.iloc[-1].bid_price1 + loss_tick * price_tick
            callback_ticks = 2
            if min5_kline.iloc[-1].high < ma20.ema.iloc[-1]:
                callback_ticks = 6
            current_kline_form.condition_open_price = ticks.iloc[-1].bid_price1 + price_tick * callback_ticks
            short_stop_price = current_kline_form.condition_open_price + loss_tick * price_tick
            long_stop_price = 0
            log(f'设置开空仓止损位至{short_stop_price}')
            # elif kdj < 25:
            #     callback_ticks = math.ceil(kdj - 20)
            #     current_kline_form.condition_open_price = ticks.iloc[-1].bid_price1 + price_tick * callback_ticks
            #     short_stop_price = ticks.iloc[-1].bid_price1 + loss_tick * price_tick + price_tick * callback_ticks
            #     long_stop_price = 0
            #     log(f'设置开空仓条件价位至{current_kline_form.condition_open_price},止损位至{short_stop_price}')
        # elif last_kline_form is not None:
        #     callback_ticks = 3 * (2 - last_kline_form.valid_period) if min5_kline.iloc[-1].close < min5_kline.iloc[-1].open else 0
        #     if kdj < 25:
        #         last_kline_form.condition_open_price = ticks.iloc[-1].bid_price1 + price_tick * callback_ticks
        #         short_stop_price = ticks.iloc[-1].bid_price1 + loss_tick * price_tick + price_tick * callback_ticks
        #         long_stop_price = 0
        #         log(f'设置开空仓条件价位至{last_kline_form.condition_open_price},止损位至{short_stop_price}')
            # elif kdj < 25:
            #     callback_ticks = math.ceil(kdj - 20) + callback_ticks
            #     last_kline_form.condition_open_price = ticks.iloc[-1].bid_price1 + price_tick * callback_ticks
            #     short_stop_price = ticks.iloc[-1].bid_price1 + loss_tick * price_tick + price_tick * callback_ticks
            #     long_stop_price = 0
            #     log(f'设置开空仓条件价位至{last_kline_form.condition_open_price},止损位至{short_stop_price} ')


def top_or_bottom(int_time, ma5, ma10, ma20):
    if abs(min5_kline.iloc[-1].open - min5_kline.iloc[-1].close) < 10 * price_tick:
        return None
    # if not (min5_kline.iloc[-1].open - ma20.ema.iloc[-1]) * (min5_kline.iloc[-1].close - ma20.ema.iloc[-1]) < 0:
    #     return
    if min5_kline.iloc[-1].close - min5_kline.iloc[-1].open < -10 * price_tick \
            and (min5_kline.iloc[-2:].high.max() >= min5_kline.iloc[-6:].high.max() - 5 * price_tick) \
            and abs(min5_kline.iloc[-2].close - min5_kline.iloc[-2].open) < 5 * price_tick:
        api.draw_text(min5_kline, '顶', x=-1, y=min5_kline.iloc[-1].high + 8 * price_tick, color='green')
        return KlineForm(0, KlineType.TOP, tafunc.time_to_datetime(min5_kline.iloc[-1].datetime), Direction.SHORT, 0, min5_kline.iloc[-1].close)
    elif min5_kline.iloc[-1].close > min5_kline.iloc[-1].open \
            and (min5_kline.iloc[-2:].low.min() <= min5_kline.iloc[-6:].low.min() + 5 * price_tick)\
            and abs(min5_kline.iloc[-2].close - min5_kline.iloc[-2].open) < 5 * price_tick:
        api.draw_text(min5_kline, '底', x=-1, y=min5_kline.iloc[-1].low - 11 * price_tick, color='red')
        return KlineForm(0, KlineType.BOTTOM, tafunc.time_to_datetime(min5_kline.iloc[-1].datetime), Direction.LONG, 0, min5_kline.iloc[-1].close)
    # min5_kline.drop(min5_kline[abs(min5_kline.close - min5_kline.open) < 3 * price_tick].index)
    # last_index = -1
    # middle_index = -2
    # first_index = -3
    # top_condition1 = min5_kline.iloc[last_index].open > min5_kline.iloc[last_index].close and min5_kline.iloc[middle_index].close < min5_kline.iloc[middle_index].open and (min5_kline.iloc[first_index].close - min5_kline.iloc[first_index].open) >= 0 and min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close + min5_kline.iloc[middle_index].open - min5_kline.iloc[middle_index].close > 10 * price_tick# and min5_kline.iloc[first_index].close > min5_kline.iloc[-5-first_offset-second_offset:first_index].close.max()
    # top_condition2 = min5_kline.iloc[last_index].open > min5_kline.iloc[last_index].close and (min5_kline.iloc[middle_index].close - min5_kline.iloc[middle_index].open) >= 0 and min5_kline.iloc[last_index].close < min5_kline.iloc[first_index].open < min5_kline.iloc[first_index].close and min5_kline.iloc[middle_index].close > min5_kline.iloc[-5-first_offset:middle_index].close.max() and min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close > 10 * price_tick
    # top_condition3 = min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close > 7 * price_tick and (min5_kline.iloc[middle_index].close - min5_kline.iloc[middle_index].open) >= 0 and min5_kline.iloc[last_index].close < min5_kline.iloc[first_index].close < min5_kline.iloc[first_index].open and ma5.ma.iloc[middle_index] < ma5.ma.iloc[middle_index-1] #min5_kline.iloc[middle_index].close > min5_kline.iloc[-4-first_offset:middle_index].close.max()
    # # top_condition4 = min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close > 5 * price_tick and min5_kline.iloc[middle_index].close < min5_kline.iloc[middle_index].open and (min5_kline.iloc[first_index].close - min5_kline.iloc[first_index].open) >= 0 and ma5.ma.iloc[first_index] < ma5.ma.iloc[first_index-1]
    # # 明显的顶分型快速开仓
    # top_condition5 = ((min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close > 10 * price_tick and min5_kline.iloc[last_index].close < min5_kline.iloc[middle_index].open) or (min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close > 13 * price_tick)) and (min5_kline.iloc[middle_index].close - min5_kline.iloc[middle_index].open) >= 0 and min5_kline.iloc[middle_index].close > min5_kline.iloc[-6-first_offset:middle_index].close.max() and min5_kline.iloc[middle_index].close > min5_kline.iloc[-6-first_offset:middle_index].open.max()
    # # 红三兵
    # # top_condition6 = min5_kline.iloc[last_index].open > min5_kline.iloc[last_index].close and min5_kline.iloc[last_index-1].open > min5_kline.iloc[last_index-1].close and min5_kline.iloc[last_index-2].open > min5_kline.iloc[last_index-2].close and min5_kline.iloc[last_index].volume > min5_kline.iloc[last_index-1].volume > min5_kline.iloc[last_index-2].volume
    # if top_condition5:
    #     stop_price = min5_kline.iloc[last_index-1].open
    #     api.draw_text(min5_kline, '顶', x=middle_index, y=min5_kline.iloc[middle_index].high + 8 * price_tick, color='green')
    #     top_datetime = tafunc.time_to_datetime(min5_kline.iloc[middle_index].datetime)
    #     return KlineForm(stop_price, KlineType.TOP, can_time(top_datetime.hour, top_datetime.minute), Direction.SHORT, min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close, min5_kline.iloc[last_index].close)
    # elif top_condition1:
    #     stop_price = min5_kline.iloc[middle_index].open
    #     api.draw_text(min5_kline, '顶', x=first_index, y=min5_kline.iloc[first_index].high + 8 * price_tick, color='green')
    #     top_datetime = tafunc.time_to_datetime(min5_kline.iloc[first_index].datetime)
    #     return KlineForm(stop_price, KlineType.TOP, can_time(top_datetime.hour, top_datetime.minute), Direction.SHORT, min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close, min5_kline.iloc[last_index].close)
    # elif top_condition2:
    #     stop_price = min5_kline.iloc[middle_index].close
    #     api.draw_text(min5_kline, '顶', x=middle_index, y=min5_kline.iloc[middle_index].high + 8 * price_tick, color='green')
    #     top_datetime = tafunc.time_to_datetime(min5_kline.iloc[middle_index].datetime)
    #     return KlineForm(stop_price, KlineType.TOP, can_time(top_datetime.hour, top_datetime.minute), Direction.SHORT, min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close, min5_kline.iloc[last_index].close)
    # elif top_condition3:
    #     stop_price = max(min5_kline.iloc[last_index].open, min5_kline.iloc[first_index].open)
    #     api.draw_text(min5_kline, '回调', x=middle_index, y=min5_kline.iloc[middle_index].high + 8 * price_tick, color='green')
    #     top_datetime = tafunc.time_to_datetime(min5_kline.iloc[middle_index].datetime)
    #     return KlineForm(stop_price, KlineType.CALLBACK_TOP, can_time(top_datetime.hour, top_datetime.minute), Direction.SHORT, min5_kline.iloc[last_index].open - min5_kline.iloc[last_index].close, min5_kline.iloc[last_index].close)
    # # elif top_condition4:
    # #     stop_price = min5_kline.iloc[last_index].open if min5_kline.iloc[last_index].close + 0.7 * loss_tick * price_tick < min5_kline.iloc[last_index].open else min5_kline.iloc[middle_index].open
    # #     api.draw_text(min5_kline, '回调', x=first_index, y=min5_kline.iloc[first_index].high + 8 * price_tick, color='green')
    # #     top_datetime = tafunc.time_to_datetime(min5_kline.iloc[first_index].datetime)
    # #     return KlineForm(stop_price, KlineType.CALLBACK_TOP, can_time(top_datetime.hour, top_datetime.minute), Direction.SHORT)
    # # elif top_condition6:
    # #     stop_price = min5_kline.iloc[last_index-1].open
    # #     api.draw_text(min5_kline, '趋势', x=last_index-2, y=min5_kline.iloc[last_index-2].high + 8 * price_tick, color='green')
    # #     top_datetime = tafunc.time_to_datetime(min5_kline.iloc[last_index-2].datetime)
    # #     return KlineForm(stop_price, KlineType.CALLBACK_TOP, can_time(top_datetime.hour, top_datetime.minute), Direction.SHORT)
    # bottom_condition1 = min5_kline.iloc[last_index].close > min5_kline.iloc[last_index].open and min5_kline.iloc[middle_index].close > min5_kline.iloc[middle_index].open and min5_kline.iloc[first_index].open - min5_kline.iloc[first_index].close >= 0 and min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open + min5_kline.iloc[middle_index].close - min5_kline.iloc[middle_index].open > 10 * price_tick#and min5_kline.iloc[first_index].close < min5_kline.iloc[-5-first_offset-second_offset:first_index].close.min()
    # bottom_condition2 = min5_kline.iloc[last_index].close > min5_kline.iloc[last_index].open and min5_kline.iloc[middle_index].open - min5_kline.iloc[middle_index].close >= 0 and min5_kline.iloc[last_index].close > min5_kline.iloc[first_index].open > min5_kline.iloc[first_index].close and min5_kline.iloc[middle_index].close < min5_kline.iloc[-5-first_offset:middle_index].close.min() and min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open > 10 * price_tick
    # bottom_condition3 = min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open > 7 * price_tick and min5_kline.iloc[middle_index].open - min5_kline.iloc[middle_index].close >= 0 and min5_kline.iloc[last_index].close > min5_kline.iloc[first_index].close > min5_kline.iloc[first_index].open and ma5.ma.iloc[middle_index] > ma5.ma.iloc[middle_index-1]  #and min5_kline.iloc[middle_index].close < min5_kline.iloc[-4-first_offset:middle_index].close.min()
    # # bottom_condition4 = min5_kline.iloc[last_index].close > min5_kline.iloc[last_index].open and min5_kline.iloc[middle_index].close > min5_kline.iloc[middle_index].open and min5_kline.iloc[first_index].open - min5_kline.iloc[first_index].close >= 0 and ma5.ma.iloc[first_index] > ma5.ma.iloc[first_index-1]
    # # 明显的底分型快速开仓
    # bottom_condition5 = ((min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open > 10 * price_tick and min5_kline.iloc[last_index].close > min5_kline.iloc[middle_index].open) or min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open > 13 * price_tick) and min5_kline.iloc[middle_index].open - min5_kline.iloc[middle_index].close >= 0 and min5_kline.iloc[middle_index].close < min5_kline.iloc[-6-first_offset:middle_index].close.min() and min5_kline.iloc[middle_index].close < min5_kline.iloc[-6-first_offset:middle_index].open.min()
    # # # 红三兵
    # # bottom_condition6 = min5_kline.iloc[last_index].open < min5_kline.iloc[last_index].close and min5_kline.iloc[last_index-1].open < min5_kline.iloc[last_index-1].close and min5_kline.iloc[last_index-2].open < min5_kline.iloc[last_index-2].close and min5_kline.iloc[last_index].volume > min5_kline.iloc[last_index-1].volume > min5_kline.iloc[last_index-2].volume
    # if bottom_condition5:
    #     stop_price = min5_kline.iloc[last_index-1].open
    #     api.draw_text(min5_kline, '底', x=middle_index, y=min5_kline.iloc[middle_index].low - 11 * price_tick, color='red')
    #     bottom_datetime = tafunc.time_to_datetime(min5_kline.iloc[middle_index].datetime)
    #     return KlineForm(stop_price, KlineType.BOTTOM, can_time(bottom_datetime.hour, bottom_datetime.minute), Direction.LONG, min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open, min5_kline.iloc[last_index].close)
    # elif bottom_condition1:
    #     stop_price = min5_kline.iloc[middle_index].open
    #     api.draw_text(min5_kline, '底', x=first_index, y=min5_kline.iloc[first_index].low - 11 * price_tick, color='red')
    #     bottom_datetime = tafunc.time_to_datetime(min5_kline.iloc[first_index].datetime)
    #     return KlineForm(stop_price, KlineType.BOTTOM, can_time(bottom_datetime.hour, bottom_datetime.minute), Direction.LONG, min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open + min5_kline.iloc[middle_index].close - min5_kline.iloc[middle_index].open, min5_kline.iloc[last_index].close)
    # elif bottom_condition2:
    #     stop_price = min5_kline.iloc[middle_index].close
    #     api.draw_text(min5_kline, '底', x=middle_index, y=min5_kline.iloc[middle_index].low - 11 * price_tick, color='red')
    #     bottom_datetime = tafunc.time_to_datetime(min5_kline.iloc[middle_index].datetime)
    #     return KlineForm(stop_price, KlineType.BOTTOM, can_time(bottom_datetime.hour, bottom_datetime.minute), Direction.LONG, min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open > 10 * price_tick, min5_kline.iloc[last_index].close)
    # elif bottom_condition3:
    #     stop_price = min(min5_kline.iloc[last_index].open, min5_kline.iloc[first_index].open)
    #     api.draw_text(min5_kline, '回调', x=middle_index, y=min5_kline.iloc[middle_index].low - 11 * price_tick, color='red')
    #     bottom_datetime = tafunc.time_to_datetime(min5_kline.iloc[middle_index].datetime)
    #     return KlineForm(stop_price, KlineType.CALLBACK_BOTTOM, can_time(bottom_datetime.hour, bottom_datetime.minute), Direction.LONG, min5_kline.iloc[last_index].close - min5_kline.iloc[last_index].open, min5_kline.iloc[last_index].close)
    # elif bottom_condition4:
    #     stop_price = min5_kline.iloc[last_index].open if min5_kline.iloc[last_index].close - 0.7 * loss_tick * price_tick > min5_kline.iloc[last_index].open else min5_kline.iloc[middle_index].open
    #     api.draw_text(min5_kline, '回调', x=first_index, y=min5_kline.iloc[first_index].low - 11 * price_tick, color='red')
    #     bottom_datetime = tafunc.time_to_datetime(min5_kline.iloc[first_index].datetime)
    #     return KlineForm(stop_price, KlineType.CALLBACK_BOTTOM, can_time(bottom_datetime.hour, bottom_datetime.minute), Direction.LONG)
    # elif bottom_condition6:
    #     stop_price = min5_kline.iloc[last_index-1].open
    #     api.draw_text(min5_kline, '趋势', x=last_index-2, y=min5_kline.iloc[last_index-2].high - 11 * price_tick, color='red')
    #     bottom_datetime = tafunc.time_to_datetime(min5_kline.iloc[last_index-2].datetime)
    #     return KlineForm(stop_price, KlineType.CALLBACK_BOTTOM, can_time(bottom_datetime.hour, bottom_datetime.minute), Direction.LONG)
    return None


def cal_stop_signal():
    global stop_signal, long_stop_price, short_stop_price, wait_period, open_signal, short_trend, current_kline_form, last_kline_form
    if not (long_entry_price > 0 or short_entry_price > 0):
        return
    # 已有开仓信号
    # if open_signal > 0 and long_entry_price > 0:
    #     open_signal = 0
    #     log(f'已开多仓,不计算止盈')
    #     return
    # elif open_signal < 0 and short_entry_price > 0:
    #     open_signal = 0
    #     log(f'已开空仓,不计算止盈')
    #     return
    ma20 = ta.EMA(min5_kline, 20)
    if long_entry_price > 0:
        if min5_kline.iloc[-1].low < ma20.ema.iloc[-1] or min5_kline.iloc[-2].low < ma20.ema.iloc[-2]:
            wait_period += 1
        if current_kline_form is not None and current_kline_form.type == KlineType.TOP:
            stop_signal = 1
            log('做多碰到顶分型止盈')
        # elif min5_kline.iloc[-1].close - min5_kline.iloc[-1].open > 0.7 * loss_tick * price_tick:
        #     long_stop_price = min5_kline.iloc[-1].open
        #     log(f'做多调整止损价{long_stop_price}')
        # elif min5_kline.iloc[-1].close - min5_kline.iloc[-1].open > 2 * price_tick and min5_kline.iloc[-2].close - min5_kline.iloc[-2].open > 2 * price_tick:
        #     long_stop_price = min5_kline.iloc[-2].open
        #     log(f'做多调整止损价{long_stop_price}')
        # elif min5_kline.iloc[-1].close - min5_kline.iloc[-1].open < -0.7 * loss_tick * price_tick:
        #     stop_signal = 1
        #     log(f'做多回调{min5_kline.iloc[-1].close - min5_kline.iloc[-1].open}过大进行止盈')
        # elif min5_kline.iloc[-1].close < ma20:
        #     stop_signal = 1
        #     log('做多突破MA20止盈')
        if wait_period == max_wait_period:
            # 还未盈利就止损，已经盈利改变止损点
            # if ticks.iloc[-1].bid_price1 <= long_entry_price:
            #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},wait_period:{wait_period}到达{max_wait_period}时还未盈利赶紧止损')
                stop_signal = 1
            # else:
            #     long_stop_price = long_entry_price
            #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},wait_period:{wait_period}到达{max_wait_period}时已经盈利改变止损点至开仓价{long_entry_price}')
        # elif wait_period >= max_wait_period and ticks.iloc[-1].bid_price1 <= long_entry_price:
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},wait_period:{wait_period}到达等待最大周期还未盈利赶紧止损')
        #     stop_signal = 1
        # elif ma40.ma.iloc[-1] < ma40.ma.iloc[-2]:
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},ma40拐头向下赶紧止损')
        #     stop_signal = 1
        elif min5_kline.iloc[-1].open > min5_kline.iloc[-1].close and min5_kline.iloc[-1].volume > 1.8 * min5_kline.iloc[-2].volume:
            log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},放巨量还收绿赶紧止损')
            stop_signal = 1
        # elif min5_kline.iloc[-1].high - max(min5_kline.iloc[-1].close, min5_kline.iloc[-1].open) > 2 * abs(min5_kline.iloc[-1].close - min5_kline.iloc[-1].open):
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},长下影线赶紧止损')
        #     stop_signal = 1
    elif short_entry_price > 0:
        if min5_kline.iloc[-1].high > ma20.ema.iloc[-1] or min5_kline.iloc[-2].high > ma20.ema.iloc[-2]:
            wait_period += 1
        if current_kline_form is not None and current_kline_form.type == KlineType.BOTTOM:
            stop_signal = -1
            log('做空碰到底分型止盈')
        # elif min5_kline.iloc[-1].open - min5_kline.iloc[-1].close > 0.7 * loss_tick * price_tick:
        #     short_stop_price = min5_kline.iloc[-1].open
        #     log(f'做空调整止损价{short_stop_price}')
        # elif min5_kline.iloc[-1].close - min5_kline.iloc[-1].open < -2 * price_tick and min5_kline.iloc[-2].close - min5_kline.iloc[-2].open < -2 * price_tick:
        #     short_stop_price = min5_kline.iloc[-2].open
        #     log(f'做空调整止损价{short_stop_price}')
        # elif min5_kline.iloc[-1].close - min5_kline.iloc[-1].open > 0.7 * loss_tick * price_tick:
        #     stop_signal = -1
        #     log(f'做空回调{min5_kline.iloc[-1].close - min5_kline.iloc[-1].open}过大进行止盈')
        # elif min5_kline.iloc[-1].close > ma20:
        #     stop_signal = -1
        #     log('做空突破MA20止盈')
        if wait_period == max_wait_period:
            # 还未盈利就止损，已经盈利改变止损点
            # if ticks.iloc[-1].ask_price1 >= short_entry_price:
            #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},wait_period:{wait_period}到达{max_wait_period}时还未盈利赶紧止损')
                stop_signal = -1
            # else:
            #     short_stop_price = short_entry_price
            #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},wait_period:{wait_period}到达{max_wait_period}时已经盈利改变止损点至开仓价{short_entry_price}')
        # elif wait_period >= max_wait_period and ticks.iloc[-1].ask_price1 >= short_entry_price:
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},wait_period:{wait_period}到达等待最大周期还未盈利赶紧止损')
        #     stop_signal = -1
        # elif ma40.ma.iloc[-1] > ma40.ma.iloc[-2]:
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},ma40拐头向上赶紧止损')
        #     stop_signal = -1
        elif min5_kline.iloc[-1].close > min5_kline.iloc[-1].open and min5_kline.iloc[-1].volume > 1.8 * min5_kline.iloc[-2].volume:
            log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},放巨量还收红赶紧止损')
            stop_signal = -1
        # elif min(min5_kline.iloc[-1].close, min5_kline.iloc[-1].open) - min5_kline.iloc[-1].low > 2 * abs(min5_kline.iloc[-1].close - min5_kline.iloc[-1].open):
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},长下影线赶紧止损')
        #     stop_signal = -1

def target_profit_stop():
    global stop_signal, long_stop_price, short_stop_price, wait_period, open_signal, short_trend, current_kline_form, max_profit
    # 已有开仓信号
    if open_signal > 0 and long_entry_price > 0:
        open_signal = 0
        log(f'已开多仓,不计算止盈')
        return
    elif open_signal < 0 and short_entry_price > 0:
        open_signal = 0
        log(f'已开空仓,不计算止盈')
        return
    if not (long_entry_price > 0 or short_entry_price > 0):
        return
    # if short_trend == 0 and shock_down_limit < ticks.iloc[-1].last_price < shock_upper_limit:
    #     if long_entry_price > 0 and ticks.iloc[-1].bid_price1 >= long_entry_price + 0.5 * profit_tick * price_tick:
    #         log('到达震荡区间止盈价格平仓')
    #         stop_signal = 1
    #     elif short_entry_price > 0 and ticks.iloc[-1].ask_price1 <= short_entry_price - 0.5 * profit_tick * price_tick:
    #         log('到达震荡区间止盈价格平仓')
    #         stop_signal = -1
    # else:
    # max_profit = max(max_profit, ticks.iloc[-1].bid_price1 - long_entry_price) if long_entry_price > 0 else max(max_profit, short_entry_price - ticks.iloc[-1].ask_price1)
    if long_entry_price > 0:
        # if max_profit > max_profit_limit * price_tick and (ticks.iloc[-1].bid_price1 - long_entry_price) < 0.5 * max_profit:
        #     log(f'最大利润减半赶紧止盈:max_profit{max_profit}')
        #     stop_signal = 1
        # if middle_trend > 0 and long_trend > 0:
        #     return
        if ticks.iloc[-1].bid_price1 >= long_entry_price + big_profit_tick * price_tick:
            log(f'做多到达止盈价格平仓')
            stop_signal = 1
        # elif ticks.iloc[-1].bid_price1 >= long_entry_price + profit_tick * price_tick:
        #     if long_stop_price < long_entry_price:
        #         long_stop_price = long_entry_price
        #         log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},已经盈利{profit_tick}跳，修改止损位至{long_stop_price}')
    elif short_entry_price > 0:
        # if max_profit > max_profit_limit * price_tick and (short_entry_price - ticks.iloc[-1].ask_price1) < 0.5 * max_profit:
        #     log(f'最大利润减半赶紧止盈:max_profit{max_profit}')
        #     stop_signal = -1
        # if middle_trend < 0 and long_trend < 0:
        #     return
        if ticks.iloc[-1].ask_price1 <= short_entry_price - big_profit_tick * price_tick:
            log(f'做空到达止盈价格平仓')
            stop_signal = -1
        # elif ticks.iloc[-1].ask_price1 <= short_entry_price - profit_tick * price_tick:
        #     if short_stop_price > short_entry_price:
        #         short_stop_price = short_entry_price
        #         log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},已经盈利{profit_tick}跳，修改止损位至{short_stop_price}')

# ---------------------交易模块----------------------#
def trade_mode():
    global open_signal, stop_signal, long_stop_price, short_stop_price, condition_open_long_price, condition_open_short_price, profit_time_left, last_open_kline_form, last_kline_form
    last_price = ticks.iloc[-1].last_price
    bid_price = ticks.iloc[-1].bid_price1
    ask_price = ticks.iloc[-1].ask_price1
    now_datetime = tafunc.time_to_datetime(ticks.iloc[-1].datetime)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute

    # 开仓
    if open_signal == 1:
        trySendOrder(lots, 1, -1)
        open_signal = 0
        condition_open_short_price = 0
        condition_open_long_price = 0
    elif open_signal == -1:
        open_signal = 0
        trySendOrder(lots, -1, 1)
        condition_open_short_price = 0
        condition_open_long_price = 0
    # 回调开仓
    if last_kline_form is not None and last_kline_form.direction == Direction.LONG and last_kline_form.condition_open_price > 0 and ticks.iloc[-1].ask_price1 <= last_kline_form.condition_open_price:
        log(f'以条件价{last_kline_form.condition_open_price}开多')
        trySendOrder(lots, 1, -1)
        condition_open_long_price = 0
    elif last_kline_form is not None and last_kline_form.direction == Direction.SHORT and last_kline_form.condition_open_price > 0 and ticks.iloc[-1].bid_price1 >= last_kline_form.condition_open_price:
        log(f'以条件价{last_kline_form.condition_open_price}开空')
        trySendOrder(lots, -1, 1)
        condition_open_short_price = 0
    # 止损
    if long_stop_price > 0 and bid_price <= long_stop_price:
        trySendOrder(lots, 0, 1)
        long_stop_price = 0
    elif short_stop_price > 0 and ask_price >= short_stop_price:
        trySendOrder(lots, 0, -1)
        short_stop_price = 0
    # 止盈
    if stop_signal == 1:
        if (middle_trend < 0 or long_trend < 0) and bid_price - long_entry_price > 18 * price_tick:
            profit_time_left -= 1
            log(f'盈利后改变{profit_time_left}')
        # elif bid_price - long_entry_price >= 8 * price_tick:
            # profit_time_left -= 0.5
            # log(f'盈利后改变{profit_time_left}')
        trySendOrder(lots, 0, 1)
        long_stop_price = 0
        stop_signal = 0
    elif stop_signal == -1:
        if (middle_trend > 0 or long_trend > 0) and short_entry_price - ask_price > 18 * price_tick:
            profit_time_left -= 1
            log(f'盈利后改变{profit_time_left}')
        # elif short_entry_price - ask_price >= 8 * price_tick:
        #     profit_time_left -= 0.5
        #     log(f'盈利后改变{profit_time_left}')
        trySendOrder(lots, 0, -1)
        short_stop_price = 0
        stop_signal = 0
    return 1


# -----------------控制仓位发送委托单-----------------#
def trySendOrder(volume, KC, PC):
    global long_entry_price, short_entry_price, wait_period, max_profit, last_kline_form, traded
    # 平仓发单
    if (PC == 1):
        if (position.pos_long > 0):
            api.insert_order(sym, "SELL", "CLOSETODAY", position.pos_long, ticks.iloc[-1].bid_price1, advanced="FAK")
            log(f"交易:平多, price:{ticks.iloc[-1].bid_price1}")
            long_entry_price = 0
            wait_period = 0
            max_profit = 0
            last_kline_form = None
            traded = True
    elif (PC == -1):
        if (position.pos_short > 0):
            api.insert_order(sym, "BUY", "CLOSETODAY", position.pos_short, ticks.iloc[-1].ask_price1, advanced="FAK")
            log(f"交易:平空, price:{ticks.iloc[-1].ask_price1}")
            short_entry_price = 0
            wait_period = 0
            max_profit = 0
            last_kline_form = None
            traded = True
    # 开仓发单
    if (KC == 1):
        api.insert_order(sym, "BUY", "OPEN", volume, ticks.iloc[-1].ask_price1, advanced="FAK")
        log(f"交易:开多, price:{ticks.iloc[-1].ask_price1}")
        long_entry_price = ticks.iloc[-1].ask_price1
        short_entry_price = 0
        last_kline_form = None
    elif (KC == -1):
        api.insert_order(sym, "SELL", "OPEN", volume, ticks.iloc[-1].bid_price1, advanced="FAK")
        log(f"交易:开空, price:{ticks.iloc[-1].bid_price1}")
        short_entry_price = ticks.iloc[-1].bid_price1
        long_entry_price = 0
        last_kline_form = None
    return 1


# ---------------------收盘清仓---------------------#
def close_and_clearn():
    global is_trading, condition_open_long_price, condition_open_short_price, short_trend, last_kline_form, current_kline_form, shock_down_limit, shock_upper_limit, last_poc_price, profit_time_left, open_ready_price, last_open_kline_form
    if (is_trading == 1):
        trySendOrder(lots, 0, 1)
        trySendOrder(lots, 0, -1)
        is_trading = 0
        # 删除原始积累数据，每次循环重新计算：
        stop_signal = 0
        open_signal = 0
        long_stop_price = 0
        short_stop_price = 0
        condition_open_long_price = 0
        condition_open_short_price = 0
        last_poc_price = 0
        profit_time_left = 1
        open_ready_price = 0
        last_open_kline_form = None
        print("收盘清仓，停止运行，停止交易")
    return 1


# --------------------处理时间函数------------------#
def can_time(hour, minute):
    hour = str(hour)
    minute = str(minute)
    if len(minute) == 1:
        minute = "0" + minute
    return int(hour + minute)


# --------------------限制交易时间------------------#
def Ctrltime():
    global is_trading, short_trend, shock_upper_limit, shock_down_limit
    # 限制交易时间
    # hour_new = int(time.strptime(quote.datetime, "%Y-%m-%d %H:%M:%S.%f"))
    now = tafunc.time_to_datetime(ticks.iloc[-1].datetime)
    hour_new = now.hour  # 格式化时间戳，并获取小时
    minute_new = now.minute  # 格式化时间戳，并获取分钟
    # day_new = int(time.strftime("%d", quote.datetime))  # 格式化时间戳，并获取日期
    # print(can_time(hour_new,minute_new))
    if (1130 <= can_time(hour_new, minute_new) < 1330):  return 0
    if (1015 <= can_time(hour_new, minute_new) < 1030):  return 0
    if (900 <= can_time(hour_new, minute_new) < 1458): is_trading = 1
    if (NT == 1):
        if ((2100 <= can_time(hour_new, minute_new) < 2258)):
            is_trading = 1  # 夜盘交易时间段21-23
    elif (NT == 2):
        if ((2100 <= can_time(hour_new, minute_new) <= 2359) or (0 <= can_time(hour_new, minute_new) < 228)):
            is_trading = 1  # 夜盘交易时间段21-02.30
    if (is_trading == 0): return 0
    # 收盘清仓
    if 1128 <= can_time(hour_new, minute_new) < 1130:
        # 中午清仓观望
        close_and_clearn()
    if (NT == 1):
        if ((1458 <= can_time(hour_new, minute_new) < 1500) or (2258 <= can_time(hour_new, minute_new) < 2300)):  # 23点结束
            close_and_clearn()
            short_trend = 0
            shock_upper_limit = 0
            shock_down_limit = 0
    elif (NT == 2):
        if ((1458 <= can_time(hour_new, minute_new) < 1500) or (228 <= can_time(hour_new, minute_new) < 230)):  # 凌晨2.30结束
            close_and_clearn()
            short_trend = 0
            shock_upper_limit = 0
            shock_down_limit = 0
    return 1

day_ma5 = ta.MA(day_kline, 5)
day_ma20 = ta.MA(day_kline, 20)
long_trend = 1 if day_kline.close.iloc[-2] >= day_ma20.ma.iloc[-2] else -1
middle_trend = 1 if day_ma5.ma.iloc[-2] >= day_ma5.ma.iloc[-3] else -1
is_first_traded = -2
inited = False
while True:
    try:
        api.wait_update()
        if api.is_changing(min5_kline.iloc[-1], ["volume"]) and min5_kline.iloc[-1].volume > 0:
            tq_now = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime)
            # 核心模块
            price_logger.debug(f'天勤k线:time:{tq_now}, open:{min5_kline.iloc[-1].open}, close:{min5_kline.iloc[-1].close}, high:{min5_kline.iloc[-1].high}, low:{min5_kline.iloc[-1].low}, volume:{min5_kline.iloc[-1].volume}')
            CoreModule()
        if api.is_changing(ticks):
            # 限制交易时间
            Ctrltime()
        if api.is_changing(day_kline.iloc[-1], ["volume"]) and day_kline.iloc[-1].volume > 0:
            day_ma5 = ta.MA(day_kline, 5)
            day_ma20 = ta.MA(day_kline, 20)
            long_trend = 1 if day_kline.close.iloc[-1] >= day_ma20.ma.iloc[-1] else -1
            middle_trend = 1 if day_ma5.ma.iloc[-1] >= day_ma5.ma.iloc[-2] else -1
        # 交易模块
        target_profit_stop()
        trade_mode()
    except BacktestFinished as e:
        pass
