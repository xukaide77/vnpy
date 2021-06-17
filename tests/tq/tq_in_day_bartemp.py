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
from vnpy.trader.utility import BarData, TickData, extract_vt_symbol, get_trading_date, ceil_to, floor_to
from tests.tq.tq_utility import *
import math


os.path.dirname(__file__)
logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'in_day_trade_last'),
                           name='trade_pp',
                           log_level=logging.DEBUG,
                           backtesing=True)
price_logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'in_day_price_last'),
                      name='price_pp',
                      log_level=logging.DEBUG,
                      backtesing=True)

# 参数10380
lots = 2  # 下单手数
NT = 1  # 默认1是正常时间，2是凌晨2.30
profit_tick = 5  # 止盈跳
big_profit_tick = 7  # 止盈跳
loss_tick = 30  # 止损跳
trade_period = 3 #单位分钟
max_wait_period = 5
time_limit = 9 # 距离分型k线根数
max_profit_limit = 50 #超过限制后利润减半止盈
templates: List[KlineForm] = [LiYongQiang()]

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
current_signal: Signal = None
last_open_kline_form: KlineForm = None
shock_upper_limit = 0
shock_down_limit = 0
profit_time_left = 1
max_profit = 0
open_ready_price = 0
traded = False
key_price: KeyPrice = KeyPrice()
last_is_trend = False
current_trend = 0


# 默认
# api = TqApi(backtest=TqReprofit_tickay(date(2020, 9, 18)), auth="songshu123,7088044")
api = TqApi(TqSim(10000), backtest=TqBacktest(start_dt=date(2021, 1, 14), end_dt=date(2021, 1, 14)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(10000), backtest=TqBacktest(start_dt=datetime(2020, 11, 27, 9, 0), end_dt=datetime(2020, 11, 27, 15, 0)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(100000), backtest=TqReprofit_tickay(reprofit_tickay_dt=date(2020, 9, 9)), web_gui=True, auth="xukaide77,xk82513994")
sym = "DCE.pp2105"
# sym = 'CFFEX.IC2011'6509
ex, sy = sym.split('.')
min5_kline = api.get_kline_serial(sym, trade_period * 60, data_length=kline_length)
day_kline = api.get_kline_serial(sym, 24 * 60 * 60, data_length=30)
# 映射交易合约
# SYMBOL = quote.underlying_symbol 2119
order = api.get_order()
position = api.get_position(sym)
# 获得 ag2012 tick序列的引用
# ticks = api.get_tick_serial(sym)
# 获得最小变动单位
price_tick = api.get_quote(sym).price_tick
symbol, exchange = extract_vt_symbol(sy + '.' + ex)
kline_delta = np.zeros(kline_length - 1)
key_price.price_tick = price_tick

def log(msg: str):
    logger.debug('{} {}'.format(tafunc.time_to_datetime(min5_kline.iloc[-1].datetime), msg))


# ----------------核心计算模块(每个bar更新一次)--------#
def CoreModule():
    global last_kline_form, current_signal, last_open_kline_form
    cal_key_price()
    if is_trading == 0:
        return
    cal_stop_signal()
    cal_open_signal()
    current_signal = None

    # 删除原始积累数据，每次循环重新计算：
    # if current_signal is not None:
    #     last_kline_form = current_signal
    # elif last_kline_form is not None:
    #     if last_kline_form.direction == Direction.LONG and ticks.iloc[-1].bid_price1 < last_kline_form.stop_price:
    #         last_kline_form = None
    #     elif last_kline_form.direction == Direction.SHORT and ticks.iloc[-1].ask_price1 > last_kline_form.stop_price:
    #         last_kline_form = None
    # if open_signal != 0:
    #     last_kline_form = None
    #     last_open_kline_form = current_signal
    # current_signal = None
    return 1


def cal_open_signal():
    global long_stop_price, short_stop_price, open_signal, wait_period, short_trend, current_signal, shock_upper_limit, shock_down_limit, profit_time_left, open_ready_price, middle_trend, last_open_kline_form, last_kline_form, traded
    # 画一次指标线
    # ma5 = ta.MA(min5_kline, 5)
    ma10 = ta.EMA(min5_kline, 10)
    # ma20 = ta.MA(min5_kline, 20)  # 使用 tqsdk 自带指标函数计算均线
    # ma40 = ta.MA(min5_kline, 40)
    # # min5_kline["ma5_MAIN"] = ma5.ma
    min5_kline["ma10_MAIN"] = ma10.ema
    # min5_kline["ma20_MAIN"] = ma20.ma
    # min5_kline["ma40_MAIN"] = ma40.ma
    # # min5_kline['ma5_MAIN.color'] = 'white'
    # # min5_kline['ma10_MAIN.color'] = 'yellow'
    # min5_kline['ma20_MAIN.color'] = 'purple'
    # min5_kline['ma40_MAIN.color'] = 'green'

    # 委托时间
    now_datetime = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime) + timedelta(minutes=trade_period)
    # now_datetime = parser.parse(now)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute
    int_time = can_time(now_hour, now_minute)
    latest_price_datetime = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime)
    # now_datetime = parser.parse(now)
    price_int_time = can_time(latest_price_datetime.hour, latest_price_datetime.minute)
    if (now_hour == 9 and now_minute < 30) or (now_hour == 21 and now_minute < 30) or (1445 <= int_time <= 1500) or (2242 <= int_time <= 2300):
        # 前分钟不开仓
        return 0

    if long_entry_price > 0 or short_entry_price > 0:
        return
    current_signal = recognize_form()
    if last_open_kline_form is not None and current_signal is not None and current_signal.timestamp == last_open_kline_form.timestamp:
        log(f'已经根据{last_open_kline_form.timestamp}信号开过仓')
        return

    if profit_time_left == 0:
        return
    # if last_is_trend:
    #     return
    if current_signal is not None and current_signal.direction == Direction.LONG:
        if long_entry_price > 0:
            long_stop_price = current_signal.stop_price
            return
        stop_condition = (min5_kline.iloc[-1].close - loss_tick * price_tick) < current_signal.stop_price#or (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < ma20.ma.iloc[-1]
        log(f'stop_condition:{stop_condition},kline_form.stop_price:{current_signal.stop_price}')
        # if current_signal.type == KlineType.CALLBACK_BOTTOM and middle_trend < 0:
        #     return

        open_signal = 1
        # stop_price = max(min(current_signal.stop_price, ticks.iloc[-1].ask_price1 - 0.7 * loss_tick * price_tick), ma20.ma.iloc[-1])
        stop_price = max(current_signal.stop_price, min5_kline.iloc[-1].close - loss_tick * price_tick)
        log(f'设置开多仓止损位至{stop_price}')
        long_stop_price = stop_price
        short_stop_price = 0
        # else:
        #     if current_signal.type == KlineType.CALLBACK_BOTTOM:
        #         return
        #     open_ready_price = current_signal.stop_price + loss_tick * price_tick
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},段趋势:{short_trend}，准备开多时止损太大等回调价{open_ready_price}')
    elif current_signal is not None and current_signal.direction == Direction.SHORT:
        stop_condition = (min5_kline.iloc[-1].close + loss_tick * price_tick) > current_signal.stop_price# or (ticks.iloc[-1].bid_price1 + loss_tick * price_tick) > ma20.ma.iloc[-1]
        log(f'stop_condition:{stop_condition},kline_form.stop_price:{current_signal.stop_price}')
        # if current_signal.type == KlineType.CALLBACK_TOP and middle_trend > 0:
        #     return

        open_signal = -1
        # stop_price = min(max(ticks.iloc[-1].bid_price1 + 0.7 * loss_tick * price_tick, current_signal.stop_price), ma20.ma.iloc[-1])
        stop_price = min(current_signal.stop_price, min5_kline.iloc[-1].close + loss_tick * price_tick)
        log('下跌趋势设置开空仓止损位至{}'.format(stop_price))
        short_stop_price = stop_price
        long_stop_price = 0




def recognize_form() -> Signal:
    for form_template in templates:
        signal = form_template.verify(min5_kline, key_price)
        if signal is not None:
            api.draw_text(min5_kline, form_template.name, x=-1, y=min5_kline.iloc[-1].high + 8 * price_tick, color='green')
            log(f'识别到形态{form_template.name}出信号')
            return signal
    return None

def cal_key_price():
    global current_trend
    now_datetime = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime) + timedelta(minutes=trade_period)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute
    if (now_hour == 9 and now_minute == 3) or (now_hour == 21 and now_minute == 3):
        key_price.first_high = min5_kline.iloc[-1].high
        key_price.first_low = min5_kline.iloc[-1].low
    elif (now_hour == 9 and now_minute <= 30) or (now_hour == 21 and now_minute <= 30):
        if min5_kline.iloc[-1].close >= min5_kline.iloc[-1].open:
            high = min5_kline.iloc[-1].close + ceil_to(0.5 * (min5_kline.iloc[-1].high - min5_kline.iloc[-1].close), price_tick) if min5_kline.iloc[-1].high - min5_kline.iloc[-1].close > 10 * price_tick else min5_kline.iloc[-1].high
            low = min5_kline.iloc[-1].open - ceil_to(0.5 * (min5_kline.iloc[-1].open - min5_kline.iloc[-1].low), price_tick) if min5_kline.iloc[-1].open - min5_kline.iloc[-1].low > 10 * price_tick else min5_kline.iloc[-1].low
            if high > key_price.open_high:
                key_price.open_high = high
            if low < key_price.open_low:
                key_price.open_low = low
        elif min5_kline.iloc[-1].close < min5_kline.iloc[-1].open:
            high = min5_kline.iloc[-1].open + ceil_to(0.5 * (min5_kline.iloc[-1].high - min5_kline.iloc[-1].open), price_tick) if min5_kline.iloc[-1].high - min5_kline.iloc[-1].open > 10 * price_tick else min5_kline.iloc[-1].high
            low = min5_kline.iloc[-1].close - ceil_to(0.5 * (min5_kline.iloc[-1].close - min5_kline.iloc[-1].low), price_tick) if min5_kline.iloc[-1].close - min5_kline.iloc[-1].low > 10 * price_tick else min5_kline.iloc[-1].low
            if high > key_price.open_high:
                key_price.open_high = high
            if low < key_price.open_low:
                key_price.open_low = low
    elif ((now_hour == 9 and now_minute > 30) or (now_hour == 21 and now_minute > 30)) and key_price.first_high != 0:
        if min(min5_kline.iloc[-1].open, min5_kline.iloc[-1].close) <= key_price.open_low:
            key_price.open_low = min(min5_kline.iloc[-1].open, min5_kline.iloc[-1].close)
        elif max(min5_kline.iloc[-1].open, min5_kline.iloc[-1].close) >= key_price.open_high:
            key_price.open_high = max(min5_kline.iloc[-1].open, min5_kline.iloc[-1].close)
        else:
            if key_price.first_high > key_price.open_high and key_price.first_high - key_price.open_high < 10 * price_tick:
                key_price.open_high = key_price.first_high
            if key_price.first_low < key_price.open_low and key_price.open_low - key_price.first_low < 10 * price_tick:
                key_price.open_low = key_price.first_low
            key_price.first_high = 0
            key_price.first_low = 0
            api.draw_line(min5_kline, x1=-10, y1=key_price.open_high, x2=-1,  y2=key_price.open_high, line_type='SEG', color='red')
            api.draw_line(min5_kline, x1=-10, y1=key_price.open_low, x2=-1,  y2=key_price.open_low, line_type='SEG', color='blue')
    boll = BOLL(min5_kline, 20, 6, 1)
    if min5_kline.iloc[-1].open < boll.bottom.iloc[-1] and min5_kline.iloc[-1].close < boll.bottom.iloc[-1]:
        key_price.short_trend_count += 1
        if key_price.short_trend_count > 11:
            current_trend = -1
            log(f'走出趋势{current_trend}')
    elif min5_kline.iloc[-1].open > boll.top.iloc[-1] and min5_kline.iloc[-1].close > boll.top.iloc[-1]:
        key_price.long_trend_count += 1
        if key_price.long_trend_count > 11:
            current_trend = 1
            log(f'走出趋势{current_trend}')
    else:
        key_price.short_trend_count = 0
        key_price.long_trend_count = 0



def cal_stop_signal():
    global stop_signal, long_stop_price, short_stop_price, wait_period, open_signal, short_trend, current_signal, last_kline_form
    if not (long_entry_price > 0 or short_entry_price > 0):
        return

    vrsi = ta.VRSI(min5_kline, 3)
    ma10 = ta.EMA(min5_kline, 10)
    if long_entry_price > 0:
        wait_period += 1
        if min5_kline.iloc[-1].close < ma10.ema.iloc[-1]:
            stop_signal = 1
            log('跌破10日线止盈')
        elif vrsi.vrsi.iloc[-2] > 75 and vrsi.vrsi.iloc[-1] < 75 and wait_period > 3:
            stop_signal = 1
            log('放量止盈')
        elif min5_kline.iloc[-1].close - min5_kline.iloc[-1].open < -20 * price_tick:
            stop_signal = 1
            log(f'做多回调{min5_kline.iloc[-1].close - min5_kline.iloc[-1].open}过大进行止盈')
        elif wait_period >= max_wait_period and min5_kline.iloc[-1].close < key_price.open_high:
            log(f'wait_period:{wait_period}到达等待最大周期还未盈利赶紧止损')
            stop_signal = 1
        # elif min5_kline.iloc[-1].high - max(min5_kline.iloc[-1].close, min5_kline.iloc[-1].open) > 2 * abs(min5_kline.iloc[-1].close - min5_kline.iloc[-1].open):
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},长下影线赶紧止损')
        #     stop_signal = 1
    elif short_entry_price > 0:
        wait_period += 1
        if min5_kline.iloc[-1].close > ma10.ema.iloc[-1]:
            stop_signal = -1
            log('升破10日线止盈')
        elif vrsi.vrsi.iloc[-2] > 75 and vrsi.vrsi.iloc[-1] < 75 and wait_period > 3:
            stop_signal = -1
            log('放量止盈')
        elif min5_kline.iloc[-1].close - min5_kline.iloc[-1].open > 20 * price_tick:
            stop_signal = -1
            log(f'做空回调{min5_kline.iloc[-1].close - min5_kline.iloc[-1].open}过大进行止盈')
        elif wait_period >= max_wait_period and min5_kline.iloc[-1].close > key_price.open_low:
            log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},wait_period:{wait_period}到达等待最大周期还未盈利赶紧止损')
            stop_signal = -1
        # elif min(min5_kline.iloc[-1].close, min5_kline.iloc[-1].open) - min5_kline.iloc[-1].low > 2 * abs(min5_kline.iloc[-1].close - min5_kline.iloc[-1].open):
        #     log(f'长趋势:{long_trend},中趋势:{middle_trend},短趋势:{short_trend},长下影线赶紧止损')
        #     stop_signal = -1

# ---------------------交易模块----------------------#
def trade_mode():
    global open_signal, stop_signal, long_stop_price, short_stop_price, condition_open_long_price, condition_open_short_price, profit_time_left
    last_price = min5_kline.iloc[-1].close

    # 开仓
    if open_signal == 1:
        if not long_entry_price > 0:
            trySendOrder(lots, 1, 0)
        open_signal = 0
        condition_open_short_price = 0
        condition_open_long_price = 0
        key_price.after_close_high = 0
        key_price.after_close_low = 0
    elif open_signal == -1:
        if not short_entry_price > 0:
            trySendOrder(lots, -1, 0)
        open_signal = 0
        condition_open_short_price = 0
        condition_open_long_price = 0
        key_price.after_close_high = 0
        key_price.after_close_low = 0
    # 止盈
    if stop_signal == 1:
        if last_price - long_entry_price > 20 * price_tick:
            profit_time_left -= 1
            log(f'盈利后改变{profit_time_left}')
        trySendOrder(lots, 0, 1)
        long_stop_price = 0
        stop_signal = 0
        key_price.after_close_low = min5_kline.iloc[-1].low
    elif stop_signal == -1:
        if short_entry_price - last_price > 20 * price_tick:
            profit_time_left -= 1
            log(f'盈利后改变{profit_time_left}')
        trySendOrder(lots, 0, -1)
        short_stop_price = 0
        stop_signal = 0
        key_price.after_close_high = min5_kline.iloc[-1].high
        # 止损
    if long_stop_price > 0 and last_price < long_stop_price:
        trySendOrder(lots, 0, 1)
        long_stop_price = 0
    elif short_stop_price > 0 and last_price > short_stop_price:
        trySendOrder(lots, 0, -1)
        short_stop_price = 0
    return 1


# -----------------控制仓位发送委托单-----------------#
def trySendOrder(volume, KC, PC):
    global long_entry_price, short_entry_price, wait_period, max_profit, last_kline_form, traded
    last_price = min5_kline.iloc[-1].close
    # 平仓发单
    if (PC == 1):
        if (position.pos_long > 0):
            api.insert_order(sym, "SELL", "CLOSETODAY", position.pos_long, limit_price=None)
        log(f"交易:平多, price:{last_price}")
        long_entry_price = 0
        wait_period = 0
        max_profit = 0
        last_kline_form = None
        traded = True
    elif (PC == -1):
        if (position.pos_short > 0):
            api.insert_order(sym, "BUY", "CLOSETODAY", position.pos_short, limit_price=None)
        log(f"交易:平空, price:{last_price}")
        short_entry_price = 0
        wait_period = 0
        max_profit = 0
        last_kline_form = None
        traded = True
    # 开仓发单
    if (KC == 1):
        api.insert_order(sym, "BUY", "OPEN", volume, limit_price=None)
        log(f"交易:开多, price:{last_price}")
        long_entry_price = last_price
        short_entry_price = 0
    elif (KC == -1):
        api.insert_order(sym, "SELL", "OPEN", volume, limit_price=None)
        log(f"交易:开空, price:{last_price}")
        short_entry_price = last_price
        long_entry_price = 0
    return 1


# ---------------------收盘清仓---------------------#
def close_and_clearn():
    global is_trading, condition_open_long_price, condition_open_short_price, short_trend, last_kline_form, current_signal, shock_down_limit, shock_upper_limit, last_poc_price, profit_time_left, open_ready_price, last_open_kline_form, current_trend, last_is_trend
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
        profit_time_left = 1
        open_ready_price = 0
        last_open_kline_form = None
        key_price.clear()
        last_is_trend = not current_trend == 0
        current_trend = 0
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
    now = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime) + timedelta(minutes=trade_period)
    hour_new = now.hour  # 格式化时间戳，并获取小时
    minute_new = now.minute  # 格式化时间戳，并获取分钟
    # day_new = int(time.strftime("%d", quote.datetime))  # 格式化时间戳，并获取日期
    # print(can_time(hour_new,minute_new))
    if (900 <= can_time(hour_new, minute_new) < 1457):
        is_trading = 1
    if (NT == 1):
        if ((2100 <= can_time(hour_new, minute_new) < 2257)):
            is_trading = 1  # 夜盘交易时间段21-23
    elif (NT == 2):
        if ((2100 <= can_time(hour_new, minute_new) < 2357) or (0 <= can_time(hour_new, minute_new) < 227)):
            is_trading = 1  # 夜盘交易时间段21-02.30
    if (is_trading == 0):
        return 0
    # 收盘清仓
    # if 1128 <= can_time(hour_new, minute_new) < 1130:天勤k线
    #     # 中午清仓观望
    #     close_and_clearn()
    if (NT == 1):
        if ((1457 <= can_time(hour_new, minute_new) < 1500) or (2257 <= can_time(hour_new, minute_new) < 2300)):  # 23点结束
            close_and_clearn()
    elif (NT == 2):
        if ((1457 <= can_time(hour_new, minute_new) < 1500) or (227 <= can_time(hour_new, minute_new) < 230)):  # 凌晨2.30结束
            close_and_clearn()
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
            trade_mode()
            Ctrltime()
        if api.is_changing(day_kline.iloc[-1], ["volume"]) and day_kline.iloc[-1].volume > 0:
            day_ma5 = ta.MA(day_kline, 5)
            day_ma20 = ta.MA(day_kline, 20)
            long_trend = 1 if day_kline.close.iloc[-1] >= day_ma20.ma.iloc[-1] else -1
            middle_trend = 1 if day_ma5.ma.iloc[-1] >= day_ma5.ma.iloc[-2] else -1
        # 交易模块
        # target_profit_stop()
    except BacktestFinished as e:
        pass
