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
import json


os.path.dirname(__file__)
logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'wave_trade'),
                           name='trade',
                           log_level=logging.DEBUG,
                           backtesing=True)
price_logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'wave_price'),
                      name='price',
                      log_level=logging.DEBUG,
                      backtesing=True)

# 参数10380
lots = 3  # 下单手数
NT = 1  # 默认1是正常时间，2是凌晨2.30
bdfj_param = 6
templates: List[KlineForm] = [KdjIndicator()]
trades: List[Trade] = []
exhibition_times = []

# 全局变量
current_signal: Signal = None
key_price: KeyPrice = KeyPrice()
last_is_trend = False
main_trend = 0
callback_trend = 0
sleep_days = 0

with open(os.path.join(os.path.dirname(__file__), 'DCE.i切换.txt'), mode='r', encoding='UTF-8') as f:
    time2symbol = json.load(f)
for time, symbol in time2symbol.items():
    exhibition_times.append(time)

# 默认
# api = TqApi(backtest=TqReprofit_tickay(date(2020, 9, 18)), auth="songshu123,7088044")
api = TqApi(TqSim(100000), backtest=TqBacktest(start_dt=date(2016, 1, 1), end_dt=date(2021, 3, 1)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(10000), backtest=TqBacktest(start_dt=datetime(2020, 11, 27, 9, 0), end_dt=datetime(2020, 11, 27, 15, 0)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(100000), backtest=TqReprofit_tickay(reprofit_tickay_dt=date(2020, 9, 9)), web_gui=True, auth="xukaide77,xk82513994")
# sym = "DCE.i1609"
# sym = "DCE.pp2105"
main_sym = 'KQ.m@DCE.i'
index__sym = 'KQ.i@DCE.i'
sym = time2symbol[exhibition_times[0]]
# sym = 'CFFEX.IC2011'6509
small_kline = api.get_kline_serial(main_sym, 1 * 60, data_length=20)
exhibition_times.pop(0)
# min5_kline = api.get_kline_serial(sym, 60 * 5, data_length=50)
# hour_kline = api.get_kline_serial(sym, 60 * 60, data_length=20)
day_kline = api.get_kline_serial(main_sym, 24 * 60 * 60, data_length=30)
week_kline = api.get_kline_serial(index__sym, 60 * 60 * 24 * 7, data_length=7)
# 映射交易合约
# SYMBOL = quote.underlying_symbol 2119
order = api.get_order()
position = api.get_position(sym)
# ticks = api.get_tick_serial(sym)
# 获得最小变动单位
price_tick = api.get_quote(sym).price_tick
key_price.price_tick = price_tick

def log(msg: str):
    logger.debug('{} {}'.format(tafunc.time_to_datetime(small_kline.iloc[-1].datetime) + timedelta(minutes=1), msg))

# ----------------核心计算模块(每个bar更新一次)--------#
def CoreModule():
    confirm_callback_trend()
    cal_key_price()
    return 1


def cal_open_signal():
    global current_signal
    # 画一次指标线
    atr = ATR_TD(day_kline)
    # ma5 = ta.MA(min5_kline, 5)
    # ma10 = ta.EMA(min5_kline, 10)
    # ma20 = ta.MA(min5_kline, 20)  # 使用 tqsdk 自带指标函数计算均线
    # ma40 = ta.MA(min5_kline, 40)
    day_kline["mid_MAIN"] = atr.mid
    day_kline["top_MAIN"] = atr.top
    day_kline["bottom_MAIN"] = atr.bottom
    # min5_kline["ma40_MAIN"] = ma40.ma
    # # min5_kline['ma5_MAIN.color'] = 'white'
    day_kline['mid_MAIN.color'] = 'yellow'
    day_kline['top_MAIN.color'] = 'purple'
    day_kline['bottom_MAIN.color'] = 'green'

    # 委托时间
    now_datetime = tafunc.time_to_datetime(small_kline.iloc[-1].datetime) + timedelta(minutes=1)
    # now_datetime = parser.parse(now)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute

    current_signal = recognize_form()
    if current_signal is not None and current_signal.direction == Direction.LONG:
        # stop_condition = (min5_kline.iloc[-1].close - loss_tick * price_tick) < current_signal.stop_price#or (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < ma20.ma.iloc[-1]
        # log(f'stop_condition:{stop_condition},kline_form.stop_price:{current_signal.stop_price}')
        api.draw_text(day_kline, '突破', x=-1, y=day_kline.iloc[-2].high + 8 * price_tick, color='red')
        log(f'设置开多仓开仓价{key_price.open_ready_price},止损位至{current_signal.stop_price},止盈价:{current_signal.stop_profit_price}')
    elif current_signal is not None and current_signal.direction == Direction.SHORT:
        # stop_condition = (min5_kline.iloc[-1].close + loss_tick * price_tick) > current_signal.stop_price# or (ticks.iloc[-1].bid_price1 + loss_tick * price_tick) > ma20.ma.iloc[-1]
        # log(f'stop_condition:{stop_condition},kline_form.stop_price:{current_signal.stop_price}')
        api.draw_text(day_kline, '突破', x=-1, y=day_kline.iloc[-2].low - 5 * price_tick, color='green')
        log(f'设置开空仓开仓价{key_price.open_ready_price},止损位至{current_signal.stop_price},止盈价:{current_signal.stop_profit_price}')

def recognize_form() -> Signal:
    signal = None
    if len(trades) > 0 and trades[-1].stop_profit_count == 0:
        # 有还未止盈过单子不建新仓
        return
    if key_price.open_ready_price > 0 and key_price.trend == 1 and small_kline.iloc[-1].close > key_price.open_ready_price:
        now = tafunc.time_to_datetime(small_kline.iloc[-1].datetime)
        hour_new = now.hour  # 格式化时间戳，并获取小时
        minute_new = now.minute
        if can_time(hour_new, minute_new) == 2100 or can_time(hour_new, minute_new) == 900:
            # 跳空时寻找第二最近的突破点
            i = -1
            while i > -20:
                if (day_kline.iloc[i].high > day_kline.iloc[i-1].high or day_kline.iloc[i].low < day_kline.iloc[i-1].low) and day_kline.iloc[i].high > small_kline.iloc[-1].close:
                    key_price.open_ready_price = day_kline.iloc[i].high + 1
                    key_price.stop_loss_price = day_kline.iloc[i].low - 1
                    key_price.stop_profit_price = ATR_TD(day_kline).iloc[i].top
                    log(f'跳空修改开仓价为{key_price.open_ready_price},止损价为{key_price.stop_loss_price},止盈价为{key_price.stop_profit_price}')
                    return None
                i = i - 1
        signal = Signal(key_price.stop_loss_price, Direction.LONG, tafunc.time_to_datetime(small_kline.iloc[-1].datetime), SignalType.TREND, key_price.stop_profit_price)
    elif key_price.open_ready_price > 0 and key_price.trend == -1 and small_kline.iloc[-1].close < key_price.open_ready_price:
        now = tafunc.time_to_datetime(small_kline.iloc[-1].datetime)
        hour_new = now.hour  # 格式化时间戳，并获取小时
        minute_new = now.minute
        if can_time(hour_new, minute_new) == 2100 or can_time(hour_new, minute_new) == 900:
            # 跳空时寻找第二最近的突破点
            i = -1
            while i > -20:
                if (day_kline.iloc[i].high > day_kline.iloc[i-1].high or day_kline.iloc[i].low < day_kline.iloc[i-1].low) and day_kline.iloc[i].low < small_kline.iloc[-1].close:
                    key_price.open_ready_price = day_kline.iloc[i].low - 1
                    key_price.stop_loss_price = day_kline.iloc[i].high + 1
                    key_price.stop_profit_price = ATR_TD(day_kline).iloc[i].bottom
                    log(f'跳空修改开仓价为{key_price.open_ready_price},止损价为{key_price.stop_loss_price},止盈价为{key_price.stop_profit_price}')
                    return None
                i = i - 1
        signal = Signal(key_price.stop_loss_price, Direction.SHORT, tafunc.time_to_datetime(small_kline.iloc[-1].datetime), SignalType.TREND, key_price.stop_profit_price)
    return signal

def confirm_main_trend():
    global main_trend
    if week_kline.iloc[-2].close > max(week_kline.iloc[-bdfj_param:-2].close) and week_kline.iloc[-2].high > max(week_kline.iloc[-bdfj_param:-2].high) and main_trend <= 0:
        main_trend = 1
        log(f'主趋势改变，目前主趋势为{main_trend}，调整趋势为{callback_trend}')
    elif week_kline.iloc[-2].close < min(week_kline.iloc[-bdfj_param:-2].close) and week_kline.iloc[-2].low < min(week_kline.iloc[-bdfj_param:-2].low) and main_trend >= 0:
        main_trend = -1
        log(f'主趋势改变，目前主趋势为{main_trend}，调整趋势为{callback_trend}')

def confirm_callback_trend():
    global callback_trend
    if day_kline.iloc[-1].close > max(day_kline.iloc[-bdfj_param:-1].close) and day_kline.iloc[-1].high > max(day_kline.iloc[-bdfj_param:-1].high) and callback_trend <= 0:
        callback_trend = 1
        log(f'调整趋势改变，目前主趋势为{main_trend}，调整趋势为{callback_trend}')
    elif day_kline.iloc[-1].close < min(day_kline.iloc[-bdfj_param:-1].close) and day_kline.iloc[-1].low < min(day_kline.iloc[-bdfj_param:-1].low) and callback_trend >= 0:
        callback_trend = -1
        log(f'调整趋势改变，目前主趋势为{main_trend}，调整趋势为{callback_trend}')

def cal_high_and_low(kline: DataFrame):
    i = -1
    while i > -10:
        if kline.iloc[i].high > kline.iloc[i-1].high or kline.iloc[i].low < kline.iloc[i-1].low:
            return kline.iloc[i].high + 1, kline.iloc[i].low - 1
        i = i - 1

def cal_key_price():
    global main_trend, callback_trend
    if main_trend > 0 and callback_trend < 0:
        if is_up(day_kline.iloc[-1]):
            # 止跌形态中最近的一根非内包k线
            key_price.open_ready_price, key_price.stop_loss_price = cal_high_and_low(day_kline)
            key_price.trend = 1
            # key_price.stop_profit_price = ATR_TD(day_kline).iloc[-1].top
            key_price.stop_profit_price = min(key_price.open_ready_price + ta.ATR(day_kline, 10).atr.iloc[-1] * 2.618, ATR_TD(day_kline).iloc[-1].top)
            key_price.pre_high = max(day_kline.iloc[-6:-2].close)
            log(f'修改开多仓突破价{key_price.open_ready_price},止盈价{key_price.stop_profit_price}')
    elif main_trend < 0 and callback_trend > 0:
        if is_down(day_kline.iloc[-1]):
            # 止跌形态中最近的一根非内包k线
            key_price.stop_loss_price, key_price.open_ready_price = cal_high_and_low(day_kline)
            key_price.trend = -1
            # key_price.stop_profit_price = ATR_TD(day_kline).iloc[-1].bottom
            key_price.stop_profit_price = max(key_price.open_ready_price - ta.ATR(day_kline, 10).atr.iloc[-1] * 2.618, ATR_TD(day_kline).iloc[-1].top)
            key_price.pre_low = min(day_kline.iloc[-6:-2].close)
            log(f'修改开空仓突破价{key_price.open_ready_price},止盈价{key_price.stop_profit_price}')

# ---------------------交易模块----------------------#
def trade_mode():
    global current_signal, sym, sleep_days, position
    last_price = small_kline.iloc[-1].close
    now = tafunc.time_to_datetime(small_kline.iloc[-1].datetime)
    hour_new = now.hour  # 格式化时间戳，并获取小时
    minute_new = now.minute  # 格式化时间戳，并获取分钟
    if current_signal is not None:
        if current_signal.direction == Direction.LONG:
            trySendOrder(lots, 1, 0)
            trade = Trade()
            trade.open_price = last_price + price_tick
            trade.pos = lots
            trade.stop_loss_price = current_signal.stop_price
            trade.stop_profit_price = current_signal.stop_profit_price
            trade.direction = Direction.LONG
            trade.balance_price = key_price.pre_high
            trade.open_time = now
            log(f'开多仓 开仓价:{trade.open_price},手数:{trade.pos},止损价:{trade.stop_loss_price},止盈价:{trade.stop_profit_price},盈亏平衡价:{trade.balance_price}')
            trades.append(trade)
        elif current_signal.direction == Direction.SHORT:
            trySendOrder(lots, -1, 0)
            trade = Trade()
            trade.open_price = last_price - price_tick
            trade.pos = lots
            trade.stop_loss_price = current_signal.stop_price
            trade.stop_profit_price = current_signal.stop_profit_price
            trade.direction = Direction.SHORT
            trade.balance_price = key_price.pre_low
            trade.open_time = now
            log(f'开空仓 开仓价:{trade.open_price},手数:{trade.pos},止损价:{trade.stop_loss_price},止盈价:{trade.stop_profit_price},盈亏平衡价:{trade.balance_price}')
            trades.append(trade)
        current_signal = None
        key_price.clear()

    if can_time(hour_new, minute_new) == 1459 or can_time(hour_new, minute_new) == 2259:
        return
    if can_time(hour_new, minute_new) == 1458:
        atr = ATR_TD(day_kline).iloc[-1]
        for trade in trades[::-1]:
            if trade.stop_profit_count == 0 and trade.direction == Direction.LONG and day_kline.iloc[-2].close > trade.stop_profit_price and last_price < day_kline.iloc[-2].close:
                # 第一轮减仓
                trySendOrder(trade.pos / 3, 0, 1)
                log(f'订单{trade.open_time}以价格{last_price}第一次减仓{trade.pos / 3}')
                trade.pos -= trade.pos / 3
                trade.stop_profit_count += 1
                return
            elif trade.stop_profit_count == 0 and trade.direction == Direction.SHORT and day_kline.iloc[-2].close < trade.stop_profit_price and last_price > day_kline.iloc[-2].close:
                trySendOrder(trade.pos / 3, 0, -1)
                log(f'订单{trade.open_time}以价格{last_price}第一次减仓{trade.pos / 3}')
                trade.pos -= trade.pos / 3
                trade.stop_profit_count += 1
                return
            elif trade.stop_profit_count == 1 and trade.direction == Direction.LONG and \
                    ((day_kline.iloc[-2].close > atr.top and last_price < atr.top) or (last_price < day_kline.iloc[-2].low)):
                # 第二轮减仓
                trySendOrder(trade.pos / 2, 0, 1)
                log(f'订单{trade.open_time}以价格{last_price}第二次减仓{trade.pos / 2}')
                trade.pos -= trade.pos / 2
                trade.stop_profit_count += 1
                return
            elif trade.stop_profit_count == 1 and trade.direction == Direction.SHORT and (
                    (day_kline.iloc[-2].close < atr.bottom and last_price > atr.bottom) or (last_price > day_kline.iloc[-2].high)):
                # 第二轮减仓
                trySendOrder(trade.pos / 2, 0, -1)
                log(f'订单{trade.open_time}以价格{last_price}第二次减仓{trade.pos / 2}')
                trade.pos -= trade.pos / 2
                trade.stop_profit_count += 1
                return
            elif trade.direction == Direction.LONG and main_trend == -1:
                # 第三轮减仓
                trySendOrder(trade.pos, 0, 1)
                log(f'订单{trade.open_time}以价格{last_price}因主趋势改变清仓{trade.pos}')
                trade.pos -= trade.pos
                trade.stop_profit_count += 1
                trades.remove(trade)
                return
            elif trade.direction == Direction.SHORT and main_trend == 1:
                trySendOrder(trade.pos, 0, -1)
                log(f'订单{trade.open_time}以价格{last_price}因主趋势改变清仓{trade.pos}')
                trade.pos -= trade.pos
                trade.stop_profit_count += 1
                trades.remove(trade)
                return
        now = tafunc.time_to_datetime(day_kline.iloc[-1].datetime)
        if now == datetime.strptime(exhibition_times[0], '%Y-%m-%d'):
            # 变换主力合约前一日尾盘清仓
            for trade in trades:
                if trade.direction == Direction.LONG:
                    trySendOrder(trade.pos, 0, 1)
                elif trade.direction == Direction.SHORT:
                    trySendOrder(trade.pos, 0, -1)
            trades.clear()
            sym = time2symbol[exhibition_times[0]]
            position = api.get_position(sym)
            exhibition_times.pop(0)
            sleep_days = 7
            return
    for trade in trades[::-1]:
        # if trade.stop_profit_count == 1 and trade.direction == Direction.LONG and last_price < day_kline.iloc[-2].low:
        #     # 第二轮减仓
        #     trySendOrder(trade.pos / 2, 0, 1)
        #     log(f'订单{trade.open_time}以价格{last_price}第二次减仓{trade.pos / 2}')
        #     trade.pos -= trade.pos / 2
        #     trade.stop_profit_count += 1
        # elif trade.stop_profit_count == 1 and trade.direction == Direction.SHORT and last_price > day_kline.iloc[-2].high:
        #     trySendOrder(trade.pos / 2, 0, -1)
        #     log(f'订单{trade.open_time}以价格{last_price}第二次减仓{trade.pos / 2}')
        #     trade.pos -= trade.pos / 2
        #     trade.stop_profit_count += 1
        if trade.direction == Direction.LONG and last_price < trade.stop_loss_price:
            # 止损
            trySendOrder(trade.pos, 0, 1)
            log(f'订单{trade.open_time}以价格{last_price}止损{trade.pos}')
            trade.pos = 0
            trades.remove(trade)
        elif trade.direction == Direction.SHORT and last_price > trade.stop_loss_price:
            trySendOrder(trade.pos, 0, -1)
            log(f'订单{trade.open_time}以价格{last_price}止损{trade.pos}')
            trade.pos = 0
            trades.remove(trade)
        elif trade.stop_loss_price != trade.open_price and trade.direction == Direction.LONG and day_kline.iloc[-2].close > trade.balance_price and day_kline.iloc[-2].low > trade.open_price:
            # 设置盈亏平衡价
            trade.stop_loss_price = trade.open_price
            log(f'订单{trade.open_time}以开仓价格{trade.open_price}作为止损价')
        elif trade.stop_loss_price != trade.open_price and trade.direction == Direction.SHORT and day_kline.iloc[-2].close < trade.balance_price and day_kline.iloc[-2].high < trade.open_price:
            trade.stop_loss_price = trade.open_price
            log(f'订单{trade.open_time}以开仓价格{trade.open_price}作为止损价')
    return 1

# -----------------控制仓位发送委托单-----------------#
def trySendOrder(volume, KC, PC):
    global long_entry_price, short_entry_price, wait_period, max_profit, last_kline_form, traded
    last_price = small_kline.iloc[-1].close
    # 平仓发单
    if (PC == 1):
        if (position.pos_long > 0 and position.pos_long >= volume):
            api.insert_order(sym, "SELL", "CLOSETODAY", volume, limit_price=None)
            log(f"交易:平多, price:{last_price}")
        else:
            log(f'没有足够的仓位{position.pos_long}可以平{volume}')
    elif (PC == -1):
        if (position.pos_short > 0 and position.pos_short >= volume):
            api.insert_order(sym, "BUY", "CLOSETODAY", volume, limit_price=None)
            log(f"交易:平空, price:{last_price}")
        else:
            log(f'没有足够的仓位{position.pos_short}可以平{volume}')
    # 开仓发单
    if (KC == 1):
        api.insert_order(sym, "BUY", "OPEN", volume, limit_price=None)
        log(f"交易:开多, price:{last_price}")
    elif (KC == -1):
        api.insert_order(sym, "SELL", "OPEN", volume, limit_price=None)
        log(f"交易:开空, price:{last_price}")
    return 1


# --------------------处理时间函数------------------#
def can_time(hour, minute):
    hour = str(hour)
    minute = str(minute)
    if len(minute) == 1:
        minute = "0" + minute
    return int(hour + minute)

while True:
    try:
        api.wait_update()
        if api.is_changing(day_kline.iloc[-1], ["volume"]) and day_kline.iloc[-1].volume > 0:
            tq_now = tafunc.time_to_datetime(day_kline.iloc[-1].datetime)
            # 核心模块
            price_logger.debug(f'天勤k线:time:{tq_now}, open:{day_kline.iloc[-1].open}, close:{day_kline.iloc[-1].close}, high:{day_kline.iloc[-1].high}, low:{day_kline.iloc[-1].low}, volume:{day_kline.iloc[-1].volume}')
            CoreModule()
            if sleep_days > 0:
                log(f'刚换主力合约休息中{sleep_days}')
                sleep_days -= 1
        elif api.is_changing(small_kline.iloc[-1], ["volume"]) and small_kline.iloc[-1].volume > 0:
            if sleep_days > 0:
                continue
            cal_open_signal()
            trade_mode()
        elif api.is_changing(week_kline.iloc[-1]):
            confirm_main_trend()
    except BacktestFinished as e:
        pass
