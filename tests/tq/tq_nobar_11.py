from datetime import date, datetime, timedelta
from tqsdk import TqApi, TqReplay, TqBacktest, TqSim, BacktestFinished, tafunc, ta
from vnpy.trader.util_logger import setup_logger
import logging
import os
from dateutil import parser
from enum import Enum
from typing import Dict, List, Set, Callable
import numpy as np
import talib
from vnpy.trader.utility import BarData, TickData, extract_vt_symbol, get_trading_date

class LoseHeapType(Enum):
    """
    Direction of order/trade/position.
    """
    SUPPLY = "供给失衡"
    DEMAND = "需求失衡"

class LosebalanceHeap(object):

    min_price: float = 0
    max_price: float = 0
    level: int = 0
    type: LoseHeapType = LoseHeapType.DEMAND

    def __init__(self, min_price: float, max_price: float, level: int, type: LoseHeapType):
        self.min_price: float = min_price
        self.max_price: float = max_price
        self.level: int = level
        self.type: LoseHeapType = type

class KlineType(Enum):
    """
    Direction of order/trade/position.
    """
    TOP = "顶分型"
    BOTTOM = "底分型"

class KlineForm(object):

    stop_price: float = 0
    type: LoseHeapType = LoseHeapType.DEMAND
    timestamp: int

    def __init__(self, stop_price: float, type: KlineType, timestamp: int):
        self.stop_price: float = stop_price
        self.type: KlineType = type
        self.timestamp = timestamp

os.path.dirname(__file__)
logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'in_day_trade'),
                           name='trade_pp',
                           log_level=logging.DEBUG,
                           backtesing=True)
price_logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'in_day_price'),
                      name='price_pp',
                      log_level=logging.DEBUG,
                      backtesing=True)

# 参数14505
XVol = 1000  # 有效成交量
N = 3  # 主买主卖失衡比率
X = 25  # 顶底微单判断倍数
losebalance_limit = 3  # 连续失衡的次数，即堆积
lots = 2  # 下单手数
TN = 600  # 默认120个TICK更新一次模块，即1分钟
NT = 1  # 默认1是正常时间，2是凌晨2.30
opening_profit_tick = 15  # 止盈跳
profit_tick = 20  # 止盈跳
loss_tick = 15  # 止损跳
open_stop_tick = 10  # 开仓止损跳数限制
trade_period = 3 #单位分钟
max_wait_period = 6
time_limit = 3 # 距离分型k线根数

# 全局变量
runCoreModule = 0
is_trading = 0
total_ask_vol = 0
total_buy_vol = 0
sumA = 0
sumB = 0
D_high = 0
D_Low = 0
poc_vol = 0
Aprice = set()
ask_price2vol = {}
Bprice = set()
bid_price2vol = {}
A_lastP = []
B_lastP = []
heap_flag = 0
PG = 0
PGG = 0
PNN = 0
Up_Aprice = 0
Dn_Bprice = 0
total_vol = 0
red_or_green = 0
poc_price = 0
last_poc_price = 0
Max_price = 0
Min_price = 0
M_minprice = 0
W_maxprice = 0
poc_Ask = 0
poc_Bid = 0
long_entry_price = 0
short_entry_price = 0
demand_heap: List[LosebalanceHeap] = []
supply_heap: List[LosebalanceHeap] = []
long_stop_price: float = 0
short_stop_price: float = 0
open_signal = 0
stop_signal = 0
wait_period = 0
condition_open_long_price = 0
condition_open_short_price = 0
kline_length = 45
consum_delta = 0
current_trend = 0
last_kline_form: KlineForm = None
current_kline_form: KlineForm = None
shock_upper_limit = 0
shock_down_limit = 0
profit_time_left = 1

# 默认
# api = TqApi(backtest=TqReprofit_tickay(date(2020, 9, 18)), auth="songshu123,7088044")
api = TqApi(TqSim(10000), backtest=TqBacktest(start_dt=date(2020, 11, 1), end_dt=date(2020, 11, 30)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(20000), backtest=TqBacktest(start_dt=datetime(2020, 11, 2, 21, 0), end_dt=datetime(2020, 11, 2, 22, 0)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(100000), backtest=TqReprofit_tickay(reprofit_tickay_dt=date(2020, 9, 9)), web_gui=True, auth="xukaide77,xk82513994")
sym = "DCE.pp2101"
# sym = 'CFFEX.IC2011'6509
ex, sy = sym.split('.')
min5_kline = api.get_kline_serial(sym, trade_period * 60, data_length=kline_length)
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
    global total_ask_vol, total_buy_vol, sumA, sumB, D_high, D_Low, poc_vol, heap_flag, consum_delta, last_kline_form, current_kline_form, last_poc_price
    global heap_flag, Up_Aprice, Dn_Bprice, total_vol, poc_price, PG, PN, Max_price, Min_price, poc_Ask, poc_Bid, red_or_green, delta
    lx = 0
    sx = 0
    demand_heap.clear()
    supply_heap.clear()

    print("//***********核心模块开始计算***********//")
    # 时间
    dt = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime)
    # 总成交量计算
    total_vol = total_ask_vol + total_buy_vol
    if total_vol == 0:
        return
    # 主动买卖Delta
    delta = total_ask_vol - total_buy_vol
    kline_delta[:-1] = kline_delta[1:]
    kline_delta[-1] = delta

    # if total_vol < XVol:
    #     log("成交量小于阈值，计算但不允许交易")

    # 判断上一根是阳线还是阴线
    # red_or_green=IIF(C[1]>O[1],1,-1)
    red_or_green = 0 if min5_kline.iloc[-1].close == min5_kline.iloc[-1].open else (1 if min5_kline.iloc[-1].close > min5_kline.iloc[-1].open else -1)
    # 斜对角计算(核心)
    A_lastP = list(Aprice)
    A_lastP.sort(reverse=True)
    B_lastP = list(Bprice)
    B_lastP.sort(reverse=True)
    all_price = list(Aprice | Bprice)
    all_price.sort(reverse=True)
    D_high = all_price[0]
    D_Low = all_price[len(all_price) - 1]
    i_Max = len(all_price)
    # 核心条件计算
    demand_losebalance_count = 0
    supply_losebalance_count = 0
    max_losebalance_price = 0
    min_losebalance_price = 0
    volume_list = []
    bid_volume_list = []
    ask_volume_list = []
    for i in range(i_Max):
        ask_vol = ask_price2vol.get(str(all_price[i]), 0)
        buy_vol = bid_price2vol.get(str(all_price[i]), 0)
        price_logger.debug(f"[{dt}], i ={i}")
        price_logger.debug(f"主动买 {all_price[i]} = {ask_vol}")
        price_logger.debug(f"主动卖 {all_price[i]} = {buy_vol}")
        # # 同价位需求失衡
        # demand_losebalance = ask_vol > buy_vol * N and A_lastP[i] >= B_lastP[i]
        # # 同价位供给失衡
        # supply_losebalance = buy_vol > ask_vol * N and A_lastP[i] >= B_lastP[i]
        # 斜对角需求失衡
        demand_losebalance = i < i_Max - 1 and ask_vol > bid_price2vol.get(str(all_price[i + 1]), 0) * N
        # 斜对角供给失衡
        supply_losebalance = i > 0 and buy_vol > ask_price2vol.get(str(all_price[i - 1]), 0) * N

        # 需求堆积
        if demand_losebalance:
            if supply_losebalance_count >= losebalance_limit:
                # Commentary("供给堆积出现")
                heap = LosebalanceHeap(min_losebalance_price, max_losebalance_price, supply_losebalance_count, LoseHeapType.SUPPLY)
                supply_heap.append(heap)
            supply_losebalance_count = 0
            demand_losebalance_count = demand_losebalance_count + 1
            if demand_losebalance_count == 1:
                max_losebalance_price = all_price[i]
            min_losebalance_price = all_price[i]
        elif supply_losebalance:
            if demand_losebalance_count >= losebalance_limit:
                # Commentary("需求堆积出现")
                heap = LosebalanceHeap(min_losebalance_price, max_losebalance_price, demand_losebalance_count, LoseHeapType.DEMAND)
                demand_heap.append(heap)
            demand_losebalance_count = 0
            supply_losebalance_count = supply_losebalance_count + 1
            if supply_losebalance_count == 1:
                max_losebalance_price = all_price[i]
            min_losebalance_price = all_price[i]
        else:
            # Commentary("需求堆积失败，连续需求失衡的累计次数："+Text(demand_losebalance_count))
            if demand_losebalance_count >= losebalance_limit:
                # Commentary("需求堆积出现")
                heap = LosebalanceHeap(min_losebalance_price, max_losebalance_price, demand_losebalance_count, LoseHeapType.DEMAND)
                demand_heap.append(heap)
            # 供给堆积
            elif supply_losebalance_count >= losebalance_limit:
                # Commentary("供给堆积出现")
                heap = LosebalanceHeap(min_losebalance_price, max_losebalance_price, supply_losebalance_count, LoseHeapType.SUPPLY)
                supply_heap.append(heap)
            demand_losebalance_count = 0
            supply_losebalance_count = 0
            min_losebalance_price = 0
            max_losebalance_price = 0

        # POC算法
        price_total_vol = ask_vol + buy_vol
        volume_list.append(price_total_vol)
        bid_volume_list.append(buy_vol)
        ask_volume_list.append(ask_vol)
        if price_total_vol > poc_vol:
            # 每个BAR的POC数据
            poc_vol = price_total_vol
            poc_price = all_price[i]

    volume_list.sort(reverse=True)
    bid_volume_list.sort(reverse=True)
    ask_volume_list.sort(reverse=True)
    median_volume = volume_list[round(len(volume_list) * 0.5)]
    big_bid_volume = volume_list[round(len(bid_volume_list) * 0.25)]
    big_ask_volume = volume_list[round(len(ask_volume_list) * 0.25)]
    tiny_flag = 0

    price_logger.debug(f'time:{dt}, delta:{delta}, poc_price:{poc_price}, poc_volume:{poc_vol}, median_vol:{median_volume}, buy:{total_ask_vol}, sell:{total_buy_vol}, total:{total_vol}, 最新价:{ticks.iloc[-1].last_price}')
    if last_poc_price > 0:
        # if poc_vol < 2 * median_volume:
        #     api.draw_line(min5_kline, x1=-2, y1=last_poc_price, x2=-1,  y2=last_poc_price, line_type='SEG', color='green')
        #     # 无效poc_price
        #     poc_price = 0
        # else:
        api.draw_line(min5_kline, x1=-2, y1=last_poc_price, x2=-1,  y2=last_poc_price, line_type='SEG', color='black')

    # consum_delta
    # consum_delta += delta
    # min5_kline["consum_B2"] = consum_delta
    # min5_kline["consum_B2.board"] = "B2"  # 设置附图: 可以设置任意字符串,同一字符串表示同一副图
    # min5_kline["consum_B2.color"] = "green"  # 设置为绿色. 以下设置颜色方式都可行: "green", "#00FF00", "rgb(0,255,0)", "rgba(0,125,0,0.5)"

    # ema3_delta
    # ema3_delta = talib.EMA(kline_delta, 3)[-1]
    # min5_kline["consum_B2"] = ema3_delta
    # min5_kline["consum_B2.board"] = "B2"
    # min5_kline["consum_B2.color"] = "green"

    # ema5_delta = talib.EMA(kline_delta, 5)[-1]
    # min5_kline["consum_B3"] = ema5_delta
    # min5_kline["consum_B3.board"] = "B3"
    # min5_kline["consum_B3.color"] = "green"

    # ema10_delta = talib.EMA(kline_delta, 10)[-1]
    # min5_kline["consum_B4"] = ema10_delta
    # min5_kline["consum_B4.board"] = "B4"
    # min5_kline["consum_B4.color"] = "green"

    # 画图
    api.draw_text(min5_kline, str(int(delta)), x=-1, y=min5_kline.iloc[-1].high + 4 * price_tick, color='red' if delta > 0 else 'green')
    api.draw_text(min5_kline, str(int(total_vol)), x=-1, y=min5_kline.iloc[-1].low - 7 * price_tick, color='blue')

    if len(demand_heap) > 0:
        for heap in demand_heap:
            price_logger.debug(f'demand_heap:min:{heap.min_price}, max:{heap.max_price}, level:{heap.level}')
            for heap_price in np.arange(heap.min_price, heap.max_price + price_tick, price_tick):
                api.draw_line(min5_kline, x1=-2, y1=heap_price, x2=-1,  y2=heap_price, line_type='SEG', color='red')

    if len(supply_heap) > 0:
        for heap in supply_heap:
            price_logger.debug(f'supply_heap:min:{heap.min_price}, max:{heap.max_price}, level:{heap.level}')
            for heap_price in np.arange(heap.min_price, heap.max_price + price_tick, price_tick):
                api.draw_line(min5_kline, x1=-2, y1=heap_price, x2=-1,  y2=heap_price, line_type='SEG', color='blue')

    cal_open_signal(delta, poc_price, red_or_green, tiny_flag)
    cal_stop_signal(delta, poc_price, red_or_green)
    # cal_opening_open_signal(delta, poc_price, red_or_green)
    # cal_opening_stop_signal(delta, poc_price, red_or_green)


    # 删除原始积累数据，每次循环重新计算：
    i_Max = 0
    sumB = 0
    sumA = 0
    A_lastP.clear()
    B_lastP.clear()
    ask_price2vol.clear()
    bid_price2vol.clear()
    Aprice.clear()
    Bprice.clear()
    all_price.clear()
    total_ask_vol = 0
    total_buy_vol = 0
    ask_vol = 0
    buy_vol = 0
    last_poc_price = poc_price
    poc_price = 0
    poc_vol = 0
    last_kline_form = current_kline_form
    current_kline_form = None
    print("/------------------/")
    return 1


def cal_open_signal(delta, poc_price, red_or_green, tiny_flag):
    global long_stop_price, short_stop_price, open_signal, wait_period, current_trend, current_kline_form, shock_upper_limit, shock_down_limit, profit_time_left
    # 画一次指标线
    ma5 = ta.MA(min5_kline, 5)
    ma10 = ta.MA(min5_kline, 10)
    ma20 = ta.MA(min5_kline, 20)  # 使用 tqsdk 自带指标函数计算均线
    ma40 = ta.MA(min5_kline, 40)
    # min5_kline["ma5_MAIN"] = ma5.ma
    # min5_kline["ma10_MAIN"] = ma10.ma
    min5_kline["ma20_MAIN"] = ma20.ma
    # min5_kline["ma40_MAIN"] = ma40.ma
    # min5_kline['ma5_MAIN.color'] = 'white'
    # min5_kline['ma10_MAIN.color'] = 'yellow'
    min5_kline['ma20_MAIN.color'] = 'purple'
    # min5_kline['ma40_MAIN.color'] = 'green'

    # 委托时间
    now_datetime = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime) + timedelta(minutes=trade_period)
    # now_datetime = parser.parse(now)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute
    if (now_hour == 9 and now_minute == 30) or (now_hour == 21 and now_minute == 30):
        # 前分钟不开仓
        if ma5.ma.iloc[-1] > ma10.ma.iloc[-1] > ma20.ma.iloc[-1] > ma40.ma.iloc[-1]:
            # 向上趋势
            current_trend = 1
        elif ma5.ma.iloc[-1] < ma10.ma.iloc[-1] < ma20.ma.iloc[-1] < ma40.ma.iloc[-1]:
            # 向下趋势
            current_trend = -1
        else:
            # 震荡
            current_trend = 0
            shock_upper_limit = max(min5_kline.close.iloc[-10:].max(), min5_kline.open.iloc[-10:].max())
            shock_down_limit = min(min5_kline.close.iloc[-10:].min(), min5_kline.open.iloc[-10:].min())
            log(f'开盘震荡,上限:{shock_upper_limit},下限:{shock_down_limit}')
        return 0
    int_time = can_time(now_hour, now_minute)
    if (now_hour == 9 and now_minute <= 30) or (now_hour == 13 and now_minute <= 40) or (now_hour == 21 and now_minute <= 30) or (now_hour == 11 and now_minute >= 15) or (1330 <= int_time < 1459) or (2245 <= int_time < 2259):
        # 前分钟不开仓
        return 0
    if profit_time_left < 1:
        return

    latest_price_datetime = tafunc.time_to_datetime(min5_kline.iloc[-1].datetime)
    # now_datetime = parser.parse(now)
    price_int_time = can_time(latest_price_datetime.hour, latest_price_datetime.minute)
    kline_form = top_or_bottom(price_int_time)
    current_kline_form = kline_form
    if current_trend == 1 and ticks.iloc[-1].bid_price1 > ma20.ma.iloc[-1]:
        if kline_form is not None and kline_form.type == KlineType.BOTTOM:
            stop_condition = (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < kline_form.stop_price or (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < ma20.ma.iloc[-1]
            log(f'current_trend:{current_trend},stop_condition:{stop_condition},kline_form.stop_price:{kline_form.stop_price},ma:{ma20.ma.iloc[-1]}')
            if stop_condition:
                open_signal = 1
                log('上涨趋势设置开多仓止损位至{}'.format(kline_form.stop_price))
                long_stop_price = kline_form.stop_price
                short_stop_price = 0
        elif kline_form is None and last_kline_form is not None and last_kline_form.type == KlineType.BOTTOM:
            time_condition = last_kline_form.timestamp + trade_period * time_limit > price_int_time
            stop_condition = (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < ma20.ma.iloc[-1]
            log(f'current_trend:{current_trend},time_condition:{time_condition},stop_condition:{stop_condition},ma20:{ma20.ma.iloc[-1]}')
            if time_condition and stop_condition:
                stop_price = ticks.iloc[-1].ask_price1 - loss_tick * price_tick
                open_signal = 1
                log('上涨趋势设置开多仓止损位至{}'.format(stop_price))
                long_stop_price = stop_price
                short_stop_price = 0
    elif current_trend == -1 and ticks.iloc[-1].ask_price1 < ma20.ma.iloc[-1]:
        if kline_form is not None and kline_form.type == KlineType.TOP:
            stop_condition = (ticks.iloc[-1].bid_price1 + loss_tick * price_tick) > kline_form.stop_price or (ticks.iloc[-1].bid_price1 + loss_tick * price_tick) > ma20.ma.iloc[-1]
            log(f'current_trend:{current_trend},stop_condition:{stop_condition},kline_form.stop_price:{kline_form.stop_price},ma20:{ma20.ma.iloc[-1]}')
            if stop_condition:
                open_signal = -1
                log('下跌趋势设置开空仓止损位至{}'.format(kline_form.stop_price))
                short_stop_price = kline_form.stop_price
                long_stop_price = 0
        elif kline_form is None and last_kline_form is not None and last_kline_form.type == KlineType.TOP:
            time_condition = last_kline_form.timestamp + trade_period * time_limit > price_int_time
            stop_condition = (ticks.iloc[-1].bid_price1 + loss_tick * price_tick) > ma20.ma.iloc[-1]
            log(f'current_trend:{current_trend},time_condition:{time_condition},stop_condition:{stop_condition},ma20:{ma20.ma.iloc[-1]}')
            if time_condition and stop_condition:
                stop_price = ticks.iloc[-1].bid_price1 + loss_tick * price_tick
                open_signal = -1
                log('下跌趋势设置开空仓止损位至{}'.format(stop_price))
                short_stop_price = stop_price
                long_stop_price = 0
    elif current_trend == 0:
        if ticks.iloc[-1].bid_price1 > shock_upper_limit and ticks.iloc[-1].bid_price1 > ma20.ma.iloc[-1]:
            if kline_form is not None and kline_form.type == KlineType.BOTTOM:
                stop_condition = (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < kline_form.stop_price or (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < ma20.ma.iloc[-1]
                log(f'current_trend:{current_trend},stop_condition:{stop_condition},kline_form.stop_price:{kline_form.stop_price},ma20:{ma20.ma.iloc[-1]}')
                if stop_condition:
                    open_signal = 1
                    log('震荡突破设置开多仓止损位至{}'.format(kline_form.stop_price))
                    long_stop_price = kline_form.stop_price
                    short_stop_price = 0
            elif kline_form is None and last_kline_form is not None and last_kline_form.type == KlineType.BOTTOM:
                time_condition = last_kline_form.timestamp + trade_period * time_limit > price_int_time
                stop_condition = (ticks.iloc[-1].ask_price1 - loss_tick * price_tick) < ma20.ma.iloc[-1]
                log(f'current_trend:{current_trend},time_condition:{time_condition},stop_condition:{stop_condition},ma20:{ma20.ma.iloc[-1]}')
                if time_condition and stop_condition:
                    stop_price = ticks.iloc[-1].ask_price1 - loss_tick * price_tick
                    open_signal = 1
                    log('震荡突破设置开多仓止损位至{}'.format(stop_price))
                    long_stop_price = stop_price
                    short_stop_price = 0
        elif ticks.iloc[-1].ask_price1 < shock_down_limit and ticks.iloc[-1].ask_price1 < ma20.ma.iloc[-1]:
            if kline_form is not None and kline_form.type == KlineType.TOP:
                stop_condition = (ticks.iloc[-1].bid_price1 + loss_tick * price_tick) > kline_form.stop_price or (ticks.iloc[-1].bid_price1 + loss_tick * price_tick) > ma20.ma.iloc[-1]
                log(f'current_trend:{current_trend},stop_condition:{stop_condition},kline_form.stop_price:{kline_form.stop_price},ma20:{ma20.ma.iloc[-1]}')
                if stop_condition:
                    open_signal = -1
                    log('震荡突破设置开空仓止损位至{}'.format(kline_form.stop_price))
                    short_stop_price = kline_form.stop_price
                    long_stop_price = 0
            elif kline_form is None and last_kline_form is not None and last_kline_form.type == KlineType.TOP:
                time_condition = last_kline_form.timestamp + trade_period * time_limit > price_int_time
                stop_condition = (ticks.iloc[-1].bid_price1 + loss_tick * price_tick) > ma20.ma.iloc[-1]
                log(f'current_trend:{current_trend},time_condition:{time_condition},stop_condition:{stop_condition},ma20:{ma20.ma.iloc[-1]}')
                if time_condition and stop_condition:
                    stop_price = ticks.iloc[-1].bid_price1 + loss_tick * price_tick
                    open_signal = -1
                    log('震荡突破设置开空仓止损位至{}'.format(stop_price))
                    short_stop_price = stop_price
                    long_stop_price = 0
        elif shock_down_limit < ticks.iloc[-1].last_price < shock_upper_limit:
            if kline_form is not None and kline_form.type == KlineType.BOTTOM:
                stop_condition = (ticks.iloc[-1].ask_price1 - 0.75 * loss_tick * price_tick) < kline_form.stop_price
                log(f'current_trend:{current_trend},stop_condition:{stop_condition},kline_form.stop_price:{kline_form.stop_price}')
                if stop_condition:
                    open_signal = 1
                    log('震荡区间设置开多仓止损位至{}'.format(kline_form.stop_price))
                    long_stop_price = kline_form.stop_price
                    short_stop_price = 0
            elif kline_form is not None and kline_form.type == KlineType.TOP:
                stop_condition = (ticks.iloc[-1].bid_price1 + 0.5 * loss_tick * price_tick) > kline_form.stop_price
                log(f'current_trend:{current_trend},stop_condition:{stop_condition},kline_form.stop_price:{kline_form.stop_price}')
                if stop_condition:
                    open_signal = -1
                    log('震荡区间设置开空仓止损位至{}'.format(kline_form.stop_price))
                    short_stop_price = kline_form.stop_price
                    long_stop_price = 0


def top_or_bottom(int_time):
    if abs(min5_kline.iloc[-1].open - min5_kline.iloc[-1].close) < 3 * price_tick:
        return None
    first_offset = 0
    second_offset = 0
    for i in range(2, len(min5_kline)):
        if abs(min5_kline.iloc[-i].open - min5_kline.iloc[-i].close) < 2 * price_tick:
            first_offset += 1
            continue
        break
    for i in range(3 + first_offset, len(min5_kline)):
        if abs(min5_kline.iloc[-i].open - min5_kline.iloc[-i].close) < 2 * price_tick:
            second_offset += 1
            continue
        break
    # min5_kline.drop(min5_kline[abs(min5_kline.close - min5_kline.open) < 3 * price_tick].index)
    last_index = -1
    middle_index = -2-first_offset
    first_index = -3-first_offset-second_offset
    top_condition1 = min5_kline.iloc[last_index].open > min5_kline.iloc[last_index].close and min5_kline.iloc[middle_index].close < min5_kline.iloc[middle_index].open and (min5_kline.iloc[first_index].close - min5_kline.iloc[first_index].open) >= 0 and min5_kline.iloc[first_index].close > min5_kline.iloc[-5-first_offset-second_offset:first_index].close.max()
    top_condition2 = min5_kline.iloc[last_index].open > min5_kline.iloc[last_index].close and (min5_kline.iloc[middle_index].close - min5_kline.iloc[middle_index].open) >= 0 and min5_kline.iloc[middle_index].close > min5_kline.iloc[-4-first_offset:middle_index].close.max() and min5_kline.iloc[last_index].close < min([min5_kline.iloc[first_index].open, min5_kline.iloc[first_index].close, min5_kline.iloc[middle_index].open])
    if top_condition1:
        stop_price = min5_kline.iloc[last_index].open if min5_kline.iloc[last_index].close + 0.7 * loss_tick * price_tick < min5_kline.iloc[last_index].open else min5_kline.iloc[middle_index].open
        api.draw_text(min5_kline, '顶', x=first_index, y=min5_kline.iloc[first_index].high + 8 * price_tick, color='red')
        return KlineForm(stop_price, KlineType.TOP, int_time)
    elif top_condition2:
        stop_price = min5_kline.iloc[middle_index].close
        api.draw_text(min5_kline, '顶', x=middle_index, y=min5_kline.iloc[middle_index].high + 8 * price_tick, color='red')
        return KlineForm(stop_price, KlineType.TOP, int_time)
    bottom_condition1 = min5_kline.iloc[last_index].close > min5_kline.iloc[last_index].open and min5_kline.iloc[middle_index].close > min5_kline.iloc[middle_index].open and min5_kline.iloc[first_index].open - min5_kline.iloc[first_index].close >= 0 and min5_kline.iloc[first_index].close < min5_kline.iloc[-5-first_offset-second_offset:first_index].close.min()
    bottom_condition2 = min5_kline.iloc[last_index].close > min5_kline.iloc[last_index].open and min5_kline.iloc[middle_index].open - min5_kline.iloc[middle_index].close >= 0 and min5_kline.iloc[middle_index].close < min5_kline.iloc[-4-first_offset:middle_index].close.min() and min5_kline.iloc[last_index].close > max([min5_kline.iloc[first_index].open, min5_kline.iloc[first_index].close, min5_kline.iloc[middle_index].open])
    if bottom_condition1:
        stop_price = min5_kline.iloc[last_index].open if min5_kline.iloc[last_index].close - 0.7 * loss_tick * price_tick > min5_kline.iloc[last_index].open else min5_kline.iloc[middle_index].open
        api.draw_text(min5_kline, '底', x=first_index, y=min5_kline.iloc[first_index].low - 11 * price_tick, color='blue')
        return KlineForm(stop_price, KlineType.BOTTOM, int_time)
    elif bottom_condition2:
        stop_price = min5_kline.iloc[middle_index].close
        api.draw_text(min5_kline, '底', x=middle_index, y=min5_kline.iloc[middle_index].low - 11 * price_tick, color='blue')
        return KlineForm(stop_price, KlineType.BOTTOM, int_time)
    return None


def cal_stop_signal(delta, poc_price, red_or_green):
    global stop_signal, long_stop_price, short_stop_price, wait_period, open_signal, current_trend, current_kline_form, last_kline_form
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

    ma20 = ta.MA(min5_kline, 20).ma.iloc[-1]
    if long_entry_price > 0:
        if current_kline_form is not None and current_kline_form.type == KlineType.TOP:
            stop_signal = 1
            log('做多碰到顶分型止盈')
        elif min5_kline.iloc[-1].close - min5_kline.iloc[-1].open > 0.7 * loss_tick * price_tick:
            long_stop_price = min5_kline.iloc[-1].open
            log(f'做多调整止损价{long_stop_price}')
        elif min5_kline.iloc[-1].close > min5_kline.iloc[-1].open and min5_kline.iloc[-2].close > min5_kline.iloc[-2].open:
            long_stop_price = min5_kline.iloc[-2].open
            log(f'做多调整止损价{long_stop_price}')
        elif not (current_trend == 0 and shock_down_limit < long_entry_price < shock_upper_limit) and min5_kline.iloc[-1].close < ma20:
            stop_signal = 1
            log('做多突破MA20止盈')
    elif short_entry_price > 0:
        if current_kline_form is not None and current_kline_form.type == KlineType.BOTTOM:
            stop_signal = -1
            log('做空碰到底分型止盈')
        elif min5_kline.iloc[-1].open - min5_kline.iloc[-1].close > 0.7 * loss_tick * price_tick:
            short_stop_price = min5_kline.iloc[-1].open
            log(f'做空调整止损价{short_stop_price}')
        elif min5_kline.iloc[-1].close < min5_kline.iloc[-1].open and min5_kline.iloc[-2].close < min5_kline.iloc[-2].open:
            short_stop_price = min5_kline.iloc[-2].open
            log(f'做空调整止损价{short_stop_price}')
        elif not (current_trend == 0 and shock_down_limit < short_entry_price < shock_upper_limit) and min5_kline.iloc[-1].close > ma20:
            stop_signal = -1
            log('做空突破MA20止盈')

def target_profit_stop():
    global stop_signal, long_stop_price, short_stop_price, wait_period, open_signal, current_trend, current_kline_form
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
    if current_trend == 0 and shock_down_limit < ticks.iloc[-1].last_price < shock_upper_limit:
        if long_entry_price > 0 and ticks.iloc[-1].bid_price1 >= long_entry_price + 0.5 * profit_tick * price_tick:
            stop_signal = 1
        elif short_entry_price > 0 and ticks.iloc[-1].ask_price1 <= short_entry_price - 0.5 * profit_tick * price_tick:
            stop_signal = -1
    else:
        if long_entry_price > 0 and ticks.iloc[-1].bid_price1 >= long_entry_price + profit_tick * price_tick:
            stop_signal = 1
        elif short_entry_price > 0 and ticks.iloc[-1].ask_price1 <= short_entry_price - profit_tick * price_tick:
            stop_signal = -1


def cal_opening_open_signal(delta, poc_price, red_or_green):
    global long_stop_price, short_stop_price, open_signal
    # 委托时间
    now_datetime = tafunc.time_to_datetime(ticks.iloc[-1].datetime)
    # now_datetime = parser.parse(now)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute
    if (now_hour == 9 and now_minute <= 4) or (now_hour == 13 and now_minute <= 4) or (now_hour == 21 and now_minute <= 4):
        # 前3分钟不开仓
        return 0
    main_long_condition = red_or_green >= 0 and delta > 0
    main_short_condition = red_or_green <= 0 and delta < 0


def cal_opening_stop_signal(delta, poc_price, red_or_green):
    global stop_signal, long_stop_price, short_stop_price, open_signal
    # 已有开仓信号
    if open_signal > 0 and long_entry_price > 0:
        open_signal = 0
        log(f'已开多仓,不计算止盈')
        return
    elif open_signal < 0 and short_entry_price > 0:
        open_signal = 0
        log(f'已开空仓,不计算止盈')
        return
    if long_entry_price == 0 and short_entry_price == 0:
        log(f'开盘时无开仓,不计算止盈')
        return



class TickType(Enum):
    BUY = '主动买'
    SELL = '主动卖'
    UNKNOWN = '未知'

# ---------------------数据录入----------------------#
def is_contain_price(price1, price2, target_price):
    return price1 >= target_price >= price2 or price2 >= target_price >= price1

def cal_red_or_green(ticks, last_price) -> TickType:
    if is_contain_price(ticks.iloc[-2].bid_price1, ticks.iloc[-1].bid_price1, last_price) and not (is_contain_price(ticks.iloc[-2].ask_price1, ticks.iloc[-1].ask_price1, last_price)):
        return TickType.SELL
    elif (not is_contain_price(ticks.iloc[-2].bid_price1, ticks.iloc[-1].bid_price1, last_price)) and is_contain_price(ticks.iloc[-2].ask_price1, ticks.iloc[-1].ask_price1, last_price):
        return TickType.BUY
    elif is_contain_price(ticks.iloc[-2].bid_price1, ticks.iloc[-1].bid_price1, last_price) and is_contain_price(ticks.iloc[-2].ask_price1, ticks.iloc[-1].ask_price1, last_price):
        return TickType.SELL if ticks.iloc[-2].bid_price1 >= ticks.iloc[-1].bid_price1 else TickType.BUY
    else:
        bid_diff = abs(ticks.iloc[-1].bid_price1 - last_price)
        ask_diff = abs(ticks.iloc[-1].ask_price1 - last_price)
        if bid_diff < ask_diff:
            return TickType.SELL
        elif bid_diff > ask_diff:
            return TickType.BUY
        else:
            if ticks.iloc[-1].bid_volume1 < ticks.iloc[-1].ask_volume1:
                return TickType.SELL
            elif ticks.iloc[-1].bid_volume1 > ticks.iloc[-1].ask_volume1:
                return TickType.BUY
            else:
                price_logger.warning(f'无法判断tick方向 {tafunc.time_to_datetime(ticks.iloc[-1].datetime)} last_price:{last_price}, tick_ask_price:{ticks.iloc[-1].ask_price1}, tick_bid_price:{ticks.iloc[-1].bid_price1}')
                return TickType.UNKNOWN

def loadData():
    global total_ask_vol, total_buy_vol, sumA, sumB
    SS_vol = 0
    temp_price = ticks.iloc[-1].last_price
    tick_ask_price = ticks.iloc[-1].ask_price1
    tick_bid_price = ticks.iloc[-1].bid_price1
    tick_volume = ticks.iloc[-1].volume - ticks.iloc[-2].volume
    dt = tafunc.time_to_datetime(ticks.iloc[-1].datetime)

    if temp_price is None or not temp_price > 0:
        price_logger.warn(f'{dt} 无法取得最新价 {temp_price}')
        return 0

    # 判断tick方向
    if tick_volume == 0:
        return
    tick_type = cal_red_or_green(ticks, temp_price)
    if tick_type == TickType.BUY:
        total_ask_vol = total_ask_vol + tick_volume
        Aprice.add(temp_price)
        ask_price2vol[str(temp_price)] = ask_price2vol.get(str(temp_price), 0) + tick_volume
    elif tick_type == TickType.SELL:
        total_buy_vol = total_buy_vol + tick_volume
        Bprice.add(temp_price)
        bid_price2vol[str(temp_price)] = bid_price2vol.get(str(temp_price), 0) + tick_volume
    return 1


# ---------------------交易模块----------------------#
def trade_mode():
    global heap_flag, Up_Aprice, Dn_Bprice, total_vol, poc_price, PG, PGG, PNN, Max_price, Min_price, M_minprice, W_maxprice, D_Low, open_signal, stop_signal, long_stop_price, short_stop_price, condition_open_long_price, condition_open_short_price, profit_time_left
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
    # 条件开仓
    if condition_open_long_price > 0 and ticks.iloc[-1].ask_price1 <= condition_open_long_price:
        log(f'以条件价{condition_open_long_price}开多')
        trySendOrder(lots, 1, -1)
        condition_open_long_price = 0
    elif condition_open_short_price > 0 and ticks.iloc[-1].bid_price1 >= condition_open_short_price:
        log(f'以条件价{condition_open_short_price}开空')
        trySendOrder(lots, -1, 1)
        condition_open_short_price = 0
    # 止损
    if long_stop_price > 0 and last_price < long_stop_price:
        trySendOrder(lots, 0, 1)
        long_stop_price = 0
    elif short_stop_price > 0 and last_price > short_stop_price:
        trySendOrder(lots, 0, -1)
        short_stop_price = 0
    # 止盈
    if stop_signal == 1:
        if bid_price - long_entry_price > 15 * price_tick:
            profit_time_left -= 1
        trySendOrder(lots, 0, 1)
        long_stop_price = 0
        stop_signal = 0
    elif stop_signal == -1:
        if short_entry_price - ask_price > 15 * price_tick:
            profit_time_left -= 1
        trySendOrder(lots, 0, -1)
        short_stop_price = 0
        stop_signal = 0


    # if (heap_flag == 1 and Up_Aprice > 0 and total_vol >= XVol and red_or_green > 0 and poc_price > 0 and delta > 0):
    #     # 满足需求堆积，但是小于POC价格，只平空不开多。
    #     if (last_price >= Up_Aprice and last_price < poc_price):
    #         trySendOrder(lots, 0, -1)
    #         heap_flag = 0
    #         Up_Aprice = 0
    #         logger.info(now + "小于POC价格【需求】堆积出现只平空不开多，平空: " + str(lots) + " 手，平空价格:= " + str(last_price) + ", Up_Aprice:= " + str(Up_Aprice) + " ,heap_flag:= " + str(heap_flag) + " , delta:= " + str(delta))
    #         # 满足需求堆积，同时大于POC价格，开多平空。
    #     if (last_price >= Up_Aprice and last_price > poc_price):
    #         trySendOrder(lots, heap_flag, -1)
    #         heap_flag = 0
    #         Up_Aprice = 0
    #         logger.info(now + "大于POC价格【需求】堆积出现只开多平空，开多: " + str(lots) + " 手，开多价格:= " + str(last_price) + ", Up_Aprice:= " + str(Up_Aprice) + " ,heap_flag:= " + str(heap_flag) + " , delta:= " + str(delta))
    # elif (heap_flag == -1 and Dn_Bprice > 0 and total_vol >= XVol and red_or_green < 0 and poc_price > 0 and delta < 0):
    #     # 满足供给堆积，但是大于POC价格，只平多不开空；
    #     if (last_price <= Dn_Bprice and last_price > poc_price):
    #         trySendOrder(lots, 0, 1)
    #         heap_flag = 0
    #         Dn_Bprice = 0
    #         logger.info(now + "大于POC价格【供给】堆积出现只平多不开空，平多: " + str(lots) + " 手，平多价格:= " + str(last_price) + ", Dn_Bprice:= " + str(Dn_Bprice) + " ,heap_flag:= " + str(heap_flag) + " , delta:= " + str(delta))
    #         # 满足供给堆积，同时小于POC价格，开空平多；
    #     if (last_price <= Dn_Bprice and last_price < poc_price):
    #         trySendOrder(lots, heap_flag, 1)
    #         heap_flag = 0
    #         Dn_Bprice = 0
    #         logger.info(now + "小于POC价格【供给】堆积出现开空平多，开空: " + str(lots) + " 手，开空价格:= " + str(last_price) + ", Dn_Bprice:= " + str(Dn_Bprice) + " ,heap_flag:= " + str(heap_flag) + " , delta:= " + str(delta))
    # if (PG == 1 and delta <= 0 and total_vol >= XVol):
    #     if (Max_price > 0):
    #         logger.info(now + "顶部微单出现,Max_price:= " + str(Max_price))
    #         trySendOrder(lots, 0, PG)
    #         PG = 0
    #         M_minprice = D_Low
    #         logger.info(now + "顶部微单出现，平多: " + str(lots) + " 手，平多价格:= " + str(last_price) + ", Max_price:= " + str(Max_price) + " ,PG:= " + str(PG) + " , delta:= " + str(delta))
    #         if (last_price < poc_price and poc_price > 0):
    #             PGG = -1
    #             print(now + "价格在POC下方，开空准备，价格下限: " + str(M_minprice) + "开空开关PGG:= " + str(PGG))
    #             Max_price = 0
    # if (PGG == -1 and last_price <= M_minprice and M_minprice > 0):
    #     trySendOrder(lots, PGG, 1)
    #     logger.info(now + "开空，跌破顶部bar低价：" + str(M_minprice) + "Text:= " + str(PGG))  # 已有同向持仓，就不再开仓。
    #     PGG = 0
    #     M_minprice = 0
    # if (tiny_flag == -1 and delta >= 0 and total_vol >= XVol):
    #     if (Min_price > 0):
    #         logger.info(now + "底部微单出现,Min_price:= " + str(Min_price))
    #         trySendOrder(lots, 0, tiny_flag)
    #         tiny_flag = 0
    #         W_maxprice = D_high
    #         logger.info(now + "底部微单出现，平空：" + str(lots) + " 手，平空价格:= " + str(last_price) + ", Min_price:= " + str(Min_price) + " ,tiny_flag:= " + str(tiny_flag) + " , delta:= " + str(delta))
    #         if (last_price > poc_price and poc_price > 0):
    #             PNN = 1
    #             # Print("价格在POC上方，开多准备，价格上限: "+Text(W_maxprice)+"开多开关PNN:= "+Text(PNN))
    #             Min_price = 0
    # if (PNN == 1 and last_price >= W_maxprice and W_maxprice > 0):
    #     trySendOrder(lots, PNN, -1)
    #     logger.info(now + "开多，突破底部bar高价：" + str(W_maxprice) + "Text:= " + str(PNN))  # 已有同向持仓，就不再开仓。
    #     PNN = 0
    #     W_maxprice = 0
    return 1


# -----------------控制仓位发送委托单-----------------#
def trySendOrder(volume, KC, PC):
    global long_entry_price, short_entry_price, wait_period
    # 平仓发单
    if (PC == 1):
        if (position.pos_long > 0):
            api.insert_order(sym, "SELL", "CLOSETODAY", position.pos_long, ticks.iloc[-1].bid_price1, advanced="FAK")
            log(f"交易:平多, price:{ticks.iloc[-1].bid_price1}")
            long_entry_price = 0
            wait_period = 0
    elif (PC == -1):
        if (position.pos_short > 0):
            api.insert_order(sym, "BUY", "CLOSETODAY", position.pos_short, ticks.iloc[-1].ask_price1, advanced="FAK")
            log(f"交易:平空, price:{ticks.iloc[-1].ask_price1}")
            short_entry_price = 0
            wait_period = 0
    # 开仓发单
    if (KC == 1):
        api.insert_order(sym, "BUY", "OPEN", volume, ticks.iloc[-1].ask_price1, advanced="FAK")
        log(f"交易:开多, price:{ticks.iloc[-1].ask_price1}")
        long_entry_price = ticks.iloc[-1].ask_price1
        short_entry_price = 0
        wait_period = 1
    elif (KC == -1):
        api.insert_order(sym, "SELL", "OPEN", volume, ticks.iloc[-1].bid_price1, advanced="FAK")
        log(f"交易:开空, price:{ticks.iloc[-1].bid_price1}")
        short_entry_price = ticks.iloc[-1].bid_price1
        long_entry_price = 0
        wait_period = 1
    return 1


# ---------------------收盘清仓---------------------#
def close_and_clearn():
    global is_trading, total_ask_vol, total_buy_vol, sumB, sumA, poc_price, poc_Ask, poc_Bid, poc_vol, condition_open_long_price, condition_open_short_price, current_trend, last_kline_form, current_kline_form, shock_down_limit, shock_upper_limit, last_poc_price, profit_time_left
    if (is_trading == 1):
        trySendOrder(lots, 0, 1)
        trySendOrder(lots, 0, -1)
        is_trading = 0
        # 删除原始积累数据，每次循环重新计算：
        sumB = 0
        sumA = 0
        A_lastP.clear()
        B_lastP.clear()
        ask_price2vol.clear()
        bid_price2vol.clear()
        Aprice.clear()
        Bprice.clear()
        total_ask_vol = 0
        total_buy_vol = 0
        poc_Ask = 0
        poc_Bid = 0
        poc_vol = 0
        poc_price = 0
        stop_signal = 0
        open_signal = 0
        long_stop_price = 0
        short_stop_price = 0
        condition_open_long_price = 0
        condition_open_short_price = 0
        last_poc_price = 0
        profit_time_left = 1
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
    global is_trading, current_trend, shock_upper_limit, shock_down_limit
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
            current_trend = 0
            shock_upper_limit = 0
            shock_down_limit = 0
    elif (NT == 2):
        if ((1458 <= can_time(hour_new, minute_new) < 1500) or (228 <= can_time(hour_new, minute_new) < 230)):  # 凌晨2.30结束
            close_and_clearn()
            current_trend = 0
            shock_upper_limit = 0
            shock_down_limit = 0
    return 1


# --------------------止盈止损------------------#
# def target_profit_stop():
#     # 最新价
#     last_price = ticks.iloc[-1].last_price
#     # 委托时间
#     order_time = ticks.iloc[-1].datetime
#     # 多平
#     sell_cond1 = last_price >= long_entry_price + price_tick * profit_tick and long_entry_price > 0
#     sell_cond2 = last_price <= long_entry_price - price_tick * loss_tick and long_entry_price > 0
#     # 空平
#     cover_cond1 = last_price <= short_entry_price - price_tick * profit_tick and short_entry_price > 0
#     cover_cond2 = last_price >= short_entry_price + price_tick * loss_tick and short_entry_price > 0
#     if position.pos_long > 0:
#         if sell_cond1 == True:
#             trySendOrder(lots, 0, 1)
#             logger.info(order_time + " 多头止盈L，价格：" + str(last_price) + "，数量:= " + str(lots) + "盈利跳数:= " + str(profit_tick))
#         elif sell_cond2 == True:
#             trySendOrder(lots, 0, 1)
#             logger.info(order_time + " 多头止损S，价格：" + str(last_price) + "，数量:= " + str(lots) + "亏损跳数:= " + str(loss_tick))
#     elif position.pos_short > 0:
#         if cover_cond1:
#             trySendOrder(lots, 0, -1)
#             logger.info(order_time + " 空头止盈L，价格：" + str(last_price) + "，数量:= " + str(lots) + "盈利跳数:= " + str(profit_tick))
#         if cover_cond2:
#             trySendOrder(lots, 0, -1)
#             logger.info(order_time + " 空头止损S，价格：" + str(last_price) + "，数量:= " + str(lots) + "亏损跳数:= " + str(loss_tick))
#     return 1

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
            print("/////////5分钟逻辑运行结束/////////")
        if api.is_changing(ticks):
            # 限制交易时间
            Ctrltime()
            # 在交易时间内运行
            if is_trading > 0:
                if is_first_traded == -2:
                    api.insert_order(sym, "BUY", "OPEN", 1, ticks.iloc[-1].ask_price1, advanced="FAK")
                    is_first_traded += 1
                elif is_first_traded == -1:
                    api.insert_order(sym, "SELL", "CLOSETODAY", 1, ticks.iloc[-1].bid_price1, advanced="FAK")
                    is_first_traded += 1
                # 数据录入
                loadData()
        # 交易模块
        target_profit_stop()
        trade_mode()
    except BacktestFinished as e:
        pass
