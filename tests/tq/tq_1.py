from datetime import date, datetime
from tqsdk import TqApi, TqReplay, TqBacktest, TqSim, BacktestFinished, tafunc, ta
from vnpy.trader.util_logger import setup_logger
import logging
import os
from dateutil import parser
from enum import Enum
from typing import Dict, List, Set, Callable
import numpy as np
import talib
from vnpy.trader.utility import BarData, TickData, BarGenerator, ArrayManager, extract_vt_symbol, get_trading_date

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

os.path.dirname(__file__)
logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'order_flow_trade1'),
                           name='trade1',
                           log_level=logging.DEBUG,
                           backtesing=True)
price_logger = setup_logger(file_name=os.path.join(os.path.dirname(__file__), 'order_flow_price1'),
                      name='price1',
                      log_level=logging.DEBUG,
                      backtesing=True)

# 参数11218
XVol = 1000  # 有效成交量
N = 3  # 主买主卖失衡比率
X = 25  # 顶底微单判断倍数
losebalance_limit = 3  # 连续失衡的次数，即堆积
lots = 1  # 下单手数
TN = 600  # 默认120个TICK更新一次模块，即1分钟
NT = 2  # 默认1是正常时间，2是凌晨2.30
opening_profit_tick = 15  # 止盈跳
profit_tick = 20  # 止盈跳
loss_tick = 10  # 止损跳
open_stop_tick = 20  # 开仓止损跳数限制
trade_period = 5 #单位分钟
max_wait_period = 6

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
is_tq_min5_ok = False
last_bar: BarData = None
wait_period = 0
period_min_price = 0
period_max_price = 0

# 默认
# api = TqApi(backtest=TqReprofit_tickay(date(2020, 9, 18)), auth="songshu123,7088044")
api = TqApi(TqSim(20000), backtest=TqBacktest(start_dt=date(2020, 11, 12), end_dt=date(2020, 11, 12)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(20000), backtest=TqBacktest(start_dt=datetime(2020, 11, 2, 21, 0), end_dt=datetime(2020, 11, 2, 22, 0)), web_gui=True, auth="xukaide77,xk82513994")
# api = TqApi(TqSim(100000), backtest=TqReprofit_tickay(reprofit_tickay_dt=date(2020, 9, 9)), web_gui=True, auth="xukaide77,xk82513994")
sym = "SHFE.ag2012"
ex, sy = sym.split('.')
min5_kline = api.get_kline_serial(sym, trade_period * 60, data_length=30)
# 映射交易合约
# SYMBOL = quote.underlying_symbol 2119
order = api.get_order()
position = api.get_position(sym)
# 获得 ag2012 tick序列的引用
ticks = api.get_tick_serial(sym)
# 获得最小变动单位
price_tick = api.get_quote(sym).price_tick
symbol, exchange = extract_vt_symbol(sy + '.' + ex)


def on_bar(bar: BarData):
    bg.update_bar(bar)

def on_5min_bar(bar: BarData):
    global last_bar
    price_logger.debug(f'合成k线:time:{bar.datetime}, open:{bar.open_price}, close:{bar.close_price}, high:{bar.high_price}, low:{bar.low_price}, volume:{bar.volume}')
    last_bar = bar
    pass

def log(msg: str):
    logger.debug('{} {}'.format(tafunc.time_to_datetime(min5_kline.iloc[-2].datetime), msg))

bg = BarGenerator(on_bar, trade_period, on_5min_bar)
am = ArrayManager(10)

# ----------------核心计算模块(每个bar更新一次)--------#
def CoreModule():
    global total_ask_vol, total_buy_vol, sumA, sumB, D_high, D_Low, poc_vol, heap_flag, last_bar, period_min_price, period_max_price
    global heap_flag, Up_Aprice, Dn_Bprice, total_vol, poc_price, PG, PN, Max_price, Min_price, poc_Ask, poc_Bid, red_or_green, delta
    lx = 0
    sx = 0
    demand_heap.clear()
    supply_heap.clear()

    print("//***********核心模块开始计算***********//")
    # 时间
    dt = tafunc.time_to_datetime(min5_kline.iloc[-2].datetime)
    # 总成交量计算
    total_vol = total_ask_vol + total_buy_vol
    # 主动买卖Delta
    delta = total_ask_vol - total_buy_vol
    last_bar.open_interest = delta
    am.update_bar(last_bar)
    period_min_price = min(period_min_price, am.low[-1])
    period_max_price = max(period_max_price, am.high[-1])
    # if total_vol < XVol:
    #     log("成交量小于阈值，计算但不允许交易")

    # 判断上一根是阳线还是阴线
    # red_or_green=IIF(C[1]>O[1],1,-1)
    red_or_green = 0 if min5_kline.iloc[-2].close == min5_kline.iloc[-2].open else (1 if min5_kline.iloc[-2].close > min5_kline.iloc[-2].open else -1)
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
    top_tiny = 0
    bottom_tiny = 0
    if len(A_lastP) >= 4 and len(B_lastP) >= 4:
        # 顶部微单
        top1_vol = ask_price2vol.get(str(A_lastP[0]), 0)
        top2_vol = ask_price2vol.get(str(A_lastP[1]), 0)
        top_tiny = round(top2_vol / top1_vol)
        tiny_top_condition = top1_vol * X < top2_vol and top2_vol > big_ask_volume and top1_vol > 0 and top2_vol > 0
        if tiny_top_condition:
            tiny_flag = -1
        # 底部微单
        bottom1_vol = bid_price2vol.get(str(B_lastP[len(B_lastP) - 1]), 0)
        bottom2_vol = bid_price2vol.get(str(B_lastP[len(B_lastP) - 2]), 0)
        bottom_tiny = round(bottom2_vol / bottom1_vol)
        tiny_bottom_condition = bottom1_vol * X < bottom2_vol and bottom2_vol > big_bid_volume and bottom1_vol > 0 and bottom2_vol > 0
        # if tiny_bottom_condition:
        #     tiny_flag = 1
        # if top_tiny > 5 and top2_vol > big_ask_volume:
        #     api.draw_text(min5_kline, str(top_tiny), x=-2, y=min5_kline.iloc[-2].high + 10 * price_tick, color='black')
        # if bottom_tiny > 5 and bottom2_vol > big_bid_volume:
        #     api.draw_text(min5_kline, str(bottom_tiny), x=-2, y=min5_kline.iloc[-2].low - 13 * price_tick, color='black')

    price_logger.debug(f'time:{dt}, delta:{delta}, poc_price:{poc_price}, poc_volume:{poc_vol}, median_vol:{median_volume}, buy:{total_ask_vol}, sell:{total_buy_vol}, total:{total_vol}, 最新价:{ticks.iloc[-1].last_price}')
    if poc_vol < 2 * median_volume:
        api.draw_line(min5_kline, x1=-2, y1=poc_price, x2=-1,  y2=poc_price, line_type='SEG', color='green')
        # 无效poc_price
        poc_price = 0
    else:
        api.draw_line(min5_kline, x1=-2, y1=poc_price, x2=-1,  y2=poc_price, line_type='SEG', color='black')
    # 画一次指标线
    # ma = ta.MA(min5_kline, 15)  # 使用 tqsdk 自带指标函数计算均线
    # min5_kline["ma_MAIN"] = ma.ma
    # 画图
    api.draw_text(min5_kline, str(int(delta)), x=-2, y=min5_kline.iloc[-2].high + 4 * price_tick, color='red' if delta > 0 else 'green')
    api.draw_text(min5_kline, str(int(total_vol)), x=-2, y=min5_kline.iloc[-2].low - 7 * price_tick, color='blue')

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
    poc_price = 0
    poc_vol = 0
    last_bar = None
    is_tq_min5_ok = False
    print("/------------------/")
    return 1


def cal_open_signal(delta, poc_price, red_or_green, tiny_flag):
    global long_stop_price, short_stop_price, open_signal, wait_period
    # 委托时间
    now_datetime = tafunc.time_to_datetime(ticks.iloc[-1].datetime)
    # now_datetime = parser.parse(now)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute
    if (now_hour == 9 and now_minute <= 9) or (now_hour == 13 and now_minute <= 39) or (now_hour == 21 and now_minute <= 9) or (100 <= can_time(now_hour, now_minute) < 259):
        # 前5分钟不开仓
        return 0
    main_long_condition = len(demand_heap) > 0 and len(supply_heap) == 0 and red_or_green > 0 and delta > 0
    main_short_condition = len(supply_heap) > 0  and len(demand_heap) == 0 and red_or_green < 0 and delta < 0
    delta_ma3 = 0 if am.count < 4 else talib.EMA(am.open_interest, 3)[-2]
    delta_ratio_ma3 = 0 if am.count < 4 else talib.EMA(abs(am.open_interest/am.volume), 3)[-2]
    volume_ma3 = 0 if am.count < 4 else talib.EMA(am.volume, 3)[-2]
    # delta大于MA3
    limit_open_condition_delta = am.open_interest[-1] > delta_ma3 if am.open_interest[-1] > 0 else am.open_interest[-1] < delta_ma3
    limit_open_condition_delta_ratio = abs(am.open_interest[-1] / am.volume[-1]) > delta_ratio_ma3
    # volume大于MA3
    limit_open_condition_volume = (now_hour == 9 and now_minute <= 15) or (now_hour == 13 and now_minute <= 45) or (now_hour == 21 and now_minute <= 15) or am.volume[-1] > volume_ma3
    # 失衡堆积、poc不在上下影线、不在顶端(3个价位以内)
    limit_long_condition_poc = True if poc_price == 0 else poc_price < max(am.close[-1], am.open[-1]) + 2 * price_tick and poc_price < am.high[-1] - 2 * price_tick
    limit_short_condition_poc = True if poc_price == 0 else poc_price > min(am.close[-1], am.open[-1]) - 2 * price_tick and poc_price > am.low[-1] + 2 * price_tick
    limit_long_condition_heap = len(demand_heap) > 0 and demand_heap[0].max_price < max(am.close[-1], am.open[-1]) + 2 * price_tick and demand_heap[0].max_price < am.high[-1] - 2 * price_tick
    limit_short_condition_heap = len(supply_heap) > 0 and supply_heap[len(supply_heap) - 1].min_price > min(am.close[-1], am.open[-1]) - 2 * price_tick and supply_heap[len(supply_heap) - 1].min_price > am.low[-1] + 2 * price_tick
    # 多个失衡堆积,开仓
    multi_heap_long_condition = len(demand_heap) > 1 and len(supply_heap) == 0 and red_or_green > 0 and delta > 0
    multi_heap_short_condition = len(supply_heap) > 1 and len(demand_heap) == 0 and red_or_green < 0 and delta < 0
    # 止损价格差距太大不开仓
    long_stop_tick = 999 if len(demand_heap) == 0 else int((ticks.iloc[-1].ask_price1 - demand_heap[0].min_price - 2 * price_tick) / price_tick)
    short_stop_tick = 999 if len(supply_heap) == 0 else int((supply_heap[-1].max_price + 2 * price_tick - ticks.iloc[-1].bid_price1) / price_tick)
    limit_long_condition_stop = long_stop_tick < open_stop_tick
    limit_short_condition_stop = short_stop_tick < open_stop_tick
    log(f'趋势开仓参数:main_long_condition:{main_long_condition}, main_short_condition:{main_short_condition}, delta_ma3:{delta_ma3}, volume_ma3:{volume_ma3}, '
        f'limit_open_condition_delta:{limit_open_condition_delta}, limit_open_condition_delta_ratio:{limit_open_condition_delta_ratio}, limit_open_condition_volume:{limit_open_condition_volume}, '
        f'limit_long_condition_poc:{limit_long_condition_poc}, limit_short_condition_poc:{limit_short_condition_poc}, limit_long_condition_heap:{limit_long_condition_heap}, limit_short_condition_heap:{limit_short_condition_heap}, long_stop_tick:{long_stop_tick}, short_stop_tick:{short_stop_tick}, '
        f'multi_heap_long_condition:{multi_heap_long_condition}, multi_heap_short_condition:{multi_heap_short_condition}')

    if ((main_long_condition and limit_open_condition_delta and limit_open_condition_delta_ratio and limit_open_condition_volume and limit_long_condition_poc and limit_long_condition_heap) or multi_heap_long_condition) and limit_long_condition_stop:
        if long_stop_price > 0:
            log('调整止损位至{}'.format(demand_heap[0].min_price - price_tick))
            wait_period = 1
        else:
            log('设置开仓止损位至{}'.format(demand_heap[0].min_price - price_tick))
        long_stop_price = demand_heap[0].min_price - price_tick
        short_stop_price = 0
        open_signal = 1
    if ((main_short_condition and limit_open_condition_delta and limit_open_condition_delta_ratio and limit_open_condition_volume and limit_short_condition_poc and limit_short_condition_heap) or multi_heap_short_condition) and limit_short_condition_stop:
        if short_stop_price > 0:
            log('调整止损位至{}'.format(supply_heap[-1].max_price + price_tick))
            wait_period = 1
        else:
            log('设置开仓止损位至{}'.format(supply_heap[-1].max_price + price_tick))
        short_stop_price = supply_heap[-1].max_price + price_tick
        long_stop_price = 0
        open_signal = -1

    # 反转开仓,poc和堆积在上下影线，比如绿k线的需求堆积在下影线有反转上涨趋势
    reverse_long_condition = poc_price > 0 and poc_price < min(am.close[-1], am.open[-1]) and len(demand_heap) > 0 and demand_heap[0].max_price < min(am.close[-1], am.open[-1])
    reverse_short_condition = poc_price > 0 and poc_price > max(am.close[-1], am.open[-1]) and len(supply_heap) > 0 and supply_heap[0].min_price > max(am.close[-1], am.open[-1])
    if reverse_long_condition or reverse_short_condition:
        long_stop_tick = 999 if not reverse_long_condition else int((demand_heap[-1].min_price - 2 * price_tick - ticks.iloc[-1].ask_price1) / price_tick)
        short_stop_tick = 999 if not reverse_short_condition else int((supply_heap[0].max_price + 2 * price_tick - ticks.iloc[-1].bid_price1) / price_tick)
        limit_long_condition_stop = long_stop_tick < open_stop_tick
        limit_short_condition_stop = short_stop_tick < open_stop_tick
        log(f'反转开仓参数:reverse_long_condition:{reverse_long_condition}, reverse_short_condition:{reverse_short_condition}, limit_open_condition_volume:{limit_open_condition_volume}, long_stop_tick:{long_stop_tick}, short_stop_tick:{short_stop_tick}')
        if reverse_long_condition and limit_open_condition_volume and limit_long_condition_stop:
            if long_stop_price > 0:
                log('调整止损位至{}'.format(demand_heap[-1].min_price - price_tick))
            else:
                log('设置开仓止损位至{}'.format(demand_heap[-1].min_price - price_tick))
            long_stop_price = demand_heap[-1].min_price - price_tick
            short_stop_price = 0
            open_signal = 1
        elif reverse_short_condition and limit_open_condition_volume and limit_short_condition_stop:
            if short_stop_price > 0:
                log('调整止损位至{}'.format(supply_heap[0].max_price + price_tick))
            else:
                log('设置开仓止损位至{}'.format(supply_heap[0].max_price + price_tick))
            short_stop_price = supply_heap[0].max_price + price_tick
            long_stop_price = 0
            open_signal = -1

    # 反转开仓2,堆积在上下影线，比如绿k线的需求堆积在上影线，有继续下跌趋势
    reverse_long_condition2 = red_or_green > 0 and len(supply_heap) > 0 and supply_heap[0].max_price < min(am.close[-1], am.open[-1]) - price_tick
    reverse_short_condition2 = red_or_green < 0 and len(demand_heap) > 0 and demand_heap[0].min_price > max(am.close[-1], am.open[-1]) + price_tick
    # if reverse_long_condition2 or reverse_short_condition2:
    #     long_stop_tick = 999 if not reverse_long_condition2 else int((supply_heap[-1].min_price - 2 * price_tick - ticks.iloc[-1].ask_price1) / price_tick)
    #     short_stop_tick = 999 if not reverse_short_condition2 else int((demand_heap[0].max_price + 2 * price_tick - ticks.iloc[-1].bid_price1) / price_tick)
    #     limit_long_condition_stop = long_stop_tick < open_stop_tick
    #     limit_short_condition_stop = short_stop_tick < open_stop_tick
    #     log(f'反转开仓参数2:reverse_long_condition2:{reverse_long_condition2}, reverse_short_condition2:{reverse_short_condition2}, limit_open_condition_volume:{limit_open_condition_volume}, long_stop_tick:{long_stop_tick}, short_stop_tick:{short_stop_tick}')
    #     if reverse_long_condition2 and limit_open_condition_volume and limit_long_condition_stop:
    #         if long_stop_price > 0:
    #             log('调整止损位至{}'.format(supply_heap[-1].min_price - price_tick))
    #         else:
    #             log('设置开仓止损位至{}'.format(supply_heap[-1].min_price - price_tick))
    #         long_stop_price = supply_heap[-1].min_price - price_tick
    #         short_stop_price = 0
    #         open_signal = 1
    #     elif reverse_short_condition2 and limit_open_condition_volume and limit_short_condition_stop:
    #         if short_stop_price > 0:
    #             log('调整止损位至{}'.format(demand_heap[0].max_price + price_tick))
    #         else:
    #             log('设置开仓止损位至{}'.format(demand_heap[0].max_price + price_tick))
    #         short_stop_price = demand_heap[0].max_price + price_tick
    #         long_stop_price = 0
    #         open_signal = -1

# if tiny_flag != 0 and limit_open_condition_volume:
    #     long_stop_tick = 999 if tiny_flag < 0 else int((ticks.iloc[-1].ask_price1 - min5_kline.iloc[-2].low - 2 * price_tick) / price_tick)
    #     short_stop_tick = 999 if tiny_flag > 0 else int((min5_kline.iloc[-2].high + 2 * price_tick - ticks.iloc[-1].bid_price1) / price_tick)
    #     limit_long_condition_stop = long_stop_tick < open_stop_tick
    #     limit_short_condition_stop = short_stop_tick < open_stop_tick
    #     log(f'反转开仓参数:tiny_flag:{tiny_flag}, long_stop_tick:{long_stop_tick}, short_stop_tick:{short_stop_tick}')
    #     if limit_short_condition_stop:
    #         if short_stop_price > 0:
    #             log('调整止损位至{}'.format(min5_kline.iloc[-2].high + price_tick))
    #         else:
    #             log('设置开仓止损位至{}'.format(min5_kline.iloc[-2].high + price_tick))
    #         short_stop_price = min5_kline.iloc[-2].high + price_tick
    #         long_stop_price = 0
    #         open_signal = -1
    #     elif limit_long_condition_stop:
    #         if long_stop_price > 0:
    #             log('调整止损位至{}'.format(min5_kline.iloc[-2].low - price_tick))
    #         else:
    #             log('设置开仓止损位至{}'.format(min5_kline.iloc[-2].low - price_tick))
    #         long_stop_price = min5_kline.iloc[-2].low - price_tick
    #         short_stop_price = 0
    #         open_signal = 1

def cal_stop_signal(delta, poc_price, red_or_green):
    global stop_signal, long_stop_price, short_stop_price, wait_period, open_signal
    # 已有开仓信号
    if open_signal > 0 and long_entry_price > 0:
        open_signal = 0
        log(f'已开多仓,不计算止盈')
        return
    elif open_signal < 0 and short_entry_price > 0:
        open_signal = 0
        log(f'已开空仓,不计算止盈')
        return
    if wait_period > 0:
        wait_period += 1
    else:
        # 未开仓
        return

    # current_price_condition = (long_entry_price > 0 and ticks.iloc[-1].last_price > ma_price) or (short_entry_price > 0 and ticks.iloc[-1].last_price < ma_price)
    main_profit_condition = (long_entry_price > 0 and int((ticks.iloc[-1].last_price - long_entry_price) / price_tick) > profit_tick) or \
                            (short_entry_price > 0 and int((short_entry_price - ticks.iloc[-1].last_price) / price_tick) > profit_tick)
    if (not main_profit_condition) and wait_period <= max_wait_period:
        log(f'未到盈利点{profit_tick}或者开仓未满{max_wait_period}个周期,不计算止盈')
        return
    delta_ma3 = 0 if am.count < 4 else talib.EMA(am.open_interest, 3)[-2]
    abs_delta_ma3 = 0 if am.count < 4 else talib.EMA(abs(am.open_interest), 3)[-2]
    delta_ratio_ma3 = 0 if am.count < 4 else talib.EMA(abs(am.open_interest/am.volume), 3)[-2]
    volume_ma3 = 0 if am.count < 4 else talib.EMA(am.volume, 3)[-2]
    # delta大于MA3
    limit_close_condition_delta = am.open_interest[-1] > delta_ma3 if am.open_interest[-1] > 0 else am.open_interest[-1] < delta_ma3
    # volume大于MA3
    limit_close_condition_volume = am.volume[-1] > volume_ma3
    # poc出现在上下影线
    close_long_condition_poc = poc_price > 0 and long_entry_price > 0 and poc_price > (max(am.close[-1], am.open[-1]) + price_tick)
    close_short_condition_poc = poc_price > 0 and short_entry_price > 0 and poc_price < (min(am.close[-1], am.open[-1]) - price_tick)
    log(f'poc出现在上下影线:delta_ma3:{delta_ma3}, abs_delta_ma3:{abs_delta_ma3}, volume_ma3:{volume_ma3}, limit_close_condition_delta:{limit_close_condition_delta}, limit_close_condition_volume:{limit_close_condition_volume}, close_long_condition_poc:{close_long_condition_poc}, close_short_condition_poc:{close_short_condition_poc}')
    if limit_close_condition_volume and close_long_condition_poc:
        stop_signal = 1
        return
    elif limit_close_condition_volume and close_short_condition_poc:
        stop_signal = -1
        return
    # 连续出现3个正号delta变小，并且出现负号delta
    close_long_condition_delta = long_entry_price > 0 and am.open_interest[-4] > 0 and am.open_interest[-3] > 0 and am.open_interest[-2] > 0 \
                                 and  am.open_interest[-1] < 0 and am.open_interest[-4] > am.open_interest[-3] > am.open_interest[-2]
    close_short_condition_delta = short_entry_price > 0 and am.open_interest[-4] < 0 and am.open_interest[-3] < 0 and am.open_interest[-2] < 0 \
                                 and  am.open_interest[-1] > 0 and am.open_interest[-4] < am.open_interest[-3] < am.open_interest[-2]
    log(f'3个正号delta变小:close_long_condition_delta:{close_long_condition_delta}, close_short_condition_delta:{close_short_condition_delta}')
    if close_long_condition_delta:
        stop_signal = 1
        return
    elif close_short_condition_delta:
        stop_signal = -1
        return
    # 2个负号delta连续增大
    close_long_condition_delta2 = long_entry_price > 0 and am.open_interest[-2] < 0 and am.open_interest[-1] < 0 and am.open_interest[-1] < am.open_interest[-2] and (am.volume[-1] > volume_ma3 or abs(am.open_interest[-1] / am.volume[-1]) > delta_ratio_ma3)
    close_short_condition_delta2 = short_entry_price > 0 and am.open_interest[-2] > 0 and am.open_interest[-1] > 0 and am.open_interest[-1] > am.open_interest[-2] and (am.volume[-1] > volume_ma3 or abs(am.open_interest[-1] / am.volume[-1]) > delta_ratio_ma3)
    log(f'2个负号delta连续增大:close_long_condition_delta2:{close_long_condition_delta2}, close_short_condition_delta2:{close_short_condition_delta2}')
    if close_long_condition_delta2:
        stop_signal = 1
        return
    elif close_short_condition_delta2:
        stop_signal = -1
        return
    # 突然出现超大负向delta，并且收了上下影线或者变色
    close_long_condition_delta3 = long_entry_price > 0 and am.open_interest[-2] > 0 and am.open_interest[-1] < 0 and abs(am.open_interest[-1]) > 1.2 * abs_delta_ma3 and (red_or_green < 0 or am.high[-1] > max(am.close[-1], am.open[-1]))
    close_short_condition_delta3 = long_entry_price > 0 and am.open_interest[-2] < 0 and am.open_interest[-1] < 0 and abs(am.open_interest[-1]) > 1.2 * abs_delta_ma3 and (red_or_green > 0 or am.low[-1] < min(am.close[-1], am.open[-1]))
    log(f'超大负向delta:close_long_condition_delta3:{close_long_condition_delta3}, close_short_condition_delta3:{close_short_condition_delta3}')
    if close_long_condition_delta3:
        stop_signal = 1
        return
    elif close_short_condition_delta3:
        stop_signal = -1
        return
    # 正向失衡堆积出现在上下影线
    close_long_condition_heap = long_entry_price > 0 and len(demand_heap) > 0 and demand_heap[0].min_price > (max(am.close[-1], am.open[-1]) + price_tick)
    close_short_condition_heap = short_entry_price > 0 and len(supply_heap) > 0 and supply_heap[-1].max_price < (min(am.close[-1], am.open[-1]) - price_tick)
    log(f'正向失衡堆积出现在上下影线:close_long_condition_heap:{close_long_condition_heap}, close_short_condition_heap:{close_short_condition_heap}')
    if close_long_condition_heap:
        long_stop_price = max(long_stop_price, min(am.low[-1], am.open[-1]))
        # 颜色变化或者在顶端
        if red_or_green < 0 or demand_heap[0].max_price >= am.high[-1] - price_tick:
            stop_signal = 1
        return
    elif close_short_condition_heap:
        short_stop_price = min(short_stop_price, max(am.low[-1], am.open[-1]))
        # 颜色变化或者在底部
        if red_or_green > 0 or supply_heap[len(supply_heap) - 1].min_price <= am.low[-1] + price_tick:
            stop_signal = -1
        return

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
    long_delta_trend_condition = am.open_interest[-2] > 0 and delta > am.open_interest[-2]
    short_delta_trend_condition = am.open_interest[-2] < 0 and delta < am.open_interest[-2]
    long_reverse_condition = delta > 10 * abs(am.open_interest[-2]) and am.close[-2] > am.open[-2]
    short_reverse_condition = abs(delta) > 10 * abs(am.open_interest[-2]) and am.close[-2] < am.open[-2]
    delta_ma3 = 0 if am.count < 4 else talib.EMA(abs(am.open_interest), 3)[-2]
    delta_ratio_ma3 = 0 if am.count < 4 else talib.EMA(abs(am.open_interest/am.volume), 3)[-2]
    volume_ma3 = 0 if am.count < 4 else talib.EMA(am.volume, 3)[-2]
    # delta大于MA3
    limit_open_condition_delta = abs(am.open_interest[-1]) > delta_ma3
    limit_open_condition_delta_ratio = abs(am.open_interest[-1] / am.volume[-1]) > delta_ratio_ma3
    # volume大于MA3
    limit_open_condition_volume = (now_hour == 9 and now_minute <= 15) or (now_hour == 13 and now_minute <= 45) or (now_hour == 21 and now_minute <= 15) or am.volume[-1] > volume_ma3
    log(f'开盘时开仓参数:main_long_condition:{main_long_condition}, main_short_condition:{main_short_condition}, long_delta_trend_condition:{long_delta_trend_condition}, '
        f'short_delta_trend_condition:{short_delta_trend_condition}, limit_open_condition_delta:{limit_open_condition_delta}, long_reverse_condition:{long_reverse_condition}, short_reverse_condition:{short_reverse_condition}')

    if main_long_condition and limit_open_condition_delta and (long_delta_trend_condition or long_reverse_condition):
        stop_price = ticks.iloc[-1].ask_price1 - loss_tick * price_tick
        if long_stop_price > 0:
            log('调整止损位至{}'.format(stop_price))
            wait_period = 1
        else:
            log('设置开仓止损位至{}'.format(stop_price))
        long_stop_price = stop_price
        short_stop_price = 0
        open_signal = 1
    if main_short_condition and limit_open_condition_delta and (short_delta_trend_condition or short_reverse_condition):
        stop_price = ticks.iloc[-1].bid_price1 + loss_tick * price_tick
        if short_stop_price > 0:
            log('调整止损位至{}'.format(stop_price))
            wait_period = 1
        else:
            log('设置开仓止损位至{}'.format(stop_price))
        short_stop_price = stop_price
        long_stop_price = 0
        open_signal = -1

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

    abs_delta_ma3 = 0 if am.count < 4 else talib.EMA(abs(am.open_interest), 3)[-2]
    close_long_condition_delta0 = long_entry_price > 0 and am.open_interest[-1] < -1 * abs_delta_ma3 and red_or_green < 0
    close_short_condition_delta0 = short_entry_price > 0 and am.open_interest[-1] > 1 * abs_delta_ma3 and red_or_green > 0
    log(f'开盘时止损参数:close_long_condition_delta0:{close_long_condition_delta0}, close_short_condition_delta0:{close_short_condition_delta0}, abs_delta_ma3:{abs_delta_ma3}')
    if close_long_condition_delta0:
        stop_signal = 1
        return
    elif close_short_condition_delta0:
        stop_signal = -1
        return

    main_profit_condition = (long_entry_price > 0 and int((ticks.iloc[-1].last_price - long_entry_price) / price_tick) > opening_profit_tick) or \
                            (short_entry_price > 0 and int((short_entry_price - ticks.iloc[-1].last_price) / price_tick) > opening_profit_tick)
    if not main_profit_condition:
        log(f'开盘时盈利未到{loss_tick},不计算止盈')
        return
    delta_ratio_ma3 = 0 if am.count < 4 else talib.EMA(abs(am.open_interest/am.volume), 3)[-2]

    limit_close_condition_delta_ratio = abs(am.open_interest[-1] / am.volume[-1]) > delta_ratio_ma3
    close_long_condition_delta = long_entry_price > 0 and delta < 0 and red_or_green < 0 and limit_close_condition_delta_ratio
    close_short_condition_delta = short_entry_price > 0 and delta > 0 and red_or_green > 0 and limit_close_condition_delta_ratio
    log(f'开盘时止盈参数:close_long_condition_delta:{close_long_condition_delta}, close_short_condition_delta:{close_short_condition_delta}, delta_ratio_ma3:{delta_ratio_ma3}')
    if close_long_condition_delta:
        stop_signal = 1
        return
    elif close_short_condition_delta:
        stop_signal = -1
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

    tick = TickData(
        gateway_name='tq',
        symbol=symbol,
        exchange=exchange,
        datetime=dt,
        date=dt.strftime('%Y-%m-%d'),
        time=dt.strftime('%H:%M:%S.%f'),
        trading_day=get_trading_date(dt),
        last_price=temp_price,
        volume=ticks.iloc[-1].volume
    )
    bg.update_tick(tick)
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
    global heap_flag, Up_Aprice, Dn_Bprice, total_vol, poc_price, PG, PGG, PNN, Max_price, Min_price, M_minprice, W_maxprice, D_Low, open_signal, stop_signal, long_stop_price, short_stop_price
    last_price = ticks.iloc[-1].last_price
    now_datetime = tafunc.time_to_datetime(ticks.iloc[-1].datetime)
    now_hour = now_datetime.hour
    now_minute = now_datetime.minute

    # 开仓
    if open_signal == 1:
        trySendOrder(lots, 1, -1)
        open_signal = 0
    elif open_signal == -1:
        open_signal = 0
        trySendOrder(lots, -1, 1)
    # 止损
    if long_stop_price > 0 and last_price < long_stop_price:
        trySendOrder(lots, 0, 1)
        long_stop_price = 0
    elif short_stop_price > 0 and last_price > short_stop_price:
        trySendOrder(lots, 0, -1)
        short_stop_price = 0
    # 止盈
    if stop_signal == 1:
        trySendOrder(lots, 0, 1)
        long_stop_price = 0
        stop_signal = 0
    elif stop_signal == -1:
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
    global is_trading, total_ask_vol, total_buy_vol, sumB, sumA, poc_price, poc_Ask, poc_Bid, poc_vol, last_bar, is_tq_min5_ok, bg, am
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
        is_tq_min5_ok = False
        last_bar = None
        bg = BarGenerator(on_bar, trade_period, on_5min_bar)
        am = ArrayManager(10)
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
    global is_trading
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
    elif (NT == 2):
        if ((1458 <= can_time(hour_new, minute_new) < 1500) or (228 <= can_time(hour_new, minute_new) < 230)):  # 凌晨2.30结束
            close_and_clearn()
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
while True:
    try:
        api.wait_update()
        if api.is_changing(min5_kline):
            is_tq_min5_ok = True
        if is_tq_min5_ok and last_bar is not None:
            tq_now = tafunc.time_to_datetime(min5_kline.iloc[-2].datetime)
            vnpy_now = last_bar.datetime
            if tq_now.hour != vnpy_now.hour or tq_now.minute != vnpy_now.minute:
                logger.warning(f'time:{tafunc.time_to_datetime(ticks.iloc[-1].datetime)} 天勤k线和合成k线没有对齐！tq:{tq_now}, vnpy:{vnpy_now}')
                if tq_now.hour > vnpy_now.hour:
                    last_bar = None
                elif tq_now.hour < vnpy_now.hour:
                    is_tq_min5_ok = False
                elif tq_now.minute > vnpy_now.minute:
                    last_bar = None
                elif tq_now.minute < vnpy_now.minute:
                    is_tq_min5_ok = False
                continue
            # 核心模块
            price_logger.debug(f'天勤k线:time:{tq_now}, open:{min5_kline.iloc[-2].open}, close:{min5_kline.iloc[-2].close}, high:{min5_kline.iloc[-2].high}, low:{min5_kline.iloc[-2].low}, volume:{min5_kline.iloc[-2].volume}')
            CoreModule()
            # print("需求堆积Up_Aprice:= " + str(Up_Aprice) + "  供给堆积Dn_Bprice:= " + str(Dn_Bprice) + " 堆积状态heap_flag:= " + str(heap_flag) + " 阳线Or阴线:= " + str(red_or_green))
            # print("顶部微单状态:= " + str(PG) + "  底部微单状态:= " + str(tiny_flag) + " 顶部最高价:= " + str(Max_price) + " 底部最低价:= " + str(Min_price))
            # print("POCVoL:= " + str(poc_vol) + "  poc_Ask:= " + str(poc_Ask) + " poc_Bid:= " + str(poc_Bid))
            print("/////////5分钟逻辑运行结束/////////")
        if api.is_changing(ticks):
            # print(sum(order.volume_left for oid, order in order.items() if order.status == "ALIVE"))
            # ticks.iloc[-1]返回序列中最后一个tick
            # print("tick变化", ticks.iloc[-1])
            # print(ticks.iloc[-1].bid_price1, ticks.iloc[-1].ask_price1,ticks.iloc[-1].volume,ticks.iloc[-1].last_price)
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
                # target_profit_stop()
                trade_mode()
    except BacktestFinished as e:
        pass
