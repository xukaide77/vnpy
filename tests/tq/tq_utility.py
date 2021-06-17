from enum import Enum
from abc import ABC, abstractmethod
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from vnpy.trader.constant import Direction
from tqsdk import TqApi, TqReplay, TqBacktest, TqSim, BacktestFinished, tafunc, ta

class SignalType(Enum):
    TREND = "趋势"
    REBOUND = "反弹"

class Signal(object):
    open_price: float = 0
    stop_price: float = 0
    direction: Direction = None
    timestamp: datetime
    signal_type: SignalType
    stop_profit_price: float = 0

    def __init__(self, stop_price, direction: Direction, timestamp: datetime, signal_type: SignalType, stop_profit_price: float = 0):
        self.stop_price = stop_price
        self.direction = direction
        self.timestamp = timestamp
        self.signal_type = signal_type
        self.stop_profit_price = stop_profit_price

class KlineType(Enum):
    """
    Direction of order/trade/position.
    """
    TOP = "顶分型"
    BOTTOM = "底分型"
    CALLBACK_TOP = '回调顶'
    CALLBACK_BOTTOM = '回调底'

class KlineForm(ABC):

    stop_price: float = 0
    type: KlineType = KlineType.TOP
    timestamp: int
    direction: Direction = None
    name: str

    # def __init__(self, stop_price: float, direction: Direction, timestamp: int):
    #     self.stop_price: float = stop_price
    #     self.timestamp = timestamp
    #     self.direction = direction

    @abstractmethod
    def verify(self, kline: DataFrame) -> Signal:
        pass

class KeyPrice(object):
    yesterday_high: float = 0
    yesterday_low: float = 99999999
    today_high: float = 0
    today_low: float = 99999999
    # 第一次清仓前的极端价，突破后用于二浪开仓
    pre_high: float = 0
    pre_low: float = 9999999
    price_tick: float = 0
    # 第一次清仓以后极端价，用于二浪三只乌鸦开仓
    after_close_low: float = 0
    after_close_high: float = 0
    last_fail_high_close: float = 0
    last_fail_low_close: float = 0
    open_high: float = 0
    open_low: float = 9999999
    first_high: float = 0
    first_low: float = 9999999
    long_trend_count: int = 0
    short_trend_count: int = 0
    trend: int = 0
    jincha_count: int = 0
    sicha_count: int = 0
    open_ready_price: float = 0
    stop_loss_price: float = 0
    stop_profit_price: float = 0

    def clear(self):
        self.yesterday_high = self.today_high
        self.yesterday_low = self.today_low
        self.today_high = 0
        self.today_low = 9999999
        # 第一次清仓前的极端价，突破后用于二浪开仓
        self.pre_high = 0
        self.pre_low = 9999999
        # 第一次清仓以后极端价，用于二浪三只乌鸦开仓
        self.after_close_low = 0
        self.after_close_high = 0
        self.last_fail_high_close: float = 0
        self.last_fail_low_close: float = 0
        self.first_high: float = 0
        self.first_low: float = 9999999
        self.open_high: float = 0
        self.open_low: float = 9999999
        self.long_trend_count: int = 0
        self.short_trend_count: int = 0
        self.open_ready_price: float = 0
        self.stop_loss_price: float = 0
        self.stop_profit_price: float = 0

class ThreeBlack(KlineForm):

    name = '三只乌鸦'

    def verify(self, kline: DataFrame, key_p: KeyPrice) -> Signal:
        ma5 = ta.MA(kline, 5)
        ma10 = ta.MA(kline, 10)
        ma20 = ta.MA(kline, 20)
        is_long_trend = ma5.ma.iloc[-1] > ma10.ma.iloc[-1] > ma20.ma.iloc[-1]
        # 开空
        if kline.iloc[-1].close < kline.iloc[-1].open and kline.iloc[-2].close < kline.iloc[-2].open and kline.iloc[-3].close < kline.iloc[-3].open and kline.iloc[-1].close_oi < kline.iloc[-2].close_oi and key_p.today_high - key_p.yesterday_high > -10 * key_p.price_tick:
            # 连续3根k线
            if abs(kline.iloc[-3].high - key_p.today_high) <= 8 * key_p.price_tick or abs(kline.iloc[-4].high - key_p.today_high) <= 8 * key_p.price_tick or abs(kline.iloc[-5].high - key_p.today_high) <= 8 * key_p.price_tick:
                # 跌破最高价
                if kline.iloc[-1].open - kline.iloc[-1].close > 5 * key_p.price_tick \
                        and kline.iloc[-3].open - kline.iloc[-1].close > 10 * key_p.price_tick \
                        and kline.iloc[-1].close < kline.iloc[-4].open:
                    # 下跌超过10个点开仓，超过33个点跌幅过大不追
                    # if kline.iloc[-3].open - kline.iloc[-1].close > 40 * key_p.price_tick:
                    #     return None
                    return Signal(kline.iloc[-3].open, Direction.SHORT, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND)
        elif kline.iloc[-1].close < kline.iloc[-1].open and kline.iloc[-2].close < kline.iloc[-2].open and key_p.today_high - key_p.yesterday_high > -10 * key_p.price_tick and kline.iloc[-1].close_oi < kline.iloc[-2].close_oi:
            # 连续2根k线
            if abs(kline.iloc[-2].high - key_p.today_high) <= 8 * key_p.price_tick or abs(kline.iloc[-3].high - key_p.today_high) <= 8 * key_p.price_tick or abs(kline.iloc[-4].high - key_p.today_high) <= 8 * key_p.price_tick:
                # 跌破最高价
                if ((kline.iloc[-1].open - kline.iloc[-1].close > 5 * key_p.price_tick \
                        and kline.iloc[-2].open - kline.iloc[-2].close > 5 * key_p.price_tick) or (kline.iloc[-1].open - kline.iloc[-1].close > 15 * key_p.price_tick)) \
                        and kline.iloc[-1].close < kline.iloc[-3].open:
                    # 下跌超过10个点开仓，超过33个点跌幅过大不追
                    # if kline.iloc[-2].open - kline.iloc[-1].close > 40 * key_p.price_tick:
                    #     return None
                    return Signal(kline.iloc[-2].open, Direction.SHORT, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND)
        # if key_p.last_fail_low_close > 0 and kline.iloc[-1].close < kline.iloc[-1].open and kline.iloc[-1].close < key_p.last_fail_low_close:
        #     return Signal(max(kline.iloc[-1].close + 15 * key_p.price_tick, kline.iloc[-1].open), Direction.SHORT, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND)
        # if key_p.after_close_high > 0 and kline.iloc[-1].close < kline.iloc[-1].open and kline.iloc[-2].close < kline.iloc[-2].open: #and kline.iloc[-1].close_oi < kline.iloc[-2].close_oi:
        #     # 连续2根k线三眼乌鸦二浪开仓
        #     if kline.iloc[-2].high == key_p.after_close_high or kline.iloc[-3].high == key_p.after_close_high or kline.iloc[-4].high == key_p.after_close_high or kline.iloc[-5].high == key_p.after_close_high or kline.iloc[-6].high == key_p.after_close_high:
        #         # 跌破最高价
        #         if kline.iloc[-1].open - kline.iloc[-1].close > 5 * key_p.price_tick \
        #                 and kline.iloc[-2].open - kline.iloc[-1].close > 10 * key_p.price_tick \
        #                 and kline.iloc[-1].close < kline.iloc[-3].close:
        #             # 下跌超过10个点开仓，超过25个点跌幅过大不追
        #             if kline.iloc[-2].open - kline.iloc[-1].close > 25 * key_p.price_tick:
        #                 return None
        #             return Signal(kline.iloc[-2].open, Direction.SHORT, tafunc.time_to_datetime(kline.iloc[-1].datetime))
        #开多
        # if kline.iloc[-1].close > kline.iloc[-1].open and kline.iloc[-2].close > kline.iloc[-2].open and kline.iloc[-1].close_oi > kline.iloc[-2].close_oi:
        #     # 连续2根k线
        #     if abs(kline.iloc[-2].low - key_p.today_low) <= 3 * key_p.price_tick or abs(kline.iloc[-3].low - key_p.today_low) <= 3 * key_p.price_tick or abs(kline.iloc[-4].low - key_p.today_low) <= 3 * key_p.price_tick:
        #         # 升破最低价
        #         if kline.iloc[-1].close - kline.iloc[-1].open > 5 * key_p.price_tick \
        #                 and kline.iloc[-1].close - kline.iloc[-2].open > 10 * key_p.price_tick \
        #                 and kline.iloc[-1].close > kline.iloc[-3].open:
        #             # 上升超过10个点开仓，超过33个点升幅过大不追
        #             # if kline.iloc[-1].close - kline.iloc[-2].open > 40 * key_p.price_tick:
        #             #     return None
        #             return Signal(kline.iloc[-2].open, Direction.LONG, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.REBOUND)
        # elif kline.iloc[-1].close > kline.iloc[-1].open and kline.iloc[-2].close > kline.iloc[-2].open and kline.iloc[-3].close > kline.iloc[-3].open and kline.iloc[-2].close_oi > kline.iloc[-3].close_oi:
        #     # 连续3根k线
        #     if abs(kline.iloc[-3].low - key_p.today_low) <= 3 * key_p.price_tick or abs(kline.iloc[-4].low - key_p.today_low) <= 3 * key_p.price_tick or abs(kline.iloc[-5].low - key_p.today_low) <= 3 * key_p.price_tick:
        #         # 升破最低价
        #         if kline.iloc[-1].close - kline.iloc[-1].open > 5 * key_p.price_tick \
        #                 and kline.iloc[-1].close - kline.iloc[-3].open > 10 * key_p.price_tick \
        #                 and kline.iloc[-1].close > kline.iloc[-4].open:
        #             # 上升超过10个点开仓，超过33个点升幅过大不追
        #             # if kline.iloc[-1].close - kline.iloc[-3].open > 40 * key_p.price_tick:
        #             #     return None
        #             return Signal(kline.iloc[-3].open, Direction.LONG, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.REBOUND)
        # if key_p.after_close_low > 0 and kline.iloc[-1].close > kline.iloc[-1].open and kline.iloc[-2].close > kline.iloc[-2].open: #and kline.iloc[-1].close_oi < kline.iloc[-2].close_oi:
        #     # 连续2根k线三眼乌鸦二浪开仓
        #     if kline.iloc[-2].low == key_p.after_close_low or kline.iloc[-3].low == key_p.after_close_low or kline.iloc[-4].low == key_p.after_close_low:
        #         # 升破最低价
        #         if kline.iloc[-1].close - kline.iloc[-1].oppen > 5 * key_p.price_tick \
        #                 and kline.iloc[-2].close - kline.iloc[-1].open > 10 * key_p.price_tick \
        #                 and kline.iloc[-1].close > kline.iloc[-3].close:
        #             # 上升超过10个点开仓，超过25个点升幅过大不追
        #             if kline.iloc[-2].close - kline.iloc[-1].open > 25 * key_p.price_tick:
        #                 return None
        #             return Signal(kline.iloc[-2].open, Direction.LONG, tafunc.time_to_datetime(kline.iloc[-1].datetime))

class LiYongQiang(KlineForm):

    name = '李永强式突破'

    def verify(self, kline: DataFrame, key_p: KeyPrice) -> Signal:
        kdj = ta.KD(kline, 6, 3, 3)
        k = kdj.k.iloc[-1]
        d = kdj.d.iloc[-1]
        # 开空
        if kline.iloc[-2].close >= key_p.open_low and kline.iloc[-1].close < key_p.open_low:# and k < 50 and d > 20 and kline.iloc[-1].close_oi < kline.iloc[-1].open_oi:
            return Signal(max(kline.iloc[-1].close + 15 * key_p.price_tick, kline.iloc[-1].open, kline.iloc[-2].close), Direction.SHORT, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND)
        elif kline.iloc[-2].close <= key_p.open_high and kline.iloc[-1].close > key_p.open_high:# and k > 50 and d < 80 and kline.iloc[-1].close_oi > kline.iloc[-1].open_oi:
            return Signal(min(kline.iloc[-1].close - 15 * key_p.price_tick, kline.iloc[-1].open, kline.iloc[-2].close), Direction.LONG, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND)

class KdjIndicator(KlineForm):

    name = 'kdj突破'

    def verify(self, kline: DataFrame, key_p: KeyPrice) -> Signal:
        kdj = ta.KD(kline, 4, 3, 3)
        k = kdj.k
        d = kdj.d
        # 开空
        if key_p.trend == -1 and ((k.iloc[-1] < d.iloc[-1] and k.iloc[-2] > 80 and k.iloc[-1] < 80) or (k.iloc[-2] >50 and k.iloc[-1] < 50 and key_p.sicha_count >= 2)):
            return Signal(max(kline.iloc[-1].close + 100 * key_p.price_tick, kline.iloc[-1].high), Direction.SHORT, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND)
        elif key_p.trend == 1 and ((k.iloc[-1] > d.iloc[-1] and k.iloc[-2] < 20 and k.iloc[-1] > 20) or (k.iloc[-2] < 50 and k.iloc[-1] > 50 and key_p.jincha_count >= 2)):
            return Signal(min(kline.iloc[-1].close - 100 * key_p.price_tick, kline.iloc[-1].low), Direction.LONG, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND)

class WaveIndicator(KlineForm):

    name = '波段突破'

    def verify(self, kline: DataFrame, key_p: KeyPrice) -> Signal:
        if key_p.open_ready_price > 0 and key_p.trend == 1 and kline.iloc[-1].close > key_p.open_ready_price:
            return Signal(key_p.stop_loss_price, Direction.LONG, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND, key_p.stop_profit_price)
        elif key_p.open_ready_price > 0 and key_p.trend == -1 and kline.iloc[-1].close < key_p.open_ready_price:
            return Signal(key_p.stop_loss_price, Direction.SHORT, tafunc.time_to_datetime(kline.iloc[-1].datetime), SignalType.TREND, key_p.stop_profit_price)


def BOLL(df: DataFrame, n, m, p):
    new_df = pd.DataFrame()
    mid = tafunc.ema(df["close"], n)
    std = df["close"].rolling(m).std()
    new_df["mid"] = mid
    new_df["top"] = mid + p * std
    new_df["bottom"] = mid - p * std
    return new_df

def ATR_TD(df: DataFrame):
    new_df = pd.DataFrame()
    mid = tafunc.ma(df["close"], 20)
    atr = ta.ATR(df, 20).atr
    new_df["top"] = mid + 2.618 * atr
    new_df["bottom"] = mid - 2.618 * atr
    new_df["mid"] = mid
    return new_df

class Trade(object):
    pos: int
    stop_loss_price: float
    stop_profit_price: float
    open_price: float
    stop_profit_count: int = 0
    direction: Direction
    balance_price: float
    open_time: datetime



def is_up(kline: DataFrame):
    return kline.close > kline.open

def is_down(kline: DataFrame):
    return kline.close < kline.open