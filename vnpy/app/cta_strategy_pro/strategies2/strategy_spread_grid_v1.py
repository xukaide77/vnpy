import sys
from datetime import datetime, timedelta
import copy
import traceback
from collections import OrderedDict
import os
import math
# 其次，导入vnpy的基础模块
from typing import Any
import talib as ta
from vnpy.app.cta_strategy_pro import (
    CtaSpreadTemplate,
    Direction,
    Status,
    Color,
    TickData,
    BarData
)
from vnpy.trader.constant import Exchange
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
from vnpy.trader.utility import get_underlying_symbol, round_to
from vnpy.trader.setting import SETTINGS
from vnpy.component.cta_period import CtaPeriod, Period
import numpy

class Strategy_SpreadGrid_v1(CtaSpreadTemplate):
    """非标准合约的协整套利+网格交易
    针对上期所的跨月跨期套利，如镍、铝、锌
    进入主力合约交割前4个月~3个月为交易期"""

    author = 'xk'
    step_invest_pos = 1  # 每次正套/反套下单手数，默认1
    max_invest_pos = 10  # 最大下单手数
    max_grid_lots = 10  # 最大网格层级
    spd_price_tick = 0.04  # 价差的最小跳动
    grid_height_pips = 5  # 网格高度(多少跳)
    grid_win_pips = 5  # 网格止盈(多少跳)
    grid_stop_pips = 15  # 网格止损(多少跳)
    base_up_line = 1
    base_mid_line = 0
    base_dn_line = -1

    dead_line_date = ''
    force_close_date = ''
    parameters = ['max_invest_pos', 'act_vt_symbol', 'pas_vt_symbol',
                   'act_vol_ratio', 'pas_vol_ratio',
                   'spd_price_tick', 'max_grid_lots', 'grid_height_pips', 'grid_win_pips',
                   'base_up_line', 'base_mid_line', 'base_dn_line',
                   'dead_line_date', 'force_close_date',
                   'backtesting']

    def __init__(self, cta_engine: Any, strategy_name: str, vt_symbol: str, setting: dict):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.volumeList = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

        self.rebuild_up_rate = 1  # 做空网格间距放大比率
        self.rebuild_dn_rate = 1  # 做多网格间距放大比率
        self.rebuild_up_line = None
        self.rebuild_dn_line = None
        self.rebuild_up_grid = False  # 重建网格标识
        self.rebuild_dn_grid = False  # 重建网格标志
        self.rebuild_grid = False  # 分钟触发重建网格

        self.line_spd = None  # 1分钟价差K线
        self.line_ratio = None  # 1分钟比价K线
        self.line_md = None  # 1分钟残差K线
        self.line_m5 = None  # 5分钟比价K线

        self.logMsg = None
        if setting:
            self.update_setting(setting)
        self.gt = CtaGridTrade(strategy=self, price_tick=self.act_price_tick,
                               grid_win=self.grid_win_pips * self.spd_price_tick,
                               grid_height=self.grid_height_pips * self.spd_price_tick,
                               grid_stop=self.grid_stop_pips * self.spd_price_tick)
        self.create_lines()

        if self.backtesting:
            # 回测盘使用限价单方式
            self.on_init()

    def create_lines(self):
        kline_spd_setting = {}
        kline_spd_setting['name'] = 'M1_spread'
        kline_spd_setting['bar_interval'] = 15
        kline_spd_setting['para_boll_len'] = 12
        kline_spd_setting['para_boll_std_rate'] = 1.5
        kline_spd_setting['price_tick'] = self.spd_price_tick
        # kline_spd_setting['grid_height'] = self.grid_height_pips
        # kline_spd_setting['grid_win'] = self.grid_win_pips
        # kline_spd_setting['rate_list'] = self.volumeList
        kline_spd_setting['underly_symbol'] = get_underlying_symbol(self.pas_vt_symbol)
        self.line_spd = CtaMinuteBar(self, self.on_bar_spd, kline_spd_setting)
        self.klines.update({self.line_spd.name: self.line_spd})

        kline_ratio_setting = {}
        kline_ratio_setting['name'] = 'M1_ratio'
        kline_ratio_setting['bar_interval'] = 1
        kline_ratio_setting['para_active_kf'] = True
        kline_ratio_setting['price_tick'] = 0.001
        kline_ratio_setting['underly_symbol'] = get_underlying_symbol(self.pas_vt_symbol)
        self.line_ratio = CtaMinuteBar(self, self.on_bar_ratio, kline_ratio_setting)
        self.klines.update({self.line_ratio.name: self.line_ratio})

        kline_md_setting = {}
        kline_md_setting['name'] = 'M1_MeanDiff'
        kline_md_setting['bar_interval'] = 1
        kline_md_setting['para_boll_len'] = 80
        kline_md_setting['para_boll_std_rate'] = 1.5
        kline_md_setting['price_tick'] = self.spd_price_tick
        kline_md_setting['underly_symbol'] = get_underlying_symbol(self.pas_vt_symbol)
        self.line_md = CtaMinuteBar(self, self.on_bar_md, kline_md_setting)
        self.klines.update({self.line_md.name: self.line_md})

        kline_m5_setting = {}
        kline_m5_setting['name'] = 'M5'
        kline_m5_setting['para_rsi1_len'] = 14
        kline_m5_setting['para_boll_len'] = 20
        kline_m5_setting['para_boll_std_rate'] = 1.2
        kline_m5_setting['price_tick'] = 0.0001
        kline_m5_setting['underly_symbol'] = get_underlying_symbol(self.pas_vt_symbol)
        self.line_m5 = CtaMinuteBar(self, self.on_bar_m5, kline_m5_setting)
        self.line_m5.register_event(CtaMinuteBar.CB_ON_PERIOD, self.on_m5_period_changed)
        self.klines.update({self.line_m5.name: self.line_m5})

        if self.backtesting:
            self.set_klines_output()

    def set_klines_output(self):
        """设置本地输出k线"""
        self.line_spd.export_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}.csv'.format(self.strategy_name, self.line_spd.name)))
        self.line_spd.export_fields = [
            {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
            {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
            {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
            {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
            {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
            {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
            {'name': 'upper', 'source': 'line_bar', 'attr': 'line_boll_upper', 'type_': 'list'},
            {'name': 'middle', 'source': 'line_bar', 'attr': 'line_boll_middle', 'type_': 'list'},
            {'name': 'lower', 'source': 'line_bar', 'attr': 'line_boll_lower', 'type_': 'list'},
        ]

        self.line_ratio.export_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}.csv'.format(self.strategy_name, self.line_ratio.name)))
        self.line_ratio.export_fields = [
            {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
            {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
            {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
            {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
            {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
            {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
            {'name': 'kf', 'source': 'line_bar', 'attr': 'line_state_mean', 'type_': 'list'},
        ]

        self.line_m5.export_filename = os.path.abspath(
            os.path.join(self.cta_engine.get_logs_path(),
                         u'{}_{}.csv'.format(self.strategy_name, self.line_m5.name)))
        self.line_m5.export_fields = [
            {'name': 'datetime', 'source': 'bar', 'attr': 'datetime', 'type_': 'datetime'},
            {'name': 'open', 'source': 'bar', 'attr': 'open_price', 'type_': 'float'},
            {'name': 'high', 'source': 'bar', 'attr': 'high_price', 'type_': 'float'},
            {'name': 'low', 'source': 'bar', 'attr': 'low_price', 'type_': 'float'},
            {'name': 'close', 'source': 'bar', 'attr': 'close_price', 'type_': 'float'},
            {'name': 'volume', 'source': 'bar', 'attr': 'volume', 'type_': 'float'},
            {'name': 'upper', 'source': 'line_bar', 'attr': 'line_boll_upper', 'type_': 'list'},
            {'name': 'middle', 'source': 'line_bar', 'attr': 'line_boll_middle', 'type_': 'list'},
            {'name': 'lower', 'source': 'line_bar', 'attr': 'line_boll_lower', 'type_': 'list'},
            {'name': 'rsi', 'source': 'line_bar', 'attr': 'line_rsi1', 'type_': 'list'},
        ]

    def on_init(self, force: bool = False):
        if force:
            self.write_log('策略强制初始化')
            self.inited = False
            self.trading = False
        else:
            self.write_log('策略初始化')
            if self.inited:
                self.write_log('已经初始化过，不再执行')
                return

        if not self.backtesting:
            pass

        self.init_position(status_filter=[True, False])
        # 订阅主动腿/被动腿合约
        self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=self.act_vt_symbol)
        self.cta_engine.subscribe_symbol(strategy_name=self.strategy_name, vt_symbol=self.pas_vt_symbol)
        # 更新初始化标识和交易标识
        self.inited = True
        self.trading = True
        self.put_event()
        self.write_log('策略初始化完成')


    def combine_tick(self, tick):
        """合并两腿合约，成为套利合约"""
        combinable = False

        if tick.vt_symbol == self.act_vt_symbol:
            # leg1合约
            self.cur_act_tick = tick
            if self.cur_pas_tick is not None:
                if self.cur_act_tick.datetime == self.cur_pas_tick.datetime:
                    combinable = True
        elif tick.vt_symbol == self.pas_vt_symbol:
            # leg2合约
            self.cur_pas_tick = tick
            if self.cur_act_tick is not None:
                if self.cur_pas_tick.datetime == self.cur_act_tick.datetime:
                    combinable = True

        # 不能合并
        if not combinable:
            return None, None, None

        spd_tick = TickData(
            gateway_name=self.cur_act_tick.gateway_name,
            symbol=self.vt_symbol.split('.')[0],
            exchange=Exchange.SPD,
            datetime=tick.datetime,
            date=tick.date,
            time=tick.time
        )

        # 以下情况判断为单腿涨跌停，不合成价差tick
        if self.cur_act_tick.ask_price_1 == float('1.79769E08') or self.cur_act_tick.ask_price_1 == 0 or self.cur_act_tick.bid_price_1 == self.cur_act_tick.limit_up:
            self.write_log('leg1{} 涨停{},不合成价差tick'.format(self.cur_act_tick.vt_symbol, self.cur_act_tick.bid_price_1))
            return None, None, None
        if self.cur_act_tick.bid_price_1 == float('1.79769E08') or self.cur_act_tick.bid_price_1 == 0 or self.cur_act_tick.ask_price_1 == self.cur_act_tick.limit_down:
            self.write_log('leg1{} 跌停{},不合成价差tick'.format(self.cur_act_tick.vt_symbol, self.cur_act_tick.ask_price_1))
            return None, None, None
        if self.cur_pas_tick.ask_price_1 == float('1.79769E08') or self.cur_pas_tick.ask_price_1 == 0 or self.cur_pas_tick.bid_price_1 == self.cur_pas_tick.limit_up:
            self.write_log('leg2{} 涨停{},不合成价差tick'.format(self.cur_pas_tick.vt_symbol, self.cur_pas_tick.bid_price_1))
            return None, None, None
        if self.cur_pas_tick.bid_price_1 == float('1.79769E08') or self.cur_pas_tick.bid_price_1 == 0 or self.cur_pas_tick.ask_price_1 == self.cur_pas_tick.limit_down:
            self.write_log('leg2{} 跌停{},不合成价差tick'.format(self.cur_pas_tick.vt_symbol, self.cur_pas_tick.ask_price_1))
            return None, None, None

        spd_tick.ask_price_1 = self.cur_act_tick.ask_price_1 - self.cur_pas_tick.bid_price_1
        spd_tick.ask_volume_1 = min(self.cur_act_tick.ask_volume_1, self.cur_pas_tick.bid_volume_1)

        spd_tick.bid_price_1 = self.cur_act_tick.bid_price_1 - self.cur_pas_tick.ask_price_1
        spd_tick.bid_volume_1 = min(self.cur_act_tick.bid_volume_1, self.cur_pas_tick.ask_volume_1)

        ratio_tick = copy.copy(spd_tick)
        ratio_tick.ask_price_1 = self.cur_act_tick.ask_price_1 / self.cur_pas_tick.bid_price_1
        ratio_tick.bid_price_1 = self.cur_act_tick.bid_price_1 / self.cur_pas_tick.ask_price_1
        ratio_tick.last_price = (ratio_tick.ask_price_1 - ratio_tick.bid_price_1) / 2
        # 残差tick
        ratio = ratio_tick.last_price
        if len(self.line_ratio.line_state_mean) > 0:
            ratio = self.line_ratio.line_state_mean[-1]

        mean_tick = copy.copy(spd_tick)
        mean_tick.ask_price_1 = self.cur_act_tick.ask_price_1 / ratio - self.cur_pas_tick.bid_price_1
        mean_tick.bid_price_1 = self.cur_act_tick.bid_price_1 / ratio - self.cur_pas_tick.ask_price_1
        mean_tick.last_price = (mean_tick.ask_price_1 + mean_tick.bid_price_1) / 2
        return spd_tick, ratio_tick, mean_tick


    def on_tick(self, tick: TickData):
        # 更新策略执行的时间（用于回测时记录发生的时间）
        self.cur_datetime = tick.datetime

        spread_tick = None
        ratio_tick = None
        mean_tick = None
        # 合并tick=》 价差tick，价比tick，残差tick
        if self.backtesting:
            if tick.vt_symbol != self.vt_symbol:
                spread_tick, ratio_tick, mean_tick = self.combine_tick(tick)
        else:
            if tick.vt_symbol == self.act_vt_symbol:
                # leg1合约
                self.cur_act_tick = tick
            elif tick.vt_symbol == self.pas_vt_symbol:
                # leg2合约
                self.cur_pas_tick = tick
            elif tick.vt_symbol == self.vt_symbol:
                spread_tick = tick
            else:
                self.write_error('收到未知vt_symbol tick {}'.format(tick))
        if spread_tick is None:
            return

        # 修正lastPriice,大于中轴(0)时，取最小值，小于中轴时，取最大值
        if spread_tick.bid_price_1 > self.base_up_line and spread_tick.ask_price_1 > self.base_mid_line:
            spread_tick.last_price = min(spread_tick.bid_price_1, spread_tick.ask_price_1)
        elif spread_tick.bid_price_1 < self.base_mid_line and spread_tick.ask_price_1 < self.base_mid_line:
            spread_tick.last_price = max(spread_tick.bid_price_1, spread_tick.ask_price_1)

        self.cur_spd_tick = spread_tick
        if (spread_tick.datetime.hour >= 3 and spread_tick.datetime.hour <= 8) or  \
            (spread_tick.datetime.hour >= 15 and spread_tick.datetime.hour <= 20):
            if len(self.finish_orders) > 0:
                self.finish_orders.clear()
            self.write_log('竞价休市时不处理')
            return

        # self.line_ratio.on_tick(ratio_tick)
        self.line_spd.on_tick(spread_tick)
        # self.line_md.on_tick(mean_tick)
        # self.line_m5.on_tick(ratio_tick)

        if not self.inited or not self.trading:
            return


        self.cancel_logic(self.cur_datetime, reopen=self.backtesting)
        self.spread_logic()

    def spread_logic(self):
        """套利逻辑"""
        if self.entrust != 0:
            return

        # 执行平仓逻辑
        self.spread_close_long()
        self.spread_close_short()

        self.spread_open_long()
        self.spread_open_short()

    def spread_close_long(self):
        """正套单平仓逻辑"""
        if self.entrust != 0:
            return
        # 持有正套的单
        if self.position.long_pos > 0:
            if self.force_trading_close:
                self.write_log('强制平仓日期，强制平所有正套单')

            # 从网格获取，未平仓状态，价格，注意检查是否有可以平仓的网格
            opened_grids = self.gt.get_opened_grids(direction=Direction.LONG)
            for grid in opened_grids:
                if grid.order_status:
                    continue
                close_this_grid = False
                if grid.close_price < self.cur_spd_tick.bid_price_1:
                    self.write_log(f'正套单触发止盈,{grid.__dict__}')
                    close_this_grid = True
                if grid.stop_price and grid.stop_price >= self.cur_spd_tick.ask_price_1:
                    self.write_log(f'正套单触发止损,{grid.__dict__}')
                    close_this_grid = True
                if close_this_grid or self.force_trading_close:
                    vt_orderids = self.spd_sell(grid=grid, force=self.force_trading_close)
                    if vt_orderids:
                        self.write_log(f'平正套委托单号{vt_orderids}')
                    else:
                        self.write_log(f'平正套委托单失败，{grid.__dict__}')

    def spread_close_short(self):
        """反套单平仓逻辑"""
        if self.entrust != 0:
            return
        # 持有正套的单
        if self.position.short_pos < 0:
            if self.force_trading_close:
                self.write_log('强制平仓日期，强制平所有反套单')

        opened_grids = self.gt.get_opened_grids(direction=Direction.SHORT)
        for grid in opened_grids:
            if grid.order_status:
                continue
            close_this_grid = False
            if grid.close_price > self.cur_spd_tick.ask_price_1:
                self.write_log(f'反套单触发止盈,{grid.__dict__}')
                close_this_grid = True
            if grid.stop_price and grid.stop_price <= self.cur_spd_tick.bid_price_1:
                self.write_log(f'反套单触发止损,{grid.__dict__}')
                close_this_grid = True
            if close_this_grid or self.force_trading_close:
                vt_orderids = self.spd_cover(grid=grid, force=self.force_trading_close)
                if vt_orderids:
                    self.write_log(f'平反套委托单号{vt_orderids}')
                else:
                    self.write_log(f'平反套委托单失败，{grid.__dict__}')

    def spread_open_long(self):
        if self.entrust != 0:
            return
        # 检查数据
        # if not (len(self.line_spd.line_boll_middle) > 0 \
        #         and len(self.line_ratio.line_state_mean) > 0 \
        #         and len(self.line_md.line_boll_middle) > 0):
        #     return
        if not len(self.line_spd.line_boll_middle) > 0:
            return

        # 判断开多条件 and (diff_std_cond or mean_std_cond)
        if self.cur_spd_tick.ask_price_1 < self.base_dn_line and self.cur_spd_tick.ask_price_1 < self.line_spd.line_boll_lower[-1]:

            # 获取价格接近的委托单(开多单)
            pending_grids = self.gt.get_grids(direction=Direction.LONG, end=self.cur_spd_tick.ask_price_1)

            # 获取已开仓的多单
            opened_grids = self.gt.get_opened_grids(direction=Direction.LONG)

            if len(pending_grids) > 1:
                self.write_log('有多个挂单，只选择价格最低的一个')
                sorted_grids = sorted(pending_grids, key=lambda g: g.open_price)
                pending_grids = sorted_grids[0:1]

            for grid in pending_grids:
                if self.position.long_pos > self.max_invest_pos:
                    msg = '持正套数量已满，不再开正套'
                    if msg != self.logMsg:
                        self.logMsg = msg
                        self.write_log(msg)
                    continue

                if self.position.long_pos > 0 and grid.open_price > self.gt.min_dn_open_price:
                    msg = '网格开仓价{}大于所有多头网格最低价{}，不再正套'.format(grid.open_price, self.gt.min_dn_open_price)
                    if msg != self.logMsg:
                        self.logMsg = msg
                        self.write_log(msg)
                    continue

                if self.cur_spd_tick.ask_price_1 > grid.open_price:
                    msg = 'spread_tick.ask_price_1:{} > 网格:{}, 不开正套'.format(self.cur_spd_tick.ask_price_1, grid.open_price)
                    if msg != self.logMsg:
                        self.logMsg = msg
                        self.write_log(msg)
                    continue

                # 重新修改grid.volume
                estimate_volume = self.gt.volume * (self.gt.get_volume_rate(idx=len(opened_grids)))
                if grid.volume != estimate_volume:
                    self.write_log('修改grid.volume:{0}=>{1}'.format(grid.volume, estimate_volume))
                    grid.volume = estimate_volume

                grid.close_price = grid.open_price + self.gt.grid_win
                grid.stop_price = grid.open_price - self.gt.grid_stop
                grid.snapshot = {}
                vt_orderids = self.spd_buy(grid)
                if vt_orderids:
                    self.gt.save()
                    self.write_log('开正套委托单号，{}'.format(vt_orderids))
                else:
                    self.write_log('开正套委托单号失败，{}'.format(grid.__dict__))

    def spread_open_short(self):
        if self.entrust != 0:
            return
        # 检查数据
        # if not (len(self.line_spd.line_boll_middle) > 0 \
        #         and len(self.line_ratio.line_state_mean) > 0 \
        #         and len(self.line_md.line_boll_middle) > 0):
        #     return
        if not len(self.line_spd.line_boll_middle) > 0:
            return

        # 判断开空条件 and (diff_std_cond or mean_std_cond)
        if self.cur_spd_tick.bid_price_1 > self.base_up_line and self.cur_spd_tick.bid_price_1 > self.line_spd.line_boll_upper[-1]:

            # 获取价格接近的未挂单(反套单)
            pending_grids = self.gt.get_grids(direction=Direction.SHORT, end=self.cur_spd_tick.bid_price_1)

            # 获取已开仓的多单
            opened_grids = self.gt.get_opened_grids(direction=Direction.SHORT)

            if len(pending_grids) > 1:
                self.write_log('有多个挂单，只选择价格最高的一个')
                sorted_grids = sorted(pending_grids, key=lambda g: g.open_price)
                pending_grids = sorted_grids[-1:]

            for grid in pending_grids:
                if abs(self.position.short_pos) > self.max_invest_pos:
                    msg = '持反套数量已满，不再开反套'
                    if msg != self.logMsg:
                        self.logMsg = msg
                        self.write_log(msg)
                    continue

                if abs(self.position.short_pos) > 0 and grid.open_price < self.gt.max_up_open_price:
                    msg = '网格开仓价{}小于所有空头网格最高价，不再反套'.format(grid.open_price)
                    if msg != self.logMsg:
                        self.logMsg = msg
                        self.write_log(msg)
                    continue

                if self.cur_spd_tick.bid_price_1 < grid.open_price:
                    msg = 'spread_tick.bid_price_1:{} < 网格:{},不开空仓'.format(self.cur_spd_tick.bid_price_1, grid.open_price)
                    if msg != self.logMsg:
                        self.logMsg = msg
                        self.write_log(msg)
                    continue

                # 重新修改grid.volume
                estimate_volume = self.gt.volume * (self.gt.get_volume_rate(idx=len(opened_grids)))
                if grid.volume != estimate_volume:
                    self.write_log('修改grid.volume:{0}=>{1}'.format(grid.volume, estimate_volume))
                    grid.volume = estimate_volume

                grid.close_price = grid.open_price - self.gt.grid_win
                grid.stop_price = grid.open_price + self.gt.grid_height
                grid.snapshot = {}
                vt_orderids = self.spd_short(grid)
                if vt_orderids:
                    self.gt.save()
                    self.write_log('开反套委托单号，{}'.format(vt_orderids))
                else:
                    self.write_log('开反套委托单号失败，{}'.format(grid.__dict__))

    def on_bar_spd(self, bar):
        """分钟k线数据更新
        bar,k周期数据"""
        if len(self.line_spd.line_boll_upper) > 0:
            upper = self.line_spd.line_boll_upper[-1]
        else:
            upper = 0

        if len(self.line_spd.line_boll_middle) > 0:
            middle = self.line_spd.line_boll_middle[-1]
        else:
            middle = 0

        if len(self.line_spd.line_boll_lower) > 0:
            lower = self.line_spd.line_boll_middle[-1]
        else:
            lower = 0

        if len(self.line_spd.line_boll_std) > 0:
            boll_std = self.line_spd.line_boll_std[-1]
        else:
            boll_std = 0

        upper = round_to(value=upper, target=self.spd_price_tick)
        lower = round_to(value=lower, target=self.spd_price_tick)

        self.write_log(self.line_spd.get_last_bar_str())

        # 若初始化完毕，新bar比上一个bar的收盘价价差，小于5个网格（防止跳空）
        if self.inited:
            # 检查重建
            if (upper != self.rebuild_up_line) and not self.rebuild_grid:
                self.rebuild_up_line = upper
                self.gt.rebuild_grids(directions=[Direction.SHORT],
                                      upper_line=max(self.base_mid_line, self.rebuild_up_line),
                                      middle_line=middle,
                                      upper_rate=self.rebuild_up_rate,
                                      down_rate=self.rebuild_dn_rate)
                self.display_grids()

            if (lower != self.rebuild_dn_line) and not self.rebuild_grid:
                self.rebuild_dn_line = lower
                self.gt.rebuild_grids(directions=[Direction.LONG],
                                      down_line=min(self.base_mid_line, self.rebuild_dn_line),
                                      middle_line=middle,
                                      upper_rate=self.rebuild_up_rate,
                                      down_rate=self.rebuild_dn_rate)
                self.display_grids()

    def on_bar_ratio(self, bar):
        """比率线的OnBar事件"""
        self.write_log(self.line_ratio.get_last_bar_str())
        return
        l = len(self.line_ratio.line_state_mean)
        if l > 0:
            ma = self.line_ratio.line_state_mean[-1]
        else:
            ma = 1
        self.m1_atan = 0
        if l > 6:
            listClose = [x for x in self.line_ratio.line_state_mean[-7:-1]]
            maList = ta.MA(numpy.array(listClose, dtype=float), 5)
            ma5 = maList[-1]
            ma5_ref1 = maList[-2]
            if ma5 <= 0 or ma5_ref1 <= 0:
                self.write_log('[M1-Ratio]未达成')
                return
            self.m1_atan = math.atan((ma5 / ma5_ref1 - 1) * 100 * 180 / math.pi)
            self.m1_atan = round(self.m1_atan, 4)

        if self.m1_atan <= -0.2 and not (self.rebuild_dn_grid and not self.rebuild_up_grid):
            self.rebuild_up_rate = 1
            self.rebuild_dn_rate = 1.5
            self.rebuild_dn_grid = True
            self.rebuild_up_grid = False
            self.rebuild_grid = True
        elif self.m1_atan >= 0.2 and not (self.rebuild_up_grid and not self.rebuild_dn_grid):
            self.rebuild_up_rate = 1.5
            self.rebuild_dn_rate = 1
            self.rebuild_up_grid = True
            self.rebuild_dn_grid = False
            self.rebuild_grid = True
        elif -0.2 < self.m1_atan < 0.2 and not (self.rebuild_up_grid and self.rebuild_dn_grid):
            self.rebuild_up_rate = 1
            self.rebuild_dn_rate = 1
            self.rebuild_dn_grid = True
            self.rebuild_up_grid = True
            self.rebuild_grid = True

        self.write_log(self.line_ratio.get_last_bar_str())

    def on_bar_md(self, bar):
        """残差线的OnBar事件"""
        self.write_log(self.line_md.get_last_bar_str())

    def on_bar_m5(self, bar):
        """5分钟Ratio的OnBar事件"""
        if self.inited:
            self.put_event()
        self.write_log(self.line_m5.get_last_bar_str())

    def on_m5_period_changed(self, period: CtaPeriod):
        """5分钟周期状态改变的事件处理"""
        if not self.inited:
            return

        # 震荡=》 空
        if period.pre_mode == Period.SHOCK and period.mode == Period.SHORT:
            pass

        # 震荡=》 多
        elif period.pre_mode == Period.SHOCK and period.mode == Period.LONG:
            pass

        # 空极端=》 多
        elif period.pre_mode == Period.SHORT_EXTREME and period.mode == Period.LONG:
            pass

        # 多极端=》 空
        elif period.pre_mode == Period.LONG_EXTREME and period.mode == Period.SHORT:
            pass

        self.write_log(f'{period.pre_mode.value} => {period.mode.value}')