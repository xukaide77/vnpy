# flake8: noqa
"""
多周期显示K线切片，
华富资产
"""

import sys
import os
import ctypes
import bz2
import pickle
import zlib
import pandas as pd

from vnpy.trader.ui.kline.crosshair import Crosshair
from vnpy.trader.ui.kline.kline import *


class UiSnapshot(object):
    """查看切片"""

    def __init__(self):

        pass

    def show(self, snapshot_file: str,
             d: dict = None,
             trade_file: str = "",
             tns_file: str = "",
             dist_file: str = "",
             dist_include_list=[],
             use_grid=True):
        """
        显示切片
        :param snapshot_file: 切片文件路径（通过这个方法，可读取历史切片）
        :param d: 切片数据(用于实时查看）
        :param trade_file: 实盘成交文件
        :param dist_file: 格式化策略逻辑日志文件
        :param dist_include_list: 逻辑日志中，operation字段内需要过滤显示的内容
        :param use_grid: 使用同一窗口
        :return:
        """
        if d is None:
            if not os.path.exists(snapshot_file):
                print(f'{snapshot_file}不存在', file=sys.stderr)
                return

            with bz2.BZ2File(snapshot_file, 'rb') as f:
                d = pickle.load(f)

        use_zlib = d.get('zlib', False)
        klines = d.pop('klines', None)

        # 如果使用压缩，则解压
        if use_zlib and klines:
            print('use zlib decompress klines')
            klines = pickle.loads(zlib.decompress(klines))

        kline_settings = {}
        for k, v in klines.items():
            # 获取bar各种数据/指标列表
            data_list = v.pop('data_list', None)
            if data_list is None:
                continue

            # 主图指标 / 附图指标清单
            main_indicators = v.get('main_indicators', [])
            sub_indicators = v.get('sub_indicators', [])

            # 包括K线基础数据+主图指标+副图指标 =》datafrane
            df = pd.DataFrame(data_list)
            df = df.set_index(pd.DatetimeIndex(df['datetime']))

            # 更新setting
            setting = {
                "data_frame": df,
                "main_indicators": [x.get('name') for x in main_indicators],
                "sub_indicators": [x.get('name') for x in sub_indicators]
            }

            # 加载本地data目录的交易记录
            if len(trade_file) > 0 and os.path.exists(trade_file):
                setting.update({"trade_file": trade_file})

            # 加载本地data目录的事务
            if len(tns_file) > 0 and os.path.exists(tns_file):
                setting.update({"tns_file": tns_file})

            # 加载本地data目录的格式化逻辑事务
            if len(dist_file) > 0 and os.path.exists((dist_file)) and len(dist_include_list) > 0:
                setting.update({"dist_file": dist_file, "dist_include_list": dist_include_list})

            # 添加缠论线段 => dataframe => setting
            duan_list = v.get('duan_list', [])
            if len(duan_list) > 0:
                df_duan = pd.DataFrame(duan_list)
                setting.update({'duan_dataframe': df_duan})

            # 添加缠论分笔 => dataframe => setting
            bi_list = v.get('bi_list', [])
            if len(bi_list) > 0:
                df_bi = pd.DataFrame(bi_list)
                setting.update({'bi_dataframe': df_bi})

            # 添加缠论笔中枢 => dataframe => setting
            bi_zs_list = v.get('bi_zs_list', [])
            if len(bi_zs_list) > 0:
                df_bi_zs = pd.DataFrame(bi_zs_list)
                setting.update({'bi_zs_dataframe': df_bi_zs})

            # 添加缠论线段中枢 => dataframe => setting
            duan_zs_list = v.get('duan_zs_list', [])
            if len(duan_zs_list) > 0:
                df_duan_zs = pd.DataFrame(duan_zs_list)
                setting.update({'duan_zs_dataframe': df_duan_zs})

            kline_settings.update(
                {
                    k: setting
                }
            )
        # K线界面
        try:
            if use_grid:
                w = GridKline(kline_settings=kline_settings, title=d.get('strategy', ''), relocate=True)
                w.showMaximized()
            else:
                w = MultiKlineWindow(kline_settings=kline_settings, title=d.get('strategy', ''))
                w.showMaximized()

        except Exception as ex:
            print(u'exception:{},trace:{}'.format(str(ex), traceback.format_exc()))
