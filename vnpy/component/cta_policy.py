# encoding: UTF-8
from __future__ import unicode_literals
import os
import json
from datetime import datetime
from collections import OrderedDict
from vnpy.component.base import CtaComponent
from vnpy.trader.utility import get_folder_path

TNS_STATUS_OBSERVATE = 'observate'
TNS_STATUS_READY = 'ready'
TNS_STATUS_ORDERING = 'ordering'
TNS_STATUS_OPENED = 'opened'
TNS_STATUS_CLOSED = 'closed'

import numpy as np


class MyEncoder(json.JSONEncoder):
    """
    自定义转换器，处理np,datetime等不能被json转换得问题
    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return super(MyEncoder, self).default(obj)


class CtaPolicy(CtaComponent):
    """
    策略的持久化Policy组件
    """

    def __init__(self, strategy=None, **kwargs):
        """
        构造
        :param strategy:
        """
        super().__init__(strategy=strategy, kwargs=kwargs)

        self.create_time = None
        self.save_time = None

    def to_json(self):
        """
        将数据转换成dict
        datetime =》 string
        object =》 string
        :return:
        """
        j = OrderedDict()
        j['create_time'] = self.create_time.strftime('%Y-%m-%d %H:%M:%S') if self.create_time is not None else ''
        j['save_time'] = self.save_time.strftime('%Y-%m-%d %H:%M:%S') if self.save_time is not None else ''

        return j

    def from_json(self, json_data):
        """
        将数据从json_data中恢复
       :param json_data:
        :return:
        """
        self.write_log(u'将数据从json_data中恢复')

        self.create_time = datetime.now()
        create_time = json_data.get('create_time', '')

        if len(create_time) > 0:
            try:
                self.create_time = datetime.strptime(create_time, '%Y-%m-%d %H:%M:%S')
            except Exception as ex:
                self.write_error(u'解释create_time异常:{}'.format(str(ex)))
                self.create_time = datetime.now()

        save_time = json_data.get('save_time', '')
        if len(save_time) > 0:
            try:
                self.save_time = datetime.strptime(save_time, '%Y-%m-%d %H:%M:%S')
            except Exception as ex:
                self.write_error(u'解释save_time异常:{}'.format(str(ex)))
                self.save_time = datetime.now()

    def load(self):
        """
        从持久化文件中获取
        :return:
        """
        json_file = str(get_folder_path('data').joinpath(u'{}_Policy.json'.format(self.strategy.strategy_name)))

        json_data = {}
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf8') as f:
                    # 解析json文件
                    json_data = json.load(f)
            except Exception as ex:
                self.write_error(u'读取Policy文件{}出错,ex:{}'.format(json_file, str(ex)))
                json_data = {}

            # 从持久化文件恢复数据
            self.from_json(json_data)

    def save(self):
        """
        保存至持久化文件
        :return:
        """
        json_file = str(get_folder_path('data').joinpath(u'{}_Policy.json'.format(self.strategy.strategy_name)))

        try:
            # 修改为：回测时不保存
            if self.strategy and self.strategy.backtesting:
                return

            json_data = self.to_json()
            json_data['save_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(json_file, 'w', encoding='utf8') as f:
                data = json.dumps(json_data, indent=4, ensure_ascii=False, cls=MyEncoder)
                f.write(data)

        except IOError as ex:
            self.write_error(u'写入Policy文件{}出错,ex:{}'.format(json_file, str(ex)))
