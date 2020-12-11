# encoding: UTF-8

from pathlib import Path

from vnpy.trader.app import BaseApp

from .engine import AlgoBroker, APP_NAME


class AlgoBrokerApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "算法交易代理"
    engine_class = AlgoBroker
    widget_name = None
    icon_name = "algo.ico"

