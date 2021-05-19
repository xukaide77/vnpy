from pathlib import Path

from vnpy.trader.app import BaseApp
from vnpy.trader.constant import Direction
from vnpy.trader.object import TickData, BarData, TradeData, OrderData
from vnpy.trader.utility import BarGenerator, ArrayManager

from .engine import TradeCopyEngine, APP_NAME

class TradeCopyApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "跟单软件"
    engine_class = TradeCopyEngine
    widget_name = "TcManager"
    icon_name = "tc.ico"
