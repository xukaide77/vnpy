# encoding: UTF-8

import os
from pathlib import Path
from vnpy.trader.app import BaseApp
from .engine import IndexTickPublisher,IndexTickPublisherV2, APP_NAME


class IndexTickPublisherApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = u'期货指数全行情推送'
    engine_class = IndexTickPublisherV2
