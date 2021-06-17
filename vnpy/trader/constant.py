"""
General constant string used in VN Trader.
"""

from enum import Enum


class Color(Enum):
    """ Kline color """
    RED = 'Red'
    BLUE = 'Blue'
    EQUAL = 'Equal'


class Direction(Enum):
    """
    Direction of order/trade/position.
    """
    LONG = "多"
    SHORT = "空"
    NET = "净"


class Offset(Enum):
    """
    Offset of order/trade.
    """
    NONE = ""
    OPEN = "开"
    CLOSE = "平"
    CLOSETODAY = "平今"
    CLOSEYESTERDAY = "平昨"


class Status(Enum):
    """
    Order status.
    """
    SUBMITTING = "提交中"
    NOTTRADED = "未成交"
    PARTTRADED = "部分成交"
    ALLTRADED = "全部成交"
    CANCELLED = "已撤销"
    CANCELLING = "撤销中"
    REJECTED = "拒单"
    UNKNOWN = "未知"


class Product(Enum):
    """
    Product class.
    """
    EQUITY = "股票"
    FUTURES = "期货"
    OPTION = "期权"
    INDEX = "指数"
    FOREX = "外汇"
    SPOT = "现货"
    ETF = "ETF"
    LOF = "LOF"
    BOND = "债券"
    WARRANT = "权证"
    SPREAD = "价差"
    FUND = "基金"


class OrderType(Enum):
    """
    Order type.
    """
    LIMIT = "限价"
    MARKET = "市价"
    STOP = "STOP"
    FAK = "FAK"
    FOK = "FOK"


class OptionType(Enum):
    """
    Option type.
    """
    CALL = "看涨期权"
    PUT = "看跌期权"


class Exchange(Enum):
    """
    Exchange.
    """
    # Chinese
    CFFEX = "CFFEX"  # China Financial Futures Exchange
    SHFE = "SHFE"  # Shanghai Futures Exchange
    CZCE = "CZCE"  # Zhengzhou Commodity Exchange
    DCE = "DCE"  # Dalian Commodity Exchange
    INE = "INE"  # Shanghai International Energy Exchange
    SSE = "SSE"  # Shanghai Stock Exchange
    SZSE = "SZSE"  # Shenzhen Stock Exchange
    SGE = "SGE"  # Shanghai Gold Exchange
    WXE = "WXE"  # Wuxi Steel Exchange
    CFETS = "CFETS"  # China Foreign Exchange Trade System

    # Global
    SMART = "SMART"  # Smart Router for US stocks
    NYSE = "NYSE"  # New York Stock Exchnage
    NASDAQ = "NASDAQ"  # Nasdaq Exchange
    NYMEX = "NYMEX"  # New York Mercantile Exchange
    COMEX = "COMEX"  # a division of theNew York Mercantile Exchange
    GLOBEX = "GLOBEX"  # Globex of CME
    IDEALPRO = "IDEALPRO"  # Forex ECN of Interactive Brokers
    CME = "CME"  # Chicago Mercantile Exchange
    ICE = "ICE"  # Intercontinental Exchange
    SEHK = "SEHK"  # Stock Exchange of Hong Kong
    HKFE = "HKFE"  # Hong Kong Futures Exchange
    HKSE = "HKSE"  # Hong Kong Stock Exchange
    SGX = "SGX"  # Singapore Global Exchange
    CBOT = "CBT"  # Chicago Board of Trade
    CBOE = "CBOE"  # Chicago Board Options Exchange
    CFE = "CFE"  # CBOE Futures Exchange
    DME = "DME"  # Dubai Mercantile Exchange
    EUREX = "EUX"  # Eurex Exchange
    APEX = "APEX"  # Asia Pacific Exchange
    LME = "LME"  # London Metal Exchange
    BMD = "BMD"  # Bursa Malaysia Derivatives
    TOCOM = "TOCOM"  # Tokyo Commodity Exchange
    EUNX = "EUNX"  # Euronext Exchange
    KRX = "KRX"  # Korean Exchange
    AMEX = "AMEX"  # NESE American

    OANDA = "OANDA"  # oanda.com

    # CryptoCurrency
    BITMEX = "BITMEX"
    OKEX = "OKEX"
    HUOBI = "HUOBI"
    BITFINEX = "BITFINEX"
    BINANCE = "BINANCE"
    BYBIT = "BYBIT"  # bybit.com
    COINBASE = "COINBASE"
    DERIBIT = "DERIBIT"
    GATEIO = "GATEIO"
    BITSTAMP = "BITSTAMP"

    # Special Function
    LOCAL = "LOCAL"  # For local generated data
    SPD = "SPD"  # Customer Spread data


class Currency(Enum):
    """
    Currency.
    """
    USD = "USD"
    HKD = "HKD"
    CNY = "CNY"


class Interval(Enum):
    """
    Interval of bar data.
    """
    SECOND = "1s"
    MINUTE = "1m"
    HOUR = "1h"
    DAILY = "d"
    WEEKLY = "w"
    RENKO = 'renko'


class StockType(Enum):
    """股票类型（tdx）"""
    STOCK = 'stock_cn'  # 股票
    STOCKB = 'stockB_cn'  # 深圳B股票（特别）
    INDEX = 'index_cn'  # 指数
    BOND = 'bond_cn'  # 企业债券
    ETF = 'etf_cn'  # ETF
    CB = 'cb_cn'  # 可转债
    UNDEFINED = 'undefined'  # 未定义


class ChanSignals(Enum):
    """
    缠论信号
    来源：https://github.com/zengbin93/czsc
    """
    Other = "Other~其他"
    Y = "Y~是"
    N = "N~否"

    INB = "INB~向下笔买点区间"
    INS = "INS~向上笔卖点区间"

    FXB = "FXB~向下笔结束分型左侧高点升破"
    FXS = "FXS~向上笔结束分型左侧低点跌破"

    BU0 = "BU0~向上笔顶分完成"
    BU1 = "BU1~向上笔走势延伸"

    BD0 = "BD0~向下笔底分完成"
    BD1 = "BD1~向下笔走势延伸"

    # TK = Triple K
    TK1 = "TK1~三K底分"
    TK2 = "TK2~三K上涨"
    TK3 = "TK3~三K顶分"
    TK4 = "TK4~三K下跌"

    # ==================================================================================================================
    # 信号值编码规则：
    # 多空：L - 多头信号；S - 空头信号；
    # 编号：A0 - A类基础型；A1 - A类变种1 ... 以此类推；基础型有着特殊含义，用于因子组合，各种变种形态编号主要用于形态对比研究。
    # 组合规则：笔数_多空_编号；如 LA0 表示多头信号A0
    # ==================================================================================================================
    LA0 = "LA0~底背驰"
    LB0 = "LB0~双重底背驰"
    LG0 = "LG0~上颈线突破"
    LH0 = "LH0~向上中枢完成"
    LI0 = "LI0~三买"
    LJ0 = "LJ0~向上三角扩张中枢"
    LK0 = "LK0~向上三角收敛中枢"
    LL0 = "LL0~向上平台型中枢"
    # ------------------------------------------------------------------------------------------------------------------

    LA1 = "LA1~底背驰特例一"
    LA2 = "LA2~底背驰特例二"
    LA3 = "LA3~底背驰特例三"

    LB1 = "LB1~双重底背驰特例一"
    LB2 = "LB2~双重底背驰特例二"
    LB3 = "LB3~双重底背驰特例三"

    LG1 = "LG1~上颈线突破特例一"
    LG2 = "LG2~上颈线突破特例二"
    LG3 = "LG3~上颈线突破特例三"

    LH1 = "LH1~向上中枢完成特例一"
    LH2 = "LH2~向上中枢完成特例二"
    LH3 = "LH3~向上中枢完成特例三"

    LI1 = "LI1~三买特例一"
    LI2 = "LI2~三买特例二"
    LI3 = "LI3~三买特例三"

    LJ1 = "LJ1~向上三角扩张中枢特例一"
    LJ2 = "LJ2~向上三角扩张中枢特例二"
    LJ3 = "LJ3~向上三角扩张中枢特例三"

    LK1 = "LK1~向上三角收敛中枢特例一"
    LK2 = "LK2~向上三角收敛中枢特例二"
    LK3 = "LK3~向上三角收敛中枢特例三"

    LL1 = "LL1~向上平台型中枢特例一"
    LL2 = "LL2~向上平台型中枢特例二"
    LL3 = "LL3~向上平台型中枢特例三"

    # ------------------------------------------------------------------------------------------------------------------
    SA0 = "SA0~顶背驰"
    SB0 = "SB0~双重顶背驰"
    SG0 = "SG0~下颈线突破"
    SH0 = "SH0~向下中枢完成"
    SI0 = "SI0~三卖"
    SJ0 = "SJ0~向下三角扩张中枢"
    SK0 = "SK0~向下三角收敛中枢"
    SL0 = "SL0~向下平台型中枢"
    # ------------------------------------------------------------------------------------------------------------------
    SA1 = "SA1~顶背驰特例一"
    SA2 = "SA2~顶背驰特例二"
    SA3 = "SA3~顶背驰特例三"

    SB1 = "SB1~双重顶背驰特例一"
    SB2 = "SB2~双重顶背驰特例二"
    SB3 = "SB3~双重顶背驰特例三"

    SG1 = "SG1~下颈线突破特例一"
    SG2 = "SG2~下颈线突破特例二"
    SG3 = "SG3~下颈线突破特例三"

    SH1 = "SH1~向下中枢完成特例一"
    SH2 = "SH2~向下中枢完成特例二"
    SH3 = "SH3~向下中枢完成特例三"

    SI1 = "SI1~三卖特例一"
    SI2 = "SI2~三卖特例二"
    SI3 = "SI3~三卖特例三"

    SJ1 = "SJ1~向下三角扩张中枢特例一"
    SJ2 = "SJ2~向下三角扩张中枢特例二"
    SJ3 = "SJ3~向下三角扩张中枢特例三"

    SK1 = "SK1~向下三角收敛中枢特例一"
    SK2 = "SK2~向下三角收敛中枢特例二"
    SK3 = "SK3~向下三角收敛中枢特例三"

    SL1 = "SL1~向下平台型中枢特例一"
    SL2 = "SL2~向下平台型中枢特例二"
    SL3 = "SL3~向下平台型中枢特例三"

    # --------------------------------------------------------------------------------------------
    # 信号值编码规则：
    # 笔数：X3 - 三笔信号；
    # 多空：L - 多头信号；S - 空头信号；
    # 编号：A0 - A类基础型；A1 - A类变种1 ... 以此类推
    # 组合规则：笔数_多空_编号；如 X3LA0 表示三笔多头信号A0
    # ============================================================================================
    # 三笔形态信号
    # --------------------------------------------------------------------------------------------
    X3LA0 = "X3LA0~向下不重合"
    X3LB0 = "X3LB0~向下奔走型中枢"
    X3LC0 = "X3LC0~向下三角收敛中枢"
    X3LD0 = "X3LD0~向下三角扩张中枢"
    X3LE0 = "X3LE0~向下盘背中枢"
    X3LF0 = "X3LF0~向下无背中枢"

    X3SA0 = "X3SA0~向上不重合"
    X3SB0 = "X3SB0~向上奔走型中枢"
    X3SC0 = "X3SC0~向上三角收敛中枢"
    X3SD0 = "X3SD0~向上三角扩张中枢"
    X3SE0 = "X3SE0~向上盘背中枢"
    X3SF0 = "X3SF0~向上无背中枢"

