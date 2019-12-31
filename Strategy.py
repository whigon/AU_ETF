from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np


class Strategy:
    def __init__(self):
        self.open_ETF_p = 0  # 开仓ETF价格
        self.open_F_p = 0  # 开仓期货价格
        self.open_t = 0  # 开仓时间
        self.close_ETF_p = 0
        self.close_F_p = 0
        self.close_t = 0
        self.hold = 0  # 持仓状态
        self.side = ""  # 持仓方向
        self.signal_count = []  # 信号确认器
        self.price_log = defaultdict(list)  # 行情价格内存纪录
        self.log = []  # 交易日志
        self.open_log = []  # 开仓日志

    def open(self, time, ETF_p, F_p, side):
        if self.hold != 0:
            return
        self.hold = 1
        self.side = side
        self.open_ETF_p = ETF_p
        self.open_F_p = F_p
        self.open_t = time
        self.signal_count = []
        self.close_t = 0
        self.open_log.append([self.open_t, self.open_ETF_p, self.open_F_p, self.side])

    def settle(self, time, ETF_p, F_p):
        if self.hold == 0:
            return
        side = "SHORT" if self.side == "LONG" else "LONG"  # 平仓方向
        self.hold = 0
        self.close_ETF_p = ETF_p
        self.close_F_p = F_p
        self.close_t = time
        self.log.append([self.open_t, self.open_ETF_p, self.open_F_p, self.side,
                         self.close_t, self.close_ETF_p, self.close_F_p, side])
        self.open_t = 0  # 内存变量复位
        self.side = ""
        self.signal_count = []

    def signal_confirm(self, time, side):  # 交易信号确认，减少高频数据里的错点影响
        return True  # 这个策略暂时用不到信号确认，跳过。
        self.signal_count.append((time, side))
        if len(self.signal_count) < 2:
            return False
        time1 = self.signal_count[-2][0]  # 同方向的交易信号必须在60s内连续出现两次方为真，否则为假
        time2 = self.signal_count[-1][0]
        direct1 = self.signal_count[-2][1]
        direct2 = self.signal_count[-1][1]
        if time2 - time1 < timedelta(seconds=60) and direct1 == direct2:
            return True
        else:
            return False

    # 获取历史基差数据
    def get_history_prem(self, history_pre):
        for d in history_pre:
            self.price_log['prem'].append(d["diff"])

    # 获取历史开仓数据
    def get_history_open(self, time, ETF_p, F_p, side):
        self.hold = 1
        self.side = side
        self.open_ETF_p = ETF_p
        self.open_F_p = F_p
        self.open_t = time
        self.signal_count = []
        self.close_t = 0
        self.open_log.append([self.open_t, self.open_ETF_p, self.open_F_p, self.side])

    def on_market_update(self, time, bid_price, ask_price, BidPrice, AskPrice):
        ETF_mid = (bid_price + ask_price) / 2
        F_mid = (BidPrice + AskPrice) / 2
        prem = ((bid_price + ask_price) / 2 * 100 - (BidPrice + AskPrice) / 2)  # 用中间价计算基差
        prem_buy = (ask_price * 100 - BidPrice)  # 基差实际成交价
        prem_sell = (bid_price * 100 - AskPrice)
        self.price_log['prem'].append(prem)

        prem_buy = prem  # 如果是机会波动比较大品种，交易次数多单利小，为了减轻手续费损耗用盘口价限制信号，本策略不用比较好。
        prem_sell = prem

        window_lenth = 3900  # 滚动窗口长度，3s频率4小时标准差，60*60*4/3
        std_mutiplier = 3  # 标准差倍数

        if len(self.price_log['prem']) < window_lenth:
            return

        if str(time.time()).startswith('09:30') or str(time.time()).startswith('15:00'):
            pass
            # return                          #刚开盘这一分钟的历史数据比较乱，测试中这一分钟不交易，同理收盘最后1s

        price_list = self.price_log['prem'][-window_lenth:]
        mean = np.mean(price_list)
        std = np.std(price_list)

        # print("Strategy: ", time, prem_sell, mean + std * std_mutiplier)
        if self.hold == 0:
            if prem_sell > mean + std * std_mutiplier:
                if self.signal_confirm(time, "SHORT"):
                    self.open(time, bid_price, AskPrice, side="SHORT")
            elif prem_buy < mean - std * std_mutiplier:
                if self.signal_confirm(time, "LONG"):
                    self.open(time, ask_price, BidPrice, side="LONG")

        elif self.hold == 1:
            if self.side == "SHORT":
                if prem_buy < mean + std * 0:
                    if self.signal_confirm(time, "LONG"):
                        self.settle(time, ask_price, BidPrice)
            elif self.side == "LONG":
                if prem_sell > mean - std * 0:
                    if self.signal_confirm(time, "SHORT"):
                        self.settle(time, bid_price, AskPrice)

        if str(time.time()).startswith('15:00'):
            self.price_log['prem'] = self.price_log['prem'][-4800:]
